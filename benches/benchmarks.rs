use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use fallible_iterator::FallibleIterator;
use nanorand::{BufferedRng, Rng, WyRand};
use std::{fs, io::BufReader, path::Path};

extern crate dream;
use dream::*;

fn criterion_benchmark(c: &mut Criterion) {
    const TOTAL_TAGS_COUNT: usize = 100;
    const OBJECT_TAGS_COUNT: usize = 10;
    const OBJECTS_COUNT: usize = 100000;

    let mut rng = WyRand::new_seed(0);
    let mut index = Index::new(
        serde_saphyr::from_reader(BufReader::new(
            fs::File::open("benches/index_config.yml").unwrap(),
        ))
        .unwrap(),
    );

    let mut tags = (0..TOTAL_TAGS_COUNT)
        .map(|_| {
            let mut tag = vec![0u8; 16];
            rng.fill(&mut tag);
            Object::Raw(tag)
        })
        .collect::<Vec<_>>();
    index
        .lock_all_and_write(|transaction| {
            for _ in 0..OBJECTS_COUNT {
                let mut object_value = vec![0u8; 16];
                rng.fill(&mut object_value);
                let mut tags = (0..OBJECT_TAGS_COUNT)
                    .map(|_| tags[rng.generate_range(0..tags.len())].clone())
                    .collect::<Vec<_>>();
                tags.sort();
                tags.dedup();
                transaction.insert(&Object::Raw(object_value), &tags)?;
            }
            Ok(())
        })
        .unwrap()
        .database
        .lock_all_and_checkpoint()
        .unwrap();

    for search_tags_count in 1..=4 {
        c.bench_function(
            format!("search by {search_tags_count} tags").as_str(),
            |b| {
                b.iter_batched(
                    || {
                        rng.shuffle(&mut tags);
                        tags.iter()
                            .take(search_tags_count)
                            .cloned()
                            .collect::<Vec<_>>()
                    },
                    |present_tags| {
                        index.lock_all_writes_and_read(|transaction| {
                            transaction
                                .search(&present_tags, &vec![], None)?
                                .collect::<Vec<_>>()?;
                            Ok(())
                        })
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
