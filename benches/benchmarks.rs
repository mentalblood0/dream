use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use fallible_iterator::FallibleIterator;
use nanorand::{Rng, WyRand};
use std::{fs, io::BufReader};

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

extern crate dream;
use dream::*;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
struct Config {
    index: IndexConfig,
    rng_seed: u64,
    total_tags_count: usize,
    object_tags_count: usize,
    objects_count: usize,
    benchmark_in_memory: bool,
    benchmark_on_disk: bool,
}

#[cfg(feature = "serde")]
fn criterion_benchmark(bencher_context: &mut Criterion) {
    let config: Config = serde_saphyr::from_reader(BufReader::new(
        fs::File::open("benches/config.yml").unwrap(),
    ))
    .unwrap();

    let mut index = Index::new(config.index).unwrap();
    let mut rng = WyRand::new_seed(config.rng_seed);
    let mut tags = (0..config.total_tags_count)
        .map(|_| {
            let mut tag = vec![0u8; 16];
            rng.fill(&mut tag);
            Object::Raw(tag)
        })
        .collect::<Vec<_>>();
    index
        .lock_all_and_write(|transaction| {
            for _ in 0..config.objects_count {
                let mut object_value = vec![0u8; 16];
                rng.fill(&mut object_value);
                let mut tags = (0..config.object_tags_count)
                    .map(|_| tags[rng.generate_range(0..tags.len())].clone())
                    .collect::<Vec<_>>();
                tags.sort();
                tags.dedup();
                transaction.insert(&Object::Raw(object_value), &tags)?;
            }
            Ok(())
        })
        .unwrap();

    if config.benchmark_in_memory {
        for search_tags_count in 1..=4 {
            bencher_context.bench_function(
                format!("in-memory: searching all objects by {search_tags_count} tags").as_str(),
                |bencher| {
                    bencher.iter_batched(
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
    if config.benchmark_on_disk {
        index.database.lock_all_and_checkpoint().unwrap();
        for search_tags_count in 1..=4 {
            bencher_context.bench_function(
                format!("on-disk: searching all objects by {search_tags_count} tags").as_str(),
                |bencher| {
                    bencher.iter_batched(
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
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
