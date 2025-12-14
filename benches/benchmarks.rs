use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use fallible_iterator::FallibleIterator;
use nanorand::{Rng, WyRand};
use std::path::Path;

extern crate dream;
use dream::*;

fn new_default_index(test_name_for_isolation: &str) -> Index {
    let database_dir =
        Path::new(format!("/tmp/dream/benchmark/{test_name_for_isolation}").as_str()).to_path_buf();

    Index::new(IndexConfig {
        database: dream_database::DatabaseConfig {
            tables: dream_database::TablesConfig {
                tag_and_object: lawn::table::TableConfig {
                    index: lawn::index::IndexConfig {
                        path: database_dir
                            .join("tables")
                            .join("tag_and_object")
                            .join("index.idx")
                            .to_path_buf(),
                    },
                    data_pool: Box::new(lawn::fixed_data_pool::FixedDataPoolConfig {
                        path: database_dir
                            .join("tables")
                            .join("tag_and_object")
                            .join("data.dat")
                            .to_path_buf(),
                        container_size: 32,
                    }),
                },
                object_and_tag: lawn::table::TableConfig {
                    index: lawn::index::IndexConfig {
                        path: database_dir
                            .join("tables")
                            .join("object_and_tag")
                            .join("index.idx")
                            .to_path_buf(),
                    },
                    data_pool: Box::new(lawn::fixed_data_pool::FixedDataPoolConfig {
                        path: database_dir
                            .join("tables")
                            .join("object_and_tag")
                            .join("data.dat")
                            .to_path_buf(),
                        container_size: 32,
                    }),
                },
                id_to_source: lawn::table::TableConfig {
                    index: lawn::index::IndexConfig {
                        path: database_dir
                            .join("tables")
                            .join("id_to_source")
                            .join("index.idx")
                            .to_path_buf(),
                    },
                    data_pool: Box::new(lawn::variable_data_pool::VariableDataPoolConfig {
                        directory: database_dir
                            .join("tables")
                            .join("id_to_source")
                            .join("data")
                            .to_path_buf(),
                        max_element_size: 65536 as usize,
                    }),
                },
                tag_to_objects_count: lawn::table::TableConfig {
                    index: lawn::index::IndexConfig {
                        path: database_dir
                            .join("tables")
                            .join("tag_to_objects_count")
                            .join("index.idx")
                            .to_path_buf(),
                    },
                    data_pool: Box::new(lawn::fixed_data_pool::FixedDataPoolConfig {
                        path: database_dir
                            .join("tables")
                            .join("tag_to_objects_count")
                            .join("data.dat")
                            .to_path_buf(),
                        container_size: 20,
                    }),
                },
                object_to_tags_count: lawn::table::TableConfig {
                    index: lawn::index::IndexConfig {
                        path: database_dir
                            .join("tables")
                            .join("object_to_tags_count")
                            .join("index.idx")
                            .to_path_buf(),
                    },
                    data_pool: Box::new(lawn::fixed_data_pool::FixedDataPoolConfig {
                        path: database_dir
                            .join("tables")
                            .join("object_to_tags_count")
                            .join("data.dat")
                            .to_path_buf(),
                        container_size: 20,
                    }),
                },
            },
            log: dream_database::LogConfig {
                path: database_dir.join("log.dat").to_path_buf(),
            },
        },
    })
    .unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    const TOTAL_TAGS_COUNT: usize = 100;
    const OBJECT_TAGS_COUNT: usize = 10;
    const OBJECTS_COUNT: usize = 100000;

    let mut rng = WyRand::new_seed(0);
    let mut index = new_default_index("benchmark");

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
