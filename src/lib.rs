use anyhow::{Error, Result, anyhow};
use fallible_iterator::FallibleIterator;
use xxhash_rust::xxh3::xxh3_128;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use std::{collections::HashSet, ops::Deref};

#[derive(
    Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode, Eq, Ord, Hash,
)]
pub struct Id {
    pub value: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Object {
    Raw(Vec<u8>),
    Identified(Id),
}

impl Object {
    fn get_id(&self) -> Id {
        match self {
            Object::Raw(raw) => Id {
                value: xxh3_128(raw).to_le_bytes(),
            },
            Object::Identified(id) => id.clone(),
        }
    }
}

lawn::database::define_database!(dream_database {
    tag_and_object<(Id, Id), ()>,
    object_and_tag<(Id, Id), ()>,
    id_to_source<Id, Vec<u8>>,
    tag_to_objects_count<Id, u32>,
    object_to_tags_count<Id, u32>
} use {
    use super::Id;
});

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct IndexConfig {
    pub database: dream_database::DatabaseConfig,
}

pub struct Index {
    database: dream_database::Database,
}

pub struct ReadTransaction<'a> {
    database_transaction: dream_database::ReadTransaction<'a>,
}

pub struct WriteTransaction<'a, 'b> {
    database_transaction: &'a mut dream_database::WriteTransaction<'b>,
}

macro_rules! define_read_methods {
    () => {
        pub fn get_source(&self, id: &Id) -> Result<Option<Object>> {
            Ok(self
                .database_transaction
                .id_to_source
                .get(id)?
                .and_then(|value| Some(Object::Raw(value))))
        }

        pub fn has_tag(&self, object: &Object, tag: &Object) -> Result<bool> {
            Ok(self
                .database_transaction
                .object_and_tag
                .get(&(object.get_id(), tag.get_id()))?
                .is_some())
        }

        pub fn get_tags(&self, object: &Object) -> Result<Vec<Id>> {
            let object_id = object.get_id();
            self.database_transaction
                .object_and_tag
                .iter(Some(&(object_id.clone(), Id::default())))?
                .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
                .map(|((_, current_tag_id), _)| Ok(current_tag_id))
                .collect::<Vec<_>>()
        }

        pub fn search(
            &self,
            present_tags: &Vec<Object>,
            absent_tags: &Vec<Object>,
            start_after_object: Option<Id>,
        ) -> Result<Box<dyn FallibleIterator<Item = Id, Error = Error> + '_>> {
            let absent_tags_ids = {
                let mut absent_tags_ids_and_objects_count: Vec<(Id, u32)> = Vec::new();
                for tag in absent_tags {
                    let tag_id = tag.get_id();
                    if let Some(tag_objects_count) = self
                        .database_transaction
                        .tag_to_objects_count
                        .get(&tag_id)?
                    {
                        absent_tags_ids_and_objects_count.push((tag_id, tag_objects_count));
                    }
                }
                absent_tags_ids_and_objects_count
                    .sort_by_key(|(_, tag_objects_count)| *tag_objects_count);
                absent_tags_ids_and_objects_count.reverse();
                absent_tags_ids_and_objects_count
                    .into_iter()
                    .map(|(tag, _)| tag)
                    .collect::<Vec<_>>()
            };
            Ok(match present_tags.len() {
                0 => Box::new(
                    self.database_transaction
                        .object_to_tags_count
                        .iter(Some(&start_after_object.clone().unwrap_or_default()))?
                        .skip(if start_after_object.is_some() { 1 } else { 0 })
                        .map(|(object_id, _)| Ok(object_id))
                        .filter(move |object_id| {
                            fallible_iterator::convert(
                                absent_tags_ids
                                    .iter()
                                    .map(|id| Result::<Id>::Ok(id.clone())),
                            )
                            .all(|absent_tag_id| {
                                Ok(self
                                    .database_transaction
                                    .tag_and_object
                                    .get(&(absent_tag_id.clone(), object_id.clone()))?
                                    .is_none())
                            })
                        }),
                ),
                1 => {
                    let search_tag_id = present_tags[0].get_id();
                    Box::new(
                        self.database_transaction
                            .tag_and_object
                            .iter(Some(&(
                                search_tag_id.clone(),
                                start_after_object.clone().unwrap_or_default(),
                            )))?
                            .skip(if start_after_object.is_some() { 1 } else { 0 })
                            .map(|((tag_id, object_id), _)| Ok((tag_id, object_id)))
                            .take_while(move |(tag_id, _)| Ok(tag_id == &search_tag_id))
                            .map(|(_, object_id)| Ok(object_id))
                            .filter(move |object_id| {
                                fallible_iterator::convert(
                                    absent_tags_ids
                                        .iter()
                                        .map(|id| Result::<Id>::Ok(id.clone())),
                                )
                                .all(|absent_tag_id| {
                                    Ok(self
                                        .database_transaction
                                        .tag_and_object
                                        .get(&(absent_tag_id.clone(), object_id.clone()))?
                                        .is_none())
                                })
                            }),
                    )
                }
                2.. => Box::new(SearchIterator {
                    database_transaction: self.database_transaction.deref(),
                    absent_tags_ids,
                    present_tags_ids: {
                        let mut present_tags_ids_and_objects_count: Vec<(Id, u32)> = Vec::new();
                        for tag in present_tags {
                            let tag_id = tag.get_id();
                            present_tags_ids_and_objects_count.push((
                                tag_id.clone(),
                                self.database_transaction
                                    .tag_to_objects_count
                                    .get(&tag_id)?
                                    .unwrap_or(0 as u32),
                            ));
                        }
                        present_tags_ids_and_objects_count
                            .sort_by_key(|(_, tag_objects_count)| *tag_objects_count);
                        present_tags_ids_and_objects_count
                            .into_iter()
                            .map(|(tag, _)| tag)
                            .collect::<Vec<_>>()
                    },
                    start_after_object,
                    cursors: Vec::new(),
                    index_1: 0 as usize,
                    index_2: 1 as usize,
                    end: false,
                }),
            })
        }
    };
}

impl<'a> ReadTransaction<'a> {
    define_read_methods!();
}

impl<'a, 'b> WriteTransaction<'a, 'b> {
    define_read_methods!();

    pub fn insert(&mut self, object: &Object, tags: &Vec<Object>) -> Result<&mut Self> {
        let object_id = object.get_id();
        if let Object::Raw(raw) = object {
            self.database_transaction
                .id_to_source
                .insert(object_id.clone(), raw.clone());
        }
        let existent_tags = HashSet::<Id>::from_iter(self.get_tags(object)?.into_iter());
        let mut tags_added = 0 as u32;
        for tag in tags {
            let tag_id = tag.get_id();
            if existent_tags.contains(&tag_id) {
                continue;
            }
            self.database_transaction
                .tag_and_object
                .insert((tag_id.clone(), object_id.clone()), ());
            self.database_transaction
                .object_and_tag
                .insert((object_id.clone(), tag_id.clone()), ());
            if let Object::Raw(raw) = tag {
                self.database_transaction
                    .id_to_source
                    .insert(tag_id.clone(), raw.clone());
            }
            let new_tag_objects_count = self
                .database_transaction
                .tag_to_objects_count
                .get(&tag_id)?
                .unwrap_or(0 as u32)
                + 1;
            self.database_transaction
                .tag_to_objects_count
                .insert(tag_id.clone(), new_tag_objects_count);
            tags_added += 1;
        }
        self.database_transaction
            .object_to_tags_count
            .insert(object_id.clone(), existent_tags.len() as u32 + tags_added);
        Ok(self)
    }

    pub fn remove_object(&mut self, object: &Object) -> Result<&mut Self> {
        let object_id = object.get_id();
        if self
            .database_transaction
            .object_to_tags_count
            .get(&object_id)?
            .is_none()
        {
            return Ok(self);
        }
        if let Object::Raw(_) = object {
            self.database_transaction.id_to_source.remove(&object_id);
        }
        let object_and_tag_iterator = self
            .database_transaction
            .object_and_tag
            .iter(Some(&(object_id.clone(), Id::default())))?
            .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
            .collect::<Vec<_>>()?;
        for ((current_object_id, current_tag_id), _) in object_and_tag_iterator {
            self.database_transaction
                .tag_and_object
                .remove(&(current_tag_id.clone(), current_object_id.clone()));
            self.database_transaction
                .object_and_tag
                .remove(&(current_object_id, current_tag_id.clone()));
            let new_tag_objects_count = self
                .database_transaction
                .tag_to_objects_count
                .get(&current_tag_id)?
                .unwrap_or(0 as u32)
                - 1;
            self.database_transaction
                .tag_to_objects_count
                .insert(current_tag_id, new_tag_objects_count);
        }
        self.database_transaction
            .object_to_tags_count
            .remove(&object_id);

        Ok(self)
    }

    pub fn remove_tags_from_object(
        &mut self,
        object: &Object,
        tags: &Vec<Object>,
    ) -> Result<&mut Self> {
        let object_id = object.get_id();
        if self
            .database_transaction
            .object_to_tags_count
            .get(&object_id)?
            .is_none()
        {
            return Ok(self);
        }
        let tags_before_remove = HashSet::<Id>::from_iter(self.get_tags(object)?.into_iter());
        let mut tags_removed_from_object: u32 = 0;
        for tag in tags {
            let tag_id = tag.get_id();
            if !tags_before_remove.contains(&tag_id) {
                continue;
            }
            self.database_transaction
                .tag_and_object
                .remove(&(tag_id.clone(), object_id.clone()));
            self.database_transaction
                .object_and_tag
                .remove(&(object_id.clone(), tag_id.clone()));
            let new_tag_objects_count = self
                .database_transaction
                .tag_to_objects_count
                .get(&tag_id)?
                .ok_or(anyhow!("No objects count record for tag {tag:?}"))?
                - 1;
            if new_tag_objects_count > 0 {
                self.database_transaction
                    .tag_to_objects_count
                    .insert(tag_id.clone(), new_tag_objects_count.clone());
            } else {
                self.database_transaction
                    .tag_to_objects_count
                    .remove(&tag_id);
                if let Object::Raw(_) = tag {
                    self.database_transaction.id_to_source.remove(&tag_id);
                }
            }
            tags_removed_from_object += 1;
        }
        if tags_removed_from_object == tags_before_remove.len() as u32 {
            self.database_transaction
                .object_to_tags_count
                .remove(&object_id);
            if let Object::Raw(_) = object {
                self.database_transaction.id_to_source.remove(&object_id);
            }
        } else {
            self.database_transaction.object_to_tags_count.insert(
                object_id.clone(),
                tags_before_remove.len() as u32 - tags_removed_from_object,
            );
        }

        Ok(self)
    }
}

struct Cursor<'a> {
    iterator: Box<dyn FallibleIterator<Item = ((Id, Id), ()), Error = Error> + 'a>,
    current_value: Option<(Id, Id)>,
}

impl<'a> Cursor<'a> {
    fn new(
        mut iterator: Box<dyn FallibleIterator<Item = ((Id, Id), ()), Error = Error> + 'a>,
    ) -> Result<Self> {
        let current_value = iterator
            .next()?
            .and_then(|(current_value, _)| Some(current_value));
        Ok(Self {
            iterator,
            current_value,
        })
    }

    fn next(&mut self) -> Result<()> {
        self.current_value = self
            .iterator
            .next()?
            .and_then(|(current_value, _)| Some(current_value));
        Ok(())
    }
}

pub struct SearchIterator<'a> {
    database_transaction: &'a dream_database::TablesTransactions,
    present_tags_ids: Vec<Id>,
    absent_tags_ids: Vec<Id>,
    start_after_object: Option<Id>,
    cursors: Vec<Cursor<'a>>,
    index_1: usize,
    index_2: usize,
    end: bool,
}

impl<'a> FallibleIterator for SearchIterator<'a> {
    type Item = Id;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if self.end {
            return Ok(None);
        }
        loop {
            if self.cursors.len() == self.present_tags_ids.len() {
                let first_cursor_object = self.cursors[0].current_value.clone().unwrap().1;
                // dbg!(&first_cursor_object);
                // dbg!(
                //     self.cursors
                //         .iter()
                //         .map(|cursor| cursor.current_value.clone().unwrap().1)
                //         .collect::<Vec<_>>()
                // );
                // dbg!(
                //     self.cursors
                //         .iter()
                //         .map(|cursor| self
                //             .database_transaction
                //             .id_to_source
                //             .get(&cursor.current_value.clone().unwrap().1)
                //             .unwrap())
                //         .collect::<Vec<_>>()
                // );
                if self.cursors.iter().all(|cursor| {
                    cursor
                        .current_value
                        .clone()
                        .is_some_and(|current_value| current_value.1 == first_cursor_object)
                }) {
                    // println!("all equal");
                    let result = if fallible_iterator::convert(
                        self.absent_tags_ids
                            .iter()
                            .map(|id| Result::<Id>::Ok(id.clone())),
                    )
                    .all(|tag_id| {
                        Ok(self
                            .database_transaction
                            .tag_and_object
                            .get(&(tag_id.clone(), first_cursor_object.clone()))?
                            .is_none())
                    })? {
                        Some(first_cursor_object)
                    } else {
                        None
                    };
                    self.cursors[0].next()?;
                    if !self.cursors[0]
                        .current_value
                        .as_ref()
                        .is_some_and(|first_cursor_value| {
                            first_cursor_value.0 == self.present_tags_ids[0]
                        })
                    {
                        // println!("1");
                        self.end = true;
                    }
                    return Ok(result);
                }
            }

            if self.cursors.len() < self.present_tags_ids.len()
                && self.cursors.len() <= self.index_1
            {
                let mut cursor =
                    Cursor::new(self.database_transaction.tag_and_object.iter(Some(&(
                        self.present_tags_ids[self.index_1].clone(),
                        if self.index_1 == 0 {
                            self.start_after_object.clone().unwrap_or_default()
                        } else {
                            self.cursors
                                .last()
                                .unwrap()
                                .current_value
                                .clone()
                                .unwrap()
                                .1
                        },
                    )))?)?;
                if self.index_1 == 0 && self.start_after_object.is_some() {
                    cursor.next()?;
                }
                if !cursor
                    .current_value
                    .as_ref()
                    .is_some_and(|first_cursor_value| {
                        first_cursor_value.0 == self.present_tags_ids[self.index_1]
                    })
                {
                    self.end = true;
                    // println!("2");
                    return Ok(None);
                }
                self.cursors.push(cursor);
            }

            if self.cursors.len() < self.present_tags_ids.len()
                && self.cursors.len() <= self.index_2
            {
                let cursor = Cursor::new(
                    self.database_transaction.tag_and_object.iter(Some(&(
                        self.present_tags_ids[self.index_2].clone(),
                        self.cursors
                            .last()
                            .unwrap()
                            .current_value
                            .clone()
                            .unwrap()
                            .1,
                    )))?,
                )?;
                if !cursor
                    .current_value
                    .as_ref()
                    .is_some_and(|first_cursor_value| {
                        first_cursor_value.0 == self.present_tags_ids[self.index_2]
                    })
                {
                    self.end = true;
                    // println!("3");
                    return Ok(None);
                }
                self.cursors.push(cursor);
            }

            while self.cursors[self.index_2].current_value.as_ref().unwrap().1
                < self.cursors[self.index_1].current_value.as_ref().unwrap().1
            {
                self.cursors[self.index_2].next()?;
                if !self.cursors[self.index_2]
                    .current_value
                    .as_ref()
                    .is_some_and(|current_value| {
                        current_value.0 == self.present_tags_ids[self.index_2]
                    })
                {
                    self.end = true;
                    // println!("4");
                    return Ok(None);
                }
            }
            if self.cursors[self.index_2].current_value.as_ref().unwrap().1
                == self.cursors[self.index_1].current_value.as_ref().unwrap().1
            {
                self.index_1 = (self.index_1 + 1) % self.present_tags_ids.len();
                self.index_2 = (self.index_2 + 1) % self.present_tags_ids.len();
            } else {
                while self.cursors[0].current_value.as_ref().unwrap().1
                    < self.cursors[self.index_2].current_value.as_ref().unwrap().1
                {
                    self.cursors[0].next()?;
                    if !self.cursors[0]
                        .current_value
                        .as_ref()
                        .is_some_and(|current_value| current_value.0 == self.present_tags_ids[0])
                    {
                        self.end = true;
                        // println!("5");
                        return Ok(None);
                    }
                }
                self.index_1 = 0;
                self.index_2 = 1;
            }
        }
    }
}

impl Index {
    pub fn new(config: IndexConfig) -> Result<Self> {
        Ok(Self {
            database: dream_database::Database::new(config.database)?,
        })
    }

    pub fn lock_all_and_write<'a, F>(&'a mut self, mut f: F) -> Result<&'a mut Self>
    where
        F: FnMut(&mut WriteTransaction<'_, '_>) -> Result<()>,
    {
        self.database
            .lock_all_and_write(|database_write_transaction| {
                f(&mut WriteTransaction {
                    database_transaction: database_write_transaction,
                })
            })?;

        Ok(self)
    }

    pub fn lock_all_writes_and_read<F>(&self, mut f: F) -> Result<&Self>
    where
        F: FnMut(ReadTransaction) -> Result<()>,
    {
        self.database
            .lock_all_writes_and_read(|database_read_transaction| {
                f(ReadTransaction {
                    database_transaction: database_read_transaction,
                })
            })?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::{
        collections::{BTreeMap, BTreeSet},
        path::Path,
    };

    use nanorand::{Rng, WyRand};
    use pretty_assertions::assert_eq;

    fn new_default_index(test_name_for_isolation: &str) -> Index {
        let database_dir =
            Path::new(format!("/tmp/dream/test/{test_name_for_isolation}").as_str()).to_path_buf();

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

    #[test]
    fn test_simple() {
        let mut index = new_default_index("test_simple");

        let a = Object::Raw("a".as_bytes().to_vec());
        let b = Object::Raw("b".as_bytes().to_vec());
        let c = Object::Raw("c".as_bytes().to_vec());
        let o1 = Object::Raw("o1".as_bytes().to_vec());
        let o2 = Object::Raw("o2".as_bytes().to_vec());
        let o3 = Object::Raw("o3".as_bytes().to_vec());

        index
            .lock_all_and_write(|transaction| {
                transaction
                    .insert(&o1, &vec![a.clone()])
                    .unwrap()
                    .insert(&o2, &vec![a.clone(), b.clone()])
                    .unwrap()
                    .insert(&o3, &vec![a.clone(), b.clone(), c.clone()])
                    .unwrap();
                assert_eq!(
                    transaction
                        .search(&vec![a.clone(), b.clone(), c.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    [o3.get_id()]
                );
                assert_eq!(
                    transaction
                        .search(&vec![a.clone(), b.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    [o3.get_id(), o2.get_id()]
                );
                assert_eq!(
                    transaction
                        .search(&vec![a.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    [o3.get_id(), o2.get_id(), o1.get_id()]
                );

                assert_eq!(
                    transaction
                        .search(&vec![a.clone()], &vec![a.clone()], None)?
                        .collect::<Vec<_>>()?,
                    []
                );
                assert_eq!(
                    transaction
                        .search(&vec![a.clone()], &vec![], Some(o1.get_id()))?
                        .collect::<Vec<_>>()?,
                    []
                );
                assert_eq!(
                    transaction
                        .search(&vec![], &vec![], Some(o1.get_id()))?
                        .collect::<Vec<_>>()?,
                    []
                );
                assert_eq!(
                    transaction
                        .search(&vec![], &vec![a.clone(), b.clone(), c.clone()], None)?
                        .collect::<Vec<_>>()?,
                    []
                );

                assert_eq!(
                    transaction
                        .search(&vec![a.clone()], &vec![b.clone()], None)?
                        .collect::<Vec<_>>()?,
                    [o1.get_id()]
                );
                assert_eq!(
                    transaction
                        .search(&vec![a.clone()], &vec![c.clone()], None)?
                        .collect::<Vec<_>>()?,
                    [o2.get_id(), o1.get_id()]
                );

                transaction.remove_tags_from_object(&o3, &vec![a.clone(), c.clone()])?;
                assert_eq!(
                    transaction
                        .search(&vec![a.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    [o2.get_id(), o1.get_id()]
                );
                assert_eq!(
                    transaction
                        .search(&vec![b.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    [o3.get_id(), o2.get_id()]
                );
                assert_eq!(
                    transaction
                        .search(&vec![c.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    []
                );

                transaction.remove_object(&o2)?;
                assert_eq!(
                    transaction
                        .search(&vec![a.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    [o1.get_id()]
                );
                assert_eq!(
                    transaction
                        .search(&vec![b.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    [o3.get_id()]
                );
                assert_eq!(
                    transaction
                        .search(&vec![c.clone()], &vec![], None)?
                        .collect::<Vec<_>>()?,
                    []
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_generative() {
        const TOTAL_TAGS_COUNT: usize = 8;
        const OBJECT_TAGS_COUNT: usize = 3;
        const OBJECTS_COUNT: usize = 3;
        const SEARCHES_COUNT: usize = 100;

        let mut index = new_default_index("test_generative");
        let mut rng = WyRand::new_seed(0);

        let mut tags = (0..TOTAL_TAGS_COUNT)
            .map(|_| {
                let mut tag = vec![0u8; 16];
                rng.fill(&mut tag);
                Object::Raw(tag)
            })
            .collect::<Vec<_>>();
        let object_to_tags = (0..OBJECTS_COUNT)
            .map(|_| {
                let mut object_value = vec![0u8; 16];
                rng.fill(&mut object_value);
                let mut tags = (0..OBJECT_TAGS_COUNT)
                    .map(|_| tags[rng.generate_range(0..tags.len())].clone())
                    .collect::<Vec<_>>();
                tags.sort();
                tags.dedup();
                (Object::Raw(object_value), tags)
            })
            .collect::<BTreeMap<_, _>>();

        index
            .lock_all_and_write(|transaction| {
                for (object, tags) in object_to_tags.iter() {
                    transaction.insert(&object, &tags)?;
                }
                for (object, tags) in object_to_tags.iter() {
                    for tag in tags.iter() {
                        assert_eq!(transaction.has_tag(object, tag)?, true);
                    }
                    let result_tags = BTreeSet::from_iter(
                        transaction
                            .get_tags(object)?
                            .iter()
                            .map(|tag_id| transaction.get_source(tag_id).unwrap().unwrap()),
                    );
                    let correct_tags = BTreeSet::from_iter(tags.iter().cloned());
                    assert_eq!(result_tags, correct_tags);
                }
                Ok(())
            })
            .unwrap();

        let tag_to_objects = {
            let mut result: BTreeMap<Object, Vec<Object>> = BTreeMap::new();
            object_to_tags.iter().for_each(|(object, tags)| {
                tags.iter().for_each(|tag| {
                    (*result.entry(tag.clone()).or_insert(vec![])).push(object.clone());
                })
            });
            result
        };
        index
            .lock_all_writes_and_read(|transaction| {
                for (tag, objects) in tag_to_objects.iter() {
                    assert_eq!(
                        &transaction
                            .search(&vec![tag.clone()], &vec![], None)?
                            .map(|object_id| transaction
                                .get_source(&object_id)?
                                .ok_or(anyhow!("No source for object id {object_id:?} found")))
                            .collect::<Vec<_>>()?,
                        objects
                    );
                }

                for _ in 0..SEARCHES_COUNT {
                    rng.shuffle(&mut tags);
                    let present_tags = tags.iter().take(2).cloned().collect::<Vec<_>>();
                    dbg!(&tag_to_objects, &present_tags);
                    let result = BTreeSet::from_iter(
                        transaction
                            .search(&present_tags, &vec![], None)?
                            .collect::<Vec<_>>()?
                            .iter()
                            .map(|object_id| transaction.get_source(object_id).unwrap().unwrap()),
                    );
                    let correct = present_tags
                        .iter()
                        .map(|tag| {
                            BTreeSet::from_iter(tag_to_objects.get(tag).unwrap_or(&vec![]).clone())
                        })
                        .reduce(|accumulator, current| {
                            accumulator
                                .intersection(&current)
                                .cloned()
                                .collect::<BTreeSet<_>>()
                        })
                        .unwrap_or_default();
                    assert_eq!(result, correct);
                }
                Ok(())
            })
            .unwrap();
    }
}
