use std::collections::HashSet;

use fallible_iterator::FallibleIterator;
use xxhash_rust::xxh3::xxh3_128;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode, Eq, Ord, Hash,
)]
pub struct Id {
    pub value: [u8; 16],
}

#[derive(Debug, Clone)]
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
    database_read_transaction: dream_database::ReadTransaction<'a>,
}

pub struct WriteTransaction<'a> {
    database_write_transaction: dream_database::WriteTransaction<'a>,
}

impl<'a> WriteTransaction<'a> {
    pub fn insert(&mut self, object: &Object, tags: &Vec<Object>) -> Result<&mut Self, String> {
        let object_id = object.get_id();
        if let Object::Raw(raw) = object {
            self.database_write_transaction
                .id_to_source
                .insert(object_id.clone(), raw.clone());
        }
        for tag in tags {
            let tag_id = tag.get_id();
            self.database_write_transaction
                .tag_and_object
                .insert((tag_id.clone(), object_id.clone()), ());
            self.database_write_transaction
                .object_and_tag
                .insert((object_id.clone(), tag_id.clone()), ());
            if let Object::Raw(raw) = object {
                self.database_write_transaction
                    .id_to_source
                    .insert(tag_id.clone(), raw.clone());
            }
            let current_tag_objects_count = self
                .database_write_transaction
                .tag_to_objects_count
                .get(&tag_id)?
                .unwrap_or(0 as u32)
                + 1;
            self.database_write_transaction
                .tag_to_objects_count
                .insert(tag_id.clone(), current_tag_objects_count);
        }
        let current_object_tags_count = self
            .database_write_transaction
            .object_to_tags_count
            .get(&object_id)?
            .unwrap_or(0 as u32)
            + 1;
        self.database_write_transaction
            .object_to_tags_count
            .insert(object_id.clone(), current_object_tags_count);
        Ok(self)
    }

    pub fn remove_object(&mut self, object: &Object) -> Result<&mut Self, String> {
        let object_id = object.get_id();
        if self
            .database_write_transaction
            .object_to_tags_count
            .get(&object_id)?
            .is_none()
        {
            return Ok(self);
        }
        if let Object::Raw(_) = object {
            self.database_write_transaction
                .id_to_source
                .remove(&object_id);
        }
        let object_and_tag_iterator = self
            .database_write_transaction
            .object_and_tag
            .iter(Some(&(object_id.clone(), Id::default())))?
            .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
            .collect::<Vec<_>>()?;
        for ((current_object_id, current_tag_id), _) in object_and_tag_iterator {
            self.database_write_transaction
                .tag_and_object
                .remove(&(current_tag_id.clone(), current_object_id.clone()));
            self.database_write_transaction
                .object_and_tag
                .remove(&(current_object_id, current_tag_id.clone()));
            let current_tag_objects_count = self
                .database_write_transaction
                .tag_to_objects_count
                .get(&current_tag_id)?
                .unwrap_or(0 as u32)
                - 1;
            self.database_write_transaction
                .tag_to_objects_count
                .insert(current_tag_id, current_tag_objects_count);
        }
        self.database_write_transaction
            .object_to_tags_count
            .remove(&object_id);

        Ok(self)
    }

    pub fn remove_tags_from_object(
        &mut self,
        object: &Object,
        tags: &Vec<Object>,
    ) -> Result<&mut Self, String> {
        let object_id = object.get_id();
        if self
            .database_write_transaction
            .object_to_tags_count
            .get(&object_id)?
            .is_none()
        {
            return Ok(self);
        }
        let mut tags_removed_from_object: u32 = 0;
        for tag in tags {
            let tag_id = tag.get_id();
            if self
                .database_write_transaction
                .tag_and_object
                .get(&(tag_id.clone(), object_id.clone()))?
                .is_none()
            {
                continue;
            }
            self.database_write_transaction
                .tag_and_object
                .remove(&(tag_id.clone(), object_id.clone()));
            self.database_write_transaction
                .object_and_tag
                .remove(&(object_id.clone(), tag_id.clone()));
            let new_tag_count = self
                .database_write_transaction
                .tag_to_objects_count
                .get(&tag_id)?
                .ok_or(format!("No objects count record for tag {tag:?}"))?
                - 1;
            if new_tag_count > 0 {
                self.database_write_transaction
                    .tag_to_objects_count
                    .insert(tag_id.clone(), new_tag_count.clone());
            } else {
                self.database_write_transaction
                    .tag_to_objects_count
                    .remove(&tag_id);
                if let Object::Raw(_) = tag {
                    self.database_write_transaction.id_to_source.remove(&tag_id);
                }
            }
            tags_removed_from_object += 1;
        }
        let object_tags_count_before_delete = self
            .database_write_transaction
            .object_to_tags_count
            .get(&object_id)?
            .ok_or("No tags count record for object {object:?}")?;
        if tags_removed_from_object == object_tags_count_before_delete {
            self.database_write_transaction
                .object_to_tags_count
                .remove(&object_id);
            if let Object::Raw(_) = object {
                self.database_write_transaction
                    .id_to_source
                    .remove(&object_id);
            }
        } else {
            self.database_write_transaction.object_to_tags_count.insert(
                object_id.clone(),
                object_tags_count_before_delete - tags_removed_from_object,
            );
        }

        Ok(self)
    }

    pub fn get_source(&self, id: &Id) -> Result<Option<Vec<u8>>, String> {
        self.database_write_transaction.id_to_source.get(id)
    }

    pub fn has_tag(&self, object: &Object, tag: &Object) -> Result<bool, String> {
        Ok(self
            .database_write_transaction
            .object_and_tag
            .get(&(object.get_id(), tag.get_id()))?
            .is_some())
    }

    pub fn get_tags(&self, object: Object) -> Result<Vec<Id>, String> {
        let object_id = object.get_id();
        self.database_write_transaction
            .object_and_tag
            .iter(Some(&(object_id.clone(), Id::default())))?
            .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
            .map(|((_, current_tag_id), _)| Ok(current_tag_id))
            .collect::<Vec<_>>()
    }
}

struct Cursor<'a> {
    iterator: Box<dyn FallibleIterator<Item = ((Id, Id), ()), Error = String> + 'a>,
    current_value: Option<(Id, Id)>,
}

impl<'a> Cursor<'a> {
    fn new(
        mut iterator: Box<dyn FallibleIterator<Item = ((Id, Id), ()), Error = String> + 'a>,
    ) -> Result<Self, String> {
        let current_value = iterator
            .next()?
            .and_then(|(current_value, _)| Some(current_value));
        Ok(Self {
            iterator,
            current_value,
        })
    }

    fn next(&mut self) -> Result<(), String> {
        self.current_value = self
            .iterator
            .next()?
            .and_then(|(current_value, _)| Some(current_value));
        Ok(())
    }
}

pub struct SearchIterator<'a> {
    database_transaction: &'a dream_database::ReadTransaction<'a>,
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
    type Error = String;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        if self.end {
            return Ok(None);
        }
        loop {
            if self.cursors.len() == self.present_tags_ids.len() {
                let first_cursor_object = self.cursors[0].current_value.clone().unwrap().1;
                if self.cursors.iter().all(|cursor| {
                    cursor
                        .current_value
                        .clone()
                        .is_some_and(|current_value| current_value.1 == first_cursor_object)
                }) {
                    let result = if fallible_iterator::convert(
                        self.absent_tags_ids
                            .iter()
                            .map(|id| Result::<Id, String>::Ok(id.clone())),
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
                        return Ok(None);
                    }
                }
                self.index_1 = 0;
                self.index_2 = 1;
            }
        }
    }
}

impl<'a> ReadTransaction<'a> {
    pub fn search(
        &self,
        present_tags: &Vec<Object>,
        absent_tags: &Vec<Object>,
        start_after_object: Option<Id>,
    ) -> Result<Box<dyn FallibleIterator<Item = Id, Error = String> + '_>, String> {
        let present_tags_ids = {
            let mut present_tags_ids_and_objects_count: Vec<(Id, u32)> = Vec::new();
            for tag in present_tags {
                let tag_id = tag.get_id();
                present_tags_ids_and_objects_count.push((
                    tag_id.clone(),
                    self.database_read_transaction
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
        };
        let absent_tags_ids = {
            let mut absent_tags_ids_and_objects_count: Vec<(Id, u32)> = Vec::new();
            for tag in absent_tags {
                let tag_id = tag.get_id();
                if let Some(tag_objects_count) = self
                    .database_read_transaction
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
        Ok(match present_tags_ids.len() {
            0 => {
                let absent_tags_ids_set = HashSet::<Id>::from_iter(absent_tags_ids);
                Box::new(
                    self.database_read_transaction
                        .tag_and_object
                        .iter(None)?
                        .map(|((tag_id, object_id), _)| Ok((tag_id, object_id)))
                        .filter(move |(tag_id, _)| Ok(!absent_tags_ids_set.contains(tag_id)))
                        .map(|(_, object_id)| Ok(object_id)),
                )
            }
            1 => {
                let absent_tags_ids_set = HashSet::<Id>::from_iter(absent_tags_ids);
                Box::new(
                    self.database_read_transaction
                        .tag_and_object
                        .iter(Some(&(present_tags_ids[0].clone(), Id::default())))?
                        .map(|((tag_id, object_id), _)| Ok((tag_id, object_id)))
                        .take_while(move |(tag_id, _)| Ok(tag_id == &present_tags_ids[0]))
                        .filter(move |(tag_id, _)| Ok(!absent_tags_ids_set.contains(tag_id)))
                        .map(|(_, object_id)| Ok(object_id)),
                )
            }
            2.. => Box::new(SearchIterator {
                database_transaction: &self.database_read_transaction,
                absent_tags_ids,
                present_tags_ids,
                start_after_object,
                cursors: Vec::new(),
                index_1: 0 as usize,
                index_2: 1 as usize,
                end: false,
            }),
        })
    }
}

impl Index {
    pub fn new(config: IndexConfig) -> Result<Self, String> {
        Ok(Self {
            database: dream_database::Database::new(config.database)?,
        })
    }

    pub fn lock_all_and_write<F>(&mut self, f: F) -> Result<&Self, String>
    where
        F: Fn(WriteTransaction) -> Result<(), String>,
    {
        self.database
            .lock_all_and_write(|database_write_transaction| {
                f(WriteTransaction {
                    database_write_transaction,
                })
            })?;
        Ok(self)
    }

    pub fn lock_all_writes_and_read<F>(&self, f: F) -> Result<&Self, String>
    where
        F: Fn(ReadTransaction) -> Result<(), String>,
    {
        self.database
            .lock_all_writes_and_read(|database_read_transaction| {
                f(ReadTransaction {
                    database_read_transaction,
                })
            })?;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;

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
            .lock_all_and_write(|mut transaction| {
                transaction
                    .insert(&o1, &vec![a.clone()])
                    .unwrap()
                    .insert(&o2, &vec![a.clone(), b.clone()])
                    .unwrap()
                    .insert(&o3, &vec![a.clone(), b.clone(), c.clone()])
                    .unwrap();
                Ok(())
            })
            .unwrap();
        index
            .lock_all_writes_and_read(|transaction| {
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
                Ok(())
            })
            .unwrap();
    }
}
