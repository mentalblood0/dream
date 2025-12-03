use fallible_iterator::FallibleIterator;
use xxhash_rust::xxh3::xxh3_128;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode)]
pub struct Id {
    pub value: [u8; 16],
}

#[derive(Debug)]
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

const TAG_AND_OBJECT: usize = 0;
const OBJECT_AND_TAG: usize = 0;
const IDS_TO_SOURCES: usize = 0;
const TAG_TO_OBJECTS_COUNT: usize = 0;
const OBJECT_TO_TAGS_COUNT: usize = 0;

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct IndexConfig {
    pub database: lawn::database::DatabaseConfig,
}

pub struct Index {
    database: lawn::database::Database,
}

pub struct ReadTransaction<'a> {
    database_read_transaction: lawn::database::ReadTransaction<'a>,
}

pub struct WriteTransaction<'a> {
    database_write_transaction: lawn::database::WriteTransaction<'a>,
}

impl<'a> WriteTransaction<'a> {
    pub fn insert(&mut self, object: &Object, tags: &Vec<Object>) -> Result<&Self, String> {
        let object_id = object.get_id();
        if let Object::Raw(raw) = object {
            self.database_write_transaction
                .set(IDS_TO_SOURCES, &object_id, &raw)?;
        }
        for tag in tags {
            let tag_id = tag.get_id();
            self.database_write_transaction.set(
                TAG_AND_OBJECT,
                &(tag_id.clone(), object_id.clone()),
                &([] as [u8; 0]),
            )?;
            self.database_write_transaction.set(
                OBJECT_AND_TAG,
                &(object_id.clone(), tag_id.clone()),
                &([] as [u8; 0]),
            )?;
            if let Object::Raw(raw) = object {
                self.database_write_transaction
                    .set(IDS_TO_SOURCES, &tag_id, &raw)?;
            }
            self.database_write_transaction.set(
                TAG_TO_OBJECTS_COUNT,
                &tag_id,
                &(self
                    .database_write_transaction
                    .get::<Id, u32>(TAG_TO_OBJECTS_COUNT, &tag_id)?
                    .unwrap_or(0 as u32)
                    + 1),
            )?;
        }
        self.database_write_transaction.set(
            OBJECT_TO_TAGS_COUNT,
            &object_id,
            &(self
                .database_write_transaction
                .get::<Id, u32>(OBJECT_TO_TAGS_COUNT, &object_id)?
                .unwrap_or(0 as u32)
                + 1),
        )?;
        Ok(self)
    }

    pub fn remove_object(&mut self, object: &Object) -> Result<&Self, String> {
        let object_id = object.get_id();
        if self
            .database_write_transaction
            .get::<Id, Id>(OBJECT_TO_TAGS_COUNT, &object_id)?
            .is_none()
        {
            return Ok(self);
        }
        if let Object::Raw(_) = object {
            self.database_write_transaction
                .remove(IDS_TO_SOURCES, &object_id)?;
        }
        let object_and_tag_iterator = self
            .database_write_transaction
            .iter::<(Id, Id), [u8; 0]>(OBJECT_AND_TAG, Some(&(object_id.clone(), Id::default())))?
            .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
            .collect::<Vec<_>>()?;
        for ((current_object_id, current_tag_id), _) in object_and_tag_iterator {
            self.database_write_transaction.remove(
                TAG_AND_OBJECT,
                &(current_tag_id.clone(), current_object_id.clone()),
            )?;
            self.database_write_transaction
                .remove(OBJECT_AND_TAG, &(current_object_id, current_tag_id.clone()))?;
            self.database_write_transaction.set(
                TAG_TO_OBJECTS_COUNT,
                &current_tag_id,
                &(self
                    .database_write_transaction
                    .get::<Id, u32>(TAG_TO_OBJECTS_COUNT, &current_tag_id)?
                    .unwrap_or(0 as u32)
                    - 1),
            )?;
        }
        self.database_write_transaction
            .remove(OBJECT_TO_TAGS_COUNT, &object_id)?;

        Ok(self)
    }

    pub fn remove_tags_from_object(
        &mut self,
        object: &Object,
        tags: &Vec<Object>,
    ) -> Result<&Self, String> {
        let object_id = object.get_id();
        if self
            .database_write_transaction
            .get::<Id, Id>(OBJECT_TO_TAGS_COUNT, &object_id)?
            .is_none()
        {
            return Ok(self);
        }
        let mut tags_removed_from_object: u32 = 0;
        for tag in tags {
            let tag_id = tag.get_id();
            if self
                .database_write_transaction
                .get::<(Id, Id), [u8; 0]>(TAG_AND_OBJECT, &(tag_id.clone(), object_id.clone()))?
                .is_none()
            {
                continue;
            }
            self.database_write_transaction
                .remove(TAG_AND_OBJECT, &(tag_id.clone(), object_id.clone()))?;
            self.database_write_transaction
                .remove(OBJECT_AND_TAG, &(object_id.clone(), tag_id.clone()))?;
            let new_tag_count = self
                .database_write_transaction
                .get::<Id, u32>(TAG_TO_OBJECTS_COUNT, &tag_id)?
                .ok_or(format!("No objects count record for tag {tag:?}"))?
                - 1;
            if new_tag_count > 0 {
                self.database_write_transaction.set(
                    TAG_TO_OBJECTS_COUNT,
                    &tag_id,
                    &new_tag_count,
                )?;
            } else {
                self.database_write_transaction
                    .remove(TAG_TO_OBJECTS_COUNT, &tag_id)?;
                if let Object::Raw(_) = tag {
                    self.database_write_transaction
                        .remove(IDS_TO_SOURCES, &tag_id)?;
                }
            }
            tags_removed_from_object += 1;
        }
        let object_tags_count_before_delete = self
            .database_write_transaction
            .get::<Id, u32>(OBJECT_TO_TAGS_COUNT, &object_id)?
            .ok_or("No tags count record for object {object:?}")?;
        if tags_removed_from_object == object_tags_count_before_delete {
            self.database_write_transaction
                .remove(OBJECT_TO_TAGS_COUNT, &object_id)?;
            if let Object::Raw(_) = object {
                self.database_write_transaction
                    .remove(IDS_TO_SOURCES, &object_id)?;
            }
        } else {
            self.database_write_transaction.set(
                OBJECT_TO_TAGS_COUNT,
                &object_id,
                &(object_tags_count_before_delete - tags_removed_from_object),
            )?;
        }

        Ok(self)
    }

    pub fn get_source(&self, id: &Id) -> Result<Option<Vec<u8>>, String> {
        self.database_write_transaction
            .get::<Id, Vec<u8>>(IDS_TO_SOURCES, id)
    }

    pub fn has_tag(&self, object: &Object, tag: &Object) -> Result<bool, String> {
        Ok(self
            .database_write_transaction
            .get::<(Id, Id), [u8; 0]>(OBJECT_AND_TAG, &(object.get_id(), tag.get_id()))?
            .is_some())
    }

    pub fn get_tags(&self, object: Object) -> Result<Vec<Id>, String> {
        let object_id = object.get_id();
        self.database_write_transaction
            .iter::<(Id, Id), [u8; 0]>(OBJECT_AND_TAG, Some(&(object_id.clone(), Id::default())))?
            .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
            .map(|((_, current_tag_id), _)| Ok(current_tag_id))
            .collect::<Vec<_>>()
    }
}

struct Cursor<'a> {
    iterator: Box<dyn FallibleIterator<Item = ((Id, Id), [u8; 0]), Error = String> + 'a>,
    current_value: Option<(Id, Id)>,
}

impl<'a> Cursor<'a> {
    fn new(
        mut iterator: Box<dyn FallibleIterator<Item = ((Id, Id), [u8; 0]), Error = String> + 'a>,
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
    database_transaction: &'a lawn::database::ReadTransaction<'a>,
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
                            .get::<(Id, Id), [u8; 0]>(
                                TAG_AND_OBJECT,
                                &(tag_id.clone(), first_cursor_object.clone()),
                            )?
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
                        self.end = true
                    }
                    if let Some(result) = result {
                        return Ok(Some(result));
                    }
                }
            }

            if self.cursors.len() < self.present_tags_ids.len()
                && self.cursors.len() <= self.index_1
            {
                let mut cursor =
                    Cursor::new(self.database_transaction.iter::<(Id, Id), [u8; 0]>(
                        TAG_AND_OBJECT,
                        Some(&(
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
                        )),
                    )?)?;
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
                    self.database_transaction.iter::<(Id, Id), [u8; 0]>(
                        TAG_AND_OBJECT,
                        Some(&(
                            self.present_tags_ids[self.index_2].clone(),
                            self.cursors
                                .last()
                                .unwrap()
                                .current_value
                                .clone()
                                .unwrap()
                                .1,
                        )),
                    )?,
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
                self.index_2 = 0;
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
        Ok(Box::new(SearchIterator {
            database_transaction: &self.database_read_transaction,
            absent_tags_ids: {
                let mut absent_tags_ids_and_objects_count: Vec<(Id, u32)> = Vec::new();
                for tag in absent_tags {
                    let tag_id = tag.get_id();
                    if let Some(tag_objects_count) = self
                        .database_read_transaction
                        .get::<Id, u32>(TAG_TO_OBJECTS_COUNT, &tag_id)?
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
                    .collect()
            },
            present_tags_ids: {
                let mut present_tags_ids_and_objects_count: Vec<(Id, u32)> = Vec::new();
                for tag in present_tags {
                    let tag_id = tag.get_id();
                    present_tags_ids_and_objects_count.push((
                        tag_id.clone(),
                        self.database_read_transaction
                            .get::<Id, u32>(TAG_TO_OBJECTS_COUNT, &tag_id)?
                            .unwrap_or(0 as u32),
                    ));
                }
                present_tags_ids_and_objects_count
                    .sort_by_key(|(_, tag_objects_count)| *tag_objects_count);
                present_tags_ids_and_objects_count
                    .into_iter()
                    .map(|(tag, _)| tag)
                    .collect()
            },
            start_after_object,
            cursors: Vec::new(),
            index_1: 0 as usize,
            index_2: 1 as usize,
            end: false,
        }))
    }
}

impl Index {
    pub fn new(config: IndexConfig) -> Result<Self, String> {
        Ok(Self {
            database: lawn::database::Database::new(config.database)?,
        })
    }

    pub fn lock_all_and_write<F>(&mut self, f: F) -> Result<&Self, String>
    where
        F: Fn(WriteTransaction) -> (),
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
        F: Fn(ReadTransaction) -> (),
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
