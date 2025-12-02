use xxhash_rust::xxh3::xxh3_128;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, bincode::Encode, bincode::Decode)]
pub struct Id {
    pub value: [u8; 16],
}

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
    pub fn insert(&mut self, object: Object, tags: &Vec<Object>) -> Result<&mut Self, String> {
        let object_id = object.get_id();
        if let Object::Raw(ref raw) = object {
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
            if let Object::Raw(ref raw) = object {
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

    pub fn lock_all_and_read<F>(&self, f: F) -> Result<&Self, String>
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
