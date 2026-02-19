//! A tagged object indexing library with support for efficient tag-based searches.
//!
//! This library provides an indexed storage system for objects with tags, enabling
//! fast searches for objects that have certain tags and lack other tags. It uses
//! [lawn](https://docs.rs/lawn) as the underlying database for persistence.
//!
//! # Key Features
//!
//! - **Tag-based indexing**: Objects can be tagged and searched by tag combinations
//! - **Efficient searches**: Optimized query performance for common search patterns
//! - **Persistence**: All data is persisted using the lawn database
//! - **Transaction support**: Read and write transactions for safe concurrent access

pub extern crate anyhow;
pub extern crate fallible_iterator;
pub extern crate lawn;
pub extern crate serde;
pub extern crate xxhash_rust;

/// A 16-byte unique identifier for objects.
///
/// `Id` is used to uniquely identify objects and tags within the index.
/// When an object is inserted without an explicit ID, its ID is computed
/// as the xxh3-128 hash of the raw data.
#[derive(
    Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode, Eq, Ord, Hash,
)]
pub struct Id {
    /// The 16-byte identifier value.
    pub value: [u8; 16],
}

/// Represents an object that can be stored in the index.
///
/// An `Object` is either raw byte data or an already-identified object.
/// When inserting raw data, the system automatically computes a unique
/// ID based on the content hash. Objects with explicit IDs maintain
/// that identity across operations.
///
/// # Variants
///
/// - `Raw(Vec<u8>)`: Raw byte data. An ID will be computed from the content.
/// - `Identified(Id)`: An object with an explicitly assigned ID.
///
/// # Computing IDs
///
/// When using `Object::Raw`, the ID is computed using xxh3-128 hashing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Object {
    /// Raw byte data. An ID will be automatically computed from the content.
    Raw(Vec<u8>),
    /// An object with an explicitly assigned ID.
    Identified(Id),
}

impl Object {
    /// Gets the ID of this object.
    ///
    /// If the object is `Raw`, computes a new ID from the content hash.
    /// If the object is `Identified`, returns the existing ID.
    ///
    /// # Returns
    ///
    /// The unique `Id` for this object.
    pub fn get_id(&self) -> Id {
        match self {
            Object::Raw(raw) => Id {
                value: xxhash_rust::xxh3::xxh3_128(raw).to_le_bytes(),
            },
            Object::Identified(id) => id.clone(),
        }
    }
}

#[macro_export]
macro_rules! define_index {
    ($index_name:ident {
        $($table_name:ident<$key_type:ty, $value_type:ty>),* $(,)?
    } use {
        $($use_item:tt)*
    }) => {
        #[allow(dead_code)]
        mod $index_name {
        use $crate::lawn;
        lawn::database::define_database!(lawn_database {
            tag_and_object<(Id, Id), ()>,
            object_and_tag<(Id, Id), ()>,
            id_to_source<Id, Vec<u8>>,
            tag_to_objects_count<Id, u32>,
            object_to_tags_count<Id, u32>,
            $( $table_name<$key_type, $value_type> ),*
        } use {
            use $crate::Id;
            $($use_item)*
        });

        use std::{collections::HashSet, ops::Deref};
        use std::ops::Bound;

        use $crate::anyhow::{Context, Result, Error, anyhow};
        use $crate::fallible_iterator::FallibleIterator;
        use $crate::serde::{Deserialize, Serialize};

        use $crate::Object;
        use $crate::Id;

        /// Configuration for an index.
        ///
        /// Contains the database configuration and is serializable for persistence.
        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct IndexConfig {
            /// The underlying lawn database configuration.
            pub database: lawn_database::DatabaseConfig,
        }

        /// The main index structure managing the database.
        ///
        /// Use `Index::new()` to create an instance and `lock_all_and_write()`
        /// or `lock_all_writes_and_read()` to perform operations.
        pub struct Index {
            /// The underlying lawn database.
            pub database: lawn_database::Database,
        }

        /// A read-only transaction for querying the index.
        ///
        /// Created via `Index::lock_all_writes_and_read()`. Provides methods
        /// for searching and retrieving objects and tags.
        pub struct ReadTransaction<'a> {
            /// The underlying database read transaction.
            pub database_transaction: lawn_database::ReadTransaction<'a>,
        }

        /// A write transaction for mutating the index.
        ///
        /// Created via `Index::lock_all_and_write()`. Provides methods for
        /// inserting objects, removing objects, and managing tags.
        pub struct WriteTransaction<'a, 'b> {
            /// The underlying database write transaction.
            pub database_transaction: &'a mut lawn_database::WriteTransaction<'b>,
        }

        macro_rules! define_read_methods {
            () => {
                /// Retrieves the raw data for an object given its ID.
                ///
                /// # Arguments
                ///
                /// * `id` - The ID of the object to retrieve.
                ///
                /// # Returns
                ///
                /// `Ok(Some(Object::Raw(data)))` if found, `Ok(None)` if not found.
                pub fn get_source(&self, id: &Id) -> Result<Option<Object>> {
                    Ok(self
                        .database_transaction
                        .id_to_source
                        .get(id).with_context(|| format!("Can not get source for id {id:?} from id_to_source table"))?
                        .and_then(|value| Some(Object::Raw(value))))
                }

                /// Checks if an object has a specific tag.
                ///
                /// # Arguments
                ///
                /// * `object` - The object to check.
                /// * `tag` - The tag to look for.
                ///
                /// # Returns
                ///
                /// `Ok(true)` if the object has the tag, `Ok(false)` otherwise.
                pub fn has_tag(&self, object: &Object, tag: &Object) -> Result<bool> {
                    let key = &(object.get_id(), tag.get_id());
                    Ok(self
                        .database_transaction
                        .object_and_tag
                        .get(key).with_context(|| format!("Can not verify if key {key:?} exists in object_and_tag table"))?
                        .is_some())
                }

                /// Checks if any object has the given tag.
                ///
                /// # Arguments
                ///
                /// * `tag` - The tag to check for.
                ///
                /// # Returns
                ///
                /// `Ok(true)` if at least one object has the tag, `Ok(false)` otherwise.
                pub fn has_object_with_tag(&self, tag: &Object) -> Result<bool> {
                    let from_tag_and_object= &(tag.get_id(), Id {value: [0u8; 16]});
                    Ok(self
                        .database_transaction
                        .tag_and_object
                        .iter(Bound::Included(from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting from key {from_tag_and_object:?}"))?
                        .take_while(|((current_tag_id, _), _)| Ok(*current_tag_id == from_tag_and_object.0))
                        .next()?
                        .is_some())
                }

                /// Gets all tags associated with an object.
                ///
                /// # Arguments
                ///
                /// * `object` - The object to get tags for.
                ///
                /// # Returns
                ///
                /// A vector of tag IDs associated with the object.
                pub fn get_tags(&self, object: &Object) -> Result<Vec<Id>> {
                    let object_id = object.get_id();
                    let from_object_and_tag = &(object_id.clone(), Id::default());
                    self.database_transaction
                        .object_and_tag
                        .iter(Bound::Included(from_object_and_tag), false).with_context(|| format!("Can not initiate iteration over object_and_tag table starting from key {from_object_and_tag:?}"))?
                        .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
                        .map(|((_, current_tag_id), _)| Ok(current_tag_id))
                        .collect::<Vec<_>>()
                }

                /// Searches for objects matching tag criteria.
                ///
                /// Returns an iterator of object IDs that have all `present_tags`
                /// and none of the `absent_tags`.
                ///
                /// # Arguments
                ///
                /// * `present_tags` - Objects that must have all these tags.
                /// * `absent_tags` - Objects must not have any of these tags.
                /// * `start_after_object` - Optional ID to start searching after (for pagination).
                ///
                /// # Returns
                ///
                /// A fallible iterator yielding matching object IDs.
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
                                .get(&tag_id).with_context(|| format!("Can not get objects count for tag with id {tag_id:?} using tag_to_objects_count table"))?
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
                        0 => {
                            let from_object = &start_after_object.clone().unwrap_or_default();
                            Box::new(
                                self.database_transaction
                                    .object_to_tags_count
                                    .iter(Bound::Included(from_object), false).with_context(|| format!("Can not initiate iteration over object_to_tags_count table starting from key {from_object:?}"))?
                                    .skip(if start_after_object.is_some() { 1 } else { 0 })
                                    .map(|(object_id, _)| Ok(object_id))
                                    .filter(move |object_id| {
                                        fallible_iterator::convert(
                                            absent_tags_ids
                                                .iter()
                                                .map(|id| Result::<Id>::Ok(id.clone())),
                                        )
                                        .all(|absent_tag_id| {
                                            let key = &(absent_tag_id.clone(), object_id.clone());
                                            Ok(self
                                                .database_transaction
                                                .tag_and_object
                                                .get(key).with_context(|| format!("Can not verify if key {key:?} exists in tag_and_object table"))?
                                                .is_none())
                                        })
                                    }),
                            )
                        },
                        1 => {
                            let search_tag_id = present_tags[0].get_id();
                            let from_tag_and_object = &(
                                search_tag_id.clone(),
                                start_after_object.clone().unwrap_or_default(),
                            );
                            Box::new(
                                self.database_transaction
                                    .tag_and_object
                                    .iter(Bound::Included(from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting from key {from_tag_and_object:?}"))?
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
                                            let key = &(absent_tag_id.clone(), object_id.clone());
                                            Ok(self
                                                .database_transaction
                                                .tag_and_object
                                                .get(key).with_context(|| format!("Can not verify if key {key:?} exists in tag_and_object table"))?
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
                                            .get(&tag_id).with_context(|| format!("Can not get objects count for tag with id {tag_id:?} using tag_to_objects_count table"))?
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

            /// Inserts an object with associated tags.
            ///
            /// If the object already exists, adds the new tags to it.
            /// If the object is `Raw`, stores its raw data.
            ///
            /// # Arguments
            ///
            /// * `object` - The object to insert.
            /// * `tags` - Tags to associate with the object.
            ///
            /// # Returns
            ///
            /// `Ok(self)` on success, allowing method chaining.
            pub fn insert(&mut self, object: &Object, tags: &Vec<Object>) -> Result<&mut Self> {
                let object_id = object.get_id();
                if let Object::Raw(raw) = object {
                    self.database_transaction
                        .id_to_source
                        .insert(object_id.clone(), raw.clone());
                }
                let existent_tags = HashSet::<Id>::from_iter(self.get_tags(object).with_context(|| format!("Can not get tags for object {object:?}"))?.into_iter());
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
                        .get(&tag_id).with_context(|| format!("Can not get objects count for tag with id {tag_id:?} using tag_to_objects_count table"))?
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

            /// Removes an object and all its associated data.
            ///
            /// Removes the object, its raw data (if Raw), and all tag associations.
            /// Also updates the tag-to-object count for affected tags.
            ///
            /// # Arguments
            ///
            /// * `object` - The object to remove.
            ///
            /// # Returns
            ///
            /// `Ok(self)` on success.
            pub fn remove_object(&mut self, object: &Object) -> Result<&mut Self> {
                let object_id = object.get_id();
                if self
                    .database_transaction
                    .object_to_tags_count
                    .get(&object_id).with_context(|| format!("Can not get tags count for object with id {object_id:?} using object_to_tags_count table"))?
                    .is_none()
                {
                    return Ok(self);
                }
                if let Object::Raw(_) = object {
                    self.database_transaction.id_to_source.remove(&object_id);
                }
                let from_object_and_tag = &(object_id.clone(), Id::default());
                let object_and_tag_iterator = self
                    .database_transaction
                    .object_and_tag
                    .iter(Bound::Included(from_object_and_tag), false).with_context(|| format!("Can not initiate iteration over object_and_tag table starting from key {from_object_and_tag:?}"))?
                    .take_while(|((current_object_id, _), _)| Ok(current_object_id == &object_id))
                    .collect::<Vec<_>>().with_context(|| format!("Can not collect from iteration over object_and_tag table starting from key {from_object_and_tag:?} taking while object id is {object_id:?}"))?;
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
                        .get(&current_tag_id).with_context(|| format!("Can not get objects count for tag with id {current_tag_id:?} using tag_to_objects_count table"))?
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

            /// Removes specific tags from an object.
            ///
            /// If all tags are removed from an object, the object itself is also removed.
            /// Updates tag counts accordingly.
            ///
            /// # Arguments
            ///
            /// * `object` - The object to modify.
            /// * `tags` - Tags to remove from the object.
            ///
            /// # Returns
            ///
            /// `Ok(self)` on success.
            pub fn remove_tags_from_object(
                &mut self,
                object: &Object,
                tags: &Vec<Object>,
            ) -> Result<&mut Self> {
                let object_id = object.get_id();
                if self
                    .database_transaction
                    .object_to_tags_count
                    .get(&object_id).with_context(|| format!("Can not get tags count for object with id {object_id:?} using object_to_tags_count table"))?
                    .is_none()
                {
                    return Ok(self);
                }
                let tags_before_remove = HashSet::<Id>::from_iter(self.get_tags(object).with_context(|| format!("Can not get tags for object {object:?}"))?.into_iter());
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
                        .get(&tag_id).with_context(|| format!("Can not get objects count for tag with id {tag_id:?} using tag_to_objects_count table"))?
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
                    .next().with_context(|| "Can not get first value from iterator")?
                    .and_then(|(current_value, _)| Some(current_value));
                Ok(Self {
                    iterator,
                    current_value,
                })
            }

            fn next(&mut self) -> Result<()> {
                self.current_value = self
                    .iterator
                    .next().with_context(|| format!("Can not get next value from iterator after value {:?}", self.current_value))?
                    .and_then(|(current_value, _)| Some(current_value));
                Ok(())
            }
        }

        pub struct SearchIterator<'a> {
            database_transaction: &'a lawn_database::TablesTransactions,
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
                        if self.cursors.iter().all(|cursor| {
                            cursor
                                .current_value
                                .clone()
                                .is_some_and(|current_value| current_value.1 == first_cursor_object)
                        }) {
                            let result = if fallible_iterator::convert(
                                self.absent_tags_ids
                                    .iter()
                                    .map(|id| Result::<Id>::Ok(id.clone())),
                            )
                            .all(|tag_id| {
                                let key = &(tag_id.clone(), first_cursor_object.clone());
                                Ok(self
                                    .database_transaction
                                    .tag_and_object
                                    .get(key).with_context(|| format!("Can not verify if key {key:?} exists in tag_and_object table"))?
                                    .is_none())
                            })? {
                                Some(first_cursor_object)
                            } else {
                                None
                            };
                            self.cursors[0].next().with_context(|| format!("Can not get next value for first cursor after value {:?}", self.cursors[0].current_value))?;
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
                        let from_tag_and_object = &(
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
                        );
                        let mut cursor =
                            Cursor::new(self.database_transaction.tag_and_object.iter(Bound::Included(from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting from key {from_tag_and_object:?}"))?)?;
                        if self.index_1 == 0 && self.start_after_object.is_some() {
                            cursor.next().with_context(|| format!("Can not propagate newely created cursor further (even getting nothing) to skip current entry as start_after_object {:?} is provided", self.start_after_object))?;
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
                        let from_tag_and_object = &(
                            self.present_tags_ids[self.index_2].clone(),
                            self.cursors
                                .last()
                                .unwrap()
                                .current_value
                                .clone()
                                .unwrap()
                                .1,
                        );
                        let cursor = Cursor::new(
                            self.database_transaction.tag_and_object.iter(Bound::Included(from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting with key {from_tag_and_object:?}"))?,
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
                        self.cursors[self.index_2].next().with_context(|| format!("Can not propagate {:?}-th cursor further", self.index_2 + 1))?;
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
                            self.cursors[0].next().with_context(|| format!("Can not propagate first cursor further"))?;
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

        impl Index {
            /// Creates a new index from configuration.
            ///
            /// # Arguments
            ///
            /// * `config` - The index configuration.
            ///
            /// # Returns
            ///
            /// `Ok(Index)` on success.
            pub fn new(config: IndexConfig) -> Result<Self> {
                Ok(Self {
                    database: lawn_database::Database::new(config.database.clone()).with_context(|| format!("Can not create dream index using database config {:?}", config.database))?,
                })
            }

            /// Executes a write transaction with exclusive database access.
            /// Executes a write transaction with exclusive database access.
            ///
            /// Acquires a write lock (blocking all other operations) and executes the given
            /// closure with a mutable transaction. No other reads or writes can proceed
            /// concurrently.
            ///
            /// # Arguments
            ///
            /// * `f` - A closure that receives a write transaction.
            ///
            /// # Returns
            ///
            /// `Ok(R)` on success.
            pub fn lock_all_and_write<'a, F, R>(&'a mut self, mut f: F) -> Result<R>
            where
                F: FnMut(&mut WriteTransaction<'_, '_>) -> Result<R>,
            {
                self.database
                    .lock_all_and_write(|database_write_transaction| {
                        f(&mut WriteTransaction {
                            database_transaction: database_write_transaction,
                        })
                    }).with_context(|| "Can not lock lawn database and initiate write transaction")
            }

            /// Executes a read transaction with shared database access.
            ///
            /// Acquires a read lock (blocking writes) and executes the given
            /// closure with a read-only transaction. Multiple reads can proceed
            /// concurrently, but writes are blocked.
            ///
            /// # Arguments
            ///
            /// * `f` - A closure that receives a read transaction.
            ///
            /// # Returns
            ///
            /// `Ok(R)` on success.
            pub fn lock_all_writes_and_read<F, R>(&self, mut f: F) -> Result<R>
            where
                F: FnMut(ReadTransaction) -> Result<R>,
            {
                self.database
                    .lock_all_writes_and_read(|database_read_transaction| {
                        f(ReadTransaction {
                            database_transaction: database_read_transaction,
                        })
                    }).with_context(|| "Can not lock all write operations on lawn database and initiate read transaction")
            }
        }
        }
    };
}

define_index!(test_index {
} use {
});

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, BTreeSet};

    use anyhow::anyhow;
    use fallible_iterator::FallibleIterator;
    use nanorand::{Rng, WyRand};
    use pretty_assertions::assert_eq;

    fn new_default_index(test_name_for_isolation: &str) -> test_index::Index {
        test_index::Index::new(
            serde_saphyr::from_str(
                &std::fs::read_to_string("src/test_index_config.yml")
                    .unwrap()
                    .replace("TEST_NAME", test_name_for_isolation),
            )
            .unwrap(),
        )
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
        const OBJECTS_COUNT: usize = 100;
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
                    for object in objects {
                        assert_eq!(transaction.has_tag(object, tag)?, true);
                        assert_eq!(transaction.has_object_with_tag(tag)?, true);
                    }
                    assert_eq!(
                        transaction
                            .search(&vec![tag.clone()], &vec![], None)?
                            .map(|object_id| transaction
                                .get_source(&object_id)?
                                .ok_or(anyhow!("No source for object id {object_id:?} found")))
                            .collect::<BTreeSet<_>>()?,
                        BTreeSet::from_iter(objects.iter().cloned())
                    );
                }

                for _ in 0..SEARCHES_COUNT {
                    rng.shuffle(&mut tags);
                    let present_tags = tags.iter().take(2).cloned().collect::<Vec<_>>();
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
