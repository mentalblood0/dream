pub extern crate anyhow;
pub extern crate fallible_iterator;
pub extern crate lawn;
pub extern crate paste;
pub extern crate serde;

pub use lawn::bincode;

#[derive(
    Clone, Default, PartialEq, PartialOrd, Debug, bincode::Encode, bincode::Decode, Eq, Ord, Hash,
)]
#[bincode(crate = "bincode")]
pub struct Id(pub [u8; 16]);

#[macro_export]
macro_rules! define_index {
    ($index_name:ident(
        $(
            $schema_name:ident
        )*
    ) {
        $(
            $additional_schema_name:ident {
                $(
                    $table_name:ident<$key_type:ty, $value_type:ty>
                )*
            }
        )*
    } use {
        $($use_item:tt)*
    }) => {
        #[allow(dead_code)]
        mod $index_name {
        use $crate::lawn;
        lawn::database::define_database!(lawn_database {
            $(
                $schema_name {
                    tag_and_object<(Id, Id), ()>
                    object_and_tag<(Id, Id), ()>
                    object<Id, ()>
                }
            )*
            $(
                $additional_schema_name {
                    $(
                        $table_name<$key_type, $value_type>
                    )*
                }
            )*
        } use {
            use $crate::Id;
            $($use_item)*
        });

        use std::ops::{Deref, Bound};

        use $crate::{
            paste::paste,
            anyhow::{Context, Result, Error},
            fallible_iterator::FallibleIterator,
            serde::{Deserialize, Serialize},
            Id
        };

        #[derive(Serialize, Deserialize, Debug, Clone)]
        pub struct IndexConfig {
            pub database: lawn_database::DatabaseConfig,
            pub maintain_only_tag_and_object_table: bool
        }

        pub struct Index {
            pub database: lawn_database::Database,
            pub config: IndexConfig
        }

        pub struct ReadTransaction<'a> {
            pub database_transaction: lawn_database::ReadTransaction<'a>,
        }

        pub struct WriteTransaction<'a, 'b> {
            pub database_transaction: &'a mut lawn_database::WriteTransaction<'b>,
            pub index_config: &'a IndexConfig
        }

        macro_rules! define_read_methods {
            () => {
                $(
                    paste! {
                        pub fn [<$schema_name _has_tag>](&self, object: &Id, tag: &Id) -> Result<bool> {
                            let key = &(object.clone(), tag.clone());
                            self.database_transaction
                                .$schema_name
                                .object_and_tag
                                .exists(key).with_context(|| format!("Can not verify if key {key:?} exists in object_and_tag table"))
                        }

                        pub fn [<$schema_name _has_object_with_tag>](&self, tag: &Id) -> Result<bool> {
                            let from_tag_and_object= &(tag.clone(), Id::default());
                            Ok(self
                                .database_transaction
                                .$schema_name
                                .tag_and_object
                                .iter(Bound::Included(from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting from key {from_tag_and_object:?}"))?
                                .take_while(|((current_tag_id, _), _)| Ok(*current_tag_id == from_tag_and_object.0))
                                .next()?
                                .is_some())
                        }

                        pub fn [<$schema_name _get_tags>](&self, object: &Id) -> Result<Vec<Id>> {
                            let from_object_and_tag = &(object.clone(), Id::default());
                            self.database_transaction
                                .$schema_name
                                .object_and_tag
                                .iter(Bound::Included(from_object_and_tag), false).with_context(|| format!("Can not initiate iteration over object_and_tag table starting from key {from_object_and_tag:?}"))?
                                .take_while(|((current_object_id, _), _)| Ok(current_object_id == object))
                                .map(|((_, current_tag_id), _)| Ok(current_tag_id))
                                .collect::<Vec<_>>()
                        }

                        pub fn [<$schema_name _search>](
                            &self,
                            present_tags: &[Id],
                            absent_tags: &[Id],
                            start_after_object: Option<Id>,
                        ) -> Result<Box<dyn FallibleIterator<Item = Id, Error = Error> + '_>> {
                            Ok(match present_tags.len() {
                                0 => {
                                    let from_object = start_after_object.clone().unwrap_or_default();
                                    let absent_tags = absent_tags.to_vec();
                                    Box::new(
                                        self.database_transaction
                                            .$schema_name
                                            .object
                                            .iter(
                                                if let Some(start_after_object) = &start_after_object {
                                                    Bound::Excluded(start_after_object)
                                                } else {
                                                    Bound::Unbounded
                                                },
                                                false
                                            ).with_context(|| format!("Can not initiate iteration over object_to_tags_count table starting from key {from_object:?}"))?
                                            .map(|(object_id, _)| Ok(object_id))
                                            .filter(move |object_id| {
                                                for absent_tag in absent_tags.iter() {
                                                    let key = &(absent_tag.clone(), object_id.clone());
                                                    if self
                                                        .database_transaction
                                                        .$schema_name
                                                        .tag_and_object
                                                        .exists(key).with_context(|| format!("Can not verify if key {key:?} exists in tag_and_object table"))?
                                                    {
                                                        return Ok(false)
                                                    }
                                                }
                                                Ok(true)
                                            })
                                    )
                                },
                                1 => {
                                    let search_tag_id = present_tags.into_iter().next().unwrap().clone();
                                    let from_tag_and_object = (search_tag_id.clone(), start_after_object.clone().unwrap_or_default());
                                    let absent_tags = absent_tags.to_vec();
                                    Box::new(
                                        self.database_transaction
                                            .$schema_name
                                            .tag_and_object
                                            .iter(Bound::Included(&from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting from key {from_tag_and_object:?}"))?
                                            .map(|((tag_id, object_id), _)| Ok((tag_id, object_id)))
                                            .take_while(move |(tag_id, _)| Ok(tag_id == &search_tag_id))
                                            .map(|(_, object_id)| Ok(object_id))
                                            .filter(move |object_id| Ok(start_after_object.is_none() || *object_id != from_tag_and_object.1))
                                            .filter(move |object_id| {
                                                for absent_tag in absent_tags.iter() {
                                                    let key = &(absent_tag.clone(), object_id.clone());
                                                    if self
                                                        .database_transaction
                                                        .$schema_name
                                                        .tag_and_object
                                                        .exists(key).with_context(|| format!("Can not verify if key {key:?} exists in tag_and_object table"))?
                                                    {
                                                        return Ok(false)
                                                    }
                                                }
                                                Ok(true)
                                            }),
                                    )
                                }
                                2.. => Box::new([<$schema_name:camel SearchIterator>] {
                                    database_transaction: self.database_transaction.deref(),
                                    absent_tags_ids: absent_tags.to_vec(),
                                    present_tags_ids: present_tags.to_vec(),
                                    start_after_object,
                                    cursors: Vec::new(),
                                    index_1: 0 as usize,
                                    index_2: 1 as usize,
                                    end: false,
                                }),
                            })
                        }
                    }
                )+
            };
        }

        impl<'a> ReadTransaction<'a> {
            define_read_methods!();
        }

        impl<'a, 'b> WriteTransaction<'a, 'b> {
            define_read_methods!();

            $(
                paste! {
                    pub fn [<$schema_name _insert>](&mut self, object: &Id, tags: &[Id]) -> Result<&mut Self> {
                        for tag in tags {
                            self.database_transaction
                                .$schema_name
                                .tag_and_object
                                .insert((tag.clone(), object.clone()), ());
                            if !self.index_config.maintain_only_tag_and_object_table {
                                self.database_transaction
                                    .$schema_name
                                    .object_and_tag
                                    .insert((object.clone(), tag.clone()), ());
                            }
                        }
                        if !self.index_config.maintain_only_tag_and_object_table {
                            self.database_transaction
                                .$schema_name
                                .object
                                .insert(object.clone(), ());
                        }
                        Ok(self)
                    }

                    pub fn [<$schema_name _remove_object>](&mut self, object: &Id) -> Result<&mut Self> {
                        for tag in self.[<$schema_name _get_tags>](object)? {
                            self.database_transaction
                                .$schema_name
                                .tag_and_object
                                .remove(&(tag.clone(), object.clone()));
                            if !self.index_config.maintain_only_tag_and_object_table {
                                self.database_transaction
                                    .$schema_name
                                    .object_and_tag
                                    .remove(&(object.clone(), tag.clone()));
                            }
                        }
                        if !self.index_config.maintain_only_tag_and_object_table {
                            self.database_transaction
                                .$schema_name
                                .object
                                .remove(object);
                        }
                        Ok(self)
                    }

                    pub fn [<$schema_name _remove_tags_from_object>](
                        &mut self,
                        object: &Id,
                        tags: &[Id],
                    ) -> Result<&mut Self> {
                        for tag in tags {
                            self.database_transaction
                                .$schema_name
                                .tag_and_object
                                .remove(&(tag.clone(), object.clone()));
                            if !self.index_config.maintain_only_tag_and_object_table {
                                self.database_transaction
                                    .$schema_name
                                    .object_and_tag
                                    .remove(&(object.clone(), tag.clone()));
                            }
                        }
                        if (!self.index_config.maintain_only_tag_and_object_table &&
                            self.database_transaction
                                .$schema_name
                                .object_and_tag
                                .iter(Bound::Included(&(object.clone(), Id::default())), false)?
                                .take_while(|((object_left, _), _)| Ok(object_left == object))
                                .next()?.is_none())
                        {
                            self.database_transaction
                                .$schema_name
                                .object
                                .remove(object);
                        }
                        Ok(self)
                    }
                }
            )+
        }

        $(
            paste! {
                struct [<$schema_name:camel Cursor>]<'a> {
                    iterator: Box<dyn FallibleIterator<Item = ((Id, Id), ()), Error = Error> + 'a>,
                    current_value: Option<(Id, Id)>,
                }

                impl<'a> [<$schema_name:camel Cursor>]<'a> {
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

                pub struct [<$schema_name:camel SearchIterator>]<'a> {
                    database_transaction: &'a lawn_database::TablesTransactions,
                    present_tags_ids: Vec<Id>,
                    absent_tags_ids: Vec<Id>,
                    start_after_object: Option<Id>,
                    cursors: Vec<[<$schema_name:camel Cursor>]<'a>>,
                    index_1: usize,
                    index_2: usize,
                    end: bool,
                }

                impl<'a> FallibleIterator for [<$schema_name:camel SearchIterator>]<'a> {
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
                                        self.database_transaction
                                            .$schema_name
                                            .tag_and_object
                                            .exists(key).with_context(|| format!("Can not verify if key {key:?} exists in tag_and_object table"))
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
                                    [<$schema_name:camel Cursor>]::new(self.database_transaction.$schema_name.tag_and_object.iter(Bound::Included(from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting from key {from_tag_and_object:?}"))?)?;
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
                                let cursor = [<$schema_name:camel Cursor>]::new(
                                    self.database_transaction.$schema_name.tag_and_object.iter(Bound::Included(from_tag_and_object), false).with_context(|| format!("Can not initiate iteration over tag_and_object table starting with key {from_tag_and_object:?}"))?,
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
            }
        )+

        impl Index {
            pub fn new(config: IndexConfig) -> Result<Self> {
                Ok(Self {
                    database: lawn_database::Database::new(config.database.clone()).with_context(|| format!("Can not create dream index using database config {:?}", config.database))?,
                    config
                })
            }

            pub fn lock_all_and_write<F, R>(&mut self, mut f: F) -> Result<R>
            where
                F: FnMut(&mut WriteTransaction<'_, '_>) -> Result<R>,
            {
                self.database
                    .lock_all_and_write(|database_write_transaction| {
                        f(&mut WriteTransaction {
                            database_transaction: database_write_transaction,
                            index_config: &self.config
                        })
                    }).with_context(|| "Can not lock lawn database and initiate write transaction")
            }

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

define_index!(test_index(
    public
) {
} use {
});

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::{BTreeMap, BTreeSet};

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

        let t1 = Id([11; 16]);
        let t2 = Id([12; 16]);
        let t3 = Id([13; 16]);
        let o1 = Id([21; 16]);
        let o2 = Id([22; 16]);
        let o3 = Id([23; 16]);

        index
            .lock_all_and_write(|transaction| {
                transaction
                    .public_insert(&o1, std::slice::from_ref(&t1))
                    .unwrap()
                    .public_insert(&o2, &[t1.clone(), t2.clone()])
                    .unwrap()
                    .public_insert(&o3, &[t1.clone(), t2.clone(), t3.clone()])
                    .unwrap();
                assert_eq!(
                    transaction
                        .public_search(&[t1.clone(), t2.clone(), t3.clone()], &[], None)?
                        .collect::<Vec<_>>()?,
                    std::slice::from_ref(&o3)
                );
                assert_eq!(
                    transaction
                        .public_search(&[t1.clone(), t2.clone()], &[], None)?
                        .collect::<Vec<_>>()?,
                    [o2.clone(), o3.clone()]
                );
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t1), &[], None)?
                        .collect::<Vec<_>>()?,
                    [o1.clone(), o2.clone(), o3.clone()]
                );

                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t1), std::slice::from_ref(&t1), None)?
                        .collect::<Vec<_>>()?,
                    []
                );
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t1), &[], Some(o3.clone()))?
                        .collect::<Vec<_>>()?,
                    []
                );
                assert_eq!(
                    transaction
                        .public_search(&[], &[], Some(o3.clone()))?
                        .collect::<Vec<_>>()?,
                    []
                );
                assert_eq!(
                    transaction
                        .public_search(&[], &[t1.clone(), t2.clone(), t3.clone()], None)?
                        .collect::<Vec<_>>()?,
                    []
                );

                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t1), std::slice::from_ref(&t2), None)?
                        .collect::<Vec<_>>()?,
                    std::slice::from_ref(&o1)
                );
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t1), std::slice::from_ref(&t3), None)?
                        .collect::<Vec<_>>()?,
                    [o1.clone(), o2.clone()]
                );

                transaction.public_remove_tags_from_object(&o3, &[t1.clone(), t3.clone()])?;
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t1), &[], None)?
                        .collect::<Vec<_>>()?,
                    [o1.clone(), o2.clone()]
                );
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t2), &[], None)?
                        .collect::<Vec<_>>()?,
                    [o2.clone(), o3.clone()]
                );
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t3), &[], None)?
                        .collect::<Vec<_>>()?,
                    []
                );

                transaction.public_remove_object(&o2)?;
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t1), &[], None)?
                        .collect::<Vec<_>>()?,
                    std::slice::from_ref(&o1)
                );
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t2), &[], None)?
                        .collect::<Vec<_>>()?,
                    std::slice::from_ref(&o3)
                );
                assert_eq!(
                    transaction
                        .public_search(std::slice::from_ref(&t3), &[], None)?
                        .collect::<Vec<_>>()?,
                    []
                );
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn test_generative() {
        const TOTAL_TAGS_COUNT: usize = 30;
        const OBJECT_TAGS_COUNT: usize = 8;
        const OBJECTS_COUNT: usize = 10000;
        const SEARCHES_COUNT: usize = 1000;

        let mut index = new_default_index("test_generative");
        let mut rng = WyRand::new_seed(0);

        let mut tags = (0..TOTAL_TAGS_COUNT)
            .map(|_| {
                let mut result = [0u8; 16];
                rng.fill(&mut result);
                Id(result)
            })
            .collect::<Vec<_>>();
        let object_to_tags = (0..OBJECTS_COUNT)
            .map(|_| {
                let mut object = [0u8; 16];
                rng.fill(&mut object);
                let mut tags = (0..OBJECT_TAGS_COUNT)
                    .map(|_| tags[rng.generate_range(0..tags.len())].clone())
                    .collect::<Vec<_>>();
                tags.sort();
                tags.dedup();
                (Id(object), tags)
            })
            .collect::<BTreeMap<_, _>>();

        index
            .lock_all_and_write(|transaction| {
                for (object, tags) in object_to_tags.iter() {
                    transaction.public_insert(object, tags)?;
                }
                for (object, tags) in object_to_tags.iter() {
                    for tag in tags.iter() {
                        assert_eq!(transaction.public_has_tag(object, tag)?, true);
                    }
                    let result_tags = BTreeSet::from_iter(transaction.public_get_tags(object)?);
                    let correct_tags = BTreeSet::from_iter(tags.iter().cloned());
                    assert_eq!(result_tags, correct_tags);
                }
                Ok(())
            })
            .unwrap();

        let tag_to_objects = {
            let mut result: BTreeMap<Id, Vec<Id>> = BTreeMap::new();
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
                        assert_eq!(transaction.public_has_tag(object, tag)?, true);
                        assert_eq!(transaction.public_has_object_with_tag(tag)?, true);
                    }
                    assert_eq!(
                        transaction
                            .public_search(std::slice::from_ref(tag), &[], None)?
                            .collect::<BTreeSet<_>>()?,
                        BTreeSet::from_iter(objects.iter().cloned())
                    );
                }

                let mut unrestricted_search_result = transaction
                    .public_search(&[], &[], None)?
                    .collect::<Vec<_>>()?;
                unrestricted_search_result.sort();
                let mut all_objects = object_to_tags.keys().cloned().collect::<Vec<_>>();
                all_objects.sort();
                assert_eq!(unrestricted_search_result, all_objects);

                let mut nearly_unrestricted_search_result = transaction
                    .public_search(
                        &[],
                        &[],
                        Some(Id([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0])),
                    )?
                    .collect::<Vec<_>>()?;
                nearly_unrestricted_search_result.sort();
                let mut all_objects = object_to_tags.keys().cloned().collect::<Vec<_>>();
                all_objects.sort();
                assert_eq!(nearly_unrestricted_search_result, all_objects);

                for _ in 0..SEARCHES_COUNT {
                    rng.shuffle(&mut tags);
                    let present_tags = tags.iter().take(2).cloned().collect::<Vec<_>>();
                    let result = BTreeSet::from_iter(
                        transaction
                            .public_search(&present_tags, &[], None)?
                            .collect::<Vec<_>>()?,
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
