//! Database types

use std::{path::PathBuf, sync::Arc};

use educe::Educe;
use fallible_iterator::{FallibleIterator, IteratorExt as _};
use heed::{
    types::LazyDecode, BytesDecode, BytesEncode, Comparator, DatabaseFlags,
    DefaultComparator, PutFlags, RoTxn,
};

use crate::{env, Env, RwTxn};

pub mod error;

pub trait Database {
    type KC;
    type DC;

    fn name(&self) -> &str;
}

impl<DB> Database for &DB
where
    DB: Database,
{
    type KC = DB::KC;
    type DC = DB::DC;
    fn name(&self) -> &str {
        <DB as Database>::name(*self)
    }
}

/// Wrapper for [`heed::Database`] with better errors
#[derive(Educe)]
#[educe(Clone, Debug)]
struct DbWrapper<KC, DC, C = DefaultComparator> {
    heed_db: heed::Database<KC, DC, C>,
    name: String,
    path: Arc<PathBuf>,
}

impl<KC, DC, C> DbWrapper<KC, DC, C> {
    /// Create a DB, if it does not already exist, and open it if it does.
    fn create(
        env: &Env,
        rwtxn: &mut RwTxn<'_>,
        name: String,
        flags: Option<DatabaseFlags>,
    ) -> Result<Self, env::error::CreateDb>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        let mut db_opts =
            env.database_options().name(&name).types().key_comparator();
        if let Some(flags) = flags {
            db_opts.flags(flags);
        }
        let path = env.path().clone();
        let heed_db = match db_opts.create(rwtxn) {
            Ok(heed_db) => heed_db,
            Err(err) => {
                return Err(env::error::CreateDb {
                    name,
                    path: (*path).clone(),
                    source: err,
                })
            }
        };
        Ok(Self {
            heed_db,
            name,
            path,
        })
    }

    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    fn contains_key<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::TryGet>
    where
        KC: BytesEncode<'a>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        match self.heed_db.lazily_decode_data().get(rotxn, key) {
            Ok(lazy_value) => Ok(lazy_value.is_some()),
            Err(err) => {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                Err(error::TryGet {
                    db_name: self.name.clone(),
                    db_path: (*self.path).clone(),
                    key_bytes,
                    source: err,
                })
            }
        }
    }

    fn delete<'a>(
        &self,
        rwtxn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        self.heed_db.delete(rwtxn, key).map_err(|err| {
            let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                .map(|key_bytes| key_bytes.to_vec());
            error::Delete {
                db_name: self.name.clone(),
                db_path: (*self.path).clone(),
                key_bytes,
                source: err,
            }
        })
    }

    #[allow(clippy::type_complexity)]
    fn first<'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::First>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.heed_db.first(rotxn).map_err(|err| error::First {
            db_name: self.name.clone(),
            db_path: (*self.path).clone(),
            source: err,
        })
    }

    fn get_duplicates<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a>,
        key: &'a KC::EItem,
    ) -> Result<
        impl FallibleIterator<Item = DC::DItem, Error = error::IterItem> + 'txn,
        error::IterDuplicatesInit,
    >
    where
        KC: BytesDecode<'txn> + BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        match self.heed_db.get_duplicates(rotxn, key) {
            Ok(it) => Ok(it
                .into_iter()
                .flatten()
                .map({
                    let db_path = self.path.clone();
                    let name = self.name();
                    move |item| match item {
                        Ok((_key, value)) => Ok(value),
                        Err(err) => Err(error::IterItem {
                            db_name: name.to_owned(),
                            db_path: (*db_path).clone(),
                            source: err,
                        }),
                    }
                })
                .transpose_into_fallible()),
            Err(err) => {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                Err(error::IterDuplicatesInit {
                    db_name: self.name.clone(),
                    db_path: (*self.path).clone(),
                    key_bytes,
                    source: err,
                })
            }
        }
    }

    fn iter<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a>,
    ) -> Result<
        impl FallibleIterator<
                Item = (KC::DItem, DC::DItem),
                Error = error::IterItem,
            > + 'txn,
        error::IterInit,
    >
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        match self.heed_db.iter(rotxn) {
            Ok(it) => Ok(it.transpose_into_fallible().map_err({
                let db_path = self.path.clone();
                let name = self.name();
                move |err| error::IterItem {
                    db_name: name.to_owned(),
                    db_path: (*db_path).clone(),
                    source: err,
                }
            })),
            Err(err) => Err(error::IterInit {
                db_name: self.name.clone(),
                db_path: (*self.path).clone(),
                source: err,
            }),
        }
    }

    fn iter_keys<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a>,
    ) -> Result<
        impl FallibleIterator<Item = KC::DItem, Error = error::IterItem> + 'txn,
        error::IterInit,
    >
    where
        KC: BytesDecode<'txn>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        match self.heed_db.lazily_decode_data().iter(rotxn) {
            Ok(it) => Ok(it
                .transpose_into_fallible()
                .map(|(key, _)| Ok(key))
                .map_err({
                    let db_path = self.path.clone();
                    let name = self.name();
                    move |err| error::IterItem {
                        db_name: name.to_owned(),
                        db_path: (*db_path).clone(),
                        source: err,
                    }
                })),
            Err(err) => Err(error::IterInit {
                db_name: self.name.clone(),
                db_path: (*self.path).clone(),
                source: err,
            }),
        }
    }

    fn lazy_decode(&self) -> DbWrapper<KC, LazyDecode<DC>, C> {
        let heed_db = self.heed_db.lazily_decode_data();
        DbWrapper {
            heed_db,
            name: self.name().to_owned(),
            path: self.path.clone(),
        }
    }

    fn len(&self, rotxn: &RoTxn<'_>) -> Result<u64, error::Len> {
        self.heed_db.len(rotxn).map_err(|err| error::Len {
            db_name: self.name.clone(),
            db_path: (*self.path).clone(),
            source: err,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn put_with_flags<'a>(
        &self,
        rwtxn: &mut RwTxn<'_>,
        flags: PutFlags,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.heed_db
            .put_with_flags(rwtxn, flags, key, data)
            .map_err(|err| {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                let value_bytes = <DC as BytesEncode>::bytes_encode(data)
                    .map(|value_bytes| value_bytes.to_vec());
                error::Put {
                    db_name: self.name.clone(),
                    db_path: (*self.path).clone(),
                    key_bytes,
                    value_bytes,
                    source: err,
                }
            })
    }

    pub fn try_get<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>, error::TryGet>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.heed_db.get(rotxn, key).map_err(|err| {
            let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                .map(|key_bytes| key_bytes.to_vec());
            error::TryGet {
                db_name: self.name.clone(),
                db_path: (*self.path).clone(),
                key_bytes,
                source: err,
            }
        })
    }

    pub fn get<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<DC::DItem, error::Get>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.try_get(rotxn, key)?.ok_or_else(|| {
            let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                // Safety: key must encode successfully, as try_get succeeded
                .unwrap()
                .to_vec();
            error::Get::MissingValue {
                db_name: self.name.clone(),
                db_path: (*self.path).clone(),
                key_bytes,
            }
        })
    }

    /// Attempt to insert a key-value pair in this database,
    /// or if a value already exists for the key, returns the previous value.
    /// The entry is always written with the NO_OVERWRITE flag.
    /// See [`heed::Database::get_or_put`]
    pub fn try_put<'a, 'txn>(
        &'txn self,
        rwtxn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<Option<DC::DItem>, error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a> + BytesDecode<'a>,
    {
        self.heed_db.get_or_put(rwtxn, key, data).map_err(|err| {
            let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                .map(|key_bytes| key_bytes.to_vec());
            let value_bytes = <DC as BytesEncode>::bytes_encode(data)
                .map(|value_bytes| value_bytes.to_vec());
            error::Put {
                db_name: self.name.clone(),
                db_path: (*self.path).clone(),
                key_bytes,
                value_bytes,
                source: err,
            }
        })
    }
}

/// Read-only wrapper for [`heed::Database`]
#[derive(Educe)]
#[educe(Clone, Debug)]
pub struct RoDatabaseUnique<KC, DC, C = DefaultComparator> {
    inner: DbWrapper<KC, DC, C>,
}

impl<KC, DC, C> RoDatabaseUnique<KC, DC, C> {
    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    #[inline(always)]
    pub fn contains_key<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::TryGet>
    where
        KC: BytesEncode<'a>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        self.inner.contains_key(rotxn, key)
    }

    #[allow(clippy::type_complexity)]
    #[inline(always)]
    pub fn first<'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::First>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.inner.first(rotxn)
    }

    #[inline(always)]
    pub fn iter<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a>,
    ) -> Result<
        impl FallibleIterator<
                Item = (KC::DItem, DC::DItem),
                Error = error::IterItem,
            > + 'txn,
        error::IterInit,
    >
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.inner.iter(rotxn)
    }

    pub fn iter_keys<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a>,
    ) -> Result<
        impl FallibleIterator<Item = KC::DItem, Error = error::IterItem> + 'txn,
        error::IterInit,
    >
    where
        KC: BytesDecode<'txn>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        self.inner.iter_keys(rotxn)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> RoDatabaseUnique<KC, LazyDecode<DC>, C> {
        RoDatabaseUnique {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn len(&self, rotxn: &RoTxn<'_>) -> Result<u64, error::Len> {
        self.inner.len(rotxn)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    #[inline(always)]
    pub fn try_get<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>, error::TryGet>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.inner.try_get(rotxn, key)
    }

    #[inline(always)]
    pub fn get<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<DC::DItem, error::Get>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.inner.get(rotxn, key)
    }
}

impl<KC, DC, C> Database for RoDatabaseUnique<KC, DC, C> {
    type KC = KC;
    type DC = DC;
    fn name(&self) -> &str {
        Self::name(self)
    }
}

/// Wrapper for [`heed::Database`]
#[derive(Educe)]
#[educe(Clone, Debug)]
#[repr(transparent)]
pub struct DatabaseUnique<KC, DC, C = DefaultComparator> {
    inner: RoDatabaseUnique<KC, DC, C>,
}

impl<KC, DC, C> DatabaseUnique<KC, DC, C> {
    pub fn create(
        env: &Env,
        rwtxn: &mut RwTxn<'_>,
        name: String,
    ) -> Result<Self, env::error::CreateDb>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        let db_wrapper = DbWrapper::create(env, rwtxn, name, None)?;
        Ok(Self {
            inner: RoDatabaseUnique { inner: db_wrapper },
        })
    }

    #[inline(always)]
    pub fn delete<'a>(
        &self,
        rwtxn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        self.inner.inner.delete(rwtxn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> DatabaseUnique<KC, LazyDecode<DC>, C> {
        DatabaseUnique {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn put<'a>(
        &self,
        rwtxn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.inner
            .inner
            .put_with_flags(rwtxn, PutFlags::empty(), key, data)
    }

    /// Attempt to insert a key-value pair in this database,
    /// or if a value already exists for the key, returns the previous value.
    /// The entry is always written with the NO_OVERWRITE flag.
    /// See [`heed::Database::get_or_put`]
    #[inline(always)]
    pub fn try_put<'a, 'txn>(
        &'txn self,
        rwtxn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<Option<DC::DItem>, error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a> + BytesDecode<'a>,
    {
        self.inner.inner.try_put(rwtxn, key, data)
    }
}

impl<KC, DC, C> std::ops::Deref for DatabaseUnique<KC, DC, C> {
    type Target = RoDatabaseUnique<KC, DC, C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Read-only wrapper for [`heed::Database`] with duplicate keys
#[derive(Educe)]
#[educe(Clone, Debug)]
pub struct RoDatabaseDup<KC, DC, C = DefaultComparator> {
    inner: DbWrapper<KC, DC, C>,
}

impl<KC, DC, C> RoDatabaseDup<KC, DC, C> {
    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    #[inline(always)]
    pub fn contains_key<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::TryGet>
    where
        KC: BytesEncode<'a>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        self.inner.contains_key(rotxn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> RoDatabaseDup<KC, LazyDecode<DC>, C> {
        RoDatabaseDup {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn len(&self, rotxn: &RoTxn<'_>) -> Result<u64, error::Len> {
        self.inner.len(rotxn)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    #[inline(always)]
    pub fn get<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a>,
        key: &'a KC::EItem,
    ) -> Result<
        impl FallibleIterator<Item = DC::DItem, Error = error::IterItem> + 'txn,
        error::IterDuplicatesInit,
    >
    where
        KC: BytesDecode<'txn> + BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.inner.get_duplicates(rotxn, key)
    }
}

impl<KC, DC, C> Database for RoDatabaseDup<KC, DC, C> {
    type KC = KC;
    type DC = DC;
    fn name(&self) -> &str {
        Self::name(self)
    }
}

/// Wrapper for [`heed::Database`] with duplicate keys
#[derive(Educe)]
#[educe(Clone, Debug)]
#[repr(transparent)]
pub struct DatabaseDup<KC, DC, C = DefaultComparator> {
    inner: RoDatabaseDup<KC, DC, C>,
}

impl<KC, DC, C> DatabaseDup<KC, DC, C> {
    pub fn create(
        env: &Env,
        rwtxn: &mut RwTxn<'_>,
        name: String,
    ) -> Result<Self, env::error::CreateDb>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        let flags = DatabaseFlags::DUP_SORT;
        let db_wrapper = DbWrapper::create(env, rwtxn, name, Some(flags))?;
        Ok(Self {
            inner: RoDatabaseDup { inner: db_wrapper },
        })
    }

    /// Delete each item with the specified key
    #[inline(always)]
    pub fn delete_each<'a>(
        &self,
        rwtxn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        self.inner.inner.delete(rwtxn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> DatabaseDup<KC, LazyDecode<DC>, C> {
        DatabaseDup {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn put<'a>(
        &self,
        rwtxn: &mut RwTxn<'_>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.inner
            .inner
            .put_with_flags(rwtxn, PutFlags::empty(), key, data)
    }
}

impl<KC, DC, C> std::ops::Deref for DatabaseDup<KC, DC, C> {
    type Target = RoDatabaseDup<KC, DC, C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
