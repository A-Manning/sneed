//! Database types

use std::{path::Path, sync::Arc};

use educe::Educe;
use fallible_iterator::{FallibleIterator, IteratorExt as _};
use heed::{
    types::LazyDecode, BytesDecode, BytesEncode, Comparator, DatabaseFlags,
    DefaultComparator, PutFlags,
};
#[cfg(feature = "observe")]
use tokio::sync::watch;

use crate::{env, Env, RwTxn, Txn};

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
struct DbWrapper<'env_id, KC, DC, C = DefaultComparator> {
    unique_guard: Arc<generativity::Guard<'env_id>>,
    heed_db: heed::Database<KC, DC, C>,
    name: Arc<str>,
    path: Arc<Path>,
    #[cfg(feature = "observe")]
    watch: (watch::Sender<()>, watch::Receiver<()>),
}

impl<'env_id, KC, DC, C> DbWrapper<'env_id, KC, DC, C> {
    /// Create a DB, if it does not already exist, and open it if it does.
    fn create(
        env: &Env<'env_id>,
        rwtxn: &mut RwTxn<'_, 'env_id>,
        name: &str,
        flags: Option<DatabaseFlags>,
    ) -> Result<Self, env::error::CreateDb>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        let mut db_opts =
            env.database_options().name(name).types().key_comparator();
        if let Some(flags) = flags {
            db_opts.flags(flags);
        }
        let path = env.path().clone();
        let heed_db = db_opts.create(rwtxn.write_txn()).map_err(|err| {
            env::error::CreateDb {
                name: name.to_owned(),
                path: (*path).to_owned(),
                source: err,
            }
        })?;
        Ok(Self {
            unique_guard: env.unique_guard().clone(),
            heed_db,
            name: Arc::from(name),
            path,
            #[cfg(feature = "observe")]
            watch: watch::channel(()),
        })
    }

    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    fn contains_key<'a, 'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<bool, error::TryGet>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesEncode<'a>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        match self.heed_db.lazily_decode_data().get(txn.read_txn(), key) {
            Ok(lazy_value) => Ok(lazy_value.is_some()),
            Err(err) => {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                Err(error::TryGet {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    key_bytes,
                    source: err,
                })
            }
        }
    }

    fn delete<'a, 'env, 'txn>(
        &self,
        rwtxn: &'txn mut RwTxn<'env, 'env_id>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        let res =
            self.heed_db.delete(rwtxn.write_txn(), key).map_err(|err| {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                error::Delete {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    key_bytes,
                    source: err,
                }
            })?;
        #[cfg(feature = "observe")]
        let _watch_tx: Option<watch::Sender<_>> = rwtxn
            .pending_writes
            .insert(self.name.clone(), self.watch.0.clone());
        Ok(res)
    }

    #[allow(clippy::type_complexity)]
    fn first<'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::First>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.heed_db
            .first(txn.read_txn())
            .map_err(|err| error::First {
                db_name: (*self.name).to_owned(),
                db_path: (*self.path).to_owned(),
                source: err,
            })
    }

    fn get_duplicates<'a, 'env, 'txn, Tx>(
        &'a self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<
        impl FallibleIterator<Item = DC::DItem, Error = error::IterItem> + 'txn,
        error::IterDuplicatesInit,
    >
    where
        'a: 'txn,
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn> + BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        match self.heed_db.get_duplicates(txn.read_txn(), key) {
            Ok(it) => Ok(it
                .into_iter()
                .flatten()
                .map({
                    let db_path = &*self.path;
                    let name = self.name();
                    |item| match item {
                        Ok((_key, value)) => Ok(value),
                        Err(err) => Err(error::IterItem {
                            db_name: name.to_owned(),
                            db_path: db_path.to_owned(),
                            source: err,
                        }),
                    }
                })
                .transpose_into_fallible()),
            Err(err) => {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                Err(error::IterDuplicatesInit {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    key_bytes,
                    source: err,
                })
            }
        }
    }

    fn iter<'a, 'env, 'txn, Tx>(
        &'a self,
        txn: &'txn Tx,
    ) -> Result<
        impl FallibleIterator<
                Item = (KC::DItem, DC::DItem),
                Error = error::IterItem,
            > + 'txn,
        error::IterInit,
    >
    where
        'a: 'txn,
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        match self.heed_db.iter(txn.read_txn()) {
            Ok(it) => Ok(it.transpose_into_fallible().map_err({
                let db_path = &*self.path;
                let name = self.name();
                |err| error::IterItem {
                    db_name: name.to_owned(),
                    db_path: db_path.to_owned(),
                    source: err,
                }
            })),
            Err(err) => Err(error::IterInit {
                db_name: (*self.name).to_owned(),
                db_path: (*self.path).to_owned(),
                source: err,
            }),
        }
    }

    fn iter_keys<'a, 'env, 'txn, Tx>(
        &'a self,
        txn: &'txn Tx,
    ) -> Result<
        impl FallibleIterator<Item = KC::DItem, Error = error::IterItem> + 'txn,
        error::IterInit,
    >
    where
        'a: 'txn,
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        match self.heed_db.lazily_decode_data().iter(txn.read_txn()) {
            Ok(it) => Ok(it
                .transpose_into_fallible()
                .map(|(key, _)| Ok(key))
                .map_err({
                    let db_path = &*self.path;
                    let name = self.name();
                    |err| error::IterItem {
                        db_name: name.to_owned(),
                        db_path: db_path.to_owned(),
                        source: err,
                    }
                })),
            Err(err) => Err(error::IterInit {
                db_name: (*self.name).to_owned(),
                db_path: (*self.path).to_owned(),
                source: err,
            }),
        }
    }

    fn lazy_decode(&self) -> DbWrapper<'env_id, KC, LazyDecode<DC>, C> {
        let heed_db = self.heed_db.lazily_decode_data();
        DbWrapper {
            unique_guard: self.unique_guard.clone(),
            heed_db,
            name: self.name.clone(),
            path: self.path.clone(),
            #[cfg(feature = "observe")]
            watch: self.watch.clone(),
        }
    }

    fn len<'env, 'txn, Tx>(&self, txn: &'txn Tx) -> Result<u64, error::Len>
    where
        Tx: Txn<'env, 'env_id>,
    {
        self.heed_db.len(txn.read_txn()).map_err(|err| error::Len {
            db_name: (*self.name).to_owned(),
            db_path: (*self.path).to_owned(),
            source: err,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn put_with_flags<'a, 'env, 'txn>(
        &self,
        rwtxn: &'txn mut RwTxn<'env, 'env_id>,
        flags: PutFlags,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<(), error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let () = self
            .heed_db
            .put_with_flags(rwtxn.write_txn(), flags, key, data)
            .map_err(|err| {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                let value_bytes = <DC as BytesEncode>::bytes_encode(data)
                    .map(|value_bytes| value_bytes.to_vec());
                error::Put {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    key_bytes,
                    value_bytes,
                    source: err,
                }
            })?;
        #[cfg(feature = "observe")]
        let _watch_tx: Option<watch::Sender<_>> = rwtxn
            .pending_writes
            .insert(self.name.clone(), self.watch.0.clone());
        Ok(())
    }

    pub fn try_get<'a, 'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>, error::TryGet>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.heed_db.get(txn.read_txn(), key).map_err(|err| {
            let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                .map(|key_bytes| key_bytes.to_vec());
            error::TryGet {
                db_name: (*self.name).to_owned(),
                db_path: (*self.path).to_owned(),
                key_bytes,
                source: err,
            }
        })
    }

    pub fn get<'a, 'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<DC::DItem, error::Get>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.try_get(txn, key)?.ok_or_else(|| {
            let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                // Safety: key must encode successfully, as try_get succeeded
                .unwrap()
                .to_vec();
            error::Get::MissingValue {
                db_name: (*self.name).to_owned(),
                db_path: (*self.path).to_owned(),
                key_bytes,
            }
        })
    }

    /// Attempt to insert a key-value pair in this database,
    /// or if a value already exists for the key, returns the previous value.
    /// The entry is always written with the NO_OVERWRITE flag.
    /// See [`heed::Database::get_or_put`]
    pub fn try_put<'a, 'env, 'txn>(
        &'txn self,
        rwtxn: &mut RwTxn<'_, 'env_id>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<Option<DC::DItem>, error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a> + BytesDecode<'a>,
    {
        let res = self
            .heed_db
            .get_or_put(rwtxn.write_txn(), key, data)
            .map_err(|err| {
                let key_bytes = <KC as BytesEncode>::bytes_encode(key)
                    .map(|key_bytes| key_bytes.to_vec());
                let value_bytes = <DC as BytesEncode>::bytes_encode(data)
                    .map(|value_bytes| value_bytes.to_vec());
                error::Put {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    key_bytes,
                    value_bytes,
                    source: err,
                }
            })?;
        #[cfg(feature = "observe")]
        let _watch_tx: Option<watch::Sender<_>> = rwtxn
            .pending_writes
            .insert(self.name.clone(), self.watch.0.clone());
        Ok(res)
    }

    #[cfg(feature = "observe")]
    #[cfg_attr(docsrs, doc(cfg(feature = "observe")))]
    /// Receive notifications when the DB is updated
    pub fn watch(&self) -> &watch::Receiver<()> {
        let (_, rx) = &self.watch;
        rx
    }
}

/// Read-only wrapper for [`heed::Database`]
#[derive(Educe)]
#[educe(Clone, Debug)]
pub struct RoDatabaseUnique<'env_id, KC, DC, C = DefaultComparator> {
    inner: DbWrapper<'env_id, KC, DC, C>,
}

impl<'env_id, KC, DC, C> RoDatabaseUnique<'env_id, KC, DC, C> {
    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    #[inline(always)]
    pub fn contains_key<'a, 'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<bool, error::TryGet>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesEncode<'a>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        self.inner.contains_key(txn, key)
    }

    #[allow(clippy::type_complexity)]
    #[inline(always)]
    pub fn first<'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::First>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.inner.first(txn)
    }

    #[inline(always)]
    pub fn iter<'a, 'env, 'txn, Tx>(
        &'a self,
        txn: &'txn Tx,
    ) -> Result<
        impl FallibleIterator<
                Item = (KC::DItem, DC::DItem),
                Error = error::IterItem,
            > + 'txn,
        error::IterInit,
    >
    where
        'a: 'txn,
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.inner.iter(txn)
    }

    pub fn iter_keys<'a, 'env, 'txn, Tx>(
        &'a self,
        txn: &'txn Tx,
    ) -> Result<
        impl FallibleIterator<Item = KC::DItem, Error = error::IterItem> + 'txn,
        error::IterInit,
    >
    where
        'a: 'txn,
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        self.inner.iter_keys(txn)
    }

    #[inline(always)]
    pub fn lazy_decode(
        &self,
    ) -> RoDatabaseUnique<'env_id, KC, LazyDecode<DC>, C> {
        RoDatabaseUnique {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn len<'env, 'txn, Tx>(&self, txn: &'txn Tx) -> Result<u64, error::Len>
    where
        Tx: Txn<'env, 'env_id>,
    {
        self.inner.len(txn)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    #[inline(always)]
    pub fn try_get<'a, 'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<Option<DC::DItem>, error::TryGet>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.inner.try_get(txn, key)
    }

    #[inline(always)]
    pub fn get<'a, 'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<DC::DItem, error::Get>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.inner.get(txn, key)
    }

    #[cfg(feature = "observe")]
    #[cfg_attr(docsrs, doc(cfg(feature = "observe")))]
    /// Receive notifications when the DB is updated
    #[inline(always)]
    pub fn watch(&self) -> &watch::Receiver<()> {
        self.inner.watch()
    }
}

impl<KC, DC, C> Database for RoDatabaseUnique<'_, KC, DC, C> {
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
pub struct DatabaseUnique<'env_id, KC, DC, C = DefaultComparator> {
    inner: RoDatabaseUnique<'env_id, KC, DC, C>,
}

impl<'env_id, KC, DC, C> DatabaseUnique<'env_id, KC, DC, C> {
    pub fn create(
        env: &Env<'env_id>,
        rwtxn: &mut RwTxn<'_, 'env_id>,
        name: &str,
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
    pub fn delete<'a, 'env>(
        &self,
        rwtxn: &mut RwTxn<'env, 'env_id>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        self.inner.inner.delete(rwtxn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(
        &self,
    ) -> DatabaseUnique<'env_id, KC, LazyDecode<DC>, C> {
        DatabaseUnique {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn put<'a, 'env>(
        &self,
        rwtxn: &mut RwTxn<'env, 'env_id>,
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
    pub fn try_put<'a, 'env, 'txn>(
        &'txn self,
        rwtxn: &mut RwTxn<'env, 'env_id>,
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

impl<'env_id, KC, DC, C> std::ops::Deref
    for DatabaseUnique<'env_id, KC, DC, C>
{
    type Target = RoDatabaseUnique<'env_id, KC, DC, C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Read-only wrapper for [`heed::Database`] with duplicate keys
#[derive(Educe)]
#[educe(Clone, Debug)]
pub struct RoDatabaseDup<'id, KC, DC, C = DefaultComparator> {
    inner: DbWrapper<'id, KC, DC, C>,
}

impl<'env_id, KC, DC, C> RoDatabaseDup<'env_id, KC, DC, C> {
    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    #[inline(always)]
    pub fn contains_key<'a, 'env, 'txn, Tx>(
        &self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<bool, error::TryGet>
    where
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesEncode<'a>,
        LazyDecode<DC>: BytesDecode<'txn>,
    {
        self.inner.contains_key(txn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> RoDatabaseDup<'env_id, KC, LazyDecode<DC>, C> {
        RoDatabaseDup {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn len<'env, 'txn, Tx>(&self, txn: &'txn Tx) -> Result<u64, error::Len>
    where
        Tx: Txn<'env, 'env_id>,
    {
        self.inner.len(txn)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    #[inline(always)]
    pub fn get<'a, 'env, 'txn, Tx>(
        &'a self,
        txn: &'txn Tx,
        key: &'a KC::EItem,
    ) -> Result<
        impl FallibleIterator<Item = DC::DItem, Error = error::IterItem> + 'txn,
        error::IterDuplicatesInit,
    >
    where
        'a: 'txn,
        'env: 'txn,
        Tx: Txn<'env, 'env_id>,
        KC: BytesDecode<'txn> + BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.inner.get_duplicates(txn, key)
    }

    #[cfg(feature = "observe")]
    #[cfg_attr(docsrs, doc(cfg(feature = "observe")))]
    /// Receive notifications when the DB is updated
    #[inline(always)]
    pub fn watch(&self) -> &watch::Receiver<()> {
        self.inner.watch()
    }
}

impl<KC, DC, C> Database for RoDatabaseDup<'_, KC, DC, C> {
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
pub struct DatabaseDup<'env_id, KC, DC, C = DefaultComparator> {
    inner: RoDatabaseDup<'env_id, KC, DC, C>,
}

impl<'env_id, KC, DC, C> DatabaseDup<'env_id, KC, DC, C> {
    pub fn create(
        env: &Env<'env_id>,
        rwtxn: &mut RwTxn<'_, 'env_id>,
        name: &str,
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
    pub fn delete_each<'a, 'env, 'txn>(
        &self,
        rwtxn: &'txn mut RwTxn<'env, 'env_id>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        self.inner.inner.delete(rwtxn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> DatabaseDup<'env_id, KC, LazyDecode<DC>, C> {
        DatabaseDup {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn put<'a, 'env, 'txn>(
        &self,
        rwtxn: &'txn mut RwTxn<'env, 'env_id>,
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

impl<'env_id, KC, DC, C> std::ops::Deref for DatabaseDup<'env_id, KC, DC, C> {
    type Target = RoDatabaseDup<'env_id, KC, DC, C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
