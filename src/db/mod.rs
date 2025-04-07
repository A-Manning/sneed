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

use crate::{env, Env, RoTxn, RwTxn};

pub mod error;
pub use error::Error;

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

/// Wrapper for [`heed::Database`] with better errors.
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(Educe)]
#[educe(Clone(bound()), Debug(bound()))]
struct DbWrapper<KC, DC, Tag, C = DefaultComparator> {
    heed_db: heed::Database<KC, DC, C>,
    name: Arc<str>,
    path: Arc<Path>,
    tag: std::marker::PhantomData<Tag>,
    #[cfg(feature = "observe")]
    watch: (watch::Sender<()>, watch::Receiver<()>),
}

impl<KC, DC, Tag, C> DbWrapper<KC, DC, Tag, C> {
    /// Deletes all key/value pairs in this database.
    fn clear(&self, rwtxn: &mut RwTxn<'_, Tag>) -> Result<(), error::Clear> {
        let () =
            self.heed_db
                .clear(rwtxn.as_mut())
                .map_err(|err| error::Clear {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    source: err,
                })?;
        #[cfg(feature = "observe")]
        let _watch_tx: Option<watch::Sender<_>> = rwtxn
            .pending_writes
            .insert(self.name.clone(), self.watch.0.clone());
        Ok(())
    }

    /// Create a DB, if it does not already exist, and open it if it does.
    fn create(
        env: &Env<Tag>,
        rwtxn: &mut RwTxn<'_, Tag>,
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
        let heed_db = db_opts.create(rwtxn.as_mut()).map_err(|err| {
            env::error::CreateDb {
                name: name.to_owned(),
                path: (*path).to_owned(),
                source: err,
            }
        })?;
        Ok(Self {
            heed_db,
            name: Arc::from(name),
            path,
            tag: env.tag,
            #[cfg(feature = "observe")]
            watch: watch::channel(()),
        })
    }

    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    fn contains_key<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
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
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    key_bytes,
                    source: err,
                })
            }
        }
    }

    fn delete<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        let res = self.heed_db.delete(rwtxn.as_mut(), key).map_err(|err| {
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

    fn delete_one_duplicate<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        let res = self
            .heed_db
            .delete_one_duplicate(rwtxn.as_mut(), key, data)
            .map_err(|err| {
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
    fn first<'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::First>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.heed_db.first(rotxn).map_err(|err| error::First {
            db_name: (*self.name).to_owned(),
            db_path: (*self.path).to_owned(),
            source: err,
        })
    }

    fn get_duplicates<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
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

    fn iter<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
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

    fn iter_keys<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
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

    #[allow(clippy::type_complexity)]
    fn last<'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::Last>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.heed_db.last(rotxn).map_err(|err| error::Last {
            db_name: (*self.name).to_owned(),
            db_path: (*self.path).to_owned(),
            source: err,
        })
    }

    fn lazy_decode(&self) -> DbWrapper<KC, LazyDecode<DC>, Tag, C> {
        let heed_db = self.heed_db.lazily_decode_data();
        DbWrapper {
            heed_db,
            name: self.name.clone(),
            path: self.path.clone(),
            tag: self.tag,
            #[cfg(feature = "observe")]
            watch: self.watch.clone(),
        }
    }

    fn len(&self, rotxn: &RoTxn<'_, Tag>) -> Result<u64, error::Len> {
        self.heed_db.len(rotxn).map_err(|err| error::Len {
            db_name: (*self.name).to_owned(),
            db_path: (*self.path).to_owned(),
            source: err,
        })
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// Open a DB that already exists.
    fn open(
        env: &Env<Tag>,
        rotxn: &RoTxn<'_, Tag>,
        name: &str,
        flags: Option<DatabaseFlags>,
    ) -> Result<Option<Self>, env::error::OpenDb>
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
        let Some(heed_db) =
            db_opts.open(rotxn).map_err(|err| env::error::OpenDb {
                name: name.to_owned(),
                path: (*path).to_owned(),
                source: err,
            })?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            heed_db,
            name: Arc::from(name),
            path,
            tag: env.tag,
            #[cfg(feature = "observe")]
            watch: watch::channel(()),
        }))
    }

    fn put_with_flags<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
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
            .put_with_flags(rwtxn.as_mut(), flags, key, data)
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

    fn rev_iter<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
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
        match self.heed_db.rev_iter(rotxn) {
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

    pub fn try_get<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
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
                db_name: (*self.name).to_owned(),
                db_path: (*self.path).to_owned(),
                key_bytes,
                source: err,
            }
        })
    }

    pub fn get<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
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
    pub fn try_put<'a, 'txn>(
        &'txn self,
        rwtxn: &mut RwTxn<'_, Tag>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<Option<DC::DItem>, error::Put>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a> + BytesDecode<'a>,
    {
        let res = self.heed_db.get_or_put(rwtxn.as_mut(), key, data).map_err(
            |err| {
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
            },
        )?;
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

/// Read-only wrapper for [`heed::Database`].
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(Educe)]
#[educe(Clone(bound()), Debug(bound()))]
pub struct RoDatabaseUnique<KC, DC, Tag = (), C = DefaultComparator> {
    inner: DbWrapper<KC, DC, Tag, C>,
}

impl<KC, DC, Tag, C> RoDatabaseUnique<KC, DC, Tag, C> {
    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    #[inline(always)]
    pub fn contains_key<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
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
        rotxn: &'txn RoTxn<'_, Tag>,
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
        rotxn: &'txn RoTxn<'a, Tag>,
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
        rotxn: &'txn RoTxn<'a, Tag>,
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

    #[allow(clippy::type_complexity)]
    #[inline(always)]
    pub fn last<'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::Last>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.inner.last(rotxn)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> RoDatabaseUnique<KC, LazyDecode<DC>, Tag, C> {
        RoDatabaseUnique {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn len(&self, rotxn: &RoTxn<'_, Tag>) -> Result<u64, error::Len> {
        self.inner.len(rotxn)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    #[inline(always)]
    pub fn rev_iter<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
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
        self.inner.rev_iter(rotxn)
    }

    #[inline(always)]
    pub fn try_get<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
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
        rotxn: &'txn RoTxn<'_, Tag>,
        key: &'a KC::EItem,
    ) -> Result<DC::DItem, error::Get>
    where
        KC: BytesEncode<'a>,
        DC: BytesDecode<'txn>,
    {
        self.inner.get(rotxn, key)
    }

    #[cfg(feature = "observe")]
    #[cfg_attr(docsrs, doc(cfg(feature = "observe")))]
    /// Receive notifications when the DB is updated
    #[inline(always)]
    pub fn watch(&self) -> &watch::Receiver<()> {
        self.inner.watch()
    }
}

impl<KC, DC, Tag, C> Database for RoDatabaseUnique<KC, DC, Tag, C> {
    type KC = KC;
    type DC = DC;
    fn name(&self) -> &str {
        Self::name(self)
    }
}

/// Wrapper for [`heed::Database`].
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(Educe)]
#[educe(Clone(bound()), Debug(bound()))]
#[repr(transparent)]
pub struct DatabaseUnique<KC, DC, Tag = (), C = DefaultComparator> {
    inner: RoDatabaseUnique<KC, DC, Tag, C>,
}

impl<KC, DC, Tag, C> DatabaseUnique<KC, DC, Tag, C> {
    #[inline(always)]
    pub fn clear(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
    ) -> Result<(), error::Clear> {
        self.inner.inner.clear(rwtxn)
    }

    pub fn create(
        env: &Env<Tag>,
        rwtxn: &mut RwTxn<'_, Tag>,
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
    pub fn delete<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        self.inner.inner.delete(rwtxn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> DatabaseUnique<KC, LazyDecode<DC>, Tag, C> {
        DatabaseUnique {
            inner: self.inner.lazy_decode(),
        }
    }

    pub fn open(
        env: &Env<Tag>,
        rotxn: &RoTxn<'_, Tag>,
        name: &str,
    ) -> Result<Option<Self>, env::error::OpenDb>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        let Some(db_wrapper) = DbWrapper::open(env, rotxn, name, None)? else {
            return Ok(None);
        };
        Ok(Some(Self {
            inner: RoDatabaseUnique { inner: db_wrapper },
        }))
    }

    #[inline(always)]
    pub fn put<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
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
        rwtxn: &mut RwTxn<'_, Tag>,
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

impl<KC, DC, Tag, C> std::ops::Deref for DatabaseUnique<KC, DC, Tag, C> {
    type Target = RoDatabaseUnique<KC, DC, Tag, C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Read-only wrapper for [`heed::Database`] with duplicate keys.
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(Educe)]
#[educe(Clone, Debug)]
pub struct RoDatabaseDup<KC, DC, Tag = (), C = DefaultComparator> {
    inner: DbWrapper<KC, DC, Tag, C>,
}

impl<KC, DC, Tag, C> RoDatabaseDup<KC, DC, Tag, C> {
    /// Check if the provided key exists in the db.
    /// The stored value is not decoded, if it exists.
    #[inline(always)]
    pub fn contains_key<'a, 'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
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
        rotxn: &'txn RoTxn<'_, Tag>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::First>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.inner.first(rotxn)
    }

    #[allow(clippy::type_complexity)]
    #[inline(always)]
    pub fn last<'txn>(
        &self,
        rotxn: &'txn RoTxn<'_, Tag>,
    ) -> Result<Option<(KC::DItem, DC::DItem)>, error::Last>
    where
        KC: BytesDecode<'txn>,
        DC: BytesDecode<'txn>,
    {
        self.inner.last(rotxn)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> RoDatabaseDup<KC, LazyDecode<DC>, Tag, C> {
        RoDatabaseDup {
            inner: self.inner.lazy_decode(),
        }
    }

    #[inline(always)]
    pub fn len(&self, rotxn: &RoTxn<'_, Tag>) -> Result<u64, error::Len> {
        self.inner.len(rotxn)
    }

    #[inline(always)]
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    #[inline(always)]
    pub fn get<'a, 'txn>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
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

    #[cfg(feature = "observe")]
    #[cfg_attr(docsrs, doc(cfg(feature = "observe")))]
    /// Receive notifications when the DB is updated
    #[inline(always)]
    pub fn watch(&self) -> &watch::Receiver<()> {
        self.inner.watch()
    }
}

impl<KC, DC, Tag, C> Database for RoDatabaseDup<KC, DC, Tag, C> {
    type KC = KC;
    type DC = DC;
    fn name(&self) -> &str {
        Self::name(self)
    }
}

/// Wrapper for [`heed::Database`] with duplicate keys.
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(Educe)]
#[educe(Clone, Debug)]
#[repr(transparent)]
pub struct DatabaseDup<KC, DC, Tag = (), C = DefaultComparator> {
    inner: RoDatabaseDup<KC, DC, Tag, C>,
}

impl<KC, DC, Tag, C> DatabaseDup<KC, DC, Tag, C> {
    pub fn create(
        env: &Env<Tag>,
        rwtxn: &mut RwTxn<'_, Tag>,
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

    /// Delete a single key-value pair.
    #[inline(always)]
    pub fn delete_one<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
        key: &'a KC::EItem,
        data: &'a DC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
        DC: BytesEncode<'a>,
    {
        self.inner.inner.delete_one_duplicate(rwtxn, key, data)
    }

    /// Delete each item with the specified key
    #[inline(always)]
    pub fn delete_each<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
        key: &'a KC::EItem,
    ) -> Result<bool, error::Delete>
    where
        KC: BytesEncode<'a>,
    {
        self.inner.inner.delete(rwtxn, key)
    }

    #[inline(always)]
    pub fn lazy_decode(&self) -> DatabaseDup<KC, LazyDecode<DC>, Tag, C> {
        DatabaseDup {
            inner: self.inner.lazy_decode(),
        }
    }

    pub fn open(
        env: &Env<Tag>,
        rotxn: &RoTxn<'_, Tag>,
        name: &str,
    ) -> Result<Option<Self>, env::error::OpenDb>
    where
        KC: 'static,
        DC: 'static,
        C: Comparator + 'static,
    {
        let flags = DatabaseFlags::DUP_SORT;
        let Some(db_wrapper) = DbWrapper::open(env, rotxn, name, Some(flags))?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            inner: RoDatabaseDup { inner: db_wrapper },
        }))
    }

    #[inline(always)]
    pub fn put<'a>(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
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

impl<KC, DC, Tag, C> std::ops::Deref for DatabaseDup<KC, DC, Tag, C> {
    type Target = RoDatabaseDup<KC, DC, Tag, C>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
