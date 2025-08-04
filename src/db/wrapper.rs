use std::{path::Path, sync::Arc};

use educe::Educe;
use fallible_iterator::{FallibleIterator, IteratorExt as _};
use heed::{
    types::LazyDecode, BytesDecode, BytesEncode, Comparator, DatabaseFlags,
    DefaultComparator, PutFlags,
};
#[cfg(feature = "observe")]
use tokio::sync::watch;

use crate::{db::error, env, Env, RoTxn, RwTxn};

/// Wrapper for [`heed::Database`] with better errors.
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(Educe)]
#[educe(Clone(bound()), Debug(bound()))]
pub(crate) struct DbWrapper<KC, DC, Tag, C = DefaultComparator> {
    heed_db: heed::Database<KC, DC, C>,
    pub name: Arc<str>,
    path: Arc<Path>,
    tag: std::marker::PhantomData<Tag>,
    #[cfg(feature = "observe")]
    watch: (watch::Sender<()>, watch::Receiver<()>),
}

impl<KC, DC, Tag, C> DbWrapper<KC, DC, Tag, C> {
    /// Deletes all key/value pairs in this database.
    pub fn clear(
        &self,
        rwtxn: &mut RwTxn<'_, Tag>,
    ) -> Result<(), error::Clear> {
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
    pub fn create(
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
    pub fn contains_key<'a, 'txn>(
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

    pub fn delete<'a>(
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

    pub fn delete_one_duplicate<'a>(
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
    pub fn first<'txn>(
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

    pub fn get_duplicates<'a, 'txn>(
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

    /// Iterate through duplicate values
    pub fn iter_through_duplicate_values<'a, 'txn>(
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
            Ok(it) => Ok(it
                .move_through_duplicate_values()
                .transpose_into_fallible()
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

    /// Iterate through keys, skipping duplicate values
    pub fn iter_through_keys<'a, 'txn>(
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
            Ok(it) => {
                Ok(it.move_between_keys().transpose_into_fallible().map_err({
                    let db_path = &*self.path;
                    let name = self.name();
                    |err| error::IterItem {
                        db_name: name.to_owned(),
                        db_path: db_path.to_owned(),
                        source: err,
                    }
                }))
            }
            Err(err) => Err(error::IterInit {
                db_name: (*self.name).to_owned(),
                db_path: (*self.path).to_owned(),
                source: err,
            }),
        }
    }

    /// Iterate over keys, moving through duplicate values
    pub fn iter_keys_duplicate<'a, 'txn>(
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
                .move_through_duplicate_values()
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

    /// Iterate over unique keys, ignoring duplicate values
    pub fn iter_keys_unique<'a, 'txn>(
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
                .move_between_keys()
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
    pub fn last<'txn>(
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

    pub fn lazy_decode(&self) -> DbWrapper<KC, LazyDecode<DC>, Tag, C> {
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

    pub fn len(&self, rotxn: &RoTxn<'_, Tag>) -> Result<u64, error::Len> {
        self.heed_db.len(rotxn).map_err(|err| error::Len {
            db_name: (*self.name).to_owned(),
            db_path: (*self.path).to_owned(),
            source: err,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Open a DB that already exists.
    pub fn open(
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

    pub fn put_with_flags<'a>(
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

    /// Iterate over values in a range, through duplicate values
    pub fn range_through_duplicate_values<'a, 'range, 'txn, R>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
        range: &'range R,
    ) -> Result<
        impl FallibleIterator<
                Item = (KC::DItem, DC::DItem),
                Error = error::IterItem,
            > + 'txn,
        error::RangeInit,
    >
    where
        KC: BytesDecode<'txn> + BytesEncode<'range>,
        DC: BytesDecode<'txn>,
        R: std::ops::RangeBounds<KC::EItem>,
        C: Comparator,
    {
        match self.heed_db.range(rotxn, range) {
            Ok(it) => Ok(it
                .move_through_duplicate_values()
                .transpose_into_fallible()
                .map_err({
                    let db_path = &*self.path;
                    let name = self.name();
                    |err| error::IterItem {
                        db_name: name.to_owned(),
                        db_path: db_path.to_owned(),
                        source: err,
                    }
                })),
            Err(err) => {
                use std::ops::Bound;
                let range_bound_bytes = |bound| match bound {
                    Bound::Excluded(key) => <KC as BytesEncode>::bytes_encode(
                        key,
                    )
                    .map(|key_bytes| Bound::Excluded(key_bytes.to_vec())),
                    Bound::Included(key) => <KC as BytesEncode>::bytes_encode(
                        key,
                    )
                    .map(|key_bytes| Bound::Included(key_bytes.to_vec())),
                    Bound::Unbounded => Ok(Bound::Unbounded),
                };
                Err(error::RangeInit {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    range_start_bytes: range_bound_bytes(range.start_bound()),
                    range_end_bytes: range_bound_bytes(range.end_bound()),
                    source: Box::new(err),
                })
            }
        }
    }

    /// Iterate over values in a range, skipping duplicate keys
    pub fn range_through_keys<'a, 'range, 'txn, R>(
        &'a self,
        rotxn: &'txn RoTxn<'a, Tag>,
        range: &'range R,
    ) -> Result<
        impl FallibleIterator<
                Item = (KC::DItem, DC::DItem),
                Error = error::IterItem,
            > + 'txn,
        error::RangeInit,
    >
    where
        KC: BytesDecode<'txn> + BytesEncode<'range>,
        DC: BytesDecode<'txn>,
        R: std::ops::RangeBounds<KC::EItem>,
        C: Comparator,
    {
        match self.heed_db.range(rotxn, range) {
            Ok(it) => {
                Ok(it.move_between_keys().transpose_into_fallible().map_err({
                    let db_path = &*self.path;
                    let name = self.name();
                    |err| error::IterItem {
                        db_name: name.to_owned(),
                        db_path: db_path.to_owned(),
                        source: err,
                    }
                }))
            }
            Err(err) => {
                use std::ops::Bound;
                let range_bound_bytes = |bound| match bound {
                    Bound::Excluded(key) => <KC as BytesEncode>::bytes_encode(
                        key,
                    )
                    .map(|key_bytes| Bound::Excluded(key_bytes.to_vec())),
                    Bound::Included(key) => <KC as BytesEncode>::bytes_encode(
                        key,
                    )
                    .map(|key_bytes| Bound::Included(key_bytes.to_vec())),
                    Bound::Unbounded => Ok(Bound::Unbounded),
                };
                Err(error::RangeInit {
                    db_name: (*self.name).to_owned(),
                    db_path: (*self.path).to_owned(),
                    range_start_bytes: range_bound_bytes(range.start_bound()),
                    range_end_bytes: range_bound_bytes(range.end_bound()),
                    source: Box::new(err),
                })
            }
        }
    }

    /// Iterate through duplicate values
    pub fn rev_iter_through_duplicate_values<'a, 'txn>(
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
            Ok(it) => Ok(it
                .move_through_duplicate_values()
                .transpose_into_fallible()
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

    /// Iterate through keys, skipping duplicate values
    pub fn rev_iter_through_keys<'a, 'txn>(
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
            Ok(it) => {
                Ok(it.move_between_keys().transpose_into_fallible().map_err({
                    let db_path = &*self.path;
                    let name = self.name();
                    |err| error::IterItem {
                        db_name: name.to_owned(),
                        db_path: db_path.to_owned(),
                        source: err,
                    }
                }))
            }
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
