//! Database types

use educe::Educe;
use fallible_iterator::FallibleIterator;
use heed::{
    types::LazyDecode, BytesDecode, BytesEncode, Comparator, DatabaseFlags,
    DefaultComparator, PutFlags,
};
#[cfg(feature = "observe")]
use tokio::sync::watch;

use crate::{env, Env, RoTxn, RwTxn};

pub use heed::DatabaseOpenOptions as OpenOptions;

pub mod error;
pub use error::Error;
mod wrapper;

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

/// Read-only wrapper for [`heed::Database`].
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(Educe)]
#[educe(Clone(bound()), Debug(bound()))]
pub struct RoDatabaseUnique<KC, DC, Tag = (), C = DefaultComparator> {
    inner: wrapper::DbWrapper<KC, DC, Tag, C>,
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
        self.inner.iter_through_keys(rotxn)
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
        self.inner.iter_keys_unique(rotxn)
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
    pub fn range<'a, 'range, 'txn, R>(
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
        self.inner.range_through_keys(rotxn, range)
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
        self.inner.rev_iter_through_keys(rotxn)
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
        let db_wrapper = wrapper::DbWrapper::create(env, rwtxn, name, None)?;
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
        let Some(db_wrapper) =
            wrapper::DbWrapper::open(env, rotxn, name, None)?
        else {
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
    inner: wrapper::DbWrapper<KC, DC, Tag, C>,
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

    /// Iterate through duplicate values
    #[inline(always)]
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
        self.inner.iter_through_duplicate_values(rotxn)
    }

    /// Iterate through keys, skipping duplicate values
    #[inline(always)]
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
        self.inner.iter_through_keys(rotxn)
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
        self.inner.iter_keys_duplicate(rotxn)
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
        self.inner.iter_keys_unique(rotxn)
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

    /// Iterate over values in a range, through duplicate values
    #[inline(always)]
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
        self.inner.range_through_duplicate_values(rotxn, range)
    }

    /// Iterate over values in a range, skipping duplicate keys
    #[inline(always)]
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
        self.inner.range_through_keys(rotxn, range)
    }

    /// Iterate through duplicate values
    #[inline(always)]
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
        self.inner.rev_iter_through_duplicate_values(rotxn)
    }

    /// Iterate through keys, skipping duplicate values
    #[inline(always)]
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
        self.inner.rev_iter_through_keys(rotxn)
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
        let db_wrapper =
            wrapper::DbWrapper::create(env, rwtxn, name, Some(flags))?;
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
        let Some(db_wrapper) =
            wrapper::DbWrapper::open(env, rotxn, name, Some(flags))?
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
