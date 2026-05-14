pub mod error {
    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Error commiting read txn")]
    pub struct Commit {
        pub(crate) source: heed::Error,
    }

    /// General error type for RoTxn operations
    #[derive(Debug, Error)]
    pub enum Error {
        #[error(transparent)]
        Commit(#[from] Commit),
    }
}
pub use error::Error;

/// Wrapper for heed's `RoTxn`.
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[repr(transparent)]
pub struct RoTxn<'a, T = heed::AnyTls, Tag = ()> {
    pub(crate) inner: heed::RoTxn<'a, T>,
    pub(crate) _tag: std::marker::PhantomData<Tag>,
}

impl<T, Tag> RoTxn<'_, T, Tag> {
    pub fn commit(self) -> Result<(), error::Commit> {
        self.inner
            .commit()
            .map_err(|err| error::Commit { source: err })
    }
}

impl<'a, Tag> std::ops::Deref for RoTxn<'a, heed::WithTls, Tag> {
    type Target = RoTxn<'a, heed::AnyTls, Tag>;
    fn deref(&self) -> &Self::Target {
        // This is safe because heed::RoTxn also derefs via transmute
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a, Tag> std::ops::Deref for RoTxn<'a, heed::WithoutTls, Tag> {
    type Target = RoTxn<'a, heed::AnyTls, Tag>;
    fn deref(&self) -> &Self::Target {
        // This is safe because heed::RoTxn also derefs via transmute
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a, T, Tag> std::convert::AsRef<heed::RoTxn<'a, T>> for RoTxn<'a, T, Tag> {
    fn as_ref(&self) -> &heed::RoTxn<'a, T> {
        &self.inner
    }
}
