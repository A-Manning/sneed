use std::path::Path;
#[cfg(feature = "observe")]
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "observe")]
use tokio::sync::watch;

pub mod error {
    use std::path::PathBuf;

    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Error commiting write txn for database dir `{db_dir}`")]
    pub struct Commit {
        pub(crate) db_dir: PathBuf,
        pub(crate) source: heed::Error,
    }

    /// General error type for RwTxn operations
    #[derive(Debug, Error)]
    pub enum Error {
        #[error(transparent)]
        Commit(#[from] Commit),
    }
}
pub use error::Error;

#[cfg(feature = "observe")]
type PendingWrites = HashMap<Arc<str>, watch::Sender<()>>;

/// Wrapper for heed's `RwTxn`.
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
pub struct RwTxn<'a, Tag = ()> {
    pub(crate) inner: heed::RwTxn<'a>,
    pub(crate) db_dir: &'a Path,
    pub(crate) _tag: std::marker::PhantomData<Tag>,
    #[cfg(feature = "observe")]
    pub(crate) parent_pending_writes: Option<&'a mut PendingWrites>,
    #[cfg(feature = "observe")]
    pub(crate) pending_writes: PendingWrites,
}

impl<Tag> RwTxn<'_, Tag> {
    pub fn abort(self) {
        self.inner.abort()
    }

    pub fn commit(self) -> Result<(), error::Commit> {
        let () = self.inner.commit().map_err(|err| error::Commit {
            db_dir: self.db_dir.to_owned(),
            source: err,
        })?;
        #[cfg(feature = "observe")]
        match self.parent_pending_writes {
            Some(parent_pending_writes) => {
                parent_pending_writes.extend(self.pending_writes)
            }
            None => self
                .pending_writes
                .iter()
                .for_each(|(_db_name, watch_tx)| watch_tx.send_replace(())),
        }
        Ok(())
    }
}

impl<'rwtxn, Tag> std::ops::Deref for RwTxn<'rwtxn, Tag> {
    type Target = crate::RoTxn<'rwtxn, heed::WithoutTls, Tag>;
    fn deref(&self) -> &Self::Target {
        let inner: &heed::RoTxn<'rwtxn> = &self.inner;
        // This is safe because RoTxn is just a wrapper around heed::RoTxn
        unsafe { std::mem::transmute(inner) }
    }
}

impl<'rwtxn, Tag> AsMut<heed::RwTxn<'rwtxn>> for RwTxn<'rwtxn, Tag> {
    fn as_mut(&mut self) -> &mut heed::RwTxn<'rwtxn> {
        &mut self.inner
    }
}
