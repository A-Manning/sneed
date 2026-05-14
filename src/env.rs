use std::{path::Path, sync::Arc};

use heed::DatabaseOpenOptions;

use crate::{RoTxn, RwTxn};

pub use heed::EnvOpenOptions as OpenOptions;

pub mod error {
    use std::path::PathBuf;

    use thiserror::Error;

    #[derive(Debug, Error)]
    #[error("Error creating database `{name}` in `{path}`")]
    pub struct CreateDb {
        pub(crate) name: String,
        pub(crate) path: PathBuf,
        pub(crate) source: heed::Error,
    }

    #[derive(Debug, Error)]
    #[error("Error creating nested write txn for database dir `{db_dir}`")]
    pub struct NestedWriteTxn {
        pub(crate) db_dir: PathBuf,
        pub(crate) source: heed::Error,
    }

    #[derive(Debug, Error)]
    #[error("Error opening database `{name}` in `{path}`")]
    pub struct OpenDb {
        pub(crate) name: String,
        pub(crate) path: PathBuf,
        pub(crate) source: heed::Error,
    }

    #[derive(Debug, Error)]
    #[error("Error opening database env at (`{path}`)")]
    pub struct OpenEnv {
        pub(crate) path: PathBuf,
        pub(crate) source: heed::Error,
    }

    #[derive(Debug, Error)]
    #[error("Error creating read txn for database dir `{db_dir}`")]
    pub struct ReadTxn {
        pub(crate) db_dir: PathBuf,
        pub(crate) source: heed::Error,
    }

    #[derive(Debug, Error)]
    #[error("Error creating write txn for database dir `{db_dir}`")]
    pub struct WriteTxn {
        pub(crate) db_dir: PathBuf,
        pub(crate) source: heed::Error,
    }

    /// General error type for Env operations
    #[derive(Debug, Error)]
    pub enum Error {
        #[error(transparent)]
        CreateDb(#[from] CreateDb),
        #[error(transparent)]
        NestedWriteTxn(#[from] NestedWriteTxn),
        #[error(transparent)]
        OpenDb(#[from] OpenDb),
        #[error(transparent)]
        OpenEnv(#[from] OpenEnv),
        #[error(transparent)]
        ReadTxn(#[from] ReadTxn),
        #[error(transparent)]
        WriteTxn(#[from] WriteTxn),
    }
}
pub use error::Error;

/// Wrapper for heed's `Env`.
///
/// The type tag can be used to distinguish between different database
/// envs. Databases and txns opened within an `Env` will also be tagged,
/// ensuring that txns created in an `Env` can only be used for DBs
/// created/opened by the same `Env`.
#[derive(educe::Educe)]
#[educe(Clone(bound()), Debug(bound()))]
pub struct Env<T = heed::WithTls, Tag = ()> {
    inner: heed::Env<T>,
    path: Arc<Path>,
    pub(crate) tag: std::marker::PhantomData<Tag>,
}

impl<T, Tag> Env<T, Tag> {
    #[inline(always)]
    pub fn path(&self) -> &Arc<Path> {
        &self.path
    }

    #[inline(always)]
    pub(crate) fn database_options(
        &self,
    ) -> DatabaseOpenOptions<'_, '_, T, heed::Unspecified, heed::Unspecified>
    {
        self.inner.database_options()
    }

    pub fn read_txn(&self) -> Result<RoTxn<'_, T, Tag>, error::ReadTxn> {
        let inner =
            self.inner.read_txn().map_err(|err| error::ReadTxn {
                db_dir: (*self.path).to_owned(),
                source: err,
            })?;
        Ok(RoTxn {
            inner,
            _tag: self.tag,
        })
    }

    pub fn nested_write_txn<'p>(
        &'p self,
        parent: &'p mut RwTxn<'_, Tag>,
    ) -> Result<RwTxn<'p, Tag>, error::NestedWriteTxn> {
        let inner = self
            .inner
            .nested_write_txn(&mut parent.inner)
            .map_err(|err| error::NestedWriteTxn {
                db_dir: (*self.path).to_owned(),
                source: err,
            })?;
        Ok(RwTxn {
            inner,
            db_dir: &self.path,
            _tag: self.tag,
            #[cfg(feature = "observe")]
            parent_pending_writes: Some(&mut parent.pending_writes),
            #[cfg(feature = "observe")]
            pending_writes: Default::default(),
        })
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_, Tag>, error::WriteTxn> {
        let inner =
            self.inner.write_txn().map_err(|err| error::WriteTxn {
                db_dir: (*self.path).to_owned(),
                source: err,
            })?;
        Ok(RwTxn {
            inner,
            db_dir: &self.path,
            _tag: self.tag,
            #[cfg(feature = "observe")]
            parent_pending_writes: None,
            #[cfg(feature = "observe")]
            pending_writes: Default::default(),
        })
    }
}

impl<T, Tag> Env<T, Tag>
where
    T: heed::TlsUsage,
{
    /// # Safety
    /// See [`heed::EnvOpenOptions::open`]
    pub unsafe fn open(
        opts: &OpenOptions<T>,
        path: &Path,
    ) -> Result<Self, error::OpenEnv> {
        let inner = match opts.open(path) {
            Ok(env) => env,
            Err(err) => {
                return Err(error::OpenEnv {
                    path: path.to_owned(),
                    source: err,
                })
            }
        };
        Ok(Self {
            inner,
            path: Arc::from(path),
            tag: std::marker::PhantomData,
        })
    }
}
