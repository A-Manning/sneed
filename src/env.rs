use std::{path::Path, sync::Arc};

use crate::{EnvOpenOptions, RoTxn, RwTxn};

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
        OpenEnv(#[from] OpenEnv),
        #[error(transparent)]
        ReadTxn(#[from] ReadTxn),
        #[error(transparent)]
        WriteTxn(#[from] WriteTxn),
    }
}
pub use error::Error;

/// Wrapper for heed's `Env`
#[derive(Clone, Debug)]
pub struct Env<'id> {
    inner: heed::Env,
    path: Arc<Path>,
    unique_guard: Arc<generativity::Guard<'id>>,
}

impl<'id> Env<'id> {
    /// # Safety
    /// See [`heed::EnvOpenOptions::open`]
    pub unsafe fn open(
        unique_guard: generativity::Guard<'id>,
        opts: &EnvOpenOptions,
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
            unique_guard: Arc::new(unique_guard),
        })
    }

    #[inline(always)]
    pub(crate) fn unique_guard(&self) -> &Arc<generativity::Guard<'id>> {
        &self.unique_guard
    }

    #[inline(always)]
    pub fn path(&self) -> &Arc<Path> {
        &self.path
    }

    #[inline(always)]
    pub(crate) fn database_options(
        &self,
    ) -> heed::DatabaseOpenOptions<heed::Unspecified, heed::Unspecified> {
        self.inner.database_options()
    }

    pub fn read_txn(&self) -> Result<RoTxn<'_, 'id>, error::ReadTxn> {
        let inner = self.inner.read_txn().map_err(|err| error::ReadTxn {
            db_dir: (*self.path).to_owned(),
            source: err,
        })?;
        Ok(RoTxn {
            inner,
            _unique_guard: &self.unique_guard,
        })
    }

    pub fn write_txn(&self) -> Result<RwTxn<'_, 'id>, error::WriteTxn> {
        let inner = self.inner.write_txn().map_err(|err| error::WriteTxn {
            db_dir: (*self.path).to_owned(),
            source: err,
        })?;
        Ok(RwTxn {
            inner,
            db_dir: &self.path,
            _unique_guard: &self.unique_guard,
            #[cfg(feature = "observe")]
            pending_writes: Default::default(),
        })
    }
}
