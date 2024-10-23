//! Database utils

#![cfg_attr(docsrs, feature(doc_cfg))]

use heed::{BytesDecode, BytesEncode};
use thiserror::Error;
//#[cfg(feature = "serde")]
//use serde::{Deserialize, Serialize};

const UNIT_KEY_ENCODED: u8 = 0x69;

#[derive(Debug, Error)]
enum UnitKeyDecodeErrorInner {
    #[error(
        "Expected byte encoding 0x{:x}, but 0x{:x} was provided",
        UNIT_KEY_ENCODED,
        .0
    )]
    IncorrectByte(u8),
    #[error("Expected a single byte, but {} were provided", .0)]
    IncorrectBytes(usize),
}

#[derive(Debug, Error)]
#[error("Error decoding unit key")]
#[repr(transparent)]
struct UnitKeyDecodeError(#[from] UnitKeyDecodeErrorInner);

/// Unit key encoding.
/// LMDB can't use zero-sized keys, so this encodes to a single byte.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub struct UnitKey;

impl BytesDecode<'_> for UnitKey {
    type DItem = ();
    fn bytes_decode(bytes: &[u8]) -> Result<Self::DItem, heed::BoxedError> {
        match bytes {
            [UNIT_KEY_ENCODED] => Ok(()),
            [incorrect_byte] => {
                let err =
                    UnitKeyDecodeErrorInner::IncorrectByte(*incorrect_byte);
                Err(UnitKeyDecodeError(err).into())
            }
            _ => {
                let err = UnitKeyDecodeErrorInner::IncorrectBytes(bytes.len());
                Err(UnitKeyDecodeError(err).into())
            }
        }
    }
}

impl BytesEncode<'_> for UnitKey {
    type EItem = ();
    fn bytes_encode(
        (): &Self::EItem,
    ) -> Result<std::borrow::Cow<'_, [u8]>, heed::BoxedError> {
        Ok(std::borrow::Cow::Borrowed(&[UNIT_KEY_ENCODED]))
    }
}

pub mod rwtxn {
    #[cfg(feature = "observe")]
    use std::{collections::HashMap, sync::Arc};
    use std::{ops::DerefMut as _, path::Path};

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

    /// Wrapper for heed's `RwTxn`
    pub struct RwTxn<'a> {
        pub(crate) inner: heed::RwTxn<'a>,
        pub(crate) db_dir: &'a Path,
        #[cfg(feature = "observe")]
        pub(crate) pending_writes: HashMap<Arc<str>, watch::Sender<()>>,
    }

    impl RwTxn<'_> {
        pub fn commit(self) -> Result<(), error::Commit> {
            let () = self.inner.commit().map_err(|err| error::Commit {
                db_dir: self.db_dir.to_owned(),
                source: err,
            })?;
            #[cfg(feature = "observe")]
            self.pending_writes
                .iter()
                .for_each(|(_db_name, watch_tx)| watch_tx.send_replace(()));
            Ok(())
        }
    }

    impl<'rwtxn> std::ops::Deref for RwTxn<'rwtxn> {
        type Target = heed::RwTxn<'rwtxn>;
        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl std::ops::DerefMut for RwTxn<'_> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }

    impl<'rwtxn> AsMut<heed::RwTxn<'rwtxn>> for RwTxn<'rwtxn> {
        fn as_mut(&mut self) -> &mut heed::RwTxn<'rwtxn> {
            self.deref_mut()
        }
    }
}
pub use rwtxn::{Error as RwTxnError, RwTxn};

pub mod env {
    use std::{path::Path, sync::Arc};

    use heed::{DatabaseOpenOptions, EnvOpenOptions, RoTxn};

    use crate::RwTxn;

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
    pub struct Env {
        inner: heed::Env,
        path: Arc<Path>,
    }

    impl Env {
        /// # Safety
        /// See [`heed::EnvOpenOptions::open`]
        pub unsafe fn open(
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
            })
        }

        #[inline(always)]
        pub fn path(&self) -> &Arc<Path> {
            &self.path
        }

        #[inline(always)]
        pub(crate) fn database_options(
            &self,
        ) -> DatabaseOpenOptions<heed::Unspecified, heed::Unspecified> {
            self.inner.database_options()
        }

        pub fn read_txn(&self) -> Result<RoTxn<'_>, error::ReadTxn> {
            self.inner.read_txn().map_err(|err| error::ReadTxn {
                db_dir: (*self.path).to_owned(),
                source: err,
            })
        }

        pub fn write_txn(&self) -> Result<RwTxn<'_>, error::WriteTxn> {
            let inner =
                self.inner.write_txn().map_err(|err| error::WriteTxn {
                    db_dir: (*self.path).to_owned(),
                    source: err,
                })?;
            Ok(RwTxn {
                inner,
                db_dir: &self.path,
                #[cfg(feature = "observe")]
                pending_writes: Default::default(),
            })
        }
    }
}
pub use env::{Env, Error as EnvError};

pub mod db;
pub use db::{DatabaseDup, DatabaseUnique, RoDatabaseDup, RoDatabaseUnique};
