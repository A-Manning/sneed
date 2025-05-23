//! Database utils

#![cfg_attr(docsrs, feature(doc_cfg))]

use heed::{BytesDecode, BytesEncode};
use thiserror::Error;

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

pub mod rotxn {
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
    pub struct RoTxn<'a, Tag = ()> {
        pub(crate) inner: heed::RoTxn<'a>,
        pub(crate) _tag: std::marker::PhantomData<Tag>,
    }

    impl<Tag> RoTxn<'_, Tag> {
        pub fn commit(self) -> Result<(), error::Commit> {
            self.inner
                .commit()
                .map_err(|err| error::Commit { source: err })
        }
    }

    impl<'rwtxn, Tag> std::ops::Deref for RoTxn<'rwtxn, Tag> {
        type Target = heed::RoTxn<'rwtxn>;
        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }
}
pub use rotxn::{Error as RoTxnError, RoTxn};

pub mod rwtxn {
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
        type Target = crate::RoTxn<'rwtxn, Tag>;
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
}
pub use rwtxn::{Error as RwTxnError, RwTxn};

pub mod env {
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
    pub struct Env<Tag = ()> {
        inner: heed::Env,
        path: Arc<Path>,
        pub(crate) tag: std::marker::PhantomData<Tag>,
    }

    impl<Tag> Env<Tag> {
        /// # Safety
        /// See [`heed::EnvOpenOptions::open`]
        pub unsafe fn open(
            opts: &OpenOptions,
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

        pub fn read_txn(&self) -> Result<RoTxn<'_, Tag>, error::ReadTxn> {
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
            parent: &'p mut RwTxn<'p, Tag>,
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
}
pub use env::{Env, Error as EnvError};

pub mod db;
pub use db::{
    DatabaseDup, DatabaseUnique, Error as DbError, RoDatabaseDup,
    RoDatabaseUnique,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error")]
    Db(#[from] DbError),
    #[error("Database env error")]
    Env(#[from] EnvError),
    #[error("Database read error")]
    Read(#[from] RoTxnError),
    #[error("Database write error")]
    Write(#[from] RwTxnError),
}
