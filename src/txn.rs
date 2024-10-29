//! Read/Write Transactions

pub(crate) mod private {
    pub trait Sealed<'env> {
        fn read_txn<'txn>(&'txn self) -> &'txn heed::RoTxn<'env>;
    }
}

pub trait Txn<'env, 'env_id>: private::Sealed<'env> {}

pub mod rotxn {

    /// Wrapper for heed's `RoTxn`
    pub struct RoTxn<'env, 'env_id> {
        pub(crate) inner: heed::RoTxn<'env>,
        pub(crate) _unique_guard: &'env generativity::Guard<'env_id>,
    }

    impl<'env> crate::txn::private::Sealed<'env> for RoTxn<'env, '_> {
        fn read_txn(&self) -> &heed::RoTxn<'env> {
            &self.inner
        }
    }

    impl<'env, 'env_id> crate::txn::Txn<'env, 'env_id> for RoTxn<'env, 'env_id> {}
}

pub use rotxn::RoTxn;

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

    /// Wrapper for heed's `RwTxn`
    pub struct RwTxn<'env, 'env_id> {
        pub(crate) inner: heed::RwTxn<'env>,
        pub(crate) db_dir: &'env Path,
        pub(crate) _unique_guard: &'env generativity::Guard<'env_id>,
        #[cfg(feature = "observe")]
        pub(crate) pending_writes: HashMap<Arc<str>, watch::Sender<()>>,
    }

    impl<'env> RwTxn<'env, '_> {
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

        pub(crate) fn write_txn(&mut self) -> &mut heed::RwTxn<'env> {
            &mut self.inner
        }
    }

    impl<'a> crate::txn::private::Sealed<'a> for RwTxn<'a, '_> {
        fn read_txn(&self) -> &heed::RoTxn<'a> {
            &self.inner
        }
    }

    impl<'env, 'env_id> crate::txn::Txn<'env, 'env_id> for RwTxn<'env, 'env_id> {}
}
pub use rwtxn::RwTxn;
