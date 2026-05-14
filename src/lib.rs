//! Database utils

#![cfg_attr(docsrs, feature(doc_cfg))]

use heed::{BytesDecode, BytesEncode};
use thiserror::Error;

pub mod codec;

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

pub mod rotxn;
pub use rotxn::{Error as RoTxnError, RoTxn};

pub mod rwtxn;
pub use rwtxn::{Error as RwTxnError, RwTxn};

pub mod env;
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
