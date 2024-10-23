//! DB errors

use std::path::PathBuf;

use thiserror::Error;

fn display_key_bytes(key_bytes: &Result<Vec<u8>, heed::BoxedError>) -> String {
    match key_bytes {
        Ok(key_bytes) => {
            format!("key: `{}`", hex::encode(key_bytes))
        }
        Err(encode_err) => {
            format!("key encoding failed with error `{encode_err:#}`")
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "Failed to delete from db `{db_name}` at `{db_path}` ({})",
    display_key_bytes(.key_bytes)
)]
pub struct Delete {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) key_bytes:
        Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>,
    pub(crate) source: heed::Error,
}

#[derive(Debug, Error)]
#[error("Failed to read first item from db `{db_name}` at `{db_path}`")]
pub struct First {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) source: heed::Error,
}

#[derive(Debug, Error)]
#[error(
    "Failed to initialize read-only duplicates iterator for db `{db_name}` at `{db_path}` ({})",
    display_key_bytes(.key_bytes),
)]
pub struct IterDuplicatesInit {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) key_bytes:
        Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>,
    pub(crate) source: heed::Error,
}

#[derive(Debug, Error)]
#[error(
    "Failed to initialize read-only iterator for db `{db_name}` at `{db_path}`"
)]
pub struct IterInit {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) source: heed::Error,
}

#[derive(Debug, Error)]
#[error("Failed to read item of read-only iterator for db `{db_name}` at `{db_path}`")]
pub struct IterItem {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) source: heed::Error,
}

#[derive(Debug, Error)]
pub enum IterDuplicates {
    #[error(transparent)]
    Init(#[from] IterDuplicatesInit),
    #[error(transparent)]
    Item(#[from] IterItem),
}

#[derive(Debug, Error)]
pub enum Iter {
    #[error(transparent)]
    DuplicatesInit(#[from] IterDuplicatesInit),
    #[error(transparent)]
    Init(#[from] IterInit),
    #[error(transparent)]
    Item(#[from] IterItem),
}

#[derive(Debug, Error)]
#[error("Failed to read length for db `{db_name}` at `{db_path}`")]
pub struct Len {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) source: heed::Error,
}

fn display_value_bytes(
    value_bytes: &Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>,
) -> String {
    match value_bytes {
        Ok(value_bytes) => {
            format!("value: `{}`", hex::encode(value_bytes))
        }
        Err(encode_err) => {
            format!("value encoding failed with error `{encode_err:#}`")
        }
    }
}

#[derive(Debug, Error)]
#[error(
    "Failed to write to db `{db_name}` at `{db_path}` ({}, {})",
    display_key_bytes(.key_bytes),
    display_value_bytes(.value_bytes)
)]
pub struct Put {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) key_bytes:
        Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>,
    pub(crate) value_bytes:
        Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>,
    pub(crate) source: heed::Error,
}

#[derive(Debug, Error)]
#[error(
    "Failed to read from db `{db_name}` at `{db_path}` ({})",
    display_key_bytes(.key_bytes)
)]
pub struct TryGet {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) key_bytes:
        Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>>,
    pub(crate) source: heed::Error,
}

#[derive(Debug, Error)]
pub enum Get {
    #[error(transparent)]
    TryGet(#[from] TryGet),
    #[error(
        "Missing value from db `{db_name}` at `{db_path}` (key: {})",
        hex::encode(.key_bytes)
    )]
    MissingValue {
        db_name: String,
        db_path: PathBuf,
        key_bytes: Vec<u8>,
    },
}

pub mod inconsistent {
    use heed::BytesEncode;
    use thiserror::Error;

    use crate::db::Database;

    #[derive(Clone, Copy, Debug, strum::Display)]
    #[strum(serialize_all = "lowercase")]
    pub enum KeyOrValue {
        Key,
        Value,
    }

    /// Trait to extract the type that a key is inconsistent on
    pub trait ByKeyOrValue<'a> {
        type DB: Database;
        type BE: BytesEncode<'a>;

        const KEY_OR_VALUE: KeyOrValue;

        fn into_inner(self) -> Self::DB;
    }

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct ByKey<DB>(pub DB);

    impl<'a, DB> ByKeyOrValue<'a> for ByKey<DB>
    where
        DB: Database,
        DB::KC: BytesEncode<'a>,
    {
        type DB = DB;
        type BE = DB::KC;

        fn into_inner(self) -> Self::DB {
            self.0
        }

        const KEY_OR_VALUE: KeyOrValue = KeyOrValue::Key;
    }

    #[derive(Debug)]
    #[repr(transparent)]
    pub struct ByValue<DB>(pub DB);

    impl<'a, DB> ByKeyOrValue<'a> for ByValue<DB>
    where
        DB: Database,
        DB::DC: BytesEncode<'a>,
        <DB::DC as BytesEncode<'a>>::EItem: Sized,
    {
        type DB = DB;
        type BE = DB::DC;

        fn into_inner(self) -> Self::DB {
            self.0
        }

        const KEY_OR_VALUE: KeyOrValue = KeyOrValue::Value;
    }

    #[derive(Debug)]
    struct Inner {
        on: Vec<u8>,
        db0_by: KeyOrValue,
        db0_name: String,
        db1_by: KeyOrValue,
        db1_name: String,
    }

    impl Inner {
        fn new<'a, ByDb0, ByDb1>(
            on: &'a <ByDb0::BE as BytesEncode<'a>>::EItem,
            db0: ByDb0,
            db1: ByDb1,
        ) -> Self
        where
            ByDb0: ByKeyOrValue<'a>,
            ByDb1: ByKeyOrValue<'a>,
            ByDb1::BE:
                BytesEncode<'a, EItem = <ByDb0::BE as BytesEncode<'a>>::EItem>,
        {
            let on_bytes =
                // Safe to unwrap as we know that encoding will succeed
                <ByDb0::BE as BytesEncode>::bytes_encode(on).expect(
                    "Encoding should succeed when constructing inconsistent DBs error"
                );
            Self {
                on: on_bytes.to_vec(),
                db0_by: ByDb0::KEY_OR_VALUE,
                db0_name: db0.into_inner().name().to_owned(),
                db1_by: ByDb1::KEY_OR_VALUE,
                db1_name: db1.into_inner().name().to_owned(),
            }
        }
    }

    #[derive(Debug, Error)]
    #[error(
        "Inconsistent dbs: `{}` exists in both db `{}` (as {}) and in db `{}` (as {})",
        hex::encode(&.0.on),
        .0.db0_name,
        .0.db0_by,
        .0.db1_name,
        .0.db1_by,
    )]
    #[repr(transparent)]
    pub struct And(Inner);

    impl And {
        #[inline(always)]
        pub fn new<'a, ByDb0, ByDb1>(
            on: &'a <ByDb0::BE as BytesEncode<'a>>::EItem,
            db0: ByDb0,
            db1: ByDb1,
        ) -> Self
        where
            ByDb0: ByKeyOrValue<'a>,
            ByDb1: ByKeyOrValue<'a>,
            ByDb1::BE:
                BytesEncode<'a, EItem = <ByDb0::BE as BytesEncode<'a>>::EItem>,
        {
            Self(Inner::new(on, db0, db1))
        }
    }

    #[derive(Debug, Error)]
    #[error(
        "Inconsistent dbs: `{}` does not exist in db `{}` (as {}) or in db `{}` (as {})",
        hex::encode(&.0.on),
        .0.db0_name,
        .0.db0_by,
        .0.db1_name,
        .0.db1_by,
    )]
    #[repr(transparent)]
    pub struct Nor(Inner);

    impl Nor {
        #[inline(always)]
        pub fn new<'a, ByDb0, ByDb1>(
            on: &'a <ByDb0::BE as BytesEncode<'a>>::EItem,
            db0: ByDb0,
            db1: ByDb1,
        ) -> Self
        where
            ByDb0: ByKeyOrValue<'a>,
            ByDb1: ByKeyOrValue<'a>,
            ByDb1::BE:
                BytesEncode<'a, EItem = <ByDb0::BE as BytesEncode<'a>>::EItem>,
        {
            Self(Inner::new(on, db0, db1))
        }
    }

    #[derive(Debug, Error)]
    #[error(
        "Inconsistent dbs: `{}` exists in db `{}` (as {}), but not in db `{}` (as {})",
        hex::encode(&.0.on),
        .0.db0_name,
        .0.db0_by,
        .0.db1_name,
        .0.db1_by,
    )]
    #[repr(transparent)]
    pub struct Xor(Inner);

    impl Xor {
        #[inline(always)]
        pub fn new<'a, ByDb0, ByDb1>(
            on: &'a <ByDb0::BE as BytesEncode<'a>>::EItem,
            db0: ByDb0,
            db1: ByDb1,
        ) -> Self
        where
            ByDb0: ByKeyOrValue<'a>,
            ByDb1: ByKeyOrValue<'a>,
            ByDb1::BE:
                BytesEncode<'a, EItem = <ByDb0::BE as BytesEncode<'a>>::EItem>,
        {
            Self(Inner::new(on, db0, db1))
        }
    }

    #[derive(Debug, Error)]
    pub enum Error {
        #[error(transparent)]
        And(#[from] And),
        #[error(transparent)]
        Nor(#[from] Nor),
        #[error(transparent)]
        Xor(#[from] Xor),
    }
}

pub use inconsistent::Error as Inconsistent;

/// General error type for DB operations
#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Delete(#[from] Delete),
    #[error(transparent)]
    First(#[from] First),
    #[error(transparent)]
    Get(#[from] Get),
    #[error(transparent)]
    Inconsistent(#[from] inconsistent::Error),
    #[error(transparent)]
    Iter(#[from] Iter),
    #[error(transparent)]
    IterDuplicatesInit(#[from] IterDuplicatesInit),
    #[error(transparent)]
    IterDuplicates(#[from] IterDuplicates),
    #[error(transparent)]
    IterInit(#[from] IterInit),
    #[error(transparent)]
    IterItem(#[from] IterItem),
    #[error(transparent)]
    Len(#[from] Len),
    #[error(transparent)]
    Put(#[from] Put),
    #[error(transparent)]
    TryGet(#[from] TryGet),
}
