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
#[error("Failed to clear db `{db_name}` at `{db_path}`")]
pub struct Clear {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) source: heed::Error,
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
    Init(#[from] Box<IterDuplicatesInit>),
    #[error(transparent)]
    Item(#[from] IterItem),
}

impl From<IterDuplicatesInit> for IterDuplicates {
    fn from(err: IterDuplicatesInit) -> Self {
        Self::Init(Box::new(err))
    }
}

#[derive(Debug, Error)]
pub enum Iter {
    #[error(transparent)]
    DuplicatesInit(#[from] Box<IterDuplicatesInit>),
    #[error(transparent)]
    Init(#[from] IterInit),
    #[error(transparent)]
    Item(#[from] IterItem),
}

impl From<IterDuplicatesInit> for Iter {
    fn from(err: IterDuplicatesInit) -> Self {
        Self::DuplicatesInit(Box::new(err))
    }
}

#[derive(Debug, Error)]
#[error("Failed to fetch last item for db `{db_name}` at `{db_path}`")]
pub struct Last {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) source: heed::Error,
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

fn display_range_bytes(
    start_bound: &Result<std::ops::Bound<Vec<u8>>, heed::BoxedError>,
    end_bound: &Result<std::ops::Bound<Vec<u8>>, heed::BoxedError>,
) -> String {
    use std::ops::Bound;
    let start_bound = match start_bound {
        Ok(start_bound) => start_bound,
        Err(encode_err) => {
            return format!(
            "key encoding failed for range start with error `{encode_err:#}`"
        )
        }
    };
    let end_bound = match end_bound {
        Ok(end_bound) => end_bound,
        Err(encode_err) => {
            return format!(
                "key encoding failed for range end with error `{encode_err:#}`"
            )
        }
    };
    match (start_bound, end_bound) {
        (Bound::Excluded(start), Bound::Excluded(end)) => {
            format!("`({})..{}`", hex::encode(start), hex::encode(end))
        }
        (Bound::Excluded(start), Bound::Included(end)) => {
            format!("`({})..={}`", hex::encode(start), hex::encode(end))
        }
        (Bound::Excluded(start), Bound::Unbounded) => {
            format!("`({})..`", hex::encode(start))
        }
        (Bound::Included(start), Bound::Excluded(end)) => {
            format!("`{}..{}`", hex::encode(start), hex::encode(end))
        }
        (Bound::Included(start), Bound::Included(end)) => {
            format!("`{}..={}`", hex::encode(start), hex::encode(end))
        }
        (Bound::Included(start), Bound::Unbounded) => {
            format!("`{}..`", hex::encode(start))
        }
        (Bound::Unbounded, Bound::Excluded(end)) => {
            format!("`..{}`", hex::encode(end))
        }
        (Bound::Unbounded, Bound::Included(end)) => {
            format!("`..={}`", hex::encode(end))
        }
        (Bound::Unbounded, Bound::Unbounded) => "`..`".to_owned(),
    }
}

#[derive(Debug, Error)]
#[error(
    "Failed to initialize read-only iterator for db `{}` at `{}` over range ({})",
    .db_name,
    .db_path.display(),
    display_range_bytes(.range_start_bytes, .range_end_bytes)
)]
pub struct RangeInit {
    pub(crate) db_name: String,
    pub(crate) db_path: PathBuf,
    pub(crate) range_start_bytes: Result<
        std::ops::Bound<Vec<u8>>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
    pub(crate) range_end_bytes: Result<
        std::ops::Bound<Vec<u8>>,
        Box<dyn std::error::Error + Send + Sync>,
    >,
    pub(crate) source: Box<heed::Error>,
}

#[derive(Debug, Error)]
pub enum Range {
    #[error(transparent)]
    Init(#[from] Box<RangeInit>),
    #[error(transparent)]
    Item(#[from] IterItem),
}

impl From<RangeInit> for Range {
    fn from(err: RangeInit) -> Self {
        Self::Init(Box::new(err))
    }
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
    TryGet(#[from] Box<TryGet>),
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

impl From<TryGet> for Get {
    fn from(err: TryGet) -> Self {
        Self::TryGet(Box::new(err))
    }
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

    /// Data exists in both DBs
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
    pub struct And(Box<Inner>);

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
            Self(Box::new(Inner::new(on, db0, db1)))
        }
    }

    /// Data does not exist in either DB
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
    pub struct Nor(Box<Inner>);

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
            Self(Box::new(Inner::new(on, db0, db1)))
        }
    }

    /// Data exists in the first DB, but not in the other
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
    pub struct Xor(Box<Inner>);

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
            Self(Box::new(Inner::new(on, db0, db1)))
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
    Clear(#[from] Clear),
    #[error(transparent)]
    Delete(#[from] Box<Delete>),
    #[error(transparent)]
    First(#[from] First),
    #[error(transparent)]
    Get(#[from] Get),
    #[error(transparent)]
    Inconsistent(#[from] inconsistent::Error),
    #[error(transparent)]
    Iter(#[from] Iter),
    #[error(transparent)]
    IterDuplicatesInit(#[from] Box<IterDuplicatesInit>),
    #[error(transparent)]
    IterDuplicates(#[from] IterDuplicates),
    #[error(transparent)]
    IterInit(#[from] IterInit),
    #[error(transparent)]
    IterItem(#[from] IterItem),
    #[error(transparent)]
    Last(#[from] Last),
    #[error(transparent)]
    Len(#[from] Len),
    #[error(transparent)]
    Put(#[from] Box<Put>),
    #[error(transparent)]
    Range(#[from] Range),
    #[error(transparent)]
    RangeInit(#[from] Box<RangeInit>),
    #[error(transparent)]
    TryGet(#[from] Box<TryGet>),
}

impl From<Delete> for Error {
    fn from(err: Delete) -> Self {
        Self::Delete(Box::new(err))
    }
}

impl From<IterDuplicatesInit> for Error {
    fn from(err: IterDuplicatesInit) -> Self {
        Self::IterDuplicatesInit(Box::new(err))
    }
}

impl From<Put> for Error {
    fn from(err: Put) -> Self {
        Self::Put(Box::new(err))
    }
}

impl From<RangeInit> for Error {
    fn from(err: RangeInit) -> Self {
        Self::RangeInit(Box::new(err))
    }
}

impl From<TryGet> for Error {
    fn from(err: TryGet) -> Self {
        Self::TryGet(Box::new(err))
    }
}
