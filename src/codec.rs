//! Heed codecs

#[cfg(feature = "serde-wincode")]
mod serde_wincode {
    use std::{borrow::Cow, marker::PhantomData};

    use heed::{BoxedError, BytesDecode, BytesEncode};
    use serde::{Deserialize, Serialize};
    use serde_wincode::SerdeCompat;
    use wincode::{
        config::{Config, DefaultConfig},
        SchemaRead, SchemaWrite,
    };

    pub struct SerdeWincode<T, Config = DefaultConfig> {
        _marker: PhantomData<(T, Config)>,
    }

    impl<'a, T: 'a, C> BytesEncode<'a> for SerdeWincode<T, C>
    where
        T: Serialize,
        C: Config,
    {
        type EItem = T;

        fn bytes_encode(
            item: &'a Self::EItem,
        ) -> Result<Cow<'a, [u8]>, BoxedError> {
            let capacity = <SerdeCompat<T> as SchemaWrite<C>>::size_of(item)?;
            let mut buffer = Vec::with_capacity(capacity);
            <SerdeCompat<T> as SchemaWrite<C>>::write(&mut buffer, item)?;
            Ok(Cow::Owned(buffer))
        }
    }

    impl<'a, T: 'a, C> BytesDecode<'a> for SerdeWincode<T, C>
    where
        T: Deserialize<'a>,
        C: Config,
    {
        type DItem = T;

        fn bytes_decode(bytes: &'a [u8]) -> Result<Self::DItem, BoxedError> {
            <SerdeCompat<T> as SchemaRead<'a, C>>::get(bytes)
                .map_err(Into::into)
        }
    }

    unsafe impl<T, C> Send for SerdeWincode<T, C> {}
    unsafe impl<T, C> Sync for SerdeWincode<T, C> {}
}

#[cfg(feature = "serde-wincode")]
pub use serde_wincode::SerdeWincode;
