use fuel_core_storage::{
    Mappable,
    blueprint::plain::Plain,
    codec::{
        postcard::Postcard,
        primitive::Primitive,
    },
    kv_store::StorageColumn,
    structured_storage::TableWithBlueprint,
};
use fuel_indexer_types::events::SuccessfulTransactionReceipts;
use fuels::types::BlockHeight;

#[cfg(not(feature = "blocks-subscription"))]
use fuel_core_types::blockchain::header::BlockHeader;
#[cfg(feature = "blocks-subscription")]
use fuel_indexer_types::events::BlockEvent;

#[repr(u32)]
#[derive(
    Copy,
    Clone,
    Debug,
    strum_macros::EnumCount,
    strum_macros::IntoStaticStr,
    PartialEq,
    Eq,
    enum_iterator::Sequence,
    Hash,
)]
pub enum Column {
    Metadata = 0,
    LastCheckpoint = 1,
    Receipts = 2,
    #[cfg(feature = "blocks-subscription")]
    Blocks = 3,
    #[cfg(not(feature = "blocks-subscription"))]
    Headers = 4,
}

impl Column {
    /// The total count of variants in the enum.
    pub const COUNT: usize = <Self as strum::EnumCount>::COUNT;

    /// Returns the `u32` representation of the `Column`.
    pub fn as_u32(&self) -> u32 {
        *self as u32
    }
}

impl StorageColumn for Column {
    fn name(&self) -> String {
        let str: &str = self.into();
        str.to_string()
    }

    fn id(&self) -> u32 {
        self.as_u32()
    }
}

pub struct Receipts;

impl Mappable for Receipts {
    type Key = Self::OwnedKey;
    type OwnedKey = BlockHeight;
    type Value = Self::OwnedValue;
    type OwnedValue = Vec<SuccessfulTransactionReceipts>;
}

impl TableWithBlueprint for Receipts {
    type Blueprint = Plain<Primitive<4>, Postcard>;
    type Column = Column;

    fn column() -> Self::Column {
        Column::Receipts
    }
}

#[cfg(not(feature = "blocks-subscription"))]
pub struct Headers;

#[cfg(not(feature = "blocks-subscription"))]
impl Mappable for Headers {
    type Key = Self::OwnedKey;
    type OwnedKey = BlockHeight;
    type Value = Self::OwnedValue;
    type OwnedValue = BlockHeader;
}

#[cfg(not(feature = "blocks-subscription"))]
impl TableWithBlueprint for Headers {
    type Blueprint = Plain<Primitive<4>, Postcard>;
    type Column = Column;

    fn column() -> Self::Column {
        Column::Headers
    }
}

#[cfg(feature = "blocks-subscription")]
pub struct Blocks;

#[cfg(feature = "blocks-subscription")]
impl Mappable for Blocks {
    type Key = Self::OwnedKey;
    type OwnedKey = BlockHeight;
    type Value = Self::OwnedValue;
    type OwnedValue = BlockEvent;
}

#[cfg(feature = "blocks-subscription")]
impl TableWithBlueprint for Blocks {
    type Blueprint = Plain<Primitive<4>, Postcard>;
    type Column = Column;

    fn column() -> Self::Column {
        Column::Blocks
    }
}

pub struct LastCheckpoint;

impl Mappable for LastCheckpoint {
    type Key = Self::OwnedKey;
    type OwnedKey = ();
    type Value = Self::OwnedValue;
    type OwnedValue = BlockHeight;
}

impl TableWithBlueprint for LastCheckpoint {
    type Blueprint = Plain<Postcard, Primitive<4>>;
    type Column = Column;

    fn column() -> Self::Column {
        Column::LastCheckpoint
    }
}
