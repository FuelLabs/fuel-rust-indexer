use crate::{
    port::StorableEvent,
    service::TransactionEvents,
};
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
use fuel_core_types::fuel_types::BlockHeight;

#[repr(u32)]
#[derive(
    Copy,
    Clone,
    Debug,
    strum_macros::EnumCount,
    strum_macros::IntoStaticStr,
    strum_macros::EnumString,
    strum_macros::EnumVariantNames,
    PartialEq,
    Eq,
    enum_iterator::Sequence,
    Hash,
)]
#[strum(serialize_all = "snake_case")]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
#[cfg_attr(feature = "clap", clap(rename_all = "snake_case"))]
pub enum Column {
    Metadata = 0,
    Events = 1,
    LastCheckpoint = 2,
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

pub struct Events<Event>(core::marker::PhantomData<Event>);

impl<Event> Mappable for Events<Event>
where
    Event: StorableEvent,
{
    type Key = Self::OwnedKey;
    type OwnedKey = BlockHeight;
    type Value = Self::OwnedValue;
    type OwnedValue = Vec<TransactionEvents<Event>>;
}

impl<Event> TableWithBlueprint for Events<Event>
where
    Event: StorableEvent,
{
    type Blueprint = Plain<Primitive<4>, Postcard>;
    type Column = Column;

    fn column() -> Self::Column {
        Column::Events
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
