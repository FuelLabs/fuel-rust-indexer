use crate::storage::Column;
use fuel_core::{
    database::{
        Database,
        database_description::DatabaseDescription,
    },
    state::{
        historical_rocksdb::StateRewindPolicy,
        rocks_db::DatabaseConfig,
    },
};
use fuel_core_types::fuel_types::BlockHeight;
use std::path::Path;

#[derive(Copy, Clone, Debug)]
pub struct Description;

impl DatabaseDescription for Description {
    type Column = Column;
    type Height = BlockHeight;

    fn version() -> u32 {
        1
    }

    fn name() -> String {
        "events".to_string()
    }

    fn metadata_column() -> Self::Column {
        Self::Column::Metadata
    }

    fn prefix(_: &Self::Column) -> Option<usize> {
        None
    }
}

pub type Storage = Database<Description>;

pub fn open_database(
    db_path: &Path,
    database_config: DatabaseConfig,
) -> anyhow::Result<Database<Description>> {
    Ok(Database::<Description>::open_rocksdb(
        db_path,
        StateRewindPolicy::NoRewind,
        database_config,
    )?)
}
