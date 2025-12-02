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
    type Column = super::storage::Column;
    type Height = BlockHeight;

    fn version() -> u32 {
        1
    }

    fn name() -> String {
        "receipts".to_string()
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
    state_rewind_policy: StateRewindPolicy,
    database_config: DatabaseConfig,
) -> anyhow::Result<Database<Description>> {
    Ok(Database::<Description>::open_rocksdb(
        db_path,
        state_rewind_policy,
        database_config,
    )?)
}

/// Commits an empty block at the specified height to the database.
/// This is primarily used for testing rollback functionality.
#[cfg(any(test, feature = "test-helpers"))]
pub fn commit_height(
    database: &mut Storage,
    height: BlockHeight,
) -> anyhow::Result<()> {
    fuel_storage_utils::rocksdb::commit_height(database, height)
}
