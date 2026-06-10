use crate::storage::{
    Column,
    LastCheckpoint,
};
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
use fuel_core_storage::{
    StorageAsRef,
    structured_storage::StructuredStorage,
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

impl fuel_storage_utils::rocksdb::CheckpointReader for Description {
    /// Reads the events-manager's [`LastCheckpoint`] table from the
    /// pending change-set. That table is keyed by `()` and holds the
    /// block height the commit represents.
    fn read_checkpoint(
        iter: &fuel_core_storage::iter::changes_iterator::ChangesIterator<Self::Column>,
    ) -> fuel_core_storage::Result<Option<Self::Height>> {
        StructuredStorage::new(iter)
            .storage::<LastCheckpoint>()
            .get(&())
            .map(|opt| opt.map(|h| *h))
    }
}

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
pub fn commit_height(database: &mut Storage, height: BlockHeight) -> anyhow::Result<()> {
    fuel_storage_utils::rocksdb::commit_height(database, height)
}
