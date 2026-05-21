use crate::CommitLazyChanges;
use fuel_core::database::{
    Database,
    commit_changes_with_height_update,
    database_description::{
        DatabaseDescription,
        DatabaseHeight,
    },
};
use fuel_core_storage::{
    Result as StorageResult,
    iter::changes_iterator::ChangesIterator,
    transactional::Changes,
};
use std::fmt::Debug;

/// Implemented by each database [`DatabaseDescription`] that wants its
/// commits to carry a rollback marker. Given the change-set being committed,
/// returns the new height it represents (if any) — fuel-core's historical
/// rocksdb layer records that height so a later rollback can target it.
///
/// Without an implementation of this trait, `commit_changes` would always
/// pass an empty height list to fuel-core, which silently disables rollback.
pub trait CheckpointReader: DatabaseDescription {
    fn read_checkpoint(
        iter: &ChangesIterator<Self::Column>,
    ) -> StorageResult<Option<Self::Height>>;
}

impl<Description> CommitLazyChanges for Database<Description>
where
    Description: DatabaseDescription + CheckpointReader,
    Description::Height: Debug
        + PartialOrd
        + DatabaseHeight
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    fn commit_changes(&mut self, changes: Changes) -> anyhow::Result<()> {
        commit_changes_with_height_update(self, changes, |iter| {
            Ok(Description::read_checkpoint(iter)?.into_iter().collect())
        })
        .map_err(Into::into)
    }
}

/// Commits an empty block at the specified height to the database.
/// This is primarily used for testing rollback functionality.
#[cfg(any(test, feature = "test-helpers"))]
pub fn commit_height<Description>(
    database: &mut Database<Description>,
    height: Description::Height,
) -> anyhow::Result<()>
where
    Description: DatabaseDescription,
    Description::Height: Debug
        + PartialOrd
        + DatabaseHeight
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    commit_changes_with_height_update(database, Changes::default(), |_| Ok(vec![height]))
        .map_err(Into::into)
}
