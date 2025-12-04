use crate::CommitLazyChanges;
use fuel_core::database::{
    Database,
    commit_changes_with_height_update,
    database_description::{
        DatabaseDescription,
        DatabaseHeight,
    },
};
use fuel_core_storage::transactional::Changes;
use std::fmt::Debug;

impl<Description> CommitLazyChanges for Database<Description>
where
    Description: DatabaseDescription,
    Description::Height: Debug
        + PartialOrd
        + DatabaseHeight
        + serde::Serialize
        + serde::de::DeserializeOwned,
{
    fn commit_changes(&mut self, changes: Changes) -> anyhow::Result<()> {
        commit_changes_with_height_update(self, changes, |_| Ok(Vec::new()))
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
