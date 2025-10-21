use crate::CommitLazyChanges;
use fuel_core_storage::{
    Result as StorageResult,
    iter::{
        BoxedIter,
        IntoBoxedIter,
        IterDirection,
        IterableStore,
        changes_iterator::ChangesIterator,
    },
    kv_store::{
        KVItem,
        KeyItem,
        KeyValueInspect,
        StorageColumn,
        Value,
        WriteOperation,
    },
    transactional::{
        Changes,
        StorageChanges,
    },
};
use std::{
    ops::{
        Deref,
        DerefMut,
    },
    sync::{
        Arc,
        Mutex,
    },
};

/// The in-memory storage for testing purposes.
#[derive(Clone)]
pub struct InMemoryStorage<Column> {
    pub(crate) storage: Arc<Mutex<StorageChanges>>,
    _marker: core::marker::PhantomData<Column>,
}

impl<Column> Default for InMemoryStorage<Column> {
    fn default() -> Self {
        Self {
            storage: Arc::new(Mutex::new(Changes::default().into())),
            _marker: Default::default(),
        }
    }
}

impl<Column> KeyValueInspect for InMemoryStorage<Column>
where
    Column: StorageColumn,
{
    type Column = Column;

    fn get(&self, key: &[u8], column: Self::Column) -> StorageResult<Option<Value>> {
        let storage = self.storage.lock().expect("Storage lock poisoned");

        let StorageChanges::Changes(changes) = storage.deref() else {
            panic!("It is only single changes storage");
        };

        let value = changes
            .get(&column.id())
            .and_then(|entries| entries.get(key).cloned())
            .and_then(|w| match w {
                WriteOperation::Insert(value) => Some(value),
                WriteOperation::Remove => None,
            });
        Ok(value)
    }
}

impl<Column> IterableStore for InMemoryStorage<Column>
where
    Column: StorageColumn,
{
    fn iter_store(
        &self,
        column: Self::Column,
        prefix: Option<&[u8]>,
        start: Option<&[u8]>,
        direction: IterDirection,
    ) -> BoxedIter<'_, KVItem> {
        let storage = self.storage.lock().expect("Storage lock poisoned");
        ChangesIterator::new(&storage)
            .iter_store(column, prefix, start, direction)
            .collect::<Vec<_>>()
            .into_iter()
            .into_boxed()
    }

    fn iter_store_keys(
        &self,
        column: Self::Column,
        prefix: Option<&[u8]>,
        start: Option<&[u8]>,
        direction: IterDirection,
    ) -> BoxedIter<'_, KeyItem> {
        let storage = self.storage.lock().expect("Storage lock poisoned");
        ChangesIterator::new(&storage)
            .iter_store_keys(column, prefix, start, direction)
            .collect::<Vec<_>>()
            .into_iter()
            .into_boxed()
    }
}

impl<Column> CommitLazyChanges for InMemoryStorage<Column>
where
    Column: StorageColumn,
{
    fn commit_changes(&mut self, changes: Changes) -> anyhow::Result<()> {
        let mut storage = self.storage.lock().expect("Storage lock poisoned");
        let StorageChanges::Changes(storage_changes) = storage.deref_mut() else {
            panic!("It is only single changes storage");
        };

        for (column, value) in changes {
            for (key, value) in value {
                match value {
                    WriteOperation::Insert(value) => {
                        storage_changes
                            .entry(column)
                            .or_default()
                            .insert(key, WriteOperation::Insert(value));
                    }
                    WriteOperation::Remove => {
                        storage_changes.entry(column).or_default().remove(&key);
                    }
                }
            }
        }
        Ok(())
    }
}
