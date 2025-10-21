#![deny(unused_crate_dependencies)]
#![deny(warnings)]

use fuel_core_storage::{
    iter::BoxedIter,
    kv_store::KeyValueInspect,
    transactional::{
        Changes,
        IntoTransaction,
        Modifiable,
        StorageTransaction,
    },
};
use std::ops::{
    Deref,
    DerefMut,
};

pub mod in_memory_storage;
#[cfg(feature = "rocksdb")]
pub mod rocksdb;

pub trait CommitLazyChanges {
    fn commit_changes(&mut self, change: Changes) -> anyhow::Result<()>;
}

pub struct LazyStorage<S> {
    inner: Option<StorageTransaction<S>>,
}

impl<S> LazyStorage<S>
where
    S: KeyValueInspect,
    S: CommitLazyChanges,
{
    pub fn commit(&mut self) -> anyhow::Result<()> {
        let inner = self
            .inner
            .take()
            .expect("`LazyStorage` guarantees that it is not `None`; qed");
        let (mut storage, changes) = inner.into_inner();
        let result = storage.commit_changes(changes);

        self.inner = Some(storage.into_transaction());

        result
    }

    pub fn reset(&mut self) -> anyhow::Result<()> {
        let inner = self
            .inner
            .take()
            .expect("`LazyStorage` guarantees that it is not `None`; qed");
        let (storage, _) = inner.into_inner();

        self.inner = Some(storage.into_transaction());

        Ok(())
    }

    pub fn add_changes(&mut self, changes: Changes) {
        let inner = self
            .inner
            .as_mut()
            .expect("`LazyStorage` guarantees that it is not `None`; qed");
        Modifiable::commit_changes(inner, changes)
            .expect("The policy is `ConflictPolicy::Overwrite`, so it should not fail");
    }
}

impl<S> Deref for LazyStorage<S> {
    type Target = StorageTransaction<S>;

    fn deref(&self) -> &Self::Target {
        self.inner
            .as_ref()
            .expect("LazyStorage guarantees that it is not None; qed")
    }
}

impl<S> DerefMut for LazyStorage<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner
            .as_mut()
            .expect("LazyStorage guarantees that it is not None; qed")
    }
}

impl<S> LazyStorage<S>
where
    S: KeyValueInspect,
{
    pub fn new(inner: S) -> Self {
        let inner = Some(inner.into_transaction());

        Self { inner }
    }
}

pub struct StorageIterator<S, T> {
    storage: Option<*mut S>,
    iter: Option<BoxedIter<'static, T>>,
}

// SAFETY: We don't modify `storage`, so value behind the pointer is safe to be shared between threads
unsafe impl<S, T> Send for StorageIterator<S, T> where S: Send {}
unsafe impl<S, T> Sync for StorageIterator<S, T> where S: Sync {}

impl<S, T> Drop for StorageIterator<S, T> {
    fn drop(&mut self) {
        self.iter = None;
        let pointer = self
            .storage
            .take()
            .expect("Storage pointer always should exist here; qed");

        unsafe {
            // Reconstruct the Box to properly drop the storage.
            let _ = Box::from_raw(pointer);
        }
    }
}

impl<S, T> StorageIterator<S, T>
where
    S: 'static,
{
    pub fn from<F>(storage: S, get_iterator: F) -> Self
    where
        F: FnOnce(&'static S) -> BoxedIter<'static, T>,
    {
        // Move storage to the heap, and leak it to get static reference.
        let storage = Box::new(storage);
        let storage = Box::into_raw(storage);

        // SAFETY: We just leaked the storage, so it is safe to create a static reference.
        let iter = unsafe { get_iterator(&*storage) };
        Self {
            storage: Some(storage),
            iter: Some(iter),
        }
    }
}

impl<S, T> Iterator for StorageIterator<S, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(iter) = self.iter.as_mut() {
            iter.next()
        } else {
            None
        }
    }
}
