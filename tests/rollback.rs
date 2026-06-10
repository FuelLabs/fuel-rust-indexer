//! Verifies that `CommitLazyChanges::commit_changes` records the block
//! height alongside each commit so fuel-core's historical-rocksdb layer can
//! roll the database back step by step. Drives both the receipts-manager
//! and events-manager storages, commits 10 blocks of data with non-trivial
//! per-height writes, then rolls back one block at a time and re-checks the
//! state after each step.
//!
//! If commit_changes ever regresses to passing an empty height list to
//! fuel-core (the pre-fix behaviour) this test will fail on the first
//! `rollback_last_block` call with `Database doesn't have a height to
//! rollback`.

use fuel_core::state::{
    historical_rocksdb::StateRewindPolicy,
    rocks_db::{
        ColumnsPolicy,
        DatabaseConfig,
    },
};
use fuel_core_storage::{
    StorageAsMut,
    StorageAsRef,
    transactional::{
        IntoTransaction,
        ReadTransaction,
    },
};
use fuel_core_types::{
    fuel_tx::{
        Bytes32,
        Receipt,
        TxPointer,
    },
    fuel_types::BlockHeight,
};
use fuel_events_manager::{
    port::StorableEvent,
    rocksdb::open_database as open_events_database,
    service::TransactionEvents,
    storage::{
        Events,
        LastCheckpoint as EventsLastCheckpoint,
    },
};
use fuel_indexer_types::events::{
    ExecutionStatus,
    TransactionReceipts,
};
use fuel_receipts_manager::{
    rocksdb::open_database as open_receipts_database,
    storage::{
        LastCheckpoint as ReceiptsLastCheckpoint,
        Receipts,
    },
};
use fuel_storage_utils::CommitLazyChanges;
use std::{
    num::NonZeroU64,
    sync::Arc,
};

const N: u32 = 10;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct TestEvent {
    block: u32,
    idx: u32,
}

impl StorableEvent for TestEvent {}

fn db_config() -> DatabaseConfig {
    DatabaseConfig {
        cache_capacity: None,
        max_fds: 512,
        columns_policy: ColumnsPolicy::Lazy,
    }
}

fn rewind_policy() -> StateRewindPolicy {
    StateRewindPolicy::RewindRange {
        size: NonZeroU64::new(N as u64).unwrap(),
    }
}

fn make_receipts(block: u32) -> TransactionReceipts {
    // Deterministic, height-dependent fixture so each block's payload is
    // unique and we can tell rollback actually undid the right thing.
    let mut tx_id_bytes = [0u8; 32];
    tx_id_bytes[..4].copy_from_slice(&block.to_be_bytes());
    TransactionReceipts {
        tx_pointer: TxPointer::new(BlockHeight::from(block), 0),
        tx_id: Bytes32::from(tx_id_bytes),
        receipts: Arc::new(vec![Receipt::Return {
            id: Default::default(),
            val: block as u64,
            pc: 0,
            is: 0,
        }]),
        execution_status: ExecutionStatus::Success,
    }
}

fn make_events(block: u32) -> Vec<TransactionEvents<TestEvent>> {
    let mut tx_id_bytes = [0u8; 32];
    tx_id_bytes[..4].copy_from_slice(&block.to_be_bytes());
    vec![TransactionEvents {
        tx_id: Bytes32::from(tx_id_bytes),
        tx_pointer: TxPointer::new(BlockHeight::from(block), 0),
        events: vec![TestEvent { block, idx: 0 }],
    }]
}

#[tokio::test]
async fn rollback__step_by_step_undoes_each_commit() {
    let receipts_dir = tempdir::TempDir::new("rollback-receipts").unwrap();
    let events_dir = tempdir::TempDir::new("rollback-events").unwrap();

    let mut receipts_db =
        open_receipts_database(receipts_dir.path(), rewind_policy(), db_config())
            .expect("receipts db open");
    let mut events_db =
        open_events_database(events_dir.path(), rewind_policy(), db_config())
            .expect("events db open");

    // --- commit 10 blocks ---
    for block in 1..=N {
        let height = BlockHeight::from(block);

        // receipts side
        let mut tx = (&receipts_db).into_transaction();
        tx.storage_as_mut::<Receipts>()
            .insert(&height, &vec![make_receipts(block)])
            .expect("insert receipts");
        tx.storage_as_mut::<ReceiptsLastCheckpoint>()
            .insert(&(), &height)
            .expect("insert receipts checkpoint");
        receipts_db
            .commit_changes(tx.into_changes())
            .expect("commit receipts");

        // events side
        let mut tx = (&events_db).into_transaction();
        tx.storage_as_mut::<Events<TestEvent>>()
            .insert(&height, &make_events(block))
            .expect("insert events");
        tx.storage_as_mut::<EventsLastCheckpoint>()
            .insert(&(), &height)
            .expect("insert events checkpoint");
        events_db
            .commit_changes(tx.into_changes())
            .expect("commit events");
    }

    // Sanity at the tip.
    assert_checkpoint(&receipts_db, &events_db, Some(N));
    for block in 1..=N {
        assert_block_present(&receipts_db, &events_db, block);
    }

    // --- roll back one block at a time ---
    for block in (1..=N).rev() {
        receipts_db.rollback_last_block().unwrap_or_else(|e| {
            panic!("receipts rollback at height {block} failed: {e}")
        });
        events_db
            .rollback_last_block()
            .unwrap_or_else(|e| panic!("events rollback at height {block} failed: {e}"));

        let new_tip = block.checked_sub(1).filter(|h| *h > 0);

        // After rolling back height `block`:
        //   * the LastCheckpoint should match `new_tip` (or be absent at 0)
        //   * the data inserted at `block` should be gone
        //   * the data at lower heights should still be present
        assert_checkpoint(&receipts_db, &events_db, new_tip);
        assert_block_absent(&receipts_db, &events_db, block);
        for earlier in 1..block {
            assert_block_present(&receipts_db, &events_db, earlier);
        }
    }
}

fn assert_checkpoint(
    receipts_db: &fuel_receipts_manager::rocksdb::Storage,
    events_db: &fuel_events_manager::rocksdb::Storage,
    expected: Option<u32>,
) {
    let receipts_cp = receipts_db
        .read_transaction()
        .storage_as_ref::<ReceiptsLastCheckpoint>()
        .get(&())
        .expect("read receipts checkpoint")
        .map(|h| **h);
    let events_cp = events_db
        .read_transaction()
        .storage_as_ref::<EventsLastCheckpoint>()
        .get(&())
        .expect("read events checkpoint")
        .map(|h| **h);
    assert_eq!(receipts_cp, expected, "receipts LastCheckpoint mismatch");
    assert_eq!(events_cp, expected, "events LastCheckpoint mismatch");
}

fn assert_block_present(
    receipts_db: &fuel_receipts_manager::rocksdb::Storage,
    events_db: &fuel_events_manager::rocksdb::Storage,
    block: u32,
) {
    let height = BlockHeight::from(block);

    let receipts_tx = receipts_db.read_transaction();
    let stored = receipts_tx
        .storage_as_ref::<Receipts>()
        .get(&height)
        .expect("read receipts")
        .expect("receipts present");
    assert_eq!(stored.as_slice(), &[make_receipts(block)]);

    let events_tx = events_db.read_transaction();
    let stored = events_tx
        .storage_as_ref::<Events<TestEvent>>()
        .get(&height)
        .expect("read events")
        .expect("events present");
    assert_eq!(stored.as_slice(), make_events(block).as_slice());
}

fn assert_block_absent(
    receipts_db: &fuel_receipts_manager::rocksdb::Storage,
    events_db: &fuel_events_manager::rocksdb::Storage,
    block: u32,
) {
    let height = BlockHeight::from(block);
    assert!(
        receipts_db
            .read_transaction()
            .storage_as_ref::<Receipts>()
            .get(&height)
            .expect("read receipts")
            .is_none(),
        "receipts at {block} should be rolled back"
    );
    assert!(
        events_db
            .read_transaction()
            .storage_as_ref::<Events<TestEvent>>()
            .get(&height)
            .expect("read events")
            .is_none(),
        "events at {block} should be rolled back"
    );
}
