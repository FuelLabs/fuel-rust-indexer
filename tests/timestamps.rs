//! Regression test for checkpoint timestamps on the unstable event stream.
//!
//! Every checkpoint the stream yields must carry its block header's timestamp,
//! never zero. Historical checkpoints — replayed from the events storage, which
//! persists only events and no block header — used to arrive with a zero
//! timestamp and relied on a fragile downstream backfill. The events manager
//! now stamps them from the receipts storage via `port::BlockTimestamps`. This
//! drives a real fuel-core node end-to-end so a regression back to zero fails
//! here.

use fuel_core::{
    service::{
        Config,
        FuelService,
        ServiceTrait,
    },
    state::{
        historical_rocksdb::StateRewindPolicy,
        rocks_db::{
            ColumnsPolicy,
            DatabaseConfig,
        },
    },
};
use fuel_core_types::fuel_types::BlockHeight;
use fuel_event_streams::{
    fuel_events_manager::{
        port::StorableEvent,
        service::UnstableEvent,
    },
    service::Config as StreamsConfig,
    try_parse_events,
};
use fuels::{
    core::codec::DecoderConfig,
    prelude::Provider,
    tx::Receipt,
};
use futures::StreamExt;
use std::time::Duration;
use url::Url;

fuels::prelude::abigen!(Contract(
    name = "OrderBook",
    abi = "artifacts/order-book-abi.json"
));

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
enum Event {
    Created { timestamp: u64 },
    Matched { timestamp: u64 },
}

impl StorableEvent for Event {}

fn parse_o2_logs(decoder: DecoderConfig, receipt: &Receipt) -> Option<Event> {
    try_parse_events!(
        [decoder, receipt]
        OrderCreatedEvent => |event| {
            Some(Event::Created {
                timestamp: event.timestamp.unix,
            })
        },
        OrderMatchedEvent => |event| {
            Some(Event::Matched {
                timestamp: event.timestamp.unix,
            })
        }
    )
}

#[tokio::test]
async fn unstable_stream__historical_checkpoints_carry_block_timestamp() {
    const BLOCKS: u32 = 5;

    // Given: a running indexer against a fresh node.
    let node = FuelService::new_node(Config::local_node()).await.unwrap();
    let url = Url::parse(format!("http://{}", node.bound_address).as_str()).unwrap();

    let temp_dir = tempdir::TempDir::new("database").unwrap();
    let database_config = DatabaseConfig {
        cache_capacity: None,
        max_fds: 512,
        columns_policy: ColumnsPolicy::Lazy,
    };

    let indexer = fuel_event_streams::service::new_logs_streams(
        parse_o2_logs,
        temp_dir.path().to_path_buf(),
        StateRewindPolicy::NoRewind,
        database_config,
        // Follow finalized blocks (not preconfirmations).
        StreamsConfig::new(0u32.into(), false, vec![url.clone()]),
    )
    .unwrap();
    indexer.start_and_await().await.unwrap();

    // Produce several committed blocks (each with a real header timestamp)
    // while the indexer is live, so it ingests and persists them.
    let provider = Provider::connect(url.as_str()).await.unwrap();
    provider.produce_blocks(BLOCKS, None).await.unwrap();

    // Wait until every produced block has been indexed, so subscribing from
    // genesis serves the whole range from storage — the historical path under
    // test, which is where the zero-timestamp bug lived.
    tokio::time::timeout(
        Duration::from_secs(60),
        indexer
            .shared
            .events()
            .await_height(BlockHeight::from(BLOCKS)),
    )
    .await
    .expect("indexer did not catch up to the produced tip in time")
    .unwrap();

    // When: we replay the events manager's unstable stream from genesis. This
    // is the layer that owns the fix — it must stamp the checkpoints it
    // replays from its own storage, without relying on any downstream backfill.
    let mut stream = indexer
        .shared
        .events()
        .unstable_events_starting_from(0u32.into())
        .await
        .unwrap();

    // Then: every checkpoint up to the tip carries a non-zero timestamp.
    let mut highest_checkpoint = 0u32;
    while highest_checkpoint < BLOCKS {
        let event = tokio::time::timeout(Duration::from_secs(30), stream.next())
            .await
            .expect("timed out waiting for the next checkpoint")
            .expect("stream ended before reaching the produced tip")
            .expect("stream yielded an error");

        if let UnstableEvent::Checkpoint(checkpoint) = event {
            let height = u32::from(checkpoint.block_height);
            assert!(
                checkpoint.timestamp > 0,
                "checkpoint at height {height} has a zero timestamp"
            );
            highest_checkpoint = highest_checkpoint.max(height);
        }
    }
}
