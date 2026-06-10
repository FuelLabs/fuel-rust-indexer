//! Live test against an external fuel-core node with the protobuf RPC
//! enabled. Marked `#[ignore]` so it never runs in CI by default — invoke
//! explicitly:
//!
//! ```sh
//! FUEL_GRAPHQL_URL=http://localhost:4000 \
//! FUEL_RPC_URL=http://localhost:4001 \
//! cargo test -p integration-tests --features fuel-receipts-manager/rpc \
//!     -- --ignored rpc_sync
//! ```

use fuel_core_types::fuel_types::BlockHeight;
use fuel_receipts_manager::{
    adapters::multi_source_fetcher::{
        MultiSourceFetcher,
        MultiSourceRpcConfig,
    },
    port::Fetcher,
};
use futures::StreamExt;
use std::num::NonZeroUsize;
use url::Url;

const DEFAULT_START_HEIGHT: u32 = 31_000_000;
const DEFAULT_COUNT: u32 = 100;

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_url(name: &str, default: &str) -> Url {
    let raw = std::env::var(name).unwrap_or_else(|_| default.to_string());
    Url::parse(&raw).expect("invalid URL")
}

#[tokio::test]
#[ignore = "requires a live fuel-core with protobuf RPC enabled"]
async fn rpc_sync__fetches_100_blocks_from_31m() {
    let graphql_url = env_url("FUEL_GRAPHQL_URL", "http://localhost:4000");
    let rpc_url = env_url("FUEL_RPC_URL", "http://localhost:4001");
    let start = env_u32("RPC_SYNC_START", DEFAULT_START_HEIGHT);
    let count = env_u32("RPC_SYNC_COUNT", DEFAULT_COUNT);

    let batch = env_u32("RPC_SYNC_BATCH", 10) as usize;
    let conc = env_u32("RPC_SYNC_CONCURRENCY", 10) as usize;

    let fetcher = MultiSourceFetcher::new_rpc(MultiSourceRpcConfig {
        main_graphql_urls: vec![graphql_url],
        main_rpc_url: rpc_url,
        subscription_sources: vec![],
        heartbeat_capacity: NonZeroUsize::new(1024).unwrap(),
        event_capacity: NonZeroUsize::new(1024).unwrap(),
        blocks_request_batch_size: batch,
        blocks_request_concurrency: conc,
        pending_blocks_limit: 1024,
    })
    .await
    .expect("failed to build RPC fetcher");

    let last = fetcher
        .last_height()
        .await
        .expect("get_aggregated_height failed");
    assert!(
        *last >= start + count - 1,
        "node aggregator height {last} is below requested range \
         {start}..={}",
        start + count - 1
    );

    let end = start + count - 1;
    let started_at = std::time::Instant::now();
    let stream = fetcher.finalized_blocks_for_range(start..=end);
    futures::pin_mut!(stream);

    let mut received = Vec::with_capacity(count as usize);
    while let Some(block) = stream.next().await {
        let block = block.expect("RPC block range yielded error");
        received.push(block);
        if received.len() == count as usize {
            break;
        }
    }

    assert_eq!(
        received.len(),
        count as usize,
        "expected {count} blocks, got {}",
        received.len()
    );

    let mut expected = BlockHeight::from(start);
    for block in &received {
        assert_eq!(
            *block.header.height(),
            expected,
            "block heights must be contiguous and start at {start}"
        );
        expected = expected.succ().unwrap();
    }

    let elapsed = started_at.elapsed();
    let total_txs: usize = received.iter().map(|b| b.statuses.len()).sum();
    println!(
        "fetched {} blocks ({}..={}) with {} total transactions in {:?} \
         (batch={batch}, concurrency={conc})",
        received.len(),
        start,
        end,
        total_txs,
        elapsed,
    );
}
