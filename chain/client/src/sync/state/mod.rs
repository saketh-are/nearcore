pub mod chain_requests;
mod downloader;
mod external;
mod network;
mod shard;
mod task_tracker;
mod util;

use crate::metrics;
use crate::sync::external::{ExternalConnection, create_bucket_readonly};
use chain_requests::ChainSenderForStateSync;
use downloader::StateSyncDownloader;
use external::StateSyncDownloadSourceExternal;
use futures::future::BoxFuture;
use near_async::futures::{FutureSpawner, FutureSpawnerExt};
use near_async::messaging::{AsyncSender, IntoSender};
use near_async::time::{Clock, Duration};
use near_chain::Chain;
use near_chain::types::RuntimeAdapter;
use near_chain_configs::{
    ExternalStorageConfig, ExternalStorageLocation, StateSyncConfig, SyncConcurrency, SyncConfig,
};
use near_client_primitives::types::{ShardSyncStatus, StateSyncStatus};
use near_epoch_manager::EpochManagerAdapter;
use near_network::types::{PeerManagerMessageRequest, PeerManagerMessageResponse};
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::state_sync::{ShardStateSyncResponse, ShardStateSyncResponseHeader};
use near_primitives::types::ShardId;
use near_store::Store;
use network::{StateSyncDownloadSourcePeer, StateSyncDownloadSourcePeerSharedState};
use parking_lot::Mutex;
use shard::{StateSyncShardHandle, run_state_sync_for_shard};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use task_tracker::{TaskHandle, TaskTracker};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError;
use tokio_util::sync::CancellationToken;

/// Module that manages state sync. Internally, it spawns multiple tasks to download state sync
/// headers and parts in parallel for the requested shards, but externally, all that it exposes
/// is a single `run` method that should be called periodically, returning that we're either
/// done or still in progress, while updating the externally visible status.
pub struct StateSync {
    store: Store,
    future_spawner: Arc<dyn FutureSpawner>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    runtime: Arc<dyn RuntimeAdapter>,

    /// We keep a reference to this so that peer messages received about state sync can be
    /// given to the StateSyncDownloadSourcePeer.
    peer_source_state: Arc<Mutex<StateSyncDownloadSourcePeerSharedState>>,

    /// The main downloading logic.
    downloader: Arc<StateSyncDownloader>,

    /// Internal parallelization limiters as well as status tracker. We need a handle here to
    /// export statuses of the workers to the debug page.
    downloading_task_tracker: TaskTracker,
    computation_task_tracker: TaskTracker,

    /// Multi-sender to handle requests that must be performed on the thread that owns the Chain.
    chain_requests_sender: ChainSenderForStateSync,

    /// There is one entry in this map for each shard that is being synced.
    shard_syncs: HashMap<(CryptoHash, ShardId), StateSyncShardHandle>,

    /// Concurrency limits.
    concurrency_config: SyncConcurrency,
}

impl StateSync {
    /// Note: `future_spawner` is used to spawn futures that perform state sync tasks.
    /// However, there is internal limiting of parallelization as well (to make sure
    /// that we do not overload rocksdb, peers, or external storage), so it is
    /// preferred to pass in a spawner that has a lot of concurrency.
    pub fn new(
        clock: Clock,
        store: Store,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        runtime: Arc<dyn RuntimeAdapter>,
        network_adapter: AsyncSender<PeerManagerMessageRequest, PeerManagerMessageResponse>,
        external_timeout: Duration,
        p2p_timeout: Duration,
        retry_backoff: Duration,
        external_backoff: Duration,
        chain_id: &str,
        sync_config: &StateSyncConfig,
        chain_requests_sender: ChainSenderForStateSync,
        future_spawner: Arc<dyn FutureSpawner>,
        catchup: bool,
    ) -> Self {
        let peer_source_state =
            Arc::new(Mutex::new(StateSyncDownloadSourcePeerSharedState::default()));
        let peer_source = Arc::new(StateSyncDownloadSourcePeer {
            clock: clock.clone(),
            store: store.clone(),
            request_sender: network_adapter,
            request_timeout: p2p_timeout,
            state: peer_source_state.clone(),
        }) as Arc<dyn StateSyncDownloadSource>;
        let (fallback_source, num_attempts_before_fallback, num_concurrent_requests) =
            if let SyncConfig::ExternalStorage(ExternalStorageConfig {
                location,
                num_concurrent_requests,
                num_concurrent_requests_during_catchup,
                external_storage_fallback_threshold,
            }) = &sync_config.sync
            {
                let external = match location {
                    ExternalStorageLocation::S3 { bucket, region, .. } => {
                        let bucket = create_bucket_readonly(
                            &bucket,
                            &region,
                            external_timeout.max(Duration::ZERO).unsigned_abs(),
                        );
                        if let Err(err) = bucket {
                            panic!("Failed to create an S3 bucket: {}", err);
                        }
                        ExternalConnection::S3 { bucket: Arc::new(bucket.unwrap()) }
                    }
                    ExternalStorageLocation::Filesystem { root_dir } => {
                        ExternalConnection::Filesystem { root_dir: root_dir.clone() }
                    }
                    ExternalStorageLocation::GCS { bucket, .. } => ExternalConnection::GCS {
                        gcs_client: Arc::new(
                            object_store::gcp::GoogleCloudStorageBuilder::from_env()
                                .with_bucket_name(bucket)
                                .build()
                                .unwrap(),
                        ),
                        reqwest_client: Arc::new(reqwest::Client::default()),
                        bucket: bucket.clone(),
                    },
                };
                let num_concurrent_requests = if catchup {
                    *num_concurrent_requests_during_catchup
                } else {
                    *num_concurrent_requests
                };
                let fallback_source = Arc::new(StateSyncDownloadSourceExternal {
                    clock: clock.clone(),
                    store: store.clone(),
                    chain_id: chain_id.to_string(),
                    conn: external,
                    timeout: external_timeout,
                    backoff: external_backoff,
                }) as Arc<dyn StateSyncDownloadSource>;
                (
                    Some(fallback_source),
                    *external_storage_fallback_threshold as usize,
                    num_concurrent_requests.min(sync_config.concurrency.peer_downloads),
                )
            } else {
                (None, 0, sync_config.concurrency.peer_downloads)
            };

        let downloading_task_tracker = TaskTracker::new(usize::from(num_concurrent_requests));
        let downloader = Arc::new(StateSyncDownloader {
            clock,
            store: store.clone(),
            preferred_source: peer_source,
            fallback_source,
            num_attempts_before_fallback,
            header_validation_sender: chain_requests_sender.clone().into_sender(),
            runtime: runtime.clone(),
            retry_backoff,
            task_tracker: downloading_task_tracker.clone(),
        });

        let num_concurrent_computations = if catchup {
            sync_config.concurrency.apply_during_catchup
        } else {
            sync_config.concurrency.apply
        };
        let computation_task_tracker = TaskTracker::new(usize::from(num_concurrent_computations));

        Self {
            store,
            peer_source_state,
            downloader,
            downloading_task_tracker,
            computation_task_tracker,
            future_spawner,
            epoch_manager,
            runtime,
            chain_requests_sender,
            shard_syncs: HashMap::new(),
            concurrency_config: sync_config.concurrency,
        }
    }

    /// Apply a state sync message received from a peer.
    pub fn apply_peer_message(
        &self,
        peer_id: PeerId,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        data: ShardStateSyncResponse,
    ) -> Result<(), near_chain::Error> {
        self.peer_source_state.lock().receive_peer_message(peer_id, shard_id, sync_hash, data)?;
        Ok(())
    }

    /// Main loop that should be called periodically.
    pub fn run(
        &mut self,
        sync_hash: CryptoHash,
        sync_status: &mut StateSyncStatus,
        tracking_shards: &[ShardId],
    ) -> Result<StateSyncResult, near_chain::Error> {
        let _span =
            tracing::debug_span!(target: "sync", "run_sync", sync_type = "StateSync").entered();
        tracing::debug!(%sync_hash, ?tracking_shards, "syncing state");

        let mut all_done = true;
        for shard_id in tracking_shards {
            let key = (sync_hash, *shard_id);
            let status = match self.shard_syncs.entry(key) {
                Entry::Occupied(mut entry) => match entry.get_mut().result.try_recv() {
                    Ok(result) => {
                        entry.remove();
                        if let Err(err) = result {
                            tracing::error!(%shard_id, ?err, "State sync failed for shard");
                            return Err(err);
                        }
                        ShardSyncStatus::StateSyncDone
                    }
                    Err(TryRecvError::Closed) => {
                        return Err(near_chain::Error::Other(
                            "Shard result channel somehow closed".to_owned(),
                        ));
                    }
                    Err(TryRecvError::Empty) => entry.get().status(),
                },
                Entry::Vacant(entry) => {
                    if sync_status
                        .sync_status
                        .get(&shard_id)
                        .is_some_and(|status| *status == ShardSyncStatus::StateSyncDone)
                    {
                        continue;
                    }
                    let status = Arc::new(Mutex::new(ShardSyncStatus::StateDownloadHeader));
                    let cancel = CancellationToken::new();
                    let shard_sync = run_state_sync_for_shard(
                        self.store.clone(),
                        *shard_id,
                        sync_hash,
                        self.downloader.clone(),
                        self.runtime.clone(),
                        self.epoch_manager.clone(),
                        self.computation_task_tracker.clone(),
                        status.clone(),
                        self.chain_requests_sender.clone().into_sender(),
                        cancel.clone(),
                        self.future_spawner.clone(),
                        self.concurrency_config.per_shard,
                    );
                    let (sender, receiver) = oneshot::channel();

                    self.future_spawner.spawn("shard sync", async move {
                        sender.send(shard_sync.await).ok();
                    });
                    let handle = StateSyncShardHandle { status, result: receiver, cancel };
                    let ret = handle.status();
                    entry.insert(handle);
                    ret
                }
            };
            sync_status.sync_status.insert(*shard_id, status);
            metrics::STATE_SYNC_STAGE
                .with_label_values(&[&shard_id.to_string()])
                .set(status as i64);
            if status != ShardSyncStatus::StateSyncDone {
                all_done = false;
            }
        }

        // If a shard completed syncing, we just remove it. We will not be syncing it again the next time around,
        // because we would've marked it as completed in the status for that shard.
        self.shard_syncs.retain(|(existing_sync_hash, existing_shard_id), _v| {
            tracking_shards.contains(existing_shard_id) && existing_sync_hash == &sync_hash
        });

        sync_status.download_tasks = self.downloading_task_tracker.statuses();
        sync_status.computation_tasks = self.computation_task_tracker.statuses();
        Ok(if all_done { StateSyncResult::Completed } else { StateSyncResult::InProgress })
    }
}

pub enum StateSyncResult {
    /// State sync still in progress. No action needed by the caller.
    InProgress,
    /// The state for all shards was downloaded.
    Completed,
}

/// Abstracts away the source of state sync headers and parts. Only one instance is kept per
/// state sync, NOT per shard.
pub(self) trait StateSyncDownloadSource: Send + Sync + 'static {
    fn download_shard_header(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<Result<ShardStateSyncResponseHeader, near_chain::Error>>;

    fn download_shard_part(
        &self,
        shard_id: ShardId,
        sync_hash: CryptoHash,
        part_id: u64,
        handle: Arc<TaskHandle>,
        cancel: CancellationToken,
    ) -> BoxFuture<Result<Vec<u8>, near_chain::Error>>;
}

/// Find the hash of the first block on the same epoch (and chain) of block with hash `sync_hash`.
pub fn get_epoch_start_sync_hash(
    chain: &Chain,
    sync_hash: &CryptoHash,
) -> Result<CryptoHash, near_chain::Error> {
    let mut header = chain.get_block_header(sync_hash)?;
    let mut epoch_id = *header.epoch_id();
    let mut hash = *header.hash();
    let mut prev_hash = *header.prev_hash();
    loop {
        if prev_hash == CryptoHash::default() {
            return Ok(hash);
        }
        header = chain.get_block_header(&prev_hash)?;
        if &epoch_id != header.epoch_id() {
            return Ok(hash);
        }
        epoch_id = *header.epoch_id();
        hash = *header.hash();
        prev_hash = *header.prev_hash();
    }
}
