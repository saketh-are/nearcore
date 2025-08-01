//! Chain Client Configuration
use crate::ExternalStorageLocation::GCS;
use crate::MutableConfigValue;
use bytesize::ByteSize;
#[cfg(feature = "schemars")]
use near_parameters::view::Rational32SchemarsProvider;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::{
    AccountId, BlockHeight, BlockHeightDelta, Gas, NumBlocks, NumSeats, ShardId,
};
use near_primitives::version::Version;
use near_time::Duration;
#[cfg(feature = "schemars")]
use near_time::{DurationAsStdSchemaProvider, DurationSchemarsProvider};
use num_rational::Rational32;
use std::cmp::{max, min};
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

pub const TEST_STATE_SYNC_TIMEOUT: i64 = 5;

#[derive(Debug, Copy, Clone, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum LogSummaryStyle {
    #[serde(rename = "plain")]
    Plain,
    #[serde(rename = "colored")]
    Colored,
}

/// Minimum number of epochs for which we keep store data
pub const MIN_GC_NUM_EPOCHS_TO_KEEP: u64 = 3;

/// Default number of epochs for which we keep store data
pub const DEFAULT_GC_NUM_EPOCHS_TO_KEEP: u64 = 5;

/// Default number of concurrent requests to external storage to fetch state parts.
pub const DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_EXTERNAL: u8 = 25;
pub const DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_ON_CATCHUP_EXTERNAL: u8 = 5;

/// The default number of attempts to obtain a state part from peers in the network
/// before giving up and downloading it from external storage.
pub const DEFAULT_EXTERNAL_STORAGE_FALLBACK_THRESHOLD: u64 = 3;

/// Describes the expected behavior of the node regarding shard tracking.
/// If the node is an active validator, it will also track the shards it is responsible for as a validator.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum TrackedShardsConfig {
    /// Tracks no shards (light client).
    NoShards,
    /// Tracks arbitrary shards.
    Shards(Vec<ShardUId>),
    /// Tracks all shards.
    AllShards,
    /// Tracks shards that are assigned to given validator account.
    ShadowValidator(AccountId),
    /// Rotate between these sets of tracked shards.
    /// Used to simulate the behavior of chunk only producers without staking tokens.
    Schedule(Vec<Vec<ShardId>>),
    /// Tracks shards that contain one of the given account.
    Accounts(Vec<AccountId>),
}

impl TrackedShardsConfig {
    pub fn new_empty() -> Self {
        TrackedShardsConfig::NoShards
    }

    pub fn tracks_all_shards(&self) -> bool {
        matches!(self, TrackedShardsConfig::AllShards)
    }

    pub fn tracks_any_account(&self) -> bool {
        if let TrackedShardsConfig::Accounts(accounts) = &self {
            return !accounts.is_empty();
        }
        false
    }

    /// For backward compatibility, we support `tracked_shards`, `tracked_shard_schedule`,
    /// `tracked_shadow_validator`, and `tracked_accounts` as separate configuration fields,
    /// in that order of priority.
    pub fn from_deprecated_config_values(
        tracked_shards: &Option<Vec<ShardId>>,
        tracked_shard_schedule: &Option<Vec<Vec<ShardId>>>,
        tracked_shadow_validator: &Option<AccountId>,
        tracked_accounts: &Option<Vec<AccountId>>,
    ) -> Self {
        if let Some(tracked_shards) = tracked_shards {
            // Historically, a non-empty `tracked_shards` list indicated tracking all shards, regardless of its contents.
            // For more details, see https://github.com/near/nearcore/pull/4668.
            if !tracked_shards.is_empty() {
                return TrackedShardsConfig::AllShards;
            }
        }
        if let Some(tracked_shard_schedule) = tracked_shard_schedule {
            if !tracked_shard_schedule.is_empty() {
                return TrackedShardsConfig::Schedule(tracked_shard_schedule.clone());
            }
        }
        if let Some(validator_id) = tracked_shadow_validator {
            return TrackedShardsConfig::ShadowValidator(validator_id.clone());
        }
        if let Some(accounts) = tracked_accounts {
            return TrackedShardsConfig::Accounts(accounts.clone());
        }
        TrackedShardsConfig::NoShards
    }
}

/// Configuration for garbage collection.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct GCConfig {
    /// Maximum number of blocks to garbage collect at every garbage collection
    /// call.
    pub gc_blocks_limit: NumBlocks,

    /// Maximum number of height to go through at each garbage collection step
    /// when cleaning forks during garbage collection.
    pub gc_fork_clean_step: u64,

    /// Number of epochs for which we keep store data.
    pub gc_num_epochs_to_keep: u64,

    /// How often gc should be run
    #[serde(with = "near_time::serde_duration_as_std")]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationAsStdSchemaProvider"))]
    pub gc_step_period: Duration,
}

impl Default for GCConfig {
    // Garbage Collection should be faster than the block production. As a rule
    // o thumb it should be set to be two times faster, plus a small margin. At
    // the current min block time of 600ms that means 2 blocks per 500ms.
    fn default() -> Self {
        Self {
            gc_blocks_limit: 2,
            gc_fork_clean_step: 100,
            gc_num_epochs_to_keep: DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            gc_step_period: Duration::milliseconds(500),
        }
    }
}

impl GCConfig {
    pub fn gc_num_epochs_to_keep(&self) -> u64 {
        max(MIN_GC_NUM_EPOCHS_TO_KEEP, self.gc_num_epochs_to_keep)
    }
}

fn default_num_concurrent_requests() -> u8 {
    DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_EXTERNAL
}

fn default_num_concurrent_requests_during_catchup() -> u8 {
    DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_ON_CATCHUP_EXTERNAL
}

fn default_external_storage_fallback_threshold() -> u64 {
    DEFAULT_EXTERNAL_STORAGE_FALLBACK_THRESHOLD
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ExternalStorageConfig {
    /// Location of state parts.
    pub location: ExternalStorageLocation,
    /// When fetching state parts from external storage, throttle fetch requests
    /// to this many concurrent requests.
    #[serde(default = "default_num_concurrent_requests")]
    pub num_concurrent_requests: u8,
    /// During catchup, the node will use a different number of concurrent requests
    /// to reduce the performance impact of state sync.
    #[serde(default = "default_num_concurrent_requests_during_catchup")]
    pub num_concurrent_requests_during_catchup: u8,
    /// The number of attempts the node will make to obtain a part from peers in
    /// the network before it fetches from external storage.
    #[serde(default = "default_external_storage_fallback_threshold")]
    pub external_storage_fallback_threshold: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ExternalStorageLocation {
    S3 {
        /// Location of state dumps on S3.
        bucket: String,
        /// Data may only be available in certain locations.
        region: String,
    },
    Filesystem {
        root_dir: PathBuf,
    },
    GCS {
        bucket: String,
    },
}

/// Configures how to dump state to external storage.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DumpConfig {
    /// Specifies where to write the obtained state parts.
    pub location: ExternalStorageLocation,
    /// Use in case a node that dumps state to the external storage
    /// gets in trouble.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restart_dump_for_shards: Option<Vec<ShardId>>,
    /// How often to check if a new epoch has started.
    /// Feel free to set to `None`, defaults are sensible.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    #[serde(with = "near_time::serde_opt_duration_as_std")]
    #[cfg_attr(feature = "schemars", schemars(with = "Option<DurationAsStdSchemaProvider>"))]
    pub iteration_delay: Option<Duration>,
    /// Location of a json file with credentials allowing write access to the bucket.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credentials_file: Option<PathBuf>,
}

/// Configures how to fetch state parts during state sync.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum SyncConfig {
    /// Syncs state from the peers without reading anything from external storage.
    Peers,
    /// Expects parts to be available in external storage.
    ///
    /// Usually as a fallback after some number of attempts to use peers.
    ExternalStorage(ExternalStorageConfig),
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self::Peers
    }
}

impl SyncConfig {
    /// Checks whether the object equals its default value.
    fn is_default(&self) -> bool {
        matches!(self, Self::Peers)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Copy, Clone, Debug, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct SyncConcurrency {
    /// Maximum number of "apply parts" tasks that can be performed in parallel.
    /// This is a very disk-heavy task and therefore we set this to a low limit,
    /// or else the rocksdb contention makes the whole server freeze up.
    pub apply: u8,
    /// Maximum number of "apply parts" tasks that can be performed in parallel
    /// during catchup. We set this to a very low value to avoid overloading the
    /// node while it is still performing normal tasks.
    pub apply_during_catchup: u8,
    /// Maximum number of outstanding requests for decentralized state sync.
    pub peer_downloads: u8,
    /// The maximum parallelism to use per shard. This is mostly for fairness, because
    /// the actual rate limiting is done by the TaskTrackers, but this is useful for
    /// balancing the shards a little.
    pub per_shard: u8,
}

impl Default for SyncConcurrency {
    fn default() -> Self {
        const NUM_CONCURRENT_REQUESTS_FOR_PEERS: u8 = 10;
        const NUM_CONCURRENT_REQUESTS_FOR_COMPUTATION: u8 = 4;
        const NUM_CONCURRENT_REQUESTS_FOR_COMPUTATION_DURING_CATCHUP: u8 = 1;
        const MAX_PARALLELISM_PER_SHARD_FOR_FAIRNESS: u8 = 6;
        Self {
            apply: NUM_CONCURRENT_REQUESTS_FOR_COMPUTATION,
            apply_during_catchup: NUM_CONCURRENT_REQUESTS_FOR_COMPUTATION_DURING_CATCHUP,
            peer_downloads: NUM_CONCURRENT_REQUESTS_FOR_PEERS,
            per_shard: MAX_PARALLELISM_PER_SHARD_FOR_FAIRNESS,
        }
    }
}

impl SyncConcurrency {
    fn is_default(&self) -> bool {
        PartialEq::eq(&Self::default(), self)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct StateSyncConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// `none` value disables state dump to external storage.
    pub dump: Option<DumpConfig>,
    #[serde(skip_serializing_if = "SyncConfig::is_default", default = "SyncConfig::default")]
    pub sync: SyncConfig,
    #[serde(
        skip_serializing_if = "SyncConcurrency::is_default",
        default = "SyncConcurrency::default"
    )]
    pub concurrency: SyncConcurrency,
}

impl StateSyncConfig {
    pub fn gcs_default() -> Self {
        Self {
            sync: SyncConfig::ExternalStorage(ExternalStorageConfig {
                location: GCS { bucket: "state-parts".to_string() },
                num_concurrent_requests: DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_EXTERNAL,
                num_concurrent_requests_during_catchup:
                    DEFAULT_STATE_SYNC_NUM_CONCURRENT_REQUESTS_ON_CATCHUP_EXTERNAL,
                external_storage_fallback_threshold: DEFAULT_EXTERNAL_STORAGE_FALLBACK_THRESHOLD,
            }),
            ..Default::default()
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct EpochSyncConfig {
    /// If true, even if the node started from genesis, it will not perform epoch sync.
    /// There should be no reason to set this flag in production, because on both mainnet
    /// and testnet it would be infeasible to catch up from genesis without epoch sync.
    #[serde(default)]
    pub disable_epoch_sync_for_bootstrapping: bool,
    /// If true, the node will ignore epoch sync requests from the network. It is strongly
    /// recommended not to set this flag, because it will prevent other nodes from
    /// bootstrapping. This flag is only included as a kill-switch and may be removed in a
    /// future release. Please note that epoch sync requests are heavily rate limited and
    /// cached, and therefore should not affect the performance of the node or introduce
    /// any non-negligible increase in network traffic.
    #[serde(default)]
    pub ignore_epoch_sync_network_requests: bool,
    /// This serves as two purposes: (1) the node will not epoch sync and instead resort to
    /// header sync, if the genesis block is within this many blocks from the current block;
    /// (2) the node will reject an epoch sync proof if the provided proof is for an epoch
    /// that is more than this many blocks behind the current block.
    pub epoch_sync_horizon: BlockHeightDelta,
    /// Timeout for epoch sync requests. The node will continue retrying indefinitely even
    /// if this timeout is exceeded.
    #[serde(with = "near_time::serde_duration_as_std")]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationAsStdSchemaProvider"))]
    pub timeout_for_epoch_sync: Duration,
}

impl Default for EpochSyncConfig {
    fn default() -> Self {
        Self {
            disable_epoch_sync_for_bootstrapping: false,
            ignore_epoch_sync_network_requests: false,
            // Mainnet is 43200 blocks per epoch, so let's default to epoch sync if
            // we're more than 5 epochs behind, and we accept proofs up to 2 epochs old.
            // (Epoch sync should not be picking a target epoch more than 2 epochs old.)
            epoch_sync_horizon: 216000,
            timeout_for_epoch_sync: Duration::seconds(60),
        }
    }
}

// A handle that allows the main process to interrupt resharding if needed.
// This typically happens when the main process is interrupted.
#[derive(Clone, Debug)]
pub struct ReshardingHandle {
    keep_going: Arc<AtomicBool>,
}

impl ReshardingHandle {
    pub fn new() -> Self {
        Self { keep_going: Arc::new(AtomicBool::new(true)) }
    }

    pub fn get(&self) -> bool {
        self.keep_going.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn stop(&self) -> () {
        self.keep_going.store(false, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        !self.get()
    }
}

/// Configuration for resharding.
#[derive(serde::Serialize, serde::Deserialize, Clone, Copy, Debug, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(default)]
pub struct ReshardingConfig {
    /// The soft limit on the size of a single batch. The batch size can be
    /// decreased if resharding is consuming too many resources and interfering
    /// with regular node operation.
    #[cfg_attr(feature = "schemars", schemars(with = "ByteSizeSchemarsProvider"))]
    pub batch_size: ByteSize,

    /// The delay between writing batches to the db. The batch delay can be
    /// increased if resharding is consuming too many resources and interfering
    /// with regular node operation.
    #[serde(with = "near_time::serde_duration_as_std")]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationAsStdSchemaProvider"))]
    pub batch_delay: Duration,

    /// The delay between attempts to start resharding while waiting for the
    /// state snapshot to become available.
    /// UNUSED in ReshardingV3.
    #[serde(with = "near_time::serde_duration_as_std")]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationAsStdSchemaProvider"))]
    pub retry_delay: Duration,

    /// The delay between the resharding request is received and when the actor
    /// actually starts working on it. This delay should only be used in tests.
    /// UNUSED in ReshardingV3.
    #[serde(with = "near_time::serde_duration_as_std")]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationAsStdSchemaProvider"))]
    pub initial_delay: Duration,

    /// The maximum time that the actor will wait for the snapshot to be ready,
    /// before starting resharding. Do not wait indefinitely since we want to
    /// report error early enough for the node maintainer to have time to recover.
    /// UNUSED in ReshardingV3.
    #[serde(with = "near_time::serde_duration_as_std")]
    #[cfg_attr(feature = "schemars", schemars(with = "DurationAsStdSchemaProvider"))]
    pub max_poll_time: Duration,

    /// The number of blocks applied in a single batch during shard catch up.
    /// This value can be decreased if resharding is consuming too many
    /// resources and interfering with regular node operation.
    pub catch_up_blocks: BlockHeightDelta,
}

impl Default for ReshardingConfig {
    fn default() -> Self {
        // Conservative default for a slower resharding that puts as little
        // extra load on the node as possible.
        Self {
            batch_size: ByteSize::kb(500),
            batch_delay: Duration::milliseconds(5),
            retry_delay: Duration::seconds(10),
            initial_delay: Duration::seconds(0),
            // The snapshot typically is available within a minute from the
            // epoch start. Set the default higher in case we need to wait for
            // state sync.
            max_poll_time: Duration::seconds(2 * 60 * 60), // 2 hours
            catch_up_blocks: 20,
        }
    }
}

impl ReshardingConfig {
    pub fn test() -> Self {
        Self { batch_delay: Duration::ZERO, ..ReshardingConfig::default() }
    }
}

pub fn default_header_sync_initial_timeout() -> Duration {
    Duration::seconds(10)
}

pub fn default_header_sync_progress_timeout() -> Duration {
    Duration::seconds(2)
}

pub fn default_header_sync_stall_ban_timeout() -> Duration {
    Duration::seconds(120)
}

pub fn default_state_sync_external_timeout() -> Duration {
    Duration::seconds(60)
}

pub fn default_state_sync_p2p_timeout() -> Duration {
    Duration::seconds(10)
}

pub fn default_state_sync_retry_backoff() -> Duration {
    Duration::seconds(1)
}

pub fn default_state_sync_external_backoff() -> Duration {
    Duration::seconds(60)
}

pub fn default_chunk_wait_mult() -> Rational32 {
    Rational32::new(1, 6)
}

pub fn default_header_sync_expected_height_per_second() -> u64 {
    10
}

pub fn default_sync_check_period() -> Duration {
    Duration::seconds(10)
}

pub fn default_sync_max_block_requests() -> usize {
    10
}

pub fn default_sync_step_period() -> Duration {
    Duration::milliseconds(10)
}

pub fn default_sync_height_threshold() -> u64 {
    1
}

pub fn default_epoch_sync() -> Option<EpochSyncConfig> {
    Some(EpochSyncConfig::default())
}

pub fn default_state_sync_enabled() -> bool {
    true
}

pub fn default_view_client_threads() -> usize {
    4
}

pub fn default_chunk_validation_threads() -> usize {
    4
}

pub fn default_log_summary_period() -> Duration {
    Duration::seconds(10)
}

pub fn default_state_request_throttle_period() -> Duration {
    Duration::seconds(30)
}

pub fn default_state_requests_per_throttle_period() -> usize {
    30
}

pub fn default_state_request_server_threads() -> usize {
    default_view_client_threads()
}

pub fn default_trie_viewer_state_size_limit() -> Option<u64> {
    Some(50_000)
}

pub fn default_transaction_pool_size_limit() -> Option<u64> {
    Some(100_000_000) // 100 MB.
}

pub fn default_tx_routing_height_horizon() -> BlockHeightDelta {
    4
}

pub fn default_enable_multiline_logging() -> Option<bool> {
    Some(true)
}

pub fn default_produce_chunk_add_transactions_time_limit() -> Option<Duration> {
    Some(Duration::milliseconds(200))
}

/// Returns the default size of the OrphanStateWitnessPool, ie. the maximum number of
/// state-witnesses that can be accommodated in OrphanStateWitnessPool.
pub fn default_orphan_state_witness_pool_size() -> usize {
    // With 5 shards, a capacity of 25 witnesses allows to store 5 orphan witnesses per shard.
    25
}

/// Returns the default value for maximum data-size (bytes) for a state witness to be included in
/// the OrphanStateWitnessPool.
pub fn default_orphan_state_witness_max_size() -> ByteSize {
    ByteSize::mb(40)
}

/// Returns the default value for the thread count associated with rpc-handler actor (currently
/// handling incoming transactions and chunk endorsement validations).
/// In the benchmarks no performance gains were observed when increasing the number of threads
/// above half of available cores.
pub fn default_rpc_handler_thread_count() -> usize {
    std::thread::available_parallelism().unwrap_or(NonZero::new(16 as usize).unwrap()).get() / 2
}

/// Config for the Chunk Distribution Network feature.
/// This allows nodes to push and pull chunks from a central stream.
/// The two benefits of this approach are: (1) less request/response traffic
/// on the peer-to-peer network and (2) lower latency for RPC nodes indexing the chain.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ChunkDistributionNetworkConfig {
    pub enabled: bool,
    pub uris: ChunkDistributionUris,
}

/// URIs for the Chunk Distribution Network feature.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ChunkDistributionUris {
    /// URI for pulling chunks from the stream.
    pub get: String,
    /// URI for publishing chunks to the stream.
    pub set: String,
}

/// ClientConfig where some fields can be updated at runtime.
#[derive(Clone, serde::Serialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ClientConfig {
    /// Version of the binary.
    pub version: Version,
    /// Chain id for status.
    pub chain_id: String,
    /// Listening rpc port for status.
    pub rpc_addr: Option<String>,
    /// Graceful shutdown at expected block height.
    pub expected_shutdown: MutableConfigValue<Option<BlockHeight>>,
    /// Duration to check for producing / skipping block.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub block_production_tracking_delay: Duration,
    /// Minimum duration before producing block.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub min_block_production_delay: Duration,
    /// Maximum wait for approvals before producing block.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub max_block_production_delay: Duration,
    /// Maximum duration before skipping given height.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub max_block_wait_delay: Duration,
    /// Multiplier for the wait time for all chunks to be received.
    #[cfg_attr(feature = "schemars", schemars(with = "Rational32SchemarsProvider"))]
    pub chunk_wait_mult: Rational32,
    /// Skip waiting for sync (for testing or single node testnet).
    pub skip_sync_wait: bool,
    /// How often to check that we are not out of sync.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub sync_check_period: Duration,
    /// While syncing, how long to check for each step.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub sync_step_period: Duration,
    /// Sync height threshold: below this difference in height don't start syncing.
    pub sync_height_threshold: BlockHeightDelta,
    /// Maximum number of block requests to send to peers to sync
    pub sync_max_block_requests: usize,
    /// How much time to wait after initial header sync
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub header_sync_initial_timeout: Duration,
    /// How much time to wait after some progress is made in header sync
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub header_sync_progress_timeout: Duration,
    /// How much time to wait before banning a peer in header sync if sync is too slow
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub header_sync_stall_ban_timeout: Duration,
    /// Expected increase of header head height per second during header sync
    pub header_sync_expected_height_per_second: u64,
    /// How long to wait for a response from centralized state sync
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub state_sync_external_timeout: Duration,
    /// How long to wait for a response from p2p state sync
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub state_sync_p2p_timeout: Duration,
    /// How long to wait after a failed state sync request
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub state_sync_retry_backoff: Duration,
    /// Additional waiting period after a failed request to external storage
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub state_sync_external_backoff: Duration,
    /// Minimum number of peers to start syncing.
    pub min_num_peers: usize,
    /// Period between logging summary information.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub log_summary_period: Duration,
    /// Enable coloring of the logs
    pub log_summary_style: LogSummaryStyle,
    /// Produce empty blocks, use `false` for testing.
    pub produce_empty_blocks: bool,
    /// Epoch length.
    pub epoch_length: BlockHeightDelta,
    /// Number of block producer seats
    pub num_block_producer_seats: NumSeats,
    /// Time to persist Accounts Id in the router without removing them.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub ttl_account_id_router: Duration,
    /// Horizon at which instead of fetching block, fetch full state.
    pub block_fetch_horizon: BlockHeightDelta,
    /// Time between check to perform catchup.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub catchup_step_period: Duration,
    /// Time between checking to re-request chunks.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub chunk_request_retry_period: Duration,
    /// Time between running doomslug timer.
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    pub doomslug_step_period: Duration,
    /// Behind this horizon header fetch kicks in.
    pub block_header_fetch_horizon: BlockHeightDelta,
    /// Garbage collection configuration.
    pub gc: GCConfig,
    pub tracked_shards_config: TrackedShardsConfig,
    /// Not clear old data, set `true` for archive nodes.
    pub archive: bool,
    /// save_trie_changes should be set to true iff
    /// - archive if false - non-archival nodes need trie changes to perform garbage collection
    /// - archive is true, cold_store is configured and migration to split_storage is finished - node
    /// working in split storage mode needs trie changes in order to do garbage collection on hot.
    pub save_trie_changes: bool,
    /// Whether to persist transaction outcomes to disk or not.
    pub save_tx_outcomes: bool,
    /// Number of threads for ViewClientActor pool.
    pub view_client_threads: usize,
    /// Number of threads for ChunkValidationActor pool.
    pub chunk_validation_threads: usize,
    /// Number of seconds between state requests for view client.
    /// Throttling window for state requests (headers and parts).
    #[cfg_attr(feature = "schemars", schemars(with = "DurationSchemarsProvider"))]
    #[serde(alias = "view_client_throttle_period")]
    pub state_request_throttle_period: Duration,
    /// Maximum number of state requests served per throttle period
    #[serde(alias = "view_client_num_state_requests_per_throttle_period")]
    pub state_requests_per_throttle_period: usize,
    /// Number of threads for StateRequestActor pool.
    pub state_request_server_threads: usize,
    /// Upper bound of the byte size of contract state that is still viewable. None is no limit
    pub trie_viewer_state_size_limit: Option<u64>,
    /// Max burnt gas per view method.  If present, overrides value stored in
    /// genesis file.  The value only affects the RPCs without influencing the
    /// protocol thus changing it per-node doesn’t affect the blockchain.
    pub max_gas_burnt_view: Option<Gas>,
    /// Re-export storage layer statistics as prometheus metrics.
    pub enable_statistics_export: bool,
    /// Number of threads to execute background migration work in client.
    pub client_background_migration_threads: usize,
    /// Whether to use the State Sync mechanism.
    /// If disabled, the node will do Block Sync instead of State Sync.
    pub state_sync_enabled: bool,
    /// Options for syncing state.
    pub state_sync: StateSyncConfig,
    /// Options for epoch sync.
    pub epoch_sync: EpochSyncConfig,
    /// Limit of the size of per-shard transaction pool measured in bytes. If not set, the size
    /// will be unbounded.
    pub transaction_pool_size_limit: Option<u64>,
    // Allows more detailed logging, for example a list of orphaned blocks.
    pub enable_multiline_logging: bool,
    // Configuration for resharding.
    pub resharding_config: MutableConfigValue<ReshardingConfig>,
    /// If the node is not a chunk producer within that many blocks, then route
    /// to upcoming chunk producers.
    pub tx_routing_height_horizon: BlockHeightDelta,
    /// Limit the time of adding transactions to a chunk.
    /// A node produces a chunk by adding transactions from the transaction pool until
    /// some limit is reached. This time limit ensures that adding transactions won't take
    /// longer than the specified duration, which helps to produce the chunk quickly.
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub produce_chunk_add_transactions_time_limit: MutableConfigValue<Option<Duration>>,
    /// Optional config for the Chunk Distribution Network feature.
    /// If set to `None` then this node does not participate in the Chunk Distribution Network.
    /// Nodes not participating will still function fine, but possibly with higher
    /// latency due to the need of requesting chunks over the peer-to-peer network.
    pub chunk_distribution_network: Option<ChunkDistributionNetworkConfig>,
    /// OrphanStateWitnessPool keeps instances of ChunkStateWitness which can't be processed
    /// because the previous block isn't available. The witnesses wait in the pool until the
    /// required block appears. This variable controls how many witnesses can be stored in the pool.
    pub orphan_state_witness_pool_size: usize,
    /// Maximum size of state witnesses in the OrphanStateWitnessPool.
    ///
    /// We keep only orphan witnesses which are smaller than this size.
    /// This limits the maximum memory usage of OrphanStateWitnessPool.
    #[cfg_attr(feature = "schemars", schemars(with = "ByteSizeSchemarsProvider"))]
    pub orphan_state_witness_max_size: ByteSize,
    /// Save observed instances of ChunkStateWitness to the database in DBCol::LatestChunkStateWitnesses.
    /// Saving the latest witnesses is useful for analysis and debugging.
    /// This option can cause extra load on the database and is not recommended for production use.
    pub save_latest_witnesses: bool,
    /// Save observed instances of invalid ChunkStateWitness to the database in DBCol::InvalidChunkStateWitnesses.
    /// Saving invalid witnesses is useful for analysis and debugging.
    /// This option can cause extra load on the database and is not recommended for production use.
    pub save_invalid_witnesses: bool,
    pub transaction_request_handler_threads: usize,
}

impl ClientConfig {
    pub fn test(
        skip_sync_wait: bool,
        min_block_prod_time: u64,
        max_block_prod_time: u64,
        num_block_producer_seats: NumSeats,
        archive: bool,
        save_trie_changes: bool,
        state_sync_enabled: bool,
    ) -> Self {
        assert!(
            archive || save_trie_changes,
            "Configuration with archive = false and save_trie_changes = false is not supported \
            because non-archival nodes must save trie changes in order to do garbage collection."
        );

        Self {
            version: Default::default(),
            chain_id: "unittest".to_string(),
            rpc_addr: Some("0.0.0.0:3030".to_string()),
            expected_shutdown: MutableConfigValue::new(None, "expected_shutdown"),
            block_production_tracking_delay: Duration::milliseconds(std::cmp::max(
                10,
                min_block_prod_time / 5,
            ) as i64),
            min_block_production_delay: Duration::milliseconds(min_block_prod_time as i64),
            max_block_production_delay: Duration::milliseconds(max_block_prod_time as i64),
            max_block_wait_delay: Duration::milliseconds(3 * min_block_prod_time as i64),
            chunk_wait_mult: Rational32::new(1, 6),
            skip_sync_wait,
            sync_check_period: Duration::milliseconds(100),
            sync_step_period: Duration::milliseconds(10),
            sync_height_threshold: 1,
            sync_max_block_requests: 10,
            header_sync_initial_timeout: Duration::seconds(10),
            header_sync_progress_timeout: Duration::seconds(2),
            header_sync_stall_ban_timeout: Duration::seconds(30),
            state_sync_external_timeout: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            state_sync_p2p_timeout: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            state_sync_retry_backoff: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            state_sync_external_backoff: Duration::seconds(TEST_STATE_SYNC_TIMEOUT),
            header_sync_expected_height_per_second: 1,
            min_num_peers: 1,
            log_summary_period: Duration::seconds(10),
            produce_empty_blocks: true,
            epoch_length: 10,
            num_block_producer_seats,
            ttl_account_id_router: Duration::seconds(60 * 60),
            block_fetch_horizon: 50,
            catchup_step_period: Duration::milliseconds(100),
            chunk_request_retry_period: min(
                Duration::milliseconds(100),
                Duration::milliseconds(min_block_prod_time as i64 / 5),
            ),
            doomslug_step_period: Duration::milliseconds(100),
            block_header_fetch_horizon: 50,
            gc: GCConfig { gc_blocks_limit: 100, ..GCConfig::default() },
            tracked_shards_config: TrackedShardsConfig::NoShards,
            archive,
            save_trie_changes,
            save_tx_outcomes: true,
            log_summary_style: LogSummaryStyle::Colored,
            view_client_threads: 1,
            chunk_validation_threads: 1,
            state_request_throttle_period: Duration::seconds(1),
            state_requests_per_throttle_period: 30,
            state_request_server_threads: 1,
            trie_viewer_state_size_limit: None,
            max_gas_burnt_view: None,
            enable_statistics_export: true,
            client_background_migration_threads: 1,
            state_sync_enabled,
            state_sync: StateSyncConfig::default(),
            epoch_sync: EpochSyncConfig::default(),
            transaction_pool_size_limit: None,
            enable_multiline_logging: false,
            resharding_config: MutableConfigValue::new(
                ReshardingConfig::default(),
                "resharding_config",
            ),
            tx_routing_height_horizon: 4,
            produce_chunk_add_transactions_time_limit: MutableConfigValue::new(
                default_produce_chunk_add_transactions_time_limit(),
                "produce_chunk_add_transactions_time_limit",
            ),
            chunk_distribution_network: None,
            orphan_state_witness_pool_size: default_orphan_state_witness_pool_size(),
            orphan_state_witness_max_size: default_orphan_state_witness_max_size(),
            save_latest_witnesses: false,
            save_invalid_witnesses: false,
            transaction_request_handler_threads: default_rpc_handler_thread_count(),
        }
    }
}

#[cfg(feature = "schemars")]
pub type ByteSizeSchemarsProvider = u64;
