use crate::download_file::{FileDownloadError, run_download_file};
use crate::dyn_config::LOG_CONFIG_FILENAME;
use anyhow::{Context, anyhow, bail};
use bytesize::ByteSize;
use near_async::time::{Clock, Duration};
use near_chain::runtime::NightshadeRuntime;
use near_chain_configs::test_utils::{
    TESTING_INIT_BALANCE, TESTING_INIT_STAKE, add_account_with_key, add_protocol_account,
    random_chain_id,
};
use near_chain_configs::{
    BLOCK_PRODUCER_KICKOUT_THRESHOLD, CHUNK_PRODUCER_KICKOUT_THRESHOLD,
    CHUNK_VALIDATOR_ONLY_KICKOUT_THRESHOLD, ChunkDistributionNetworkConfig, ClientConfig,
    EXPECTED_EPOCH_LENGTH, EpochSyncConfig, FAST_EPOCH_LENGTH, FISHERMEN_THRESHOLD,
    GAS_PRICE_ADJUSTMENT_RATE, GCConfig, GENESIS_CONFIG_FILENAME, Genesis, GenesisConfig,
    GenesisValidationMode, INITIAL_GAS_LIMIT, LogSummaryStyle, MAX_INFLATION_RATE,
    MIN_BLOCK_PRODUCTION_DELAY, MIN_GAS_PRICE, MutableConfigValue, MutableValidatorSigner,
    NEAR_BASE, NUM_BLOCK_PRODUCER_SEATS, NUM_BLOCKS_PER_YEAR, PROTOCOL_REWARD_RATE,
    PROTOCOL_UPGRADE_STAKE_THRESHOLD, ReshardingConfig, StateSyncConfig,
    TRANSACTION_VALIDITY_PERIOD, TrackedShardsConfig, default_chunk_validation_threads,
    default_chunk_wait_mult, default_enable_multiline_logging, default_epoch_sync,
    default_header_sync_expected_height_per_second, default_header_sync_initial_timeout,
    default_header_sync_progress_timeout, default_header_sync_stall_ban_timeout,
    default_log_summary_period, default_orphan_state_witness_max_size,
    default_orphan_state_witness_pool_size, default_produce_chunk_add_transactions_time_limit,
    default_state_request_server_threads, default_state_request_throttle_period,
    default_state_requests_per_throttle_period, default_state_sync_enabled,
    default_state_sync_external_backoff, default_state_sync_external_timeout,
    default_state_sync_p2p_timeout, default_state_sync_retry_backoff, default_sync_check_period,
    default_sync_height_threshold, default_sync_max_block_requests, default_sync_step_period,
    default_transaction_pool_size_limit, default_trie_viewer_state_size_limit,
    default_tx_routing_height_horizon, default_view_client_threads, get_initial_supply,
};
use near_config_utils::{DownloadConfigType, ValidationError, ValidationErrors};
use near_crypto::{InMemorySigner, KeyFile, KeyType, PublicKey, Signer};
use near_epoch_manager::EpochManagerHandle;
#[cfg(feature = "json_rpc")]
use near_jsonrpc::RpcConfig;
use near_network::PeerAddr;
use near_network::config::NetworkConfig;
use near_network::tcp;
use near_o11y::log_config::LogConfig;
use near_primitives::hash::CryptoHash;
use near_primitives::network::PeerId;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_test_signer;
use near_primitives::types::{
    AccountId, AccountInfo, Balance, BlockHeight, BlockHeightDelta, Gas, NumSeats, NumShards,
    ShardId,
};
use near_primitives::utils::{from_timestamp, get_num_seats_per_shard};
use near_primitives::validator_signer::{InMemoryValidatorSigner, ValidatorSigner};
use near_primitives::version::PROTOCOL_VERSION;
#[cfg(feature = "rosetta_rpc")]
use near_rosetta_rpc::RosettaRpcConfig;
use near_store::config::{
    CloudStorageConfig, STATE_SNAPSHOT_DIR, SplitStorageConfig, StateSnapshotType,
};
use near_store::{StateSnapshotConfig, Store, TrieConfig};
use near_telemetry::TelemetryConfig;
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use num_rational::Rational32;
use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{info, warn};

/// Millinear, 1/1000 of NEAR.
pub const MILLI_NEAR: Balance = NEAR_BASE / 1000;

/// Block production tracking delay.
pub const BLOCK_PRODUCTION_TRACKING_DELAY: i64 = 10;

/// Mainnet and testnet validators are configured with a different value due to
/// performance values.
pub const MAINNET_MIN_BLOCK_PRODUCTION_DELAY: i64 = 0_600;
pub const TESTNET_MIN_BLOCK_PRODUCTION_DELAY: i64 = 0_600;

/// Maximum time to delay block production without approvals is ms.
pub const MAX_BLOCK_PRODUCTION_DELAY: i64 = 2_000;

/// Mainnet and testnet validators are configured with a different value due to
/// performance values.
pub const MAINNET_MAX_BLOCK_PRODUCTION_DELAY: i64 = 1_800;
pub const TESTNET_MAX_BLOCK_PRODUCTION_DELAY: i64 = 1_800;

/// Maximum time until skipping the previous block is ms.
pub const MAX_BLOCK_WAIT_DELAY: i64 = 6_000;

/// Multiplier for the wait time for all chunks to be received.
pub const CHUNK_WAIT_DENOMINATOR: i32 = 3;

/// Horizon at which instead of fetching block, fetch full state.
const BLOCK_FETCH_HORIZON: BlockHeightDelta = 50;

/// Behind this horizon header fetch kicks in.
const BLOCK_HEADER_FETCH_HORIZON: BlockHeightDelta = 50;

/// Time between check to perform catchup.
const CATCHUP_STEP_PERIOD: i64 = 100;

/// Time between checking to re-request chunks.
const CHUNK_REQUEST_RETRY_PERIOD: i64 = 400;

/// Fast mode constants for testing/developing.
pub const FAST_MIN_BLOCK_PRODUCTION_DELAY: i64 = 120;
pub const FAST_MAX_BLOCK_PRODUCTION_DELAY: i64 = 500;

/// The minimum stake required for staking is last seat price divided by this number.
pub const MINIMUM_STAKE_DIVISOR: u64 = 10;

pub const CONFIG_FILENAME: &str = "config.json";
pub const NODE_KEY_FILE: &str = "node_key.json";
pub const VALIDATOR_KEY_FILE: &str = "validator_key.json";

pub const NETWORK_LEGACY_TELEMETRY_URL: &str = "https://explorer.{}.near.org/api/nodes";
pub const NETWORK_TELEMETRY_URL: &str = "https://telemetry.nearone.org/nodes";

fn default_doomslug_step_period() -> Duration {
    Duration::milliseconds(10)
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct Consensus {
    /// Minimum number of peers to start syncing.
    pub min_num_peers: usize,
    /// Duration to check for producing / skipping block.
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub block_production_tracking_delay: Duration,
    /// Minimum duration before producing block.
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub min_block_production_delay: Duration,
    /// Maximum wait for approvals before producing block.
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub max_block_production_delay: Duration,
    /// Maximum duration before skipping given height.
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub max_block_wait_delay: Duration,
    /// Multiplier for the wait time for all chunks to be received.
    #[serde(default = "default_chunk_wait_mult")]
    pub chunk_wait_mult: Rational32,
    /// Produce empty blocks, use `false` for testing.
    pub produce_empty_blocks: bool,
    /// Horizon at which instead of fetching block, fetch full state.
    pub block_fetch_horizon: BlockHeightDelta,
    /// Behind this horizon header fetch kicks in.
    pub block_header_fetch_horizon: BlockHeightDelta,
    /// Time between check to perform catchup.
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub catchup_step_period: Duration,
    /// Time between checking to re-request chunks.
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub chunk_request_retry_period: Duration,
    /// How much time to wait after initial header sync
    #[serde(default = "default_header_sync_initial_timeout")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub header_sync_initial_timeout: Duration,
    /// How much time to wait after some progress is made in header sync
    #[serde(default = "default_header_sync_progress_timeout")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub header_sync_progress_timeout: Duration,
    /// How much time to wait before banning a peer in header sync if sync is too slow
    #[serde(default = "default_header_sync_stall_ban_timeout")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub header_sync_stall_ban_timeout: Duration,
    /// How much to wait for a state sync response before re-requesting
    #[serde(default = "default_state_sync_external_timeout")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub state_sync_external_timeout: Duration,
    #[serde(default = "default_state_sync_p2p_timeout")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub state_sync_p2p_timeout: Duration,
    #[serde(default = "default_state_sync_retry_backoff")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub state_sync_retry_backoff: Duration,
    #[serde(default = "default_state_sync_external_backoff")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub state_sync_external_backoff: Duration,
    /// Expected increase of header head weight per second during header sync
    #[serde(default = "default_header_sync_expected_height_per_second")]
    pub header_sync_expected_height_per_second: u64,
    /// How frequently we check whether we need to sync
    #[serde(default = "default_sync_check_period")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub sync_check_period: Duration,
    /// During sync the time we wait before reentering the sync loop
    #[serde(default = "default_sync_step_period")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub sync_step_period: Duration,
    /// Time between running doomslug timer.
    #[serde(default = "default_doomslug_step_period")]
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub doomslug_step_period: Duration,
    #[serde(default = "default_sync_height_threshold")]
    pub sync_height_threshold: u64,
    /// Maximum number of block requests to send to peers to sync
    #[serde(default = "default_sync_max_block_requests")]
    pub sync_max_block_requests: usize,
}

impl Default for Consensus {
    fn default() -> Self {
        Consensus {
            min_num_peers: 3,
            block_production_tracking_delay: Duration::milliseconds(
                BLOCK_PRODUCTION_TRACKING_DELAY,
            ),
            min_block_production_delay: Duration::milliseconds(MIN_BLOCK_PRODUCTION_DELAY),
            max_block_production_delay: Duration::milliseconds(MAX_BLOCK_PRODUCTION_DELAY),
            max_block_wait_delay: Duration::milliseconds(MAX_BLOCK_WAIT_DELAY),
            chunk_wait_mult: Rational32::new(1, CHUNK_WAIT_DENOMINATOR),
            produce_empty_blocks: true,
            block_fetch_horizon: BLOCK_FETCH_HORIZON,
            block_header_fetch_horizon: BLOCK_HEADER_FETCH_HORIZON,
            catchup_step_period: Duration::milliseconds(CATCHUP_STEP_PERIOD),
            chunk_request_retry_period: Duration::milliseconds(CHUNK_REQUEST_RETRY_PERIOD),
            header_sync_initial_timeout: default_header_sync_initial_timeout(),
            header_sync_progress_timeout: default_header_sync_progress_timeout(),
            header_sync_stall_ban_timeout: default_header_sync_stall_ban_timeout(),
            state_sync_external_timeout: default_state_sync_external_timeout(),
            state_sync_p2p_timeout: default_state_sync_p2p_timeout(),
            state_sync_retry_backoff: default_state_sync_retry_backoff(),
            state_sync_external_backoff: default_state_sync_external_backoff(),
            header_sync_expected_height_per_second: default_header_sync_expected_height_per_second(
            ),
            sync_check_period: default_sync_check_period(),
            sync_step_period: default_sync_step_period(),
            doomslug_step_period: default_doomslug_step_period(),
            sync_height_threshold: default_sync_height_threshold(),
            sync_max_block_requests: default_sync_max_block_requests(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[serde(default)]
pub struct Config {
    pub genesis_file: String,
    pub genesis_records_file: Option<String>,
    pub validator_key_file: String,
    pub node_key_file: String,
    #[cfg(feature = "json_rpc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rpc: Option<RpcConfig>,
    #[cfg(feature = "rosetta_rpc")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rosetta_rpc: Option<RosettaRpcConfig>,
    #[cfg(feature = "tx_generator")]
    pub tx_generator: Option<near_transactions_generator::Config>,
    pub telemetry: TelemetryConfig,
    pub network: near_network::config_json::Config,
    pub consensus: Consensus,

    /// New field replacing the old tracked_* fields
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tracked_shards_config: Option<TrackedShardsConfig>,

    /// Deprecated: use `tracked_shards_config: { "Accounts": [...] }` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tracked_accounts: Option<Vec<AccountId>>,

    /// Deprecated: use `tracked_shards_config: { "ShadowValidator": "..." }` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tracked_shadow_validator: Option<AccountId>,

    /// Deprecated: use `tracked_shards_config: "AllShards"` or `tracked_shards_config: "NoShards"` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tracked_shards: Option<Vec<ShardId>>,

    /// Deprecated: use `tracked_shards_config: { "Schedule": [...] }` instead.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) tracked_shard_schedule: Option<Vec<Vec<ShardId>>>,

    #[serde(skip_serializing_if = "is_false")]
    pub archive: bool,
    /// If save_trie_changes is not set it will get inferred from the `archive` field as follows:
    /// save_trie_changes = !archive
    /// save_trie_changes should be set to true iff
    /// - archive is false - non-archival nodes need trie changes to perform garbage collection
    /// - archive is true and cold_store is configured - node working in split storage mode
    /// needs trie changes in order to do garbage collection on hot and populate cold State column.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub save_trie_changes: Option<bool>,
    /// Whether to persist `ExecutionOutcomeWithProof` objects into
    /// `DBCol::TransactionResultForBlock`.  Disabling this on validator
    /// nodes reduces write amplification.  RPC nodes keep it enabled so
    /// that transaction status queries continue to work.
    /// If set to `None`, the value will be treated as true if either
    /// - `archive` is true, or
    /// - All shards are tracked (i.e. node is an RPC node).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub save_tx_outcomes: Option<bool>,
    pub log_summary_style: LogSummaryStyle,
    #[serde(with = "near_async::time::serde_duration_as_std")]
    pub log_summary_period: Duration,
    // Allows more detailed logging, for example a list of orphaned blocks.
    pub enable_multiline_logging: Option<bool>,
    /// Garbage collection configuration.
    #[serde(flatten)]
    pub gc: GCConfig,
    pub view_client_threads: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_validation_threads: Option<usize>,
    /// Throttling window for state requests (headers and parts).
    #[serde(with = "near_async::time::serde_duration_as_std")]
    #[serde(alias = "view_client_throttle_period")]
    pub state_request_throttle_period: Duration,
    /// Maximum number of state requests served per throttle period
    #[serde(alias = "view_client_num_state_requests_per_throttle_period")]
    pub state_requests_per_throttle_period: usize,
    /// Number of threads for StateRequestActor pool.
    pub state_request_server_threads: usize,
    pub trie_viewer_state_size_limit: Option<u64>,
    /// If set, overrides value in genesis configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_gas_burnt_view: Option<Gas>,
    /// Different parameters to configure underlying storage.
    pub store: near_store::StoreConfig,
    /// Different parameters to configure underlying cold storage.
    /// This feature is under development, do not use in production.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cold_store: Option<near_store::StoreConfig>,
    /// Configuration for the split storage.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_storage: Option<SplitStorageConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloud_storage: Option<CloudStorageConfig>,
    /// The node will stop after the head exceeds this height.
    /// The node usually stops within several seconds after reaching the target height.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_shutdown: Option<BlockHeight>,
    /// Whether to use state sync (unreliable and corrupts the DB if fails) or do a block sync instead.
    pub state_sync_enabled: bool,
    /// Options for syncing state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_sync: Option<StateSyncConfig>,
    /// Options for epoch sync
    pub epoch_sync: Option<EpochSyncConfig>,
    /// Limit of the size of per-shard transaction pool measured in bytes. If not set, the size
    /// will be unbounded.
    ///
    /// New transactions that bring the size of the pool over this limit will be rejected. This
    /// guarantees that the node will use bounded resources to store incoming transactions.
    /// Setting this value too low (<1MB) on the validator might lead to production of smaller
    /// chunks and underutilized the capacity of the network.
    pub transaction_pool_size_limit: Option<u64>,
    // Configuration for resharding.
    pub resharding_config: ReshardingConfig,
    /// If the node is not a chunk producer within that many blocks, then route
    /// to upcoming chunk producers.
    pub tx_routing_height_horizon: BlockHeightDelta,
    /// Limit the time of adding transactions to a chunk.
    ///
    /// A node produces a chunk by adding transactions from the transaction pool until
    /// some limit is reached. This time limit ensures that adding transactions won't take
    /// longer than the specified duration, which helps to produce the chunk quickly.
    #[serde(default)]
    #[serde(with = "near_async::time::serde_opt_duration_as_std")]
    pub produce_chunk_add_transactions_time_limit: Option<Duration>,
    /// Optional config for the Chunk Distribution Network feature.
    ///
    /// If set to `None` then this node does not participate in the Chunk Distribution Network.
    /// Nodes not participating will still function fine, but possibly with higher
    /// latency due to the need of requesting chunks over the peer-to-peer network.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chunk_distribution_network: Option<ChunkDistributionNetworkConfig>,
    /// OrphanStateWitnessPool keeps instances of ChunkStateWitness which can't be processed
    /// because the previous block isn't available. The witnesses wait in the pool until the
    /// required block appears. This variable controls how many witnesses can be stored in the pool.
    pub orphan_state_witness_pool_size: usize,
    /// Maximum size (number of bytes) of state witnesses in the OrphanStateWitnessPool.
    ///
    /// We keep only orphan witnesses which are smaller than this size.
    /// This limits the maximum memory usage of OrphanStateWitnessPool.
    /// TODO(#10259) - consider merging this limit with the non-orphan witness size limit.
    pub orphan_state_witness_max_size: ByteSize,
    /// The number of the contracts kept loaded up for execution.
    ///
    /// Each loaded contract will increase the baseline memory use of the node appreciably.
    pub max_loaded_contracts: usize,
    /// Location (within `home_dir`) where to store a cache of compiled contracts.
    ///
    /// This cache can be manually emptied occasionally to keep storage usage down and does not
    /// need to be included into snapshots or dumps.
    pub contract_cache_path: PathBuf,
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

fn is_false(value: &bool) -> bool {
    !*value
}
impl Default for Config {
    fn default() -> Self {
        let store = near_store::StoreConfig::default();
        let contract_cache_path =
            [store.path.as_deref().unwrap_or_else(|| "data".as_ref()), "contract.cache".as_ref()]
                .into_iter()
                .collect();
        Config {
            genesis_file: GENESIS_CONFIG_FILENAME.to_string(),
            genesis_records_file: None,
            validator_key_file: VALIDATOR_KEY_FILE.to_string(),
            node_key_file: NODE_KEY_FILE.to_string(),
            #[cfg(feature = "json_rpc")]
            rpc: Some(RpcConfig::default()),
            #[cfg(feature = "rosetta_rpc")]
            rosetta_rpc: None,
            #[cfg(feature = "tx_generator")]
            tx_generator: None,
            telemetry: TelemetryConfig::default(),
            network: Default::default(),
            consensus: Consensus::default(),
            tracked_shards_config: None,
            tracked_accounts: None,
            tracked_shadow_validator: None,
            tracked_shards: None,
            tracked_shard_schedule: None,
            archive: false,
            save_trie_changes: None,
            save_tx_outcomes: None,
            log_summary_style: LogSummaryStyle::Colored,
            log_summary_period: default_log_summary_period(),
            gc: GCConfig::default(),
            view_client_threads: default_view_client_threads(),
            chunk_validation_threads: None,
            state_request_throttle_period: default_state_request_throttle_period(),
            state_requests_per_throttle_period: default_state_requests_per_throttle_period(),
            state_request_server_threads: default_state_request_server_threads(),
            trie_viewer_state_size_limit: default_trie_viewer_state_size_limit(),
            max_gas_burnt_view: None,
            store,
            cold_store: None,
            split_storage: None,
            cloud_storage: None,
            expected_shutdown: None,
            state_sync: None,
            epoch_sync: default_epoch_sync(),
            state_sync_enabled: default_state_sync_enabled(),
            transaction_pool_size_limit: default_transaction_pool_size_limit(),
            enable_multiline_logging: default_enable_multiline_logging(),
            resharding_config: ReshardingConfig::default(),
            tx_routing_height_horizon: default_tx_routing_height_horizon(),
            produce_chunk_add_transactions_time_limit:
                default_produce_chunk_add_transactions_time_limit(),
            chunk_distribution_network: None,
            orphan_state_witness_pool_size: default_orphan_state_witness_pool_size(),
            orphan_state_witness_max_size: default_orphan_state_witness_max_size(),
            max_loaded_contracts: 256,
            contract_cache_path,
            save_latest_witnesses: false,
            save_invalid_witnesses: false,
            transaction_request_handler_threads: 4,
        }
    }
}

impl Config {
    /// load Config from config.json without panic. Do semantic validation on field values.
    /// If config file issues occur, a ValidationError::ConfigFileError will be returned;
    /// If config semantic checks failed, a ValidationError::ConfigSemanticError will be returned
    pub fn from_file(path: &Path) -> Result<Self, ValidationError> {
        Self::from_file_skip_validation(path).and_then(|config| {
            config.validate()?;
            Ok(config)
        })
    }

    /// load Config from config.json without panic.
    /// Skips semantic validation on field values.
    /// This function should only return error for file issues.
    pub fn from_file_skip_validation(path: &Path) -> Result<Self, ValidationError> {
        let json_str =
            std::fs::read_to_string(path).map_err(|_| ValidationError::ConfigFileError {
                error_message: format!("Failed to read config from {}", path.display()),
            })?;
        let mut unrecognized_fields = Vec::new();
        let json_str_without_comments = near_config_utils::strip_comments_from_json_str(&json_str)
            .map_err(|_| ValidationError::ConfigFileError {
                error_message: format!("Failed to strip comments from {}", path.display()),
            })?;

        // Check for deprecated fields before deserialization to warn about them
        Self::check_for_deprecated_fields(&json_str_without_comments);

        let config: Config = serde_ignored::deserialize(
            &mut serde_json::Deserializer::from_str(&json_str_without_comments),
            |field| unrecognized_fields.push(field.to_string()),
        )
        .map_err(|e| ValidationError::ConfigFileError {
            error_message: format!("Failed to deserialize config from {}: {:?}", path.display(), e),
        })?;

        if !unrecognized_fields.is_empty() {
            let s = if unrecognized_fields.len() > 1 { "s" } else { "" };
            let fields = unrecognized_fields.join(", ");
            warn!(
                target: "neard",
                "{}: encountered unrecognized field{s}: {fields}",
                path.display(),
            );
        }

        Ok(config)
    }

    /// Return each deprecated key together with the preferred replacement.
    ///
    /// Empty `Vec` ⇒ no deprecated keys detected.
    /// NOTE: Keys are `'static` so the function itself does **zero** heap allocations
    /// beyond the output `Vec`.
    pub(crate) fn deprecated_fields_in_json(json_str: &str) -> Vec<(&'static str, &'static str)> {
        // Static table keeps the mapping in one obvious place
        const MAPPING: &[(&str, &str)] = &[
            ("view_client_throttle_period", "state_request_throttle_period"),
            (
                "view_client_num_state_requests_per_throttle_period",
                "state_requests_per_throttle_period",
            ),
        ];

        let Ok(value) = serde_json::from_str::<serde_json::Value>(json_str) else {
            return Vec::new(); // malformed JSON ⇒ nothing to warn about here
        };
        let Some(obj) = value.as_object() else {
            return Vec::new(); // not an object ⇒ nothing to match
        };

        MAPPING
            .iter()
            .filter(|(old, _)| obj.contains_key(*old))
            .map(|(old, new)| (*old, *new))
            .collect()
    }

    /// Check for deprecated configuration fields and emit warnings
    fn check_for_deprecated_fields(json_str: &str) {
        for (deprecated_key, new_key) in Self::deprecated_fields_in_json(json_str) {
            tracing::warn!(
                target: "neard",
                deprecated_key = %deprecated_key,
                new_key        = %new_key,
                "Deprecated config key detected – please migrate",
            );
        }
    }

    fn validate(&self) -> Result<(), ValidationError> {
        crate::config_validate::validate_config(self)
    }

    pub fn write_to_file(&self, path: &Path) -> std::io::Result<()> {
        let mut file = File::create(path)?;
        let str = serde_json::to_string_pretty(self)?;
        file.write_all(str.as_bytes())
    }

    pub fn rpc_addr(&self) -> Option<String> {
        #[cfg(feature = "json_rpc")]
        if let Some(rpc) = &self.rpc {
            return Some(rpc.addr.to_string());
        }
        None
    }

    pub fn set_rpc_addr(&mut self, addr: tcp::ListenerAddr) {
        #[cfg(feature = "json_rpc")]
        {
            self.rpc.get_or_insert(Default::default()).addr = addr;
        }
    }

    pub fn tracked_shards_config(&self) -> TrackedShardsConfig {
        if let Some(tracked_shards_config) = self.tracked_shards_config.clone() {
            return tracked_shards_config;
        }
        TrackedShardsConfig::from_deprecated_config_values(
            &self.tracked_shards,
            &self.tracked_shard_schedule,
            &self.tracked_shadow_validator,
            &self.tracked_accounts,
        )
    }
}

#[derive(Clone)]
pub struct NearConfig {
    pub config: Config,
    pub client_config: ClientConfig,
    #[cfg(feature = "tx_generator")]
    pub tx_generator: Option<near_transactions_generator::Config>,
    pub network_config: NetworkConfig,
    #[cfg(feature = "json_rpc")]
    pub rpc_config: Option<RpcConfig>,
    #[cfg(feature = "rosetta_rpc")]
    pub rosetta_rpc_config: Option<RosettaRpcConfig>,
    pub telemetry_config: TelemetryConfig,
    pub genesis: Genesis,
    /// Contains validator key for this node. This field is mutable and optional. Use with caution!
    /// Lock the value of mutable validator signer for the duration of a request to ensure consistency.
    /// Please note that the locked value should not be stored anywhere or passed through the thread boundary.
    pub validator_signer: MutableValidatorSigner,
}

impl NearConfig {
    pub fn new(
        config: Config,
        genesis: Genesis,
        network_key_pair: KeyFile,
        validator_signer: MutableValidatorSigner,
    ) -> anyhow::Result<Self> {
        Ok(NearConfig {
            config: config.clone(),
            client_config: ClientConfig {
                version: Default::default(),
                chain_id: genesis.config.chain_id.clone(),
                rpc_addr: config.rpc_addr(),
                expected_shutdown: MutableConfigValue::new(
                    config.expected_shutdown,
                    "expected_shutdown",
                ),
                block_production_tracking_delay: config.consensus.block_production_tracking_delay,
                min_block_production_delay: config.consensus.min_block_production_delay,
                max_block_production_delay: config.consensus.max_block_production_delay,
                max_block_wait_delay: config.consensus.max_block_wait_delay,
                chunk_wait_mult: config.consensus.chunk_wait_mult,
                skip_sync_wait: config.network.skip_sync_wait,
                sync_check_period: config.consensus.sync_check_period,
                sync_step_period: config.consensus.sync_step_period,
                sync_height_threshold: config.consensus.sync_height_threshold,
                sync_max_block_requests: config.consensus.sync_max_block_requests,
                header_sync_initial_timeout: config.consensus.header_sync_initial_timeout,
                header_sync_progress_timeout: config.consensus.header_sync_progress_timeout,
                header_sync_stall_ban_timeout: config.consensus.header_sync_stall_ban_timeout,
                header_sync_expected_height_per_second: config
                    .consensus
                    .header_sync_expected_height_per_second,
                state_sync_external_timeout: config.consensus.state_sync_external_timeout,
                state_sync_p2p_timeout: config.consensus.state_sync_p2p_timeout,
                state_sync_retry_backoff: config.consensus.state_sync_retry_backoff,
                state_sync_external_backoff: config.consensus.state_sync_external_backoff,
                min_num_peers: config.consensus.min_num_peers,
                log_summary_period: config.log_summary_period,
                produce_empty_blocks: config.consensus.produce_empty_blocks,
                epoch_length: genesis.config.epoch_length,
                num_block_producer_seats: genesis.config.num_block_producer_seats,
                ttl_account_id_router: config.network.ttl_account_id_router,
                // TODO(1047): this should be adjusted depending on the speed of sync of state.
                block_fetch_horizon: config.consensus.block_fetch_horizon,
                block_header_fetch_horizon: config.consensus.block_header_fetch_horizon,
                catchup_step_period: config.consensus.catchup_step_period,
                chunk_request_retry_period: config.consensus.chunk_request_retry_period,
                doomslug_step_period: config.consensus.doomslug_step_period,
                tracked_shards_config: config.tracked_shards_config(),
                archive: config.archive,
                save_trie_changes: config.save_trie_changes.unwrap_or(!config.archive),
                save_tx_outcomes: config.save_tx_outcomes.unwrap_or_else(|| {
                    config.archive
                        || config.tracked_shards_config() == TrackedShardsConfig::AllShards
                }),
                log_summary_style: config.log_summary_style,
                gc: config.gc,
                view_client_threads: config.view_client_threads,
                chunk_validation_threads: config
                    .chunk_validation_threads
                    .unwrap_or_else(default_chunk_validation_threads),
                state_request_throttle_period: config.state_request_throttle_period,
                state_requests_per_throttle_period: config.state_requests_per_throttle_period,
                state_request_server_threads: config.state_request_server_threads,
                trie_viewer_state_size_limit: config.trie_viewer_state_size_limit,
                max_gas_burnt_view: config.max_gas_burnt_view,
                enable_statistics_export: config.store.enable_statistics_export,
                client_background_migration_threads: 8,
                state_sync_enabled: config.state_sync_enabled,
                state_sync: config.state_sync.unwrap_or_default(),
                epoch_sync: config.epoch_sync.unwrap_or_default(),
                transaction_pool_size_limit: config.transaction_pool_size_limit,
                enable_multiline_logging: config.enable_multiline_logging.unwrap_or(true),
                resharding_config: MutableConfigValue::new(
                    config.resharding_config,
                    "resharding_config",
                ),
                tx_routing_height_horizon: config.tx_routing_height_horizon,
                produce_chunk_add_transactions_time_limit: MutableConfigValue::new(
                    config.produce_chunk_add_transactions_time_limit,
                    "produce_chunk_add_transactions_time_limit",
                ),
                chunk_distribution_network: config.chunk_distribution_network,
                orphan_state_witness_pool_size: config.orphan_state_witness_pool_size,
                orphan_state_witness_max_size: config.orphan_state_witness_max_size,
                save_latest_witnesses: config.save_latest_witnesses,
                save_invalid_witnesses: config.save_invalid_witnesses,
                transaction_request_handler_threads: config.transaction_request_handler_threads,
            },
            #[cfg(feature = "tx_generator")]
            tx_generator: config.tx_generator,
            network_config: NetworkConfig::new(
                config.network,
                network_key_pair.secret_key,
                validator_signer.clone(),
                config.archive,
            )?,
            telemetry_config: config.telemetry,
            #[cfg(feature = "json_rpc")]
            rpc_config: config.rpc,
            #[cfg(feature = "rosetta_rpc")]
            rosetta_rpc_config: config.rosetta_rpc,
            genesis,
            validator_signer,
        })
    }

    pub fn rpc_addr(&self) -> Option<String> {
        #[cfg(feature = "json_rpc")]
        if let Some(rpc) = &self.rpc_config {
            return Some(rpc.addr.to_string());
        }
        None
    }
}

impl NearConfig {
    /// Test tool to save configs back to the folder.
    /// Useful for dynamic creating testnet configs and then saving them in different folders.
    pub fn save_to_dir(&self, dir: &Path) {
        fs::create_dir_all(dir).expect("Failed to create directory");

        self.config.write_to_file(&dir.join(CONFIG_FILENAME)).expect("Error writing config");

        if let Some(validator_signer) = &self.validator_signer.get() {
            validator_signer
                .write_to_file(&dir.join(&self.config.validator_key_file))
                .expect("Error writing validator key file");
        }

        let network_signer = InMemorySigner::from_secret_key(
            "node".parse().unwrap(),
            self.network_config.node_key.clone(),
        );
        network_signer
            .write_to_file(&dir.join(&self.config.node_key_file))
            .expect("Error writing key file");

        self.genesis.to_file(dir.join(&self.config.genesis_file));
    }
}

#[easy_ext::ext(NightshadeRuntimeExt)]
impl NightshadeRuntime {
    pub fn from_config(
        home_dir: &Path,
        store: Store,
        config: &NearConfig,
        epoch_manager: Arc<EpochManagerHandle>,
    ) -> std::io::Result<Arc<NightshadeRuntime>> {
        #[allow(clippy::or_fun_call)] // Closure cannot return reference to a temporary value
        let state_snapshot_config =
            match config.config.store.state_snapshot_config.state_snapshot_type {
                StateSnapshotType::Enabled => StateSnapshotConfig::enabled(
                    home_dir,
                    config.config.store.path.as_ref().unwrap_or(&"data".into()),
                    STATE_SNAPSHOT_DIR,
                ),
                StateSnapshotType::Disabled => StateSnapshotConfig::Disabled,
            };
        // FIXME: this (and other contract runtime resources) should probably get constructed by
        // the caller and passed into this `NightshadeRuntime::from_config` here. But that's a big
        // refactor...
        let contract_cache = FilesystemContractRuntimeCache::with_memory_cache(
            home_dir,
            config.config.store.path.as_ref(),
            &config.config.contract_cache_path,
            config.config.max_loaded_contracts,
        )?;
        Ok(NightshadeRuntime::new(
            store,
            ContractRuntimeCache::handle(&contract_cache),
            &config.genesis.config,
            epoch_manager,
            config.client_config.trie_viewer_state_size_limit,
            config.client_config.max_gas_burnt_view,
            None,
            config.config.gc.gc_num_epochs_to_keep(),
            TrieConfig::from_store_config(&config.config.store),
            state_snapshot_config,
        ))
    }
}

/// Generates or loads a signer key from given file.
///
/// If the file already exists, loads the file (panicking if the file is
/// invalid), checks that account id in the file matches `account_id` if it’s
/// given and returns the key.  `test_seed` is ignored in this case.
///
/// If the file does not exist and `account_id` is not `None`, generates a new
/// key, saves it in the file and returns it.  If `test_seed` is not `None`, the
/// key generation algorithm is seeded with given string making it fully
/// deterministic.
fn generate_or_load_key(
    home_dir: &Path,
    filename: &str,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
) -> anyhow::Result<Option<Signer>> {
    let path = home_dir.join(filename);
    if path.exists() {
        let signer = InMemorySigner::from_file(&path)
            .with_context(|| format!("Failed initializing signer from {}", path.display()))?;
        if let Some(account_id) = account_id {
            if account_id != signer.get_account_id() {
                return Err(anyhow!(
                    "‘{}’ contains key for {} but expecting key for {}",
                    path.display(),
                    signer.get_account_id(),
                    account_id
                ));
            }
        }
        info!(target: "near", "Reusing key {} for {}", signer.public_key(), signer.get_account_id());
        Ok(Some(signer))
    } else if let Some(account_id) = account_id {
        let signer = if let Some(seed) = test_seed {
            InMemorySigner::from_seed(account_id, KeyType::ED25519, seed)
        } else {
            InMemorySigner::from_random(account_id, KeyType::ED25519).into()
        };
        info!(target: "near", "Using key {} for {}", signer.public_key(), signer.get_account_id());
        signer
            .write_to_file(&path)
            .with_context(|| anyhow!("Failed saving key to ‘{}’", path.display()))?;
        Ok(Some(signer))
    } else {
        Ok(None)
    }
}

/// Checks that validator and node keys exist.
/// If a key is needed and doesn't exist, then it gets created.
fn generate_or_load_keys(
    dir: &Path,
    config: &Config,
    chain_id: &str,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
) -> anyhow::Result<()> {
    generate_or_load_key(dir, &config.node_key_file, Some("node".parse().unwrap()), None)?;
    match chain_id {
        near_primitives::chains::MAINNET | near_primitives::chains::TESTNET => {
            generate_or_load_key(dir, &config.validator_key_file, account_id, None)?;
        }
        _ => {
            let account_id = account_id.unwrap_or_else(|| "test.near".parse().unwrap());
            generate_or_load_key(dir, &config.validator_key_file, Some(account_id), test_seed)?;
        }
    }
    Ok(())
}

fn set_block_production_delay(chain_id: &str, fast: bool, config: &mut Config) {
    match chain_id {
        near_primitives::chains::MAINNET => {
            config.consensus.min_block_production_delay =
                Duration::milliseconds(MAINNET_MIN_BLOCK_PRODUCTION_DELAY);
            config.consensus.max_block_production_delay =
                Duration::milliseconds(MAINNET_MAX_BLOCK_PRODUCTION_DELAY);
        }
        near_primitives::chains::TESTNET => {
            config.consensus.min_block_production_delay =
                Duration::milliseconds(TESTNET_MIN_BLOCK_PRODUCTION_DELAY);
            config.consensus.max_block_production_delay =
                Duration::milliseconds(TESTNET_MAX_BLOCK_PRODUCTION_DELAY);
        }
        _ => {
            if fast {
                config.consensus.min_block_production_delay =
                    Duration::milliseconds(FAST_MIN_BLOCK_PRODUCTION_DELAY);
                config.consensus.max_block_production_delay =
                    Duration::milliseconds(FAST_MAX_BLOCK_PRODUCTION_DELAY);
            }
        }
    }
}

/// Initializes Genesis, client Config, node and validator keys, and stores in the specified folder.
///
/// This method supports the following use cases:
/// * When no Config, Genesis or key files exist.
/// * When Config and Genesis files exist, but no keys exist.
/// * When all of Config, Genesis, and key files exist.
///
/// Note that this method does not support the case where the configuration file exists but the genesis file does not exist.
pub fn init_configs(
    dir: &Path,
    chain_id: Option<String>,
    account_id: Option<AccountId>,
    test_seed: Option<&str>,
    num_shards: NumShards,
    fast: bool,
    genesis: Option<&str>,
    should_download_genesis: bool,
    download_genesis_url: Option<&str>,
    download_records_url: Option<&str>,
    download_config_type: Option<DownloadConfigType>,
    download_config_url: Option<&str>,
    boot_nodes: Option<&str>,
    max_gas_burnt_view: Option<Gas>,
) -> anyhow::Result<()> {
    fs::create_dir_all(dir).with_context(|| anyhow!("Failed to create directory {:?}", dir))?;

    assert_ne!(chain_id, Some("".to_string()));
    let chain_id = chain_id.unwrap_or_else(random_chain_id);

    // Check if config already exists in home dir.
    if dir.join(CONFIG_FILENAME).exists() {
        let config = Config::from_file(&dir.join(CONFIG_FILENAME))
            .with_context(|| anyhow!("Failed to read config {}", dir.display()))?;
        let genesis_file = config.genesis_file.clone();
        let file_path = dir.join(&genesis_file);
        // Check that Genesis exists and can be read.
        // If `config.json` exists, but `genesis.json` doesn't exist,
        // that isn't supported by the `init` command.
        let _genesis = GenesisConfig::from_file(file_path).with_context(move || {
            anyhow!("Failed to read genesis config {}/{}", dir.display(), genesis_file)
        })?;
        // Check that `node_key.json` and `validator_key.json` exist.
        // Create if needed and they don't exist.
        generate_or_load_keys(dir, &config, &chain_id, account_id, test_seed)?;
        return Ok(());
    }

    let mut config = Config::default();
    // Make sure node tracks all shards, see
    // https://github.com/near/nearcore/issues/7388
    config.tracked_shards_config = Some(TrackedShardsConfig::AllShards);
    // If a config gets generated, block production times may need to be updated.
    set_block_production_delay(&chain_id, fast, &mut config);

    if let Some(url) = download_config_url {
        download_config(url, &dir.join(CONFIG_FILENAME))
            .context(format!("Failed to download the config file from {}", url))?;
        config = Config::from_file(&dir.join(CONFIG_FILENAME))?;
    } else if let Some(config_type) = download_config_type {
        let url = get_config_url(&chain_id, config_type);
        download_config(&url, &dir.join(CONFIG_FILENAME))
            .context(format!("Failed to download the config file from {}", url))?;
        config = Config::from_file(&dir.join(CONFIG_FILENAME))?;
    }

    if let Some(nodes) = boot_nodes {
        config.network.boot_nodes = nodes.to_string();
    }

    if let Some(max_gas_burnt_view) = max_gas_burnt_view {
        config.max_gas_burnt_view = Some(max_gas_burnt_view);
    }

    // Before finalizing the Config and Genesis, make sure the node and validator keys exist.
    generate_or_load_keys(dir, &config, &chain_id, account_id, test_seed)?;
    match chain_id.as_ref() {
        near_primitives::chains::MAINNET | near_primitives::chains::TESTNET => {
            if test_seed.is_some() {
                bail!("Test seed is not supported for {chain_id}");
            }
            config.telemetry.endpoints.push(NETWORK_LEGACY_TELEMETRY_URL.replace("{}", &chain_id));
            config.telemetry.endpoints.push(NETWORK_TELEMETRY_URL.to_string());
            config.state_sync = Some(StateSyncConfig::gcs_default());
        }
        _ => {
            // Create new configuration, key files and genesis for one validator.
            config.network.skip_sync_wait = true;
        }
    }

    config.write_to_file(&dir.join(CONFIG_FILENAME)).with_context(|| {
        format!("Error writing config to {}", dir.join(CONFIG_FILENAME).display())
    })?;

    match chain_id.as_ref() {
        near_primitives::chains::MAINNET => {
            let genesis = near_mainnet_res::mainnet_genesis();
            genesis.to_file(dir.join(config.genesis_file));
            info!(target: "near", "Generated mainnet genesis file in {}", dir.display());
        }
        near_primitives::chains::TESTNET => {
            if let Some(ref filename) = config.genesis_records_file {
                let records_path = dir.join(filename);

                if let Some(url) = download_records_url {
                    download_records(url, &records_path)
                        .context(format!("Failed to download the records file from {}", url))?;
                } else if should_download_genesis {
                    let url = get_records_url(&chain_id);
                    download_records(&url, &records_path)
                        .context(format!("Failed to download the records file from {}", url))?;
                }
            }

            // download genesis from s3
            let genesis_path = dir.join("genesis.json");
            let mut genesis_path_str =
                genesis_path.to_str().with_context(|| "Genesis path must be initialized")?;

            if let Some(url) = download_genesis_url {
                download_genesis(url, &genesis_path)
                    .context(format!("Failed to download the genesis file from {}", url))?;
            } else if should_download_genesis {
                let url = get_genesis_url(&chain_id);
                download_genesis(&url, &genesis_path)
                    .context(format!("Failed to download the genesis file from {}", url))?;
            } else {
                genesis_path_str = match genesis {
                    Some(g) => g,
                    None => {
                        bail!(
                            "Genesis file is required for {chain_id}.\nUse <--genesis|--download-genesis>"
                        );
                    }
                };
            }

            let mut genesis = match &config.genesis_records_file {
                Some(records_file) => {
                    let records_path = dir.join(records_file);
                    let records_path_str = records_path
                        .to_str()
                        .with_context(|| "Records path must be initialized")?;
                    Genesis::from_files(
                        genesis_path_str,
                        records_path_str,
                        GenesisValidationMode::Full,
                    )
                }
                None => Genesis::from_file(genesis_path_str, GenesisValidationMode::Full),
            }?;

            genesis.config.chain_id.clone_from(&chain_id);

            genesis.to_file(dir.join(config.genesis_file));
            info!(target: "near", "Generated for {chain_id} network node key and genesis file in {}", dir.display());
        }
        _ => {
            let validator_file = dir.join(&config.validator_key_file);
            let signer = InMemorySigner::from_file(&validator_file).unwrap();

            let mut records = vec![];
            add_account_with_key(
                &mut records,
                signer.get_account_id(),
                &signer.public_key(),
                TESTING_INIT_BALANCE,
                TESTING_INIT_STAKE,
                CryptoHash::default(),
            );
            add_protocol_account(&mut records);
            let shards = ShardLayout::multi_shard(num_shards, 0);

            let genesis_config = GenesisConfig {
                protocol_version: PROTOCOL_VERSION,
                genesis_time: from_timestamp(Clock::real().now_utc().unix_timestamp_nanos() as u64),
                chain_id,
                genesis_height: 0,
                num_block_producer_seats: NUM_BLOCK_PRODUCER_SEATS,
                num_block_producer_seats_per_shard: get_num_seats_per_shard(
                    num_shards,
                    NUM_BLOCK_PRODUCER_SEATS,
                ),
                avg_hidden_validator_seats_per_shard: (0..num_shards).map(|_| 0).collect(),
                dynamic_resharding: false,
                protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
                epoch_length: if fast { FAST_EPOCH_LENGTH } else { EXPECTED_EPOCH_LENGTH },
                gas_limit: INITIAL_GAS_LIMIT,
                gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
                block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
                chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
                chunk_validator_only_kickout_threshold: CHUNK_VALIDATOR_ONLY_KICKOUT_THRESHOLD,
                online_max_threshold: Rational32::new(99, 100),
                online_min_threshold: Rational32::new(BLOCK_PRODUCER_KICKOUT_THRESHOLD as i32, 100),
                validators: vec![AccountInfo {
                    account_id: signer.get_account_id(),
                    public_key: signer.public_key(),
                    amount: TESTING_INIT_STAKE,
                }],
                transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
                protocol_reward_rate: PROTOCOL_REWARD_RATE,
                max_inflation_rate: MAX_INFLATION_RATE,
                total_supply: get_initial_supply(&records),
                num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
                protocol_treasury_account: signer.get_account_id(),
                fishermen_threshold: FISHERMEN_THRESHOLD,
                shard_layout: shards,
                min_gas_price: MIN_GAS_PRICE,
                ..Default::default()
            };
            let genesis = Genesis::new(genesis_config, records.into())?;
            genesis.to_file(dir.join(config.genesis_file));
            info!(target: "near", "Generated node key, validator key, genesis file in {}", dir.display());
        }
    }

    Ok(())
}

/// Params specific to a localnet node, used for configuring the node for certain roles.
/// The params are not mutually exclusive, both is_validator and is_archival may be set to true.
struct LocalnetNodeParams {
    /// If true, this node is used as a boot node.
    is_boot: bool,
    /// If true, this is a validator node.
    is_validator: bool,
    // If true, this is an archival node.
    is_archival: bool,
    // If true, this is an RPC node.
    is_rpc: bool,
}

impl LocalnetNodeParams {
    fn new_validator(is_boot: bool) -> Self {
        Self { is_boot, is_validator: true, is_archival: false, is_rpc: false }
    }

    fn new_non_validator_archival() -> Self {
        Self { is_boot: false, is_validator: false, is_archival: true, is_rpc: false }
    }

    fn new_non_validator_rpc() -> Self {
        Self { is_boot: false, is_validator: false, is_archival: false, is_rpc: true }
    }

    fn new_non_validator() -> Self {
        Self { is_boot: false, is_validator: false, is_archival: false, is_rpc: false }
    }
}

/// Creates configurations for a number of localnet nodes.
///
/// # Arguments
///
/// * `seeds` - Seeds to use for creating the signing keys for accounts
/// * `num_shards` - Number of shards to partition the chain into
/// * `num_validators` - Number of validator nodes to create
/// * `num_non_validators_archival` - Number of non-validator nodes to create and configure as an archival node (storing full chain history)
/// * `num_non_validators_rpc` - Number of non-validator nodes to create and configure as an RPC node (eg. for sending transactions)
/// * `num_non_validators` - Number of additional non-validator nodes to create
/// * `tracked_shards_config` - Tracked shards config for all nodes, except for archival and RPC nodes which track all shards
pub fn create_localnet_configs_from_seeds(
    seeds: Vec<String>,
    num_shards: NumShards,
    num_validators: NumSeats,
    num_non_validators_archival: NumSeats,
    num_non_validators_rpc: NumSeats,
    num_non_validators: NumSeats,
    tracked_shards_config: TrackedShardsConfig,
) -> (Vec<Config>, Vec<ValidatorSigner>, Vec<Signer>, Genesis) {
    assert_eq!(
        seeds.len() as u64,
        num_validators + num_non_validators_archival + num_non_validators_rpc + num_non_validators,
        "Number of seeds should match the total number of nodes including validators and non-validators."
    );
    let validator_signers =
        seeds.iter().map(|seed| create_test_signer(seed.as_str())).collect::<Vec<_>>();

    let network_signers = seeds
        .iter()
        .map(|seed| InMemorySigner::from_seed("node".parse().unwrap(), KeyType::ED25519, seed))
        .collect::<Vec<_>>();
    let network_addresses =
        seeds.iter().map(|_| *tcp::ListenerAddr::reserve_for_test()).collect::<Vec<_>>();

    let shard_layout = ShardLayout::multi_shard(num_shards, 0);
    let accounts_to_add_to_genesis: Vec<AccountId> =
        seeds.iter().map(|s| s.parse().unwrap()).collect();

    let genesis = Genesis::from_accounts(
        Clock::real(),
        accounts_to_add_to_genesis,
        num_validators,
        shard_layout,
    );
    let mut configs = vec![];

    // We assign the seeds to the nodes in the following order:
    // 1. Validators (num_validators)
    // 2. Non-validator archival nodes (num_non_validators_archival)
    // 3. Non-validator RPC nodes (num_non_validators_rpc)
    // 4. Non-validator nodes (num_non_validators)

    // We use the first validator node as the boot node.
    assert!(num_validators > 0, "No validators were added");
    let boot_node = (network_addresses[0], network_signers[0].clone());

    let mut network_configs_iter =
        network_addresses.iter().copied().zip(network_signers.iter().cloned());
    for i in 0..num_validators {
        let params = LocalnetNodeParams::new_validator(i == 0);
        let config = create_localnet_config(
            num_validators,
            &tracked_shards_config,
            network_configs_iter.next().unwrap(),
            boot_node.clone(),
            params,
        );
        configs.push(config);
    }
    for _ in 0..num_non_validators_archival {
        let params = LocalnetNodeParams::new_non_validator_archival();
        let config = create_localnet_config(
            num_validators,
            &tracked_shards_config,
            network_configs_iter.next().unwrap(),
            boot_node.clone(),
            params,
        );
        configs.push(config);
    }
    for _ in 0..num_non_validators_rpc {
        let params = LocalnetNodeParams::new_non_validator_rpc();
        let config = create_localnet_config(
            num_validators,
            &tracked_shards_config,
            network_configs_iter.next().unwrap(),
            boot_node.clone(),
            params,
        );
        configs.push(config);
    }
    for _ in 0..num_non_validators {
        let params = LocalnetNodeParams::new_non_validator();
        let config = create_localnet_config(
            num_validators,
            &tracked_shards_config,
            network_configs_iter.next().unwrap(),
            boot_node.clone(),
            params,
        );
        configs.push(config);
    }

    (configs, validator_signers, network_signers, genesis)
}

fn create_localnet_config(
    num_validators: NumSeats,
    tracked_shards_config: &TrackedShardsConfig,
    network_config: (SocketAddr, Signer),
    boot_node: (SocketAddr, Signer),
    params: LocalnetNodeParams,
) -> Config {
    let mut config = Config::default();

    // Configure consensus protocol.
    config.consensus.min_block_production_delay = Duration::milliseconds(600);
    config.consensus.max_block_production_delay = Duration::milliseconds(2000);
    config.consensus.min_num_peers =
        std::cmp::min(num_validators as usize - 1, config.consensus.min_num_peers);

    // Configure networking and RPC endpoint. Enable debug-RPC by default for all nodes.
    config.rpc.get_or_insert(Default::default()).enable_debug_rpc = true;
    config.network.addr = network_config.0.to_string();
    config.network.public_addrs = vec![PeerAddr {
        addr: network_config.0,
        peer_id: PeerId::new(network_config.1.public_key()),
    }];
    config.network.allow_private_ip_in_public_addrs = true;
    config.network.tier1.connect_interval = Duration::seconds(5);
    config.set_rpc_addr(tcp::ListenerAddr::reserve_for_test());
    config.network.boot_nodes = if params.is_boot {
        "".to_string()
    } else {
        format!("{}@{}", boot_node.1.public_key(), boot_node.0)
    };
    config.network.skip_sync_wait = num_validators == 1;

    // Configure archival node with split storage (hot + cold DB).
    if params.is_archival {
        config.archive = true;
        config.cold_store.get_or_insert(config.store.clone()).path =
            Some(PathBuf::from("cold-data"));
        config.split_storage.get_or_insert(Default::default()).enable_split_storage_view_client =
            true;
        config.save_trie_changes = Some(true);
    }

    // Make non-validator archival and RPC nodes track all shards.
    // Note that validator nodes may track all or some of the shards.
    config.tracked_shards_config = if !params.is_validator && (params.is_archival || params.is_rpc)
    {
        Some(TrackedShardsConfig::AllShards)
    } else {
        Some(tracked_shards_config.clone())
    };

    // Save tx outcomes for the first validator node (for testing purposes).
    if params.is_boot {
        config.save_tx_outcomes = Some(true);
    }

    config
}

/// Create testnet configuration.
/// Sets up new ports for all nodes except the first one and sets boot node to it.
///
/// # Arguments
///
/// * `dir` - Root directory in which node-specific directories are created
/// * `num_shards` - Number of shards to partition the chain into
/// * `num_validators` - Number of validator nodes to create
/// * `num_non_validators_archival` - Number of non-validator nodes to create and configure as an archival node (storing full chain history)
/// * `num_non_validators_rpc` - Number of non-validator nodes to create and configure as an RPC node (eg. for sending transactions)
/// * `num_non_validators` - Number of additional non-validator nodes to create
/// * `prefix` - Prefix for the directory name for each node with (e.g. ‘node’ results in ‘node0’, ‘node1’, ...)
/// * `tracked_shards_config` - Tracked shards config for all nodes, except for archival and RPC nodes which track all shards
pub fn create_localnet_configs(
    num_shards: NumShards,
    num_validators: NumSeats,
    num_non_validators_archival: NumSeats,
    num_non_validators_rpc: NumSeats,
    num_non_validators: NumSeats,
    prefix: &str,
    tracked_shards_config: TrackedShardsConfig,
) -> (Vec<Config>, Vec<ValidatorSigner>, Vec<Signer>, Genesis, Vec<Signer>) {
    let num_all_nodes =
        num_validators + num_non_validators_archival + num_non_validators_rpc + num_non_validators;
    let seeds = (0..num_all_nodes).map(|i| format!("{}{}", prefix, i)).collect::<Vec<_>>();

    let (configs, validator_signers, network_signers, genesis) = create_localnet_configs_from_seeds(
        seeds,
        num_shards,
        num_validators,
        num_non_validators_archival,
        num_non_validators_rpc,
        num_non_validators,
        tracked_shards_config,
    );

    let shard_keys = vec![];
    (configs, validator_signers, network_signers, genesis, shard_keys)
}

/// Creates localnet configuration and initializes the local home directories for a number of nodes.
///
/// # Arguments
///
/// * `dir` - Root directory in which node-specific directories are created
/// * `num_shards` - Number of shards to partition the chain into
/// * `num_validators` - Number of validator nodes to create
/// * `num_non_validators_archival` - Number of non-validator nodes to create and configure as an archival node (storing full chain history)
/// * `num_non_validators_rpc` - Number of non-validator nodes to create and configure as an RPC node (eg. for sending transactions)
/// * `num_non_validators` - Number of additional non-validator nodes to create
/// * `prefix` - Prefix for the directory name for each node with (e.g. ‘node’ results in ‘node0’, ‘node1’, ...)
/// * `tracked_shards_config` - Tracked shards config for all nodes, except for archival and RPC nodes which track all shards
pub fn init_localnet_configs(
    dir: &Path,
    num_shards: NumShards,
    num_validators: NumSeats,
    num_non_validators_archival: NumSeats,
    num_non_validators_rpc: NumSeats,
    num_non_validators: NumSeats,
    prefix: &str,
    tracked_shards_config: TrackedShardsConfig,
) {
    let (configs, validator_signers, network_signers, genesis, shard_keys) =
        create_localnet_configs(
            num_shards,
            num_validators,
            num_non_validators_archival,
            num_non_validators_rpc,
            num_non_validators,
            prefix,
            tracked_shards_config,
        );

    // Save the generated configs to the corresponding files in the home directory for each node.
    let log_config = LogConfig::default();
    let num_all_nodes =
        num_validators + num_non_validators_archival + num_non_validators_rpc + num_non_validators;
    for i in 0..num_all_nodes as usize {
        let config = &configs[i];
        let node_dir = dir.join(format!("{}{}", prefix, i));
        fs::create_dir_all(&node_dir).expect("Failed to create directory");

        validator_signers[i]
            .write_to_file(&node_dir.join(&config.validator_key_file))
            .expect("Error writing validator key file");
        network_signers[i]
            .write_to_file(&node_dir.join(&config.node_key_file))
            .expect("Error writing key file");
        for key in &shard_keys {
            key.write_to_file(&node_dir.join(format!("{}_key.json", key.get_account_id())))
                .expect("Error writing shard file");
        }

        genesis.to_file(&node_dir.join(&config.genesis_file));
        config.write_to_file(&node_dir.join(CONFIG_FILENAME)).expect("Error writing config");
        log_config
            .write_to_file(&node_dir.join(LOG_CONFIG_FILENAME))
            .expect("Error writing log config");
        info!(target: "near", "Generated node key, validator key, genesis file in {}", node_dir.display());
    }
}

pub fn get_genesis_url(chain_id: &str) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/genesis.json.xz",
        chain_id,
    )
}

pub fn get_records_url(chain_id: &str) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/records.json.xz",
        chain_id,
    )
}

pub fn get_config_url(chain_id: &str, config_type: DownloadConfigType) -> String {
    format!(
        "https://s3-us-west-1.amazonaws.com/build.nearprotocol.com/nearcore-deploy/{}/{}/config.json",
        chain_id,
        config_type.to_string()
    )
}

pub fn download_genesis(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading genesis file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the genesis file to: {} ...", path.display());
    }
    result
}

pub fn download_records(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading records file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the records file to: {} ...", path.display());
    }
    result
}

pub fn download_config(url: &str, path: &Path) -> Result<(), FileDownloadError> {
    info!(target: "near", "Downloading config file from: {} ...", url);
    let result = run_download_file(url, path);
    if result.is_ok() {
        info!(target: "near", "Saved the config file to: {} ...", path.display());
    }
    result
}

#[derive(serde::Deserialize)]
struct NodeKeyFile {
    account_id: String,
    public_key: PublicKey,
    secret_key: near_crypto::SecretKey,
}

impl NodeKeyFile {
    // the file can be JSON with comments
    fn from_file(path: &Path) -> std::io::Result<Self> {
        let mut file = File::open(path)?;
        let mut json_str = String::new();
        file.read_to_string(&mut json_str)?;

        let json_str_without_comments = near_config_utils::strip_comments_from_json_str(&json_str)?;

        Ok(serde_json::from_str(&json_str_without_comments)?)
    }
}

impl From<NodeKeyFile> for KeyFile {
    fn from(this: NodeKeyFile) -> Self {
        Self {
            account_id: if this.account_id.is_empty() {
                "node".to_string()
            } else {
                this.account_id
            }
            .try_into()
            .unwrap(),
            public_key: this.public_key,
            secret_key: this.secret_key,
        }
    }
}

pub fn load_validator_key(validator_file: &Path) -> anyhow::Result<Option<Arc<ValidatorSigner>>> {
    if !validator_file.exists() {
        return Ok(None);
    }
    match InMemoryValidatorSigner::from_file(&validator_file) {
        Ok(signer) => Ok(Some(Arc::new(signer))),
        Err(_) => {
            let error_message =
                format!("Failed initializing validator signer from {}", validator_file.display());
            Err(anyhow!(error_message))
        }
    }
}

pub fn load_config(
    dir: &Path,
    genesis_validation: GenesisValidationMode,
) -> anyhow::Result<NearConfig> {
    let mut validation_errors = ValidationErrors::new();

    // if config.json has file issues, the program will directly panic
    let config = Config::from_file_skip_validation(&dir.join(CONFIG_FILENAME))?;
    // do config.json validation later so that genesis_file, validator_file and genesis_file can be validated before program panic
    if let Err(e) = config.validate() {
        validation_errors.push_errors(e)
    };

    let validator_file: PathBuf = dir.join(&config.validator_key_file);
    let validator_signer = match load_validator_key(&validator_file) {
        Ok(validator_signer) => validator_signer,
        Err(e) => {
            validation_errors.push_validator_key_file_error(e.to_string());
            None
        }
    };

    let node_key_path = dir.join(&config.node_key_file);
    let network_signer_result = NodeKeyFile::from_file(&node_key_path);
    let network_signer = match network_signer_result {
        Ok(node_key_file) => Some(node_key_file),
        Err(_) => {
            let error_message =
                format!("Failed reading node key file from {}", node_key_path.display());
            validation_errors.push_node_key_file_error(error_message);
            None
        }
    };

    let genesis_file = dir.join(&config.genesis_file);
    let genesis_result = match &config.genesis_records_file {
        // only load Genesis from file. Skip test for now.
        // this allows us to know the chain_id in order to check tracked_shards even if semantics checks fail.
        Some(records_file) => Genesis::from_files(
            &genesis_file,
            dir.join(records_file),
            GenesisValidationMode::UnsafeFast,
        ),
        None => Genesis::from_file(&genesis_file, GenesisValidationMode::UnsafeFast),
    };

    let genesis = match genesis_result {
        Ok(genesis) => {
            if let Err(e) = genesis.validate(genesis_validation) {
                validation_errors.push_errors(e)
            };
            Some(genesis)
        }
        Err(error) => {
            validation_errors.push_errors(error);
            None
        }
    };

    validation_errors.return_ok_or_error()?;

    if genesis.is_none() || network_signer.is_none() {
        panic!("Genesis and network_signer should not be None by now.")
    }
    let near_config = NearConfig::new(
        config,
        genesis.unwrap(),
        network_signer.unwrap().into(),
        MutableConfigValue::new(validator_signer, "validator_signer"),
    )?;
    Ok(near_config)
}

pub fn load_test_config(seed: &str, addr: tcp::ListenerAddr, genesis: Genesis) -> NearConfig {
    let mut config = Config::default();
    config.network.addr = addr.to_string();
    config.set_rpc_addr(tcp::ListenerAddr::reserve_for_test());
    config.consensus.min_block_production_delay =
        Duration::milliseconds(FAST_MIN_BLOCK_PRODUCTION_DELAY);
    config.consensus.max_block_production_delay =
        Duration::milliseconds(FAST_MAX_BLOCK_PRODUCTION_DELAY);
    let (signer, validator_signer) = if seed.is_empty() {
        let signer = InMemorySigner::from_random("node".parse().unwrap(), KeyType::ED25519).into();
        (signer, None)
    } else {
        let signer = InMemorySigner::from_seed(seed.parse().unwrap(), KeyType::ED25519, seed);
        let validator_signer = Arc::new(create_test_signer(seed)) as Arc<ValidatorSigner>;
        (signer, Some(validator_signer))
    };
    NearConfig::new(
        config,
        genesis,
        signer.into(),
        MutableConfigValue::new(validator_signer, "validator_signer"),
    )
    .unwrap()
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::str::FromStr;

    use itertools::Itertools;
    use near_async::time::Duration;
    use near_chain_configs::{GCConfig, Genesis, GenesisValidationMode, TrackedShardsConfig};
    use near_crypto::InMemorySigner;
    use near_primitives::types::{AccountId, NumShards, ShardId};
    use near_store::ShardUId;
    use serde_json::json;
    use tempfile::tempdir;

    use crate::config::{
        CONFIG_FILENAME, Config, create_localnet_configs, generate_or_load_key, init_configs,
    };

    #[test]
    fn test_old_tracked_config_fields_are_parsed() {
        let json_data = json!({
            "tracked_accounts": ["account1.near", "account2.near"],
            "tracked_shadow_validator": "shadow.near",
            "tracked_shards": [0, 1],
            "tracked_shard_schedule": [[0], [1, 2]]
        });

        let config: Config = serde_json::from_value(json_data).unwrap();

        assert_eq!(
            config.tracked_accounts,
            Some(vec!["account1.near".parse().unwrap(), "account2.near".parse().unwrap()])
        );
        assert_eq!(config.tracked_shadow_validator, Some("shadow.near".parse().unwrap()));
        assert_eq!(config.tracked_shards, Some(vec![ShardId::new(0), ShardId::new(1)]));
        assert_eq!(
            config.tracked_shard_schedule,
            Some(vec![vec![ShardId::new(0)], vec![ShardId::new(1), ShardId::new(2)]])
        );
    }

    #[test]
    fn test_init_config_localnet() {
        // Check that we can initialize the config with multiple shards.
        let temp_dir = tempdir().unwrap();
        init_configs(
            &temp_dir.path(),
            Some("localnet".to_string()),
            None,
            Some("seed1"),
            3,
            false,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let genesis = Genesis::from_file(
            temp_dir.path().join("genesis.json"),
            GenesisValidationMode::UnsafeFast,
        )
        .unwrap();
        assert_eq!(genesis.config.chain_id, "localnet");
        let shard_layout = &genesis.config.shard_layout;
        let shard_ids = shard_layout.shard_ids().collect_vec();
        let [s0, s1, s2] = shard_ids[..] else {
            panic!("Expected 3 shards, got {:?}", shard_ids);
        };
        assert_eq!(
            shard_layout.account_id_to_shard_id(&AccountId::from_str("foobar.near").unwrap()),
            s0
        );
        assert_eq!(
            shard_layout.account_id_to_shard_id(&AccountId::from_str("test0.near").unwrap()),
            s0
        );
        assert_eq!(
            shard_layout.account_id_to_shard_id(&AccountId::from_str("test1.near").unwrap()),
            s1
        );
        assert_eq!(
            shard_layout.account_id_to_shard_id(&AccountId::from_str("test2.near").unwrap()),
            s2
        );
    }

    #[test]
    // Tests that `init_configs()` works if both config and genesis file exists, but the node key and validator key files don't exist.
    // Test does the following:
    // * Initialize all config and key files
    // * Check that the key files exist
    // * Remove the key files
    // * Run the initialization again
    // * Check that the key files got created
    fn test_init_config_localnet_keep_config_create_node_key() {
        let temp_dir = tempdir().unwrap();
        // Initialize all config and key files.
        init_configs(
            &temp_dir.path(),
            Some("localnet".to_string()),
            Some(AccountId::from_str("account.near").unwrap()),
            Some("seed1"),
            3,
            false,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Check that the key files exist.
        let _genesis =
            Genesis::from_file(temp_dir.path().join("genesis.json"), GenesisValidationMode::Full)
                .unwrap();
        let config = Config::from_file(&temp_dir.path().join(CONFIG_FILENAME)).unwrap();
        let node_key_file = temp_dir.path().join(config.node_key_file);
        let validator_key_file = temp_dir.path().join(config.validator_key_file);
        assert!(node_key_file.exists());
        assert!(validator_key_file.exists());

        // Remove the key files.
        std::fs::remove_file(&node_key_file).unwrap();
        std::fs::remove_file(&validator_key_file).unwrap();

        // Run the initialization again.
        init_configs(
            &temp_dir.path(),
            Some("localnet".to_string()),
            Some(AccountId::from_str("account.near").unwrap()),
            Some("seed1"),
            3,
            false,
            None,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Check that the key files got created.
        let _node_signer = InMemorySigner::from_file(&node_key_file).unwrap();
        let _validator_signer = InMemorySigner::from_file(&validator_key_file).unwrap();
    }

    /// Tests that loading a config.json file works and results in values being
    /// correctly parsed and defaults being applied correctly applied.
    /// We skip config validation since we only care about Config being correctly loaded from file.
    #[test]
    fn test_config_from_file_skip_validation() {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"));

        for (has_gc, path) in
            [(true, "res/example-config-gc.json"), (false, "res/example-config-no-gc.json")]
        {
            let path = base.join(path);
            let data = std::fs::read(path).unwrap();
            let tmp = tempfile::NamedTempFile::new().unwrap();
            tmp.as_file().write_all(&data).unwrap();

            let config = Config::from_file_skip_validation(&tmp.into_temp_path()).unwrap();

            // TODO(mina86): We might want to add more checks.  Looking at all
            // values is probably not worth it but there may be some other defaults
            // we want to ensure that they happen.
            let want_gc = if has_gc {
                GCConfig {
                    gc_blocks_limit: 42,
                    gc_fork_clean_step: 420,
                    gc_num_epochs_to_keep: 24,
                    gc_step_period: Duration::milliseconds(500),
                }
            } else {
                GCConfig {
                    gc_blocks_limit: 2,
                    gc_fork_clean_step: 100,
                    gc_num_epochs_to_keep: 5,
                    gc_step_period: Duration::milliseconds(500),
                }
            };
            assert_eq!(want_gc, config.gc);

            assert_eq!(
                vec![
                    "https://explorer.mainnet.near.org/api/nodes".to_string(),
                    "https://telemetry.nearone.org/nodes".to_string()
                ],
                config.telemetry.endpoints
            );
        }
    }

    #[test]
    fn test_create_localnet_configs_track_single_shard() {
        let num_shards = 4;
        let num_validators = 4;
        let num_non_validators_archival = 2;
        let num_non_validators_rpc = 2;
        let num_non_validators = 2;
        let prefix = "node";

        let (configs, _validator_signers, _network_signers, genesis, _shard_keys) =
            create_localnet_configs(
                num_shards,
                num_validators,
                num_non_validators_archival,
                num_non_validators_rpc,
                num_non_validators,
                prefix,
                // Validators will track single shard but archival and RPC nodes will track all shards.
                TrackedShardsConfig::NoShards,
            );
        assert_eq!(
            configs.len() as u64,
            num_validators
                + num_non_validators_archival
                + num_non_validators_rpc
                + num_non_validators
        );

        // Check validator nodes.
        for i in 0..4 {
            let config = &configs[i];
            assert_eq!(config.archive, false);
            assert!(config.cold_store.is_none());
            assert!(config.split_storage.is_none());
            assert_eq!(
                config.tracked_shards_config.as_ref().unwrap(),
                &TrackedShardsConfig::NoShards
            );
        }

        // Check non-validator archival nodes.
        for i in 4..6 {
            let config = &configs[i];
            assert_eq!(config.archive, true);
            assert_eq!(
                config.cold_store.clone().unwrap().path.unwrap(),
                PathBuf::from("cold-data")
            );
            assert_eq!(config.save_trie_changes.unwrap(), true);
            assert_eq!(
                config.split_storage.clone().unwrap().enable_split_storage_view_client,
                true
            );
            assert_eq!(
                config.tracked_shards_config.as_ref().unwrap(),
                &TrackedShardsConfig::AllShards
            );
        }

        // Check non-validator RPC nodes.
        for i in 6..8 {
            let config = &configs[i];
            assert_eq!(config.archive, false);
            assert!(config.cold_store.is_none());
            assert!(config.split_storage.is_none());
            assert_eq!(
                config.tracked_shards_config.as_ref().unwrap(),
                &TrackedShardsConfig::AllShards
            );
        }

        // Check other non-validator nodes.
        for i in 8..10 {
            let config = &configs[i];
            assert_eq!(config.archive, false);
            assert!(config.cold_store.is_none());
            assert!(config.split_storage.is_none());
            assert_eq!(
                config.tracked_shards_config.as_ref().unwrap(),
                &TrackedShardsConfig::NoShards
            );
        }

        assert_eq!(genesis.config.validators.len() as u64, num_shards);
        assert_eq!(genesis.config.shard_layout.shard_ids().count() as NumShards, num_shards);
    }

    #[test]
    fn test_create_localnet_configs_track_some_shards() {
        let num_shards = 4;
        let num_validators = 4;
        let num_non_validators_archival = 2;
        let num_non_validators_rpc = 2;
        let num_non_validators = 2;
        let prefix = "node";

        // Validators will track 2 shards and non-validators will track all shards.
        let _tracked_shards =
            [ShardUId::new(0, ShardId::new(1)), ShardUId::new(0, ShardId::new(3))];
        // TODO(cloud_archival): When `TrackedShardsConfig::Shards` is added, use it here together with `tracked_shards`.
        let tracked_shards_config = TrackedShardsConfig::AllShards;

        let (configs, _validator_signers, _network_signers, genesis, _shard_keys) =
            create_localnet_configs(
                num_shards,
                num_validators,
                num_non_validators_archival,
                num_non_validators_rpc,
                num_non_validators,
                prefix,
                tracked_shards_config.clone(),
            );
        assert_eq!(
            configs.len() as u64,
            num_validators
                + num_non_validators_archival
                + num_non_validators_rpc
                + num_non_validators
        );

        // Check validator nodes.
        for i in 0..4 {
            let config = &configs[i];
            assert_eq!(config.archive, false);
            assert!(config.cold_store.is_none());
            assert!(config.split_storage.is_none());
            assert_eq!(config.tracked_shards_config.as_ref().unwrap(), &tracked_shards_config);
        }

        // Check non-validator archival nodes.
        for i in 4..6 {
            let config = &configs[i];
            assert_eq!(config.archive, true);
            assert_eq!(
                config.cold_store.clone().unwrap().path.unwrap(),
                PathBuf::from("cold-data")
            );
            assert_eq!(config.save_trie_changes.unwrap(), true);
            assert_eq!(
                config.split_storage.clone().unwrap().enable_split_storage_view_client,
                true
            );
            assert_eq!(
                config.tracked_shards_config.as_ref().unwrap(),
                &TrackedShardsConfig::AllShards
            );
        }

        // Check non-validator RPC nodes.
        for i in 6..8 {
            let config = &configs[i];
            assert_eq!(config.archive, false);
            assert!(config.cold_store.is_none());
            assert!(config.split_storage.is_none());
            assert_eq!(
                config.tracked_shards_config.as_ref().unwrap(),
                &TrackedShardsConfig::AllShards
            );
        }

        // Check other non-validator nodes.
        for i in 8..10 {
            let config = &configs[i];
            assert_eq!(config.archive, false);
            assert!(config.cold_store.is_none());
            assert!(config.split_storage.is_none());
            assert_eq!(config.tracked_shards_config.as_ref().unwrap(), &tracked_shards_config);
        }

        assert_eq!(genesis.config.validators.len() as u64, num_shards);
        assert_eq!(genesis.config.shard_layout.shard_ids().count() as NumShards, num_shards);
    }

    #[test]
    fn test_generate_or_load_key() {
        let tmp = tempfile::tempdir().unwrap();
        let home_dir = tmp.path();

        let r#gen = move |filename: &str, account: &str, seed: &str| {
            generate_or_load_key(
                home_dir,
                filename,
                if account.is_empty() { None } else { Some(account.parse().unwrap()) },
                if seed.is_empty() { None } else { Some(seed) },
            )
        };

        let test_ok = |filename: &str, account: &str, seed: &str| {
            let result = r#gen(filename, account, seed);
            let key = result.unwrap().unwrap();
            assert!(home_dir.join("key").exists());
            if !account.is_empty() {
                assert_eq!(account, key.get_account_id().as_str());
            }
            key
        };

        let test_err = |filename: &str, account: &str, seed: &str| {
            let result = r#gen(filename, account, seed);
            assert!(result.is_err());
        };

        // account_id == None → do nothing, return None
        assert!(generate_or_load_key(home_dir, "key", None, None).unwrap().is_none());
        assert!(!home_dir.join("key").exists());

        // account_id == Some, file doesn’t exist → create new key
        let key = test_ok("key", "fred", "");

        // file exists → load key, compare account if given
        assert!(key == test_ok("key", "", ""));
        assert!(key == test_ok("key", "fred", ""));
        test_err("key", "barney", "");

        // test_seed == Some → the same key is generated (same signature is produced)
        let k1 = test_ok("k1", "fred", "foo");
        let k2 = test_ok("k2", "barney", "foo");
        let k3 = test_ok("k3", "fred", "bar");

        let data: &[u8] = b"Example data for signature test";
        assert!(k1.public_key() == k2.public_key() && k1.sign(&data) == k2.sign(&data));
        assert!(k1 != k3);

        // file contains invalid JSON -> should return an error
        {
            let mut file = std::fs::File::create(&home_dir.join("bad_key")).unwrap();
            writeln!(file, "not JSON").unwrap();
        }
        test_err("bad_key", "fred", "");
    }

    #[test]
    fn detects_view_client_throttle_period() {
        let json = r#"{ "view_client_throttle_period": "10s" }"#;
        let deprecated = Config::deprecated_fields_in_json(json);
        assert_eq!(
            deprecated,
            vec![("view_client_throttle_period", "state_request_throttle_period")]
        );
    }

    #[test]
    fn detects_view_client_num_state_requests() {
        let json = r#"{ "view_client_num_state_requests_per_throttle_period": 256 }"#;
        let deprecated = Config::deprecated_fields_in_json(json);
        assert_eq!(
            deprecated,
            vec![(
                "view_client_num_state_requests_per_throttle_period",
                "state_requests_per_throttle_period"
            )]
        );
    }

    #[test]
    fn detects_both_deprecated_fields() {
        let json = r#"
        {
            "view_client_throttle_period": "10s",
            "view_client_num_state_requests_per_throttle_period": 256
        }
        "#;
        let mut deprecated = Config::deprecated_fields_in_json(json);
        deprecated.sort(); // order is implementation detail
        assert_eq!(
            deprecated,
            vec![
                (
                    "view_client_num_state_requests_per_throttle_period",
                    "state_requests_per_throttle_period"
                ),
                ("view_client_throttle_period", "state_request_throttle_period"),
            ]
        );
    }

    #[test]
    fn no_deprecated_fields_on_clean_config() {
        let json = r#"{ "state_request_throttle_period": "10s" }"#;
        assert!(Config::deprecated_fields_in_json(json).is_empty());
    }
}
