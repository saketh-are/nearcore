use crate::commands::*;
use crate::congestion_control::CongestionControlCmd;
use crate::contract_accounts::ContractAccountFilter;
use crate::replay_headers::replay_headers;
use crate::rocksdb_stats::get_rocksdb_stats;
use crate::trie_iteration_benchmark::TrieIterationBenchmarkCmd;

use crate::latest_witnesses::StateWitnessCmd;
use near_chain::types::RuntimeStorageConfig;
use near_chain_configs::{GenesisChangeConfig, GenesisValidationMode};
use near_epoch_manager::EpochManager;
use near_jsonrpc::start_http_for_readonly_debug_querying;
use near_network::tcp::ListenerAddr;
use near_primitives::account::id::AccountId;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::sharding::ChunkHash;
use near_primitives::trie_key::col;
use near_primitives::types::{BlockHeight, ShardId, StateRoot};
use near_primitives_core::types::EpochHeight;
use near_store::adapter::StoreAdapter;
use near_store::{Mode, NodeStorage, Store, Temperature};
use nearcore::entity_debug::EntityDebugHandlerImpl;
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt, load_config};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

#[derive(clap::Subcommand)]
#[clap(subcommand_required = true, arg_required_else_help = true)]
pub enum StateViewerSubCommand {
    /// Apply block at some height for shard.
    Apply(ApplyCmd),
    /// Apply a chunk, even if it's not included in any block on disk
    #[clap(alias = "apply_chunk")]
    ApplyChunk(ApplyChunkCmd),
    /// Apply blocks at a range of heights for a single shard.
    #[clap(alias = "apply_range")]
    ApplyRange(ApplyRangeCmd),
    /// Apply a receipt if it occurs in some chunk we know about,
    /// even if it's not included in any block on disk
    #[clap(alias = "apply_receipt")]
    ApplyReceipt(ApplyReceiptCmd),
    /// Apply a transaction if it occurs in some chunk we know about,
    /// even if it's not included in any block on disk
    #[clap(alias = "apply_tx")]
    ApplyTx(ApplyTxCmd),
    /// Print chain from start_index to end_index.
    Chain(ChainCmd),
    /// Check whether the node has all the blocks up to its head.
    #[clap(alias = "check_block")]
    CheckBlock,
    /// Looks up a certain chunk.
    Chunks(ChunksCmd),
    /// View chunk application stats for a chunk.
    ChunkApplyStats(ChunkApplyStatsCmd),
    /// Clear recoverable data in CachedContractCode column.
    #[clap(alias = "clear_cache")]
    ClearCache,
    /// List account names with contracts deployed.
    #[clap(alias = "contract_accounts")]
    ContractAccounts(ContractAccountsCmd),
    /// Run a readonly Debug UI API server so the Debug UI can be used to query this node.
    #[clap(alias = "debug_ui")]
    DebugUI(DebugUICmd),
    /// Dump contract data in storage of given account to binary file.
    #[clap(alias = "dump_account_storage")]
    DumpAccountStorage(DumpAccountStorageCmd),
    /// Dump deployed contract code of given account to wasm file.
    #[clap(alias = "dump_code")]
    DumpCode(DumpCodeCmd),
    /// Generate a genesis file from the current state of the DB.
    #[clap(alias = "dump_state")]
    DumpState(DumpStateCmd),
    /// Writes state to a remote redis server.
    #[clap(alias = "dump_state_redis")]
    DumpStateRedis(DumpStateRedisCmd),
    /// Generate a file that contains all transactions from a block.
    #[clap(alias = "dump_tx")]
    DumpTx(DumpTxCmd),
    /// Print `EpochInfo` of an epoch given by `--epoch_id` or by `--epoch_height`.
    #[clap(alias = "epoch_info")]
    EpochInfo(EpochInfoCmd),
    /// Regenerates epoch info based on previous epoch.
    #[clap(alias = "epoch_analysis")]
    EpochAnalysis(EpochAnalysisCmd),
    /// Looks up a certain partial chunk.
    #[clap(alias = "partial_chunks")]
    PartialChunks(PartialChunksCmd),
    /// Looks up a certain receipt.
    Receipts(ReceiptsCmd),
    /// Replay block headers from chain.
    ReplayHeaders(ReplayHeadersCmd),
    /// Dump stats for the RocksDB storage.
    #[clap(name = "rocksdb-stats", alias = "rocksdb_stats")]
    RocksDBStats(RocksDBStatsCmd),
    /// Reads all rows of a DB column and deserializes keys and values and prints them.
    ScanDbColumn(ScanDbColumnCmd),
    /// Iterates over a trie and prints the StateRecords.
    State,
    /// Dumps or applies StateChanges.
    /// Experimental tool for shard shadowing development.
    StateChanges(StateChangesCmd),
    /// Dump or apply state parts.
    StateParts(StatePartsCmd),
    /// Iterates over the Flat State and prints some statistics.
    /// e.g. large accounts, total, average and median size, middle account
    StateStats(StateStatsCmd),
    /// Benchmark how long does it take to iterate the trie.
    TrieIterationBenchmark(TrieIterationBenchmarkCmd),
    /// View head of the storage.
    #[clap(alias = "view_chain")]
    ViewChain(ViewChainCmd),
    /// View genesis block and chunks built from the config and in the store.
    #[clap(alias = "view_genesis")]
    ViewGenesis(ViewGenesisCmd),
    /// View trie structure.
    #[clap(alias = "view_trie")]
    ViewTrie(ViewTrieCmd),
    /// Tools for manually validating state witnesses.
    ///
    /// First, dump some of the stored state witnesses to a directory
    /// using the `dump` command. Supports selecting by given height, shard
    /// and epoch id, or pretty-printing on screen.
    ///
    /// Note that witnesses are only stored when `save_latest_witnesses`
    /// or `save_invalid_witnesses` are set to true in config.json.
    ///
    /// Second, validate a particular state witness from a file using the
    /// `validate` command.
    #[clap(subcommand)]
    StateWitness(StateWitnessCmd),

    /// Tools for printing and recalculating the congestion information.
    #[clap(subcommand)]
    CongestionControl(CongestionControlCmd),
}

impl StateViewerSubCommand {
    #[allow(clippy::large_stack_frames)]
    pub fn run(
        self,
        home_dir: &Path,
        genesis_validation: GenesisValidationMode,
        mode: Mode,
        temperature: Temperature,
    ) {
        let near_config = load_config(home_dir, genesis_validation)
            .unwrap_or_else(|e| panic!("Error loading config: {:#}", e));

        let store_opener = NodeStorage::opener(
            home_dir,
            &near_config.config.store,
            near_config.config.cold_store.as_ref(),
            near_config.config.cloud_storage.as_ref(),
        );

        let storage = store_opener.open_in_mode(mode).unwrap();
        let store = match temperature {
            Temperature::Hot => storage.get_hot_store(),
            // Cold store on it's own is useless in majority of subcommands
            Temperature::Cold => storage.get_split_store().unwrap(),
        };

        match self {
            StateViewerSubCommand::Apply(cmd) => cmd.run(home_dir, near_config, store, storage),
            StateViewerSubCommand::ApplyChunk(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ApplyRange(cmd) => {
                cmd.run(home_dir, near_config, store, storage)
            }
            StateViewerSubCommand::ApplyReceipt(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ApplyTx(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::Chain(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::CheckBlock => check_block_chunk_existence(near_config, store),
            StateViewerSubCommand::Chunks(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::ChunkApplyStats(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::ClearCache => clear_cache(store),
            StateViewerSubCommand::ContractAccounts(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DebugUI(cmd) => {
                cmd.run(home_dir, near_config, storage.get_hot_store(), storage.get_cold_store())
            }
            StateViewerSubCommand::DumpAccountStorage(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpCode(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpState(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpStateRedis(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::DumpTx(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::EpochInfo(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::EpochAnalysis(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::PartialChunks(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::Receipts(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::ReplayHeaders(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::RocksDBStats(cmd) => cmd.run(store_opener.path()),
            StateViewerSubCommand::ScanDbColumn(cmd) => cmd.run(store),
            StateViewerSubCommand::State => state(home_dir, near_config, store),
            StateViewerSubCommand::StateChanges(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::StateParts(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::StateStats(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ViewChain(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ViewGenesis(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::ViewTrie(cmd) => cmd.run(store),
            StateViewerSubCommand::TrieIterationBenchmark(cmd) => cmd.run(near_config, store),
            StateViewerSubCommand::StateWitness(cmd) => cmd.run(home_dir, near_config, store),
            StateViewerSubCommand::CongestionControl(cmd) => cmd.run(home_dir, near_config, store),
        }
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
#[clap(rename_all = "kebab_case")]
pub enum StorageSource {
    Trie,
    /// Use the data stored in trie, but without paying extra gas costs.
    /// This could be used to simulate flat storage when the latter is not present.
    TrieFree,
    #[value(alias("flat"))]
    FlatStorage,
    /// Implies flat storage and loads the memtries as well.
    Memtrie,
    /// Recorded storage, as used during chunk validation.
    /// Only available in "benchmark" mode.
    Recorded,
}

impl StorageSource {
    pub fn create_runtime_storage(&self, state_root: StateRoot) -> RuntimeStorageConfig {
        match self {
            StorageSource::Trie => RuntimeStorageConfig::new(state_root, false),
            StorageSource::TrieFree => RuntimeStorageConfig::new_with_db_trie_only(state_root),
            StorageSource::FlatStorage => RuntimeStorageConfig::new(state_root, true),
            // This is the same as FlatStorage handling. That's because memtrie initialization
            // happens as part of `ShardTries::load_memtrie` function call.
            StorageSource::Memtrie => RuntimeStorageConfig::new(state_root, true),
            StorageSource::Recorded => {
                panic!(
                    "For recorded storage the RuntimeStorageConfig has to be created from storage proof"
                );
            }
        }
    }
}

#[derive(clap::ValueEnum, Debug, Clone, Copy)]
pub enum SaveTrieTemperature {
    // The logic in `crate::commands::maybe_save_trie_changes` is not guaranteed to work correctly when writing
    // trie nodes in the hot storage.
    // Hot,
    Cold,
}

#[derive(clap::Parser)]
pub struct ApplyCmd {
    #[clap(long)]
    height: BlockHeight,
    #[clap(long)]
    shard_id: ShardId,
    #[clap(long, default_value = "trie")]
    storage: StorageSource,
    /// Modifies the DB column 'State' and writes the missing trie nodes generated as a result of applying the block.
    #[clap(long)]
    save_state: Option<SaveTrieTemperature>,
}

impl ApplyCmd {
    pub fn run(
        self,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
        node_storage: NodeStorage,
    ) {
        apply_block_at_height(
            self.height,
            self.shard_id,
            self.storage,
            home_dir,
            near_config,
            store,
            self.save_state.map(|temperature| initialize_write_store(temperature, node_storage)),
        )
        .unwrap();
    }
}

#[derive(clap::Parser)]
pub struct ApplyChunkCmd {
    #[clap(long)]
    chunk_hash: String,
    #[clap(long)]
    target_height: Option<u64>,
    #[clap(long, default_value = "trie")]
    storage: StorageSource,
}

impl ApplyChunkCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let hash = ChunkHash::from(CryptoHash::from_str(&self.chunk_hash).unwrap());
        apply_chunk(home_dir, near_config, store, hash, self.target_height, self.storage).unwrap()
    }
}

#[derive(clap::Parser, Copy, Clone, Debug, Eq, PartialEq)]
pub enum ApplyRangeMode {
    /// Applies chunks one after another in order of increasing heights.
    Sequential {
        /// If true, saves state transitions for state witness generation.
        #[clap(long)]
        save_state_transitions: bool,
    },
    /// Applies chunks in parallel.
    ///
    /// Useful for quick correctness check of applying chunks by comparing
    /// results with `ChunkExtra`s.
    Parallel,
    /// Applies a single block repeatedly without committing any state changes.
    Benchmark,
}

#[derive(clap::Parser)]
pub struct ApplyRangeCmd {
    #[clap(long)]
    start_index: Option<BlockHeight>,
    #[clap(long)]
    end_index: Option<BlockHeight>,
    /// All shards by default (if not specified.) Can be provided multiple times.
    #[clap(long)]
    shard_id: Vec<ShardId>,
    #[clap(long)]
    verbose_output: bool,
    #[clap(long, value_parser)]
    csv_file: Option<PathBuf>,
    #[clap(long)]
    only_contracts: bool,
    #[clap(long, default_value = "trie")]
    storage: StorageSource,
    #[clap(subcommand)]
    mode: ApplyRangeMode,
    /// Modifies the DB column 'State' and writes the missing trie nodes generated as a result of applying the blocks.
    #[clap(long)]
    save_state: Option<SaveTrieTemperature>,
}

impl ApplyRangeCmd {
    pub fn run(
        self,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
        node_storage: NodeStorage,
    ) {
        if matches!(self.mode, ApplyRangeMode::Benchmark) && self.save_state.is_some() {
            panic!("Persisting trie nodes in storage is not compatible with benchmark mode!");
        }
        apply_range(
            self.mode,
            self.storage,
            self.start_index,
            self.end_index,
            self.shard_id,
            self.verbose_output,
            self.csv_file,
            home_dir,
            near_config,
            store,
            self.save_state.map(|temperature| initialize_write_store(temperature, node_storage)),
            self.only_contracts,
        );
    }
}

#[derive(clap::Parser)]
pub struct ApplyReceiptCmd {
    #[clap(long)]
    hash: String,
    #[clap(long, default_value = "trie")]
    storage: StorageSource,
}

impl ApplyReceiptCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let hash = CryptoHash::from_str(&self.hash).unwrap();
        apply_receipt(home_dir, near_config, store, hash, self.storage).unwrap();
    }
}

#[derive(clap::Parser)]
pub struct ApplyTxCmd {
    #[clap(long)]
    hash: String,
    #[clap(long, default_value = "trie")]
    storage: StorageSource,
}

impl ApplyTxCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        let hash = CryptoHash::from_str(&self.hash).unwrap();
        apply_tx(home_dir, near_config, store, hash, self.storage).unwrap();
    }
}

#[derive(clap::Parser)]
pub struct ChainCmd {
    #[clap(long)]
    start_index: BlockHeight,
    #[clap(long)]
    end_index: BlockHeight,
    // If true, show the full hash (block hash and chunk hash) when printing.
    // If false, show only first couple chars.
    #[clap(long)]
    show_full_hashes: bool,
}

impl ChainCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        print_chain(
            self.start_index,
            self.end_index,
            home_dir,
            near_config,
            store,
            self.show_full_hashes,
        );
    }
}

#[derive(clap::Parser)]
pub struct ChunksCmd {
    #[clap(long)]
    chunk_hash: String,
}

impl ChunksCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        let chunk_hash = ChunkHash::from(CryptoHash::from_str(&self.chunk_hash).unwrap());
        get_chunk(chunk_hash, near_config, store)
    }
}

#[derive(clap::Parser)]
pub struct ChunkApplyStatsCmd {
    #[clap(long)]
    block_hash: CryptoHash,
    #[clap(long)]
    shard_id: u64,
}

impl ChunkApplyStatsCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        print_chunk_apply_stats(&self.block_hash, self.shard_id, near_config, store);
    }
}

#[derive(clap::Parser)]
pub struct ContractAccountsCmd {
    #[clap(flatten)]
    filter: ContractAccountFilter,
}

impl ContractAccountsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        contract_accounts(home_dir, store, near_config, self.filter).unwrap();
    }
}

#[derive(clap::Parser)]
pub struct DebugUICmd {
    #[clap(long)]
    port: Option<u16>,
}

impl DebugUICmd {
    pub fn run(
        self,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
        cold_store: Option<Store>,
    ) {
        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        let debug_handler = EntityDebugHandlerImpl {
            hot_store: store.clone(),
            cold_store,
            epoch_manager: epoch_manager.clone(),
            runtime: NightshadeRuntime::from_config(home_dir, store, &near_config, epoch_manager)
                .unwrap(),
        };
        let mut rpc_config = near_config.rpc_config.unwrap_or_default();
        if let Some(port) = self.port {
            rpc_config.addr = ListenerAddr::new(SocketAddr::new(rpc_config.addr.ip(), port));
        }
        actix::System::new()
            .block_on(start_http_for_readonly_debug_querying(
                rpc_config.addr,
                Arc::new(debug_handler),
            ))
            .unwrap();
    }
}

#[derive(clap::Parser)]
pub struct DumpAccountStorageCmd {
    #[clap(long)]
    account_id: String,
    #[clap(long)]
    storage_key: String,
    #[clap(long, value_parser)]
    output: PathBuf,
    #[clap(long)]
    block_height: String,
}

impl DumpAccountStorageCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_account_storage(
            self.account_id,
            self.storage_key,
            &self.output,
            self.block_height,
            home_dir,
            near_config,
            store,
        );
    }
}

#[derive(clap::Parser)]
pub struct DumpCodeCmd {
    #[clap(long)]
    account_id: String,
    #[clap(long, value_parser)]
    output: PathBuf,
}

impl DumpCodeCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_code(self.account_id, &self.output, home_dir, near_config, store);
    }
}

#[derive(clap::Parser)]
pub struct DumpStateCmd {
    /// Optionally, can specify at which height to dump state.
    #[clap(long)]
    height: Option<BlockHeight>,
    /// Dumps state records and genesis config into separate files.
    /// Has reasonable RAM requirements.
    /// Use for chains with large state, such as mainnet and testnet.
    /// If false - writes all information into a single file, which is useful for smaller networks,
    /// such as betanet.
    #[clap(long)]
    stream: bool,
    /// Location of the dumped state.
    /// This is a directory if --stream is set, and a file otherwise.
    #[clap(long, value_parser)]
    file: Option<PathBuf>,
    /// List of account IDs to dump.
    /// Note: validators will always be dumped.
    /// If not set, all account IDs will be dumped.
    #[clap(long)]
    account_ids: Option<Vec<AccountId>>,
    /// List of validators to remain validators.
    /// All other validators will be kicked, but still dumped.
    /// Their stake will be returned to balance.
    #[clap(long)]
    include_validators: Option<Vec<AccountId>>,
}

impl DumpStateCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_state(
            self.height,
            self.stream,
            self.file,
            home_dir,
            near_config,
            store,
            &GenesisChangeConfig::default()
                .with_select_account_ids(self.account_ids)
                .with_whitelist_validators(self.include_validators),
        );
    }
}

#[derive(clap::Parser)]
pub struct DumpStateRedisCmd {
    /// Optionally, can specify at which height to dump state.
    #[clap(long)]
    height: Option<BlockHeight>,
}

impl DumpStateRedisCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_state_redis(self.height, home_dir, near_config, store);
    }
}

#[derive(clap::Parser)]
pub struct DumpTxCmd {
    /// Specify the start block by height to begin dumping transactions from, inclusive.
    #[clap(long)]
    start_height: BlockHeight,
    /// Specify the end block by height to stop dumping transactions at, inclusive.
    #[clap(long)]
    end_height: BlockHeight,
    /// List of account IDs to dump.
    /// If not set, all account IDs will be dumped.
    #[clap(long)]
    account_ids: Option<Vec<AccountId>>,
    /// Optionally, can specify the path of the output.
    #[clap(long)]
    output_path: Option<String>,
}

impl DumpTxCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        dump_tx(
            self.start_height,
            self.end_height,
            home_dir,
            near_config,
            store,
            self.account_ids.as_deref(),
            self.output_path,
        )
        .expect("Failed to dump transaction...")
    }
}

#[derive(clap::Args)]
pub struct EpochInfoCmd {
    /// Which EpochInfos to process.
    #[clap(subcommand)]
    epoch_selection: crate::epoch_info::EpochSelection,
    /// Displays kickouts of the given validator and expected and missed blocks and chunks produced.
    #[clap(long)]
    validator_account_id: Option<String>,
    /// Show only information about kickouts.
    #[clap(long)]
    kickouts_summary: bool,
}

impl EpochInfoCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        print_epoch_info(
            self.epoch_selection,
            self.validator_account_id.map(|s| AccountId::from_str(&s).unwrap()),
            self.kickouts_summary,
            near_config,
            store,
        );
    }
}

#[derive(clap::Args)]
pub struct EpochAnalysisCmd {
    /// Start height of the epochs to analyze.
    #[clap(long)]
    start_height: EpochHeight,
    /// Epoch analysis mode.
    #[clap(subcommand)]
    mode: EpochAnalysisMode,
}

#[derive(clap::Subcommand)]
pub enum EpochAnalysisMode {
    /// Regenerate epoch infos based on previous epoch, assert that epoch info
    /// generation is replayable.
    /// TODO (#11476): doesn't work when start epoch height is <= 1053 because
    /// it will try to generate epoch with height 1055 and fail.
    CheckConsistency,
    /// Generate epoch infos as if latest `PROTOCOL_VERSION` was used since the
    /// start epoch height.
    /// TODO (#11477): doesn't work for start epoch height <= 544 because of
    /// `EpochOutOfBounds` error.
    Backtest,
}

impl EpochAnalysisCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        print_epoch_analysis(self.start_height, self.mode, near_config, store);
    }
}

#[derive(clap::Parser)]
pub struct PartialChunksCmd {
    #[clap(long)]
    partial_chunk_hash: String,
}

impl PartialChunksCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        let partial_chunk_hash =
            ChunkHash::from(CryptoHash::from_str(&self.partial_chunk_hash).unwrap());
        get_partial_chunk(partial_chunk_hash, near_config, store)
    }
}

#[derive(clap::Parser)]
pub struct ReceiptsCmd {
    #[clap(long)]
    receipt_id: String,
}

impl ReceiptsCmd {
    pub fn run(self, near_config: NearConfig, store: Store) {
        get_receipt(CryptoHash::from_str(&self.receipt_id).unwrap(), near_config, store)
    }
}

#[derive(clap::Parser)]
pub struct ReplayHeadersCmd {
    #[clap(long)]
    start_index: Option<BlockHeight>,
    #[clap(long)]
    end_index: Option<BlockHeight>,
}

impl ReplayHeadersCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        replay_headers(self.start_index, self.end_index, home_dir, near_config, store);
    }
}

#[derive(clap::Parser)]
pub struct RocksDBStatsCmd {
    /// Location of the dumped Rocks DB stats.
    #[clap(long, value_parser)]
    file: Option<PathBuf>,
}

impl RocksDBStatsCmd {
    pub fn run(self, store_dir: &Path) {
        get_rocksdb_stats(store_dir, self.file).expect("Couldn't get RocksDB stats");
    }
}

#[derive(clap::Parser, Debug)]
pub struct ScanDbColumnCmd {
    /// Column name, e.g. 'Block' or 'BlockHeader'.
    #[clap(long)]
    column: String,
    #[clap(long)]
    from: Option<String>,
    // List of comma-separated u8-values.
    #[clap(long)]
    from_bytes: Option<String>,
    #[clap(long)]
    from_hash: Option<CryptoHash>,
    #[clap(long)]
    to: Option<String>,
    // List of comma-separated u8-values.
    // For example, if a column key starts wth ShardUId and you want to scan starting from s2.v1 use `--from-bytes 1,0,0,0,2,0,0,0`.
    // Note that the numbers are generally saved as low-endian.
    #[clap(long)]
    to_bytes: Option<String>,
    #[clap(long)]
    to_hash: Option<CryptoHash>,
    #[clap(long)]
    max_keys: Option<usize>,
    #[clap(long, default_value = "false")]
    no_value: bool,
}

impl ScanDbColumnCmd {
    pub fn run(self, store: Store) {
        let lower_bound = Self::prefix(self.from, self.from_bytes, self.from_hash);
        let upper_bound = Self::prefix(self.to, self.to_bytes, self.to_hash);
        crate::scan_db::scan_db_column(
            &self.column,
            lower_bound.as_deref().map(|v| v.as_ref()),
            upper_bound.as_deref().map(|v| v.as_ref()),
            self.max_keys,
            self.no_value,
            store,
        )
    }

    fn prefix(
        s: Option<String>,
        bytes: Option<String>,
        hash: Option<CryptoHash>,
    ) -> Option<Vec<u8>> {
        match (s, bytes, hash) {
            (None, None, None) => None,
            (Some(s), None, None) => Some(s.into_bytes()),
            (None, Some(bytes), None) => {
                Some(bytes.split(",").map(|s| s.parse::<u8>().unwrap()).collect::<Vec<u8>>())
            }
            (None, None, Some(hash)) => Some(borsh::to_vec(&hash).unwrap()),
            _ => panic!("Need to provide exactly one of bytes, str, or hash"),
        }
    }
}

#[derive(clap::Parser)]
pub struct StateChangesCmd {
    #[clap(subcommand)]
    command: crate::state_changes::StateChangesSubCommand,
}

impl StateChangesCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        self.command.run(home_dir, near_config, store)
    }
}

#[derive(clap::Parser)]
pub struct StatePartsCmd {
    /// Shard id.
    #[clap(long)]
    shard_id: ShardId,
    /// Location of serialized state parts.
    #[clap(long)]
    root_dir: Option<PathBuf>,
    /// Store state parts in an S3 bucket.
    #[clap(long)]
    s3_bucket: Option<String>,
    /// Store state parts in an S3 bucket.
    #[clap(long)]
    s3_region: Option<String>,
    /// Store state parts in an GCS bucket.
    #[clap(long)]
    gcs_bucket: Option<String>,
    /// Dump or Apply state parts.
    #[clap(subcommand)]
    command: crate::state_parts::StatePartsSubCommand,
}

impl StatePartsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        self.command.run(
            self.shard_id,
            self.root_dir,
            self.s3_bucket,
            self.s3_region,
            self.gcs_bucket,
            home_dir,
            near_config,
            store,
        );
    }
}

#[derive(clap::Parser)]
pub struct StateStatsCmd {
    #[clap(long, default_value = "2", help = "How many parts split each printed shard into")]
    split_parts: usize,
    #[clap(long, help = "Print stats only for the given shard ID")]
    shard_uid: Option<ShardUId>,
}

impl StateStatsCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        print_state_stats(home_dir, store, near_config, self.split_parts, self.shard_uid);
    }
}

#[derive(clap::Parser)]
pub struct ViewChainCmd {
    #[clap(long)]
    height: Option<BlockHeight>,
    #[clap(long)]
    block: bool,
    #[clap(long)]
    chunk: bool,
}

impl ViewChainCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        view_chain(self.height, self.block, self.chunk, home_dir, near_config, store);
    }
}

#[derive(clap::Parser)]
pub struct ViewGenesisCmd {
    /// If true, displays the genesis block built from nearcore code that combines the
    /// contents of the genesis config (JSON) file with some hard-coded logic to set some
    /// fields of the genesis block. At any given time, the block built this way should match
    /// the genesis block recorded in the store (to be displayed with the --store option).
    #[clap(long)]
    config: bool,
    /// If true, displays the genesis block saved in the store, when the genesis block is built
    /// for the first time. At any given time, this saved block should match the genesis block
    /// built by the code (to be displayed with the --config option).
    #[clap(long)]
    store: bool,
    /// If true, compares the contents of the genesis block saved in the store with
    /// the genesis block built from the genesis config (JSON) file.
    #[clap(long, default_value = "false")]
    compare: bool,
}

impl ViewGenesisCmd {
    pub fn run(self, home_dir: &Path, near_config: NearConfig, store: Store) {
        view_genesis(home_dir, near_config, store, self.config, self.store, self.compare);
    }
}

#[derive(Clone)]
pub enum ViewTrieFormat {
    Full,
    Pretty,
}

impl clap::ValueEnum for ViewTrieFormat {
    fn value_variants<'a>() -> &'a [Self] {
        &[Self::Full, Self::Pretty]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::Full => Some(clap::builder::PossibleValue::new("full")),
            Self::Pretty => Some(clap::builder::PossibleValue::new("pretty")),
        }
    }
}

/// Possible record types in a state trie.
#[derive(Clone)]
#[repr(u8)]
pub enum RecordType {
    Account = col::ACCOUNT,
    ContractCode = col::CONTRACT_CODE,
    AccessKey = col::ACCESS_KEY,
    ReceivedData = col::RECEIVED_DATA,
    PostponedReceiptId = col::POSTPONED_RECEIPT_ID,
    PendingDataCount = col::PENDING_DATA_COUNT,
    PostponedReceipt = col::POSTPONED_RECEIPT,
    DelayedReceiptOrIndices = col::DELAYED_RECEIPT_OR_INDICES,
    ContractData = col::CONTRACT_DATA,
    PromiseYieldReceipt = col::PROMISE_YIELD_RECEIPT,
}

impl clap::ValueEnum for RecordType {
    fn value_variants<'a>() -> &'a [Self] {
        &[
            Self::Account,
            Self::ContractCode,
            Self::AccessKey,
            Self::ReceivedData,
            Self::PostponedReceiptId,
            Self::PendingDataCount,
            Self::PostponedReceipt,
            Self::DelayedReceiptOrIndices,
            Self::ContractData,
            Self::PromiseYieldReceipt,
        ]
    }

    fn to_possible_value(&self) -> Option<clap::builder::PossibleValue> {
        match self {
            Self::Account => Some(clap::builder::PossibleValue::new("account")),
            Self::ContractCode => Some(clap::builder::PossibleValue::new("contract-code")),
            Self::AccessKey => Some(clap::builder::PossibleValue::new("access-key")),
            Self::ReceivedData => Some(clap::builder::PossibleValue::new("received-data")),
            Self::PostponedReceiptId => {
                Some(clap::builder::PossibleValue::new("postponed-receipt-id"))
            }
            Self::PendingDataCount => Some(clap::builder::PossibleValue::new("pending-data-count")),
            Self::PostponedReceipt => Some(clap::builder::PossibleValue::new("postponed-receipt")),
            Self::DelayedReceiptOrIndices => {
                Some(clap::builder::PossibleValue::new("delayed-receipt-or-indices"))
            }
            Self::ContractData => Some(clap::builder::PossibleValue::new("contract-data")),
            Self::PromiseYieldReceipt => {
                Some(clap::builder::PossibleValue::new("promise-yield-receipt"))
            }
        }
    }
}

#[derive(clap::Parser)]
pub struct ViewTrieCmd {
    /// The format of the output. This can be either `full` or `pretty`.
    /// The full format will print all the trie nodes and can be rooted anywhere in the trie.
    /// The pretty format will only print leaf nodes and must be rooted in the state root but is more human friendly.
    #[clap(long, default_value = "pretty")]
    format: ViewTrieFormat,
    /// The hash of the trie node.
    /// For format=full this can be any node in the trie.
    /// For format=pretty this must the state root node.
    /// You can find the state root hash using the `view-state view-chain` command.
    #[clap(long)]
    hash: String,
    /// The id of the shard, a number between [0-NUM_SHARDS). When looking for particular
    /// account you will need to know on which shard it's located.
    #[clap(long)]
    shard_id: u32,
    /// The current shard version based on the shard layout.
    /// You can find the shard version by using the `view-state view-chain` command.
    /// It's typically equal to 0 for single shard localnet or the most recent near_primitives::shard_layout::ShardLayout for prod.
    #[clap(long)]
    shard_version: u32,
    /// The max depth of trie iteration. It's recommended to keep that value small,
    /// otherwise the output may be really large.
    /// For format=full this measures depth in terms of number of trie nodes.
    /// For format=pretty this measures depth in terms of key nibbles.
    #[clap(long)]
    max_depth: u32,
    /// Limits how many entries are printed to the output.
    #[clap(long)]
    limit: Option<u32>,
    /// Filters output to only show records of the given type.
    #[clap(long)]
    record_type: Option<RecordType>,
    /// Skips nodes which AccountId is lexicographically less than `from` (except being a prefix of `from`).
    #[clap(long)]
    from: Option<AccountId>,
    /// Skips nodes which AccountId is lexicographically greater than `to`.
    #[clap(long)]
    to: Option<AccountId>,
}

impl ViewTrieCmd {
    pub fn run(self, store: Store) {
        let hash = CryptoHash::from_str(&self.hash).unwrap();
        let record_type = self.record_type.map(|c| c as u8);

        match self.format {
            ViewTrieFormat::Full => {
                view_trie(
                    store.trie_store(),
                    hash,
                    self.shard_id,
                    self.shard_version,
                    self.max_depth,
                    self.limit,
                    record_type,
                    self.from,
                    self.to,
                )
                .unwrap();
            }
            ViewTrieFormat::Pretty => {
                view_trie_leaves(
                    store.trie_store(),
                    hash,
                    self.shard_id,
                    self.shard_version,
                    self.max_depth,
                    self.limit,
                    record_type,
                    self.from,
                    self.to,
                )
                .unwrap();
            }
        }
    }
}

fn initialize_write_store(temperature: SaveTrieTemperature, node_storage: NodeStorage) -> Store {
    match temperature {
        SaveTrieTemperature::Cold => node_storage
            .get_recovery_store()
            .expect("recovery store must be present if explicitly requested"),
    }
}
