use crate::epoch_info::iterate_and_filter;
use borsh::{BorshDeserialize, BorshSerialize};
use near_chain::{Chain, ChainGenesis, ChainStoreAccess, DoomslugThresholdMode};
use near_client::sync::external::{
    ExternalConnection, StateFileType, create_bucket_read_write, create_bucket_readonly,
    external_storage_location, external_storage_location_directory, get_num_parts_from_filename,
};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::state::PartialState;
use near_primitives::state_part::PartId;
use near_primitives::state_record::StateRecord;
use near_primitives::types::{EpochId, StateRoot};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, EpochHeight, ShardId};
use near_store::{PartialStorage, Store, Trie};
use near_time::Clock;
use nearcore::{NearConfig, NightshadeRuntime, NightshadeRuntimeExt};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(clap::ValueEnum, Clone, Debug, Default)]
pub(crate) enum LoadAction {
    #[default]
    Apply,
    Print,
    Validate,
}

#[derive(clap::Subcommand, Debug, Clone)]
pub(crate) enum StatePartsSubCommand {
    /// Load all or a single state part of a shard and perform an action over those parts.
    Load {
        /// Apply, validate or print.
        #[clap(value_enum, long)]
        action: LoadAction,
        /// If provided, this value will be used instead of looking it up in the headers.
        /// Use if those headers or blocks are not available.
        #[clap(long)]
        state_root: Option<StateRoot>,
        /// If provided, this value will be used instead of looking it up in the headers.
        /// Use if those headers or blocks are not available.
        #[clap(long)]
        sync_hash: Option<CryptoHash>,
        /// Choose a single part id.
        /// If None - affects all state parts.
        #[clap(long)]
        part_id: Option<u64>,
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
    /// Dump all or a single state part of a shard.
    Dump {
        /// Dump part ids starting from this part.
        #[clap(long)]
        part_from: Option<u64>,
        /// Dump part ids up to this part (exclusive).
        #[clap(long)]
        part_to: Option<u64>,
        /// Dump state sync header.
        #[clap(long, short, action)]
        dump_header: bool,
        /// Location of a file with write permissions to the bucket.
        #[clap(long)]
        credentials_file: Option<PathBuf>,
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
    /// Read State Header from the DB
    ReadStateHeader {
        /// Select an epoch to work on.
        #[clap(subcommand)]
        epoch_selection: EpochSelection,
    },
    /// Finalize state sync.
    Finalize {
        /// If provided, this value will be used instead of looking it up in the headers.
        /// Use if those headers or blocks are not available.
        #[clap(long)]
        sync_hash: CryptoHash,
    },
}

impl StatePartsSubCommand {
    pub(crate) fn run(
        self,
        shard_id: ShardId,
        root_dir: Option<PathBuf>,
        s3_bucket: Option<String>,
        s3_region: Option<String>,
        gcs_bucket: Option<String>,
        home_dir: &Path,
        near_config: NearConfig,
        store: Store,
    ) {
        let epoch_manager = EpochManager::new_arc_handle(
            store.clone(),
            &near_config.genesis.config,
            Some(home_dir),
        );
        let shard_tracker = ShardTracker::new(
            near_config.client_config.tracked_shards_config.clone(),
            epoch_manager.clone(),
            near_config.validator_signer.clone(),
        );
        let runtime = NightshadeRuntime::from_config(
            home_dir,
            store.clone(),
            &near_config,
            epoch_manager.clone(),
        )
        .expect("could not create the transaction runtime");
        let chain_genesis = ChainGenesis::new(&near_config.genesis.config);
        let mut chain = Chain::new_for_view_client(
            Clock::real(),
            epoch_manager,
            shard_tracker,
            runtime,
            &chain_genesis,
            DoomslugThresholdMode::TwoThirds,
            false,
            near_config.validator_signer.clone(),
        )
        .unwrap();
        let chain_id = &near_config.genesis.config.chain_id;
        let sys = actix::System::new();
        sys.block_on(async move {
            match self {
                StatePartsSubCommand::Load {
                    action,
                    state_root,
                    sync_hash,
                    part_id,
                    epoch_selection,
                } => {
                    let external = create_external_connection(
                        root_dir,
                        s3_bucket,
                        s3_region,
                        gcs_bucket,
                        None,
                        Mode::ReadOnly,
                    );
                    load_state_parts(
                        action,
                        epoch_selection,
                        shard_id,
                        part_id,
                        state_root,
                        sync_hash,
                        &mut chain,
                        chain_id,
                        store,
                        &external,
                    )
                    .await
                }
                StatePartsSubCommand::Dump {
                    part_from,
                    part_to,
                    dump_header,
                    epoch_selection,
                    credentials_file,
                } => {
                    let external = create_external_connection(
                        root_dir,
                        s3_bucket,
                        s3_region,
                        gcs_bucket,
                        credentials_file,
                        Mode::ReadWrite,
                    );
                    dump_state_parts(
                        epoch_selection,
                        shard_id,
                        part_from,
                        part_to,
                        dump_header,
                        &chain,
                        chain_id,
                        store,
                        &external,
                    )
                    .await
                }
                StatePartsSubCommand::ReadStateHeader { epoch_selection } => {
                    read_state_header(epoch_selection, shard_id, &chain, store)
                }
                StatePartsSubCommand::Finalize { sync_hash } => {
                    finalize_state_sync(sync_hash, shard_id, &mut chain)
                }
            }
            actix::System::current().stop();
        });
        sys.run().unwrap();
    }
}

enum Mode {
    ReadOnly,
    ReadWrite,
}

fn create_external_connection(
    root_dir: Option<PathBuf>,
    bucket: Option<String>,
    region: Option<String>,
    gcs_bucket: Option<String>,
    credentials_file: Option<PathBuf>,
    mode: Mode,
) -> ExternalConnection {
    if let Some(root_dir) = root_dir {
        ExternalConnection::Filesystem { root_dir }
    } else if let (Some(bucket), Some(region)) = (bucket, region) {
        let bucket = match mode {
            Mode::ReadOnly => create_bucket_readonly(&bucket, &region, Duration::from_secs(5)),
            Mode::ReadWrite => {
                create_bucket_read_write(&bucket, &region, Duration::from_secs(5), credentials_file)
            }
        }
        .expect("Failed to create an S3 bucket");
        ExternalConnection::S3 { bucket: Arc::new(bucket) }
    } else if let Some(bucket) = gcs_bucket {
        if let Some(credentials_file) = credentials_file {
            unsafe { std::env::set_var("SERVICE_ACCOUNT", &credentials_file) };
        }
        ExternalConnection::GCS {
            gcs_client: Arc::new(
                object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(&bucket)
                    .build()
                    .unwrap(),
            ),
            reqwest_client: Arc::new(reqwest::Client::default()),
            bucket,
        }
    } else {
        panic!(
            "Please provide --root-dir, or both of --s3-bucket and --s3-region, or --gcs-bucket"
        );
    }
}

#[derive(clap::Subcommand, Debug, Clone)]
pub(crate) enum EpochSelection {
    /// Current epoch.
    Current,
    /// Fetch the given epoch.
    EpochId { epoch_id: String },
    /// Fetch epochs at the given height.
    EpochHeight { epoch_height: EpochHeight },
    /// Fetch an epoch containing the given block hash.
    BlockHash { block_hash: String },
    /// Fetch an epoch containing the given block height.
    BlockHeight { block_height: BlockHeight },
}

impl EpochSelection {
    fn to_epoch_id(&self, store: Store, chain: &Chain) -> EpochId {
        match self {
            EpochSelection::Current => {
                chain.epoch_manager.get_epoch_id(&chain.head().unwrap().last_block_hash).unwrap()
            }
            EpochSelection::EpochId { epoch_id } => {
                EpochId(CryptoHash::from_str(epoch_id).unwrap())
            }
            EpochSelection::EpochHeight { epoch_height } => {
                // Fetch epochs at the given height.
                // There should only be one epoch at a given height. But this is a debug tool, let's check
                // if there are multiple epochs at a given height.
                let epoch_ids = iterate_and_filter(store, |epoch_info| {
                    epoch_info.epoch_height() == *epoch_height
                });
                assert_eq!(epoch_ids.len(), 1, "{:#?}", epoch_ids);
                epoch_ids[0]
            }
            EpochSelection::BlockHash { block_hash } => {
                let block_hash = CryptoHash::from_str(block_hash).unwrap();
                chain.epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
            EpochSelection::BlockHeight { block_height } => {
                // Fetch an epoch containing the given block height.
                let block_hash =
                    chain.chain_store().get_block_hash_by_height(*block_height).unwrap();
                chain.epoch_manager.get_epoch_id(&block_hash).unwrap()
            }
        }
    }
}

/// Returns block hash of some block of the given `epoch_info` epoch.
fn get_any_block_hash_of_epoch(epoch_info: &EpochInfo, chain: &Chain) -> CryptoHash {
    let head = chain.chain_store().head().unwrap();
    let mut cur_block_info = chain.epoch_manager.get_block_info(&head.last_block_hash).unwrap();
    // EpochManager doesn't have an API that maps EpochId to Blocks, and this function works
    // around that limitation by iterating over the epochs.
    // This workaround is acceptable because:
    // 1) Extending EpochManager's API is a major change.
    // 2) This use case is not critical at all.
    loop {
        let cur_epoch_info = chain.epoch_manager.get_epoch_info(cur_block_info.epoch_id()).unwrap();
        let cur_epoch_height = cur_epoch_info.epoch_height();
        assert!(
            cur_epoch_height >= epoch_info.epoch_height(),
            "cur_block_info: {:#?}, epoch_info.epoch_height: {}",
            cur_block_info,
            epoch_info.epoch_height()
        );
        let epoch_first_block_info =
            chain.epoch_manager.get_block_info(cur_block_info.epoch_first_block()).unwrap();
        let prev_epoch_last_block_info =
            chain.epoch_manager.get_block_info(epoch_first_block_info.prev_hash()).unwrap();

        if cur_epoch_height == epoch_info.epoch_height() {
            return *cur_block_info.hash();
        }

        cur_block_info = prev_epoch_last_block_info;
    }
}

async fn load_state_parts(
    action: LoadAction,
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_id: Option<u64>,
    maybe_state_root: Option<StateRoot>,
    maybe_sync_hash: Option<CryptoHash>,
    chain: &Chain,
    chain_id: &str,
    store: Store,
    external: &ExternalConnection,
) {
    let epoch_id = epoch_selection.to_epoch_id(store, chain);
    let (state_root, epoch_height, epoch_id, sync_hash) =
        if let (Some(state_root), Some(sync_hash), EpochSelection::EpochHeight { epoch_height }) =
            (maybe_state_root, maybe_sync_hash, &epoch_selection)
        {
            (state_root, *epoch_height, epoch_id, sync_hash)
        } else {
            let epoch = chain.epoch_manager.get_epoch_info(&epoch_id).unwrap();

            let sync_hash = get_any_block_hash_of_epoch(&epoch, chain);
            let sync_hash = match chain.get_sync_hash(&sync_hash).unwrap() {
                Some(h) => h,
                None => {
                    tracing::warn!(target: "state-parts", ?epoch_id, "sync hash not yet known");
                    return;
                }
            };

            let state_header =
                chain.state_sync_adapter.get_state_response_header(shard_id, sync_hash).unwrap();
            let state_root = state_header.chunk_prev_state_root();

            (state_root, epoch.epoch_height(), epoch_id, sync_hash)
        };

    let directory_path = external_storage_location_directory(
        chain_id,
        &epoch_id,
        epoch_height,
        shard_id,
        &StateFileType::StatePart { part_id: 0, num_parts: 0 },
    );
    let part_file_names = external.list_objects(shard_id, &directory_path).await.unwrap();
    assert!(!part_file_names.is_empty());
    let num_parts = part_file_names.len() as u64;
    assert_eq!(Some(num_parts), get_num_parts_from_filename(&part_file_names[0]));
    let part_ids = get_part_ids(part_id, part_id.map(|x| x + 1), num_parts);
    tracing::info!(
        target: "state-parts",
        epoch_height,
        %shard_id,
        num_parts,
        ?sync_hash,
        ?part_ids,
        "Loading state as seen at the beginning of the specified epoch.",
    );

    let timer = Instant::now();
    for part_id in part_ids {
        let timer = Instant::now();
        assert!(part_id < num_parts, "part_id: {}, num_parts: {}", part_id, num_parts);
        let file_type = StateFileType::StatePart { part_id, num_parts };
        let location =
            external_storage_location(chain_id, &epoch_id, epoch_height, shard_id, &file_type);
        let part = external.get_file(shard_id, &location, &file_type).await.unwrap();

        match action {
            LoadAction::Apply => {
                chain
                    .state_sync_adapter
                    .set_state_part(shard_id, sync_hash, PartId::new(part_id, num_parts), &part)
                    .unwrap();
                chain
                    .runtime_adapter
                    .apply_state_part(
                        shard_id,
                        &state_root,
                        PartId::new(part_id, num_parts),
                        &part,
                        &epoch_id,
                    )
                    .unwrap();
                tracing::info!(target: "state-parts", part_id, part_length = part.len(), elapsed_sec = timer.elapsed().as_secs_f64(), "Loaded a state part");
            }
            LoadAction::Validate => {
                assert!(chain.runtime_adapter.validate_state_part(
                    shard_id,
                    &state_root,
                    PartId::new(part_id, num_parts),
                    &part
                ));
                tracing::info!(target: "state-parts", part_id, part_length = part.len(), elapsed_sec = timer.elapsed().as_secs_f64(), "Validated a state part");
            }
            LoadAction::Print => {
                print_state_part(&state_root, PartId::new(part_id, num_parts), &part)
            }
        }
    }
    tracing::info!(target: "state-parts", total_elapsed_sec = timer.elapsed().as_secs_f64(), "Loaded all requested state parts");
}

fn print_state_part(state_root: &StateRoot, _part_id: PartId, data: &[u8]) {
    let trie_nodes: PartialState = BorshDeserialize::try_from_slice(data).unwrap();
    let trie =
        Trie::from_recorded_storage(PartialStorage { nodes: trie_nodes }, *state_root, false);
    trie.print_recursive(
        &mut std::io::stdout().lock(),
        &state_root,
        u32::MAX,
        None,
        None,
        &None,
        &None,
    );
}

async fn dump_state_parts(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    part_from: Option<u64>,
    part_to: Option<u64>,
    dump_header: bool,
    chain: &Chain,
    chain_id: &str,
    store: Store,
    external: &ExternalConnection,
) {
    let epoch_id = epoch_selection.to_epoch_id(store, chain);
    let epoch = chain.epoch_manager.get_epoch_info(&epoch_id).unwrap();
    let sync_hash = get_any_block_hash_of_epoch(&epoch, chain);
    let sync_hash = match chain.get_sync_hash(&sync_hash).unwrap() {
        Some(h) => h,
        None => {
            tracing::warn!(target: "state-parts", ?epoch_id, "sync hash not yet known");
            return;
        }
    };
    let sync_block_header = chain.get_block_header(&sync_hash).unwrap();
    let sync_prev_header = chain.get_previous_header(&sync_block_header).unwrap();
    let sync_prev_prev_hash = sync_prev_header.prev_hash();

    let state_header =
        chain.state_sync_adapter.compute_state_response_header(shard_id, sync_hash).unwrap();
    let state_root = state_header.chunk_prev_state_root();
    let num_parts = state_header.num_state_parts();
    let part_ids = get_part_ids(part_from, part_to, num_parts);
    let epoch_height = epoch.epoch_height();

    tracing::info!(
        target: "state-parts",
        epoch_height,
        epoch_id = ?epoch_id.0,
        %shard_id,
        num_parts,
        ?sync_hash,
        ?part_ids,
        ?state_root,
        "Dumping state as seen at the beginning of the specified epoch.",
    );

    let timer = Instant::now();

    // dump header
    if dump_header {
        let mut state_sync_header_buf: Vec<u8> = Vec::new();
        state_header.serialize(&mut state_sync_header_buf).unwrap();

        let file_type = StateFileType::StateHeader;
        let location =
            external_storage_location(&chain_id, &epoch_id, epoch_height, shard_id, &file_type);
        external
            .put_file(file_type, &state_sync_header_buf, shard_id, &location)
            .await
            .expect("Failed to put header into external storage.");
        tracing::info!(target: "state-parts", elapsed_sec = timer.elapsed().as_secs_f64(), "Header saved to external storage.");
    }

    // dump parts
    for part_id in part_ids {
        let timer = Instant::now();
        assert!(part_id < num_parts, "part_id: {}, num_parts: {}", part_id, num_parts);
        let state_part = chain
            .runtime_adapter
            .obtain_state_part(
                shard_id,
                sync_prev_prev_hash,
                &state_root,
                PartId::new(part_id, num_parts),
            )
            .unwrap();

        let file_type = StateFileType::StatePart { part_id, num_parts };
        let location = external_storage_location(
            &chain_id,
            &epoch_id,
            epoch.epoch_height(),
            shard_id,
            &file_type,
        );
        external.put_file(file_type, &state_part, shard_id, &location).await.unwrap();
        // part_storage.write(&state_part, part_id, num_parts);
        let elapsed_sec = timer.elapsed().as_secs_f64();
        let first_state_record = get_first_state_record(&state_root, &state_part);
        tracing::info!(
            target: "state-parts",
            part_id,
            part_length = state_part.len(),
            elapsed_sec,
            first_state_record = ?first_state_record.map(|sr| format!("{}", sr)),
            "Wrote a state part");
    }
    tracing::info!(target: "state-parts", total_elapsed_sec = timer.elapsed().as_secs_f64(), "Wrote all requested state parts");
}

/// Returns the first `StateRecord` encountered while iterating over a sub-trie in the state part.
fn get_first_state_record(state_root: &StateRoot, data: &[u8]) -> Option<StateRecord> {
    let trie_nodes = BorshDeserialize::try_from_slice(data).unwrap();
    let trie =
        Trie::from_recorded_storage(PartialStorage { nodes: trie_nodes }, *state_root, false);

    for (key, value) in trie.disk_iter().unwrap().flatten() {
        if let Some(sr) = StateRecord::from_raw_key_value(&key, value) {
            return Some(sr);
        }
    }
    None
}

/// Reads `StateHeader` stored in the DB.
fn read_state_header(
    epoch_selection: EpochSelection,
    shard_id: ShardId,
    chain: &Chain,
    store: Store,
) {
    let epoch_id = epoch_selection.to_epoch_id(store, chain);
    let epoch = chain.epoch_manager.get_epoch_info(&epoch_id).unwrap();

    let sync_hash = get_any_block_hash_of_epoch(&epoch, chain);
    let sync_hash = match chain.get_sync_hash(&sync_hash).unwrap() {
        Some(h) => h,
        None => {
            tracing::warn!(target: "state-parts", ?epoch_id, "sync hash not yet known");
            return;
        }
    };

    let state_header = chain.chain_store().get_state_header(shard_id, sync_hash);
    tracing::info!(target: "state-parts", ?epoch_id, ?sync_hash, ?state_header);
}

fn finalize_state_sync(sync_hash: CryptoHash, shard_id: ShardId, chain: &mut Chain) {
    chain.set_state_finalize(shard_id, sync_hash).unwrap()
}

fn get_part_ids(part_from: Option<u64>, part_to: Option<u64>, num_parts: u64) -> Range<u64> {
    part_from.unwrap_or(0)..part_to.unwrap_or(num_parts)
}
