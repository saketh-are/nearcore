//! Client actor orchestrates Client and facilitates network connection.
//! It should just serve as a coordinator class to handle messages and check triggers but immediately
//! pass the control to Client. This means, any real block processing or production logic should
//! be put in Client.
//! Unfortunately, this is not the case today. We are in the process of refactoring ClientActor
//! <https://github.com/near/nearcore/issues/7899>

use crate::chunk_executor_actor::ProcessedBlock;
#[cfg(feature = "test_features")]
pub use crate::chunk_producer::AdvProduceChunksMode;
#[cfg(feature = "test_features")]
use crate::client::AdvProduceBlocksMode;
use crate::client::{CatchupState, Client, EPOCH_START_INFO_BLOCKS};
use crate::config_updater::ConfigUpdater;
use crate::debug::new_network_info_view;
use crate::info::{InfoHelper, display_sync_status};
use crate::stateless_validation::chunk_endorsement::ChunkEndorsementTracker;
use crate::stateless_validation::chunk_validation_actor::{
    ChunkValidationActorInner, ChunkValidationSender, ChunkValidationSyncActor,
};
use crate::stateless_validation::partial_witness::partial_witness_actor::PartialWitnessSenderForClient;
use crate::sync::handler::SyncHandlerRequest;
use crate::sync::state::chain_requests::{
    ChainFinalizationRequest, ChainSenderForStateSync, StateHeaderValidationRequest,
};
use crate::sync_jobs_actor::{ClientSenderForSyncJobs, SyncJobsActor};
use crate::{AsyncComputationMultiSpawner, StatusResponse, metrics};
use actix::Actor;
use near_async::actix::wrapper::ActixWrapper;
use near_async::futures::{DelayedActionRunner, DelayedActionRunnerExt, FutureSpawner};
use near_async::messaging::{
    self, CanSend, Handler, IntoMultiSender, IntoSender as _, LateBoundSender, Sender, noop,
};
use near_async::time::{Clock, Utc};
use near_async::time::{Duration, Instant};
use near_async::{MultiSend, MultiSenderFrom};
use near_chain::ApplyChunksSpawner;
#[cfg(feature = "test_features")]
use near_chain::ChainStoreAccess;
use near_chain::chain::{ApplyChunksDoneMessage, BlockCatchUpRequest, BlockCatchUpResponse};
use near_chain::resharding::types::ReshardingSender;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain::state_snapshot_actor::SnapshotCallbacks;
use near_chain::test_utils::format_hash;
use near_chain::types::RuntimeAdapter;
use near_chain::{
    Block, BlockHeader, ChainGenesis, Provenance, byzantine_assert, near_chain_primitives,
};
use near_chain_configs::{ClientConfig, MutableValidatorSigner};
use near_chain_primitives::error::EpochErrorResultToChainError;
use near_chunks::adapter::ShardsManagerRequestFromClient;
use near_chunks::client::{ShardedTransactionPool, ShardsManagerResponse};
use near_client_primitives::types::{
    Error, GetClientConfig, GetClientConfigError, GetNetworkInfo, NetworkInfoResponse,
    StateSyncStatus, Status, StatusError, StatusSyncInfo, SyncStatus,
};
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_network::client::{
    BlockApproval, BlockHeadersResponse, BlockResponse, OptimisticBlockMessage, SetNetworkInfo,
    StateResponseReceived,
};
use near_network::types::ReasonForBan;
use near_network::types::{
    NetworkInfo, NetworkRequests, PeerManagerAdapter, PeerManagerMessageRequest,
};
use near_o11y::span_wrapped_msg::SpanWrapped;
use near_performance_metrics;
use near_performance_metrics_macros::perf;
use near_primitives::block::Tip;
use near_primitives::block_header::ApprovalType;
use near_primitives::epoch_info::RngSeed;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::{AccountId, BlockHeight};
use near_primitives::unwrap_or_return;
use near_primitives::utils::MaybeValidated;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, get_protocol_upgrade_schedule};
use near_primitives::views::{DetailedDebugStatus, ValidatorInfo};
#[cfg(feature = "test_features")]
use near_store::DBCol;
use near_store::adapter::StoreAdapter;
use near_telemetry::TelemetryEvent;
use parking_lot::Mutex;
use rand::seq::SliceRandom;
use rand::{Rng, thread_rng};
use std::fmt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, debug_span, error, info, trace, warn};

/// Multiplier on `max_block_time` to wait until deciding that chain stalled.
const STATUS_WAIT_TIME_MULTIPLIER: i32 = 10;
/// `max_block_production_time` times this multiplier is how long we wait before rebroadcasting
/// the current `head`
const HEAD_STALL_MULTIPLIER: u32 = 4;

pub type ClientActor = ActixWrapper<ClientActorInner>;

/// Returns random seed sampled from the current thread
fn random_seed_from_thread() -> RngSeed {
    let mut rng_seed: RngSeed = [0; 32];
    rand::thread_rng().fill(&mut rng_seed);
    rng_seed
}

/// Blocks the program until given genesis time arrives.
fn wait_until_genesis(genesis_time: &Utc) {
    loop {
        let duration = *genesis_time - Clock::real().now_utc();
        if duration <= Duration::ZERO {
            break;
        }
        tracing::info!(target: "near", "Waiting until genesis: {}d {}h {}m {}s",
              duration.whole_days(),
              (duration.whole_hours() % 24),
              (duration.whole_minutes() % 60),
              (duration.whole_seconds() % 60));
        let wait = duration.min(Duration::seconds(10)).unsigned_abs();
        std::thread::sleep(wait);
    }
}

pub struct StartClientResult {
    pub client_actor: actix::Addr<ClientActor>,
    pub client_arbiter_handle: actix::ArbiterHandle,
    pub tx_pool: Arc<Mutex<ShardedTransactionPool>>,
    pub chunk_endorsement_tracker: Arc<ChunkEndorsementTracker>,
    pub chunk_validation_actor: actix::Addr<ChunkValidationSyncActor>,
}

/// Starts client in a separate Arbiter (thread).
pub fn start_client(
    clock: Clock,
    client_config: ClientConfig,
    chain_genesis: ChainGenesis,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    shard_tracker: ShardTracker,
    runtime: Arc<dyn RuntimeAdapter>,
    node_id: PeerId,
    state_sync_future_spawner: Arc<dyn FutureSpawner>,
    network_adapter: PeerManagerAdapter,
    shards_manager_adapter: Sender<ShardsManagerRequestFromClient>,
    validator_signer: MutableValidatorSigner,
    telemetry_sender: Sender<TelemetryEvent>,
    snapshot_callbacks: Option<SnapshotCallbacks>,
    sender: Option<broadcast::Sender<()>>,
    adv: crate::adversarial::Controls,
    config_updater: Option<ConfigUpdater>,
    partial_witness_adapter: PartialWitnessSenderForClient,
    enable_doomslug: bool,
    seed: Option<RngSeed>,
    resharding_sender: ReshardingSender,
) -> StartClientResult {
    let client_arbiter = actix::Arbiter::new();
    let client_arbiter_handle = client_arbiter.handle();

    wait_until_genesis(&chain_genesis.time);

    let chain_sender_for_state_sync = LateBoundSender::<ChainSenderForStateSync>::new();
    let client_sender_for_client = LateBoundSender::<ClientSenderForClient>::new();
    let protocol_upgrade_schedule = get_protocol_upgrade_schedule(client_config.chain_id.as_str());
    let multi_spawner = AsyncComputationMultiSpawner::default();

    let chunk_validation_adapter = LateBoundSender::<ChunkValidationSender>::new();

    // TODO(spice): Initialize CoreStatementsProcessor properly.
    let spice_core_processor = CoreStatementsProcessor::new(
        runtime.store().chain_store(),
        epoch_manager.clone(),
        noop().into_sender(),
        noop().into_sender(),
    );
    let client = Client::new(
        clock.clone(),
        client_config,
        chain_genesis,
        epoch_manager.clone(),
        shard_tracker,
        runtime.clone(),
        network_adapter.clone(),
        shards_manager_adapter,
        validator_signer,
        enable_doomslug,
        seed.unwrap_or_else(random_seed_from_thread),
        snapshot_callbacks,
        multi_spawner,
        partial_witness_adapter,
        resharding_sender,
        state_sync_future_spawner,
        chain_sender_for_state_sync.as_multi_sender(),
        client_sender_for_client.as_multi_sender(),
        chunk_validation_adapter.as_multi_sender(),
        protocol_upgrade_schedule,
        spice_core_processor,
    )
    .unwrap();

    let client_sender_for_sync_jobs = LateBoundSender::<ClientSenderForSyncJobs>::new();
    let sync_jobs_actor = SyncJobsActor::new(client_sender_for_sync_jobs.as_multi_sender());
    let sync_jobs_actor_addr = sync_jobs_actor.spawn_actix_actor();

    // Create chunk validation actor
    let genesis_block = client.chain.genesis_block();
    let num_chunk_validation_threads = client.config.chunk_validation_threads;

    let chunk_validation_actor_addr = ChunkValidationActorInner::spawn_actix_actors(
        client.chain.chain_store().clone(),
        genesis_block,
        epoch_manager.clone(),
        runtime.clone(),
        network_adapter.clone().into_sender(),
        client.validator_signer.clone(),
        client.config.save_latest_witnesses,
        client.config.save_invalid_witnesses,
        {
            // The number of shards for the binary's latest `PROTOCOL_VERSION` is used as a thread limit.
            // This assumes that:
            // a) The number of shards will not grow above this limit without the binary being updated (no dynamic resharding),
            // b) Under normal conditions, the node will not process more chunks at the same time as there are shards.
            let max_num_shards = runtime.get_shard_layout(PROTOCOL_VERSION).num_shards() as usize;
            ApplyChunksSpawner::Default.into_spawner(max_num_shards)
        },
        client.config.orphan_state_witness_pool_size,
        client.config.orphan_state_witness_max_size.as_u64(),
        num_chunk_validation_threads,
    );

    let client_actor_inner = ClientActorInner::new(
        clock,
        client,
        node_id,
        network_adapter,
        telemetry_sender,
        sender,
        adv,
        config_updater,
        sync_jobs_actor_addr.into_multi_sender(),
        // TODO(spice): Pass in chunk_executor_sender.
        noop().into_sender(),
        // TODO(spice): Pass in spice_chunk_validator_sender.
        noop().into_sender(),
    )
    .unwrap();
    let tx_pool = client_actor_inner.client.chunk_producer.sharded_tx_pool.clone();
    let chunk_endorsement_tracker =
        Arc::clone(&client_actor_inner.client.chunk_endorsement_tracker);
    let client_addr = ClientActor::start_in_arbiter(&client_arbiter_handle, move |_| {
        ActixWrapper::new(client_actor_inner)
    });

    client_sender_for_sync_jobs.bind(client_addr.clone().into_multi_sender());
    client_sender_for_client.bind(client_addr.clone().into_multi_sender());
    chain_sender_for_state_sync.bind(client_addr.clone().into_multi_sender());
    chunk_validation_adapter.bind(chunk_validation_actor_addr.clone().into_multi_sender());

    StartClientResult {
        client_actor: client_addr,
        client_arbiter_handle,
        tx_pool,
        chunk_endorsement_tracker,
        chunk_validation_actor: chunk_validation_actor_addr,
    }
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct ClientSenderForClient {
    pub apply_chunks_done: Sender<SpanWrapped<ApplyChunksDoneMessage>>,
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct SyncJobsSenderForClient {
    pub block_catch_up: Sender<BlockCatchUpRequest>,
}

pub struct ClientActorInner {
    clock: Clock,

    /// Adversarial controls
    pub adv: crate::adversarial::Controls,

    pub client: Client,
    network_adapter: PeerManagerAdapter,
    network_info: NetworkInfo,
    /// Identity that represents this Client at the network level.
    /// It is used as part of the messages that identify this client.
    node_id: PeerId,
    /// Last time we announced our accounts as validators.
    last_validator_announce_time: Option<Instant>,
    /// Info helper.
    info_helper: InfoHelper,

    /// Last time handle_block_production method was called
    block_production_next_attempt: near_async::time::Utc,

    // Last time when log_summary method was called.
    log_summary_timer_next_attempt: near_async::time::Utc,

    doomslug_timer_next_attempt: near_async::time::Utc,
    sync_timer_next_attempt: near_async::time::Utc,
    sync_started: bool,
    sync_jobs_sender: SyncJobsSenderForClient,

    #[cfg(feature = "sandbox")]
    fastforward_delta: near_primitives::types::BlockHeightDelta,

    /// Synchronization measure to allow graceful shutdown.
    /// Informs the system when a ClientActor gets dropped.
    shutdown_signal: Option<broadcast::Sender<()>>,

    /// Manages updating the config.
    config_updater: Option<ConfigUpdater>,

    /// With spice chunk executor executes chunks asynchronously.
    /// Should be noop sender otherwise.
    chunk_executor_sender: Sender<ProcessedBlock>,

    /// With spice spice chunk validator validates witnesses for which it
    /// needs to be aware of new blocks.
    /// Without spice should be a noop sender.
    spice_chunk_validator_sender: Sender<ProcessedBlock>,
}

impl messaging::Actor for ClientActorInner {
    fn start_actor(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        self.start(ctx);
    }

    /// Wrapper for processing actix message which must be called after receiving it.
    ///
    /// Due to a bug in Actix library, while there are messages in mailbox, Actix
    /// will prioritize processing messages until mailbox is empty. In such case execution
    /// of any other task scheduled with `run_later` will be delayed. At the same time,
    /// we have several important functions which have to be called regularly, so we put
    /// these calls into `check_triggers` and call it here as a quick hack.
    fn wrap_handler<M, R>(
        &mut self,
        msg: M,
        ctx: &mut dyn DelayedActionRunner<Self>,
        f: impl FnOnce(&mut Self, M, &mut dyn DelayedActionRunner<Self>) -> R,
    ) -> R {
        self.check_triggers(ctx);
        let _span = tracing::debug_span!(target: "client", "NetworkClientMessage").entered();
        let msg_type = std::any::type_name::<M>();
        metrics::CLIENT_MESSAGES_COUNT.with_label_values(&[msg_type]).inc();
        let timer =
            metrics::CLIENT_MESSAGES_PROCESSING_TIME.with_label_values(&[msg_type]).start_timer();
        let res = f(self, msg, ctx);
        timer.observe_duration();
        res
    }
}

/// Before stateless validation we require validators to track all shards, see
/// https://github.com/near/nearcore/issues/7388.
/// Since stateless validation we use single shard tracking.
fn check_validator_tracked_shards(client: &Client, validator_id: &AccountId) -> Result<(), Error> {
    if !matches!(
        client.config.chain_id.as_ref(),
        near_primitives::chains::MAINNET | near_primitives::chains::TESTNET
    ) {
        return Ok(());
    }

    let epoch_id = client.chain.head()?.epoch_id;
    let epoch_info = client.epoch_manager.get_epoch_info(&epoch_id).into_chain_error()?;

    // We do not apply the check if this is not a current validator, see
    // https://github.com/near/nearcore/issues/11821.
    if epoch_info.get_validator_by_account(validator_id).is_none() {
        warn!(target: "client", "The account '{}' is not a current validator, this node won't be validating in the current epoch", validator_id);
        return Ok(());
    }

    if client.config.tracked_shards_config.tracks_all_shards() {
        panic!(
            "The `chain_id` field specified in genesis is among mainnet/testnet, so validator must not track all shards. Please set `tracked_shards_config` field in `config.json` to \"NoShards\"."
        );
    }

    Ok(())
}

impl ClientActorInner {
    pub fn new(
        clock: Clock,
        client: Client,
        node_id: PeerId,
        network_adapter: PeerManagerAdapter,
        telemetry_sender: Sender<TelemetryEvent>,
        shutdown_signal: Option<broadcast::Sender<()>>,
        adv: crate::adversarial::Controls,
        config_updater: Option<ConfigUpdater>,
        sync_jobs_sender: SyncJobsSenderForClient,
        chunk_executor_sender: Sender<ProcessedBlock>,
        spice_chunk_validator_sender: Sender<ProcessedBlock>,
    ) -> Result<Self, Error> {
        if let Some(vs) = &client.validator_signer.get() {
            info!(target: "client", "Starting validator node: {}", vs.validator_id());
            check_validator_tracked_shards(&client, vs.validator_id())?;
        }
        let info_helper = InfoHelper::new(clock.clone(), telemetry_sender, &client.config);

        let now = clock.now_utc();
        Ok(ClientActorInner {
            clock,
            adv,
            client,
            network_adapter,
            node_id,
            network_info: NetworkInfo {
                connected_peers: vec![],
                tier1_connections: vec![],
                num_connected_peers: 0,
                peer_max_count: 0,
                highest_height_peers: vec![],
                received_bytes_per_sec: 0,
                sent_bytes_per_sec: 0,
                known_producers: vec![],
                tier1_accounts_keys: vec![],
                tier1_accounts_data: vec![],
            },
            last_validator_announce_time: None,
            info_helper,
            block_production_next_attempt: now,
            log_summary_timer_next_attempt: now,
            doomslug_timer_next_attempt: now,
            sync_timer_next_attempt: now,
            sync_started: false,
            #[cfg(feature = "sandbox")]
            fastforward_delta: 0,
            shutdown_signal,
            config_updater,
            sync_jobs_sender,
            chunk_executor_sender,
            spice_chunk_validator_sender,
        })
    }
}

#[cfg(feature = "test_features")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AdvProduceBlockHeightSelection {
    /// Place the new block on top of the latest known block. Block's height will be the next
    /// integer.
    NextHeightOnLatestKnown,
    /// Place the new block on top of the latest known block. Block height is arbitrary.
    SelectedHeightOnLatestKnown { produced_block_height: BlockHeight },
    /// Place the new block on top of the current head. Block's height will be the next integer.
    NextHeightOnCurrentHead,
    /// Place the new block on top of current head. Block height is arbitrary.
    SelectedHeightOnCurrentHead { produced_block_height: BlockHeight },
    /// Place the new block on top of an existing block at height `base_block_height`. Block's
    /// height will be the next integer.
    NextHeightOnSelectedBlock { base_block_height: BlockHeight },
    /// Place the new block on top of an existing block at height `base_block_height`. Block height
    /// is arbitrary.
    SelectedHeightOnSelectedBlock {
        produced_block_height: BlockHeight,
        base_block_height: BlockHeight,
    },
}

#[cfg(feature = "test_features")]
#[derive(actix::Message, Debug)]
#[rtype(result = "Option<u64>")]
pub enum NetworkAdversarialMessage {
    AdvProduceBlocks(u64, bool),
    AdvProduceChunks(AdvProduceChunksMode),
    AdvInsertInvalidTransactions(bool),
    AdvSwitchToHeight(u64),
    AdvDisableHeaderSync,
    AdvDisableDoomslug,
    AdvGetSavedBlocks,
    AdvCheckStorageConsistency,
}

#[cfg(feature = "test_features")]
impl Handler<NetworkAdversarialMessage> for ClientActorInner {
    fn handle(&mut self, msg: NetworkAdversarialMessage) {
        Handler::<NetworkAdversarialMessage, Option<u64>>::handle(self, msg);
    }
}

#[cfg(feature = "test_features")]
impl Handler<NetworkAdversarialMessage, Option<u64>> for ClientActorInner {
    fn handle(&mut self, msg: NetworkAdversarialMessage) -> Option<u64> {
        match msg {
            NetworkAdversarialMessage::AdvDisableDoomslug => {
                info!(target: "adversary", "Turning Doomslug off");
                self.adv.set_disable_doomslug(true);
                self.client.doomslug.adv_disable();
                self.client.chain.adv_disable_doomslug();
                None
            }
            NetworkAdversarialMessage::AdvDisableHeaderSync => {
                info!(target: "adversary", "Blocking header sync");
                self.adv.set_disable_header_sync(true);
                None
            }
            NetworkAdversarialMessage::AdvProduceBlocks(num_blocks, only_valid) => {
                self.adv_produce_blocks_on(
                    num_blocks,
                    only_valid,
                    AdvProduceBlockHeightSelection::NextHeightOnLatestKnown,
                );
                None
            }
            NetworkAdversarialMessage::AdvSwitchToHeight(height) => {
                info!(target: "adversary", "Switching to height {:?}", height);
                let mut chain_store_update = self.client.chain.mut_chain_store().store_update();
                chain_store_update.save_largest_target_height(height);
                chain_store_update
                    .adv_save_latest_known(height)
                    .expect("adv method should not fail");
                chain_store_update.commit().expect("adv method should not fail");
                None
            }
            NetworkAdversarialMessage::AdvGetSavedBlocks => {
                info!(target: "adversary", "Requested number of saved blocks");
                let store = self.client.chain.chain_store().store();
                let mut num_blocks = 0;
                for _ in store.iter(DBCol::Block) {
                    num_blocks += 1;
                }
                Some(num_blocks)
            }
            NetworkAdversarialMessage::AdvCheckStorageConsistency => {
                // timeout is set to 1.5 seconds to give some room as we wait in Nightly for 2 seconds
                let timeout = 1500;
                info!(target: "adversary", "Check Storage Consistency, timeout set to {:?} milliseconds", timeout);
                let mut genesis = near_chain_configs::GenesisConfig::default();
                genesis.genesis_height = self.client.chain.chain_store().get_genesis_height();
                let mut store_validator = near_chain::store_validator::StoreValidator::new(
                    genesis,
                    self.client.epoch_manager.clone(),
                    self.client.shard_tracker.clone(),
                    self.client.runtime_adapter.clone(),
                    self.client.chain.chain_store().store(),
                    self.adv.is_archival(),
                );
                store_validator.set_timeout(timeout);
                store_validator.validate();
                if store_validator.is_failed() {
                    error!(target: "client", "Storage Validation failed, {:?}", store_validator.errors);
                    Some(0)
                } else {
                    Some(store_validator.tests_done())
                }
            }
            NetworkAdversarialMessage::AdvProduceChunks(adv_produce_chunks) => {
                info!(target: "adversary", mode=?adv_produce_chunks, "setting adversary produce chunks");
                self.client.chunk_producer.adversarial.produce_mode = Some(adv_produce_chunks);
                None
            }
            NetworkAdversarialMessage::AdvInsertInvalidTransactions(on) => {
                info!(target: "adversary", on, "invalid transactions");
                self.client.chunk_producer.adversarial.produce_invalid_tx_in_chunks = on;
                None
            }
        }
    }
}

impl Handler<SpanWrapped<OptimisticBlockMessage>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<OptimisticBlockMessage>) {
        let OptimisticBlockMessage { optimistic_block, from_peer } = msg.span_unwrap();
        debug!(target: "client", block_height = optimistic_block.inner.block_height, prev_block_hash = ?optimistic_block.inner.prev_block_hash, ?from_peer, "OptimisticBlockMessage");

        self.client.receive_optimistic_block(optimistic_block, &from_peer);
    }
}

impl Handler<SpanWrapped<BlockResponse>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<BlockResponse>) {
        let BlockResponse { block, peer_id, was_requested } = msg.span_unwrap();
        debug!(target: "client", block_height = block.header().height(), block_hash = ?block.header().hash(), "BlockResponse");
        let blocks_at_height =
            self.client.chain.chain_store().get_all_block_hashes_by_height(block.header().height());
        if was_requested
            || blocks_at_height.is_err()
            || blocks_at_height.as_ref().unwrap().is_empty()
        {
            // This is a very sneaky piece of logic.
            if self.maybe_receive_state_sync_blocks(Arc::clone(&block)) {
                // A node is syncing its state. Don't consider receiving
                // blocks other than the few special ones that State Sync expects.
                return;
            }
            self.client.receive_block(
                block,
                peer_id,
                was_requested,
                Some(self.client.myself_sender.apply_chunks_done.clone()),
            );
        } else {
            match self.client.epoch_manager.get_epoch_id_from_prev_block(block.header().prev_hash())
            {
                Ok(epoch_id) => {
                    if let Some(hashes) = blocks_at_height.unwrap().get(&epoch_id) {
                        if !hashes.contains(block.header().hash()) {
                            warn!(target: "client", "Rejecting un-requested block {}, height {}", block.header().hash(), block.header().height());
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

impl Handler<SpanWrapped<BlockHeadersResponse>, Result<(), ReasonForBan>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<BlockHeadersResponse>) -> Result<(), ReasonForBan> {
        let BlockHeadersResponse(headers, peer_id) = msg.span_unwrap();
        if self.receive_headers(headers, peer_id) {
            Ok(())
        } else {
            warn!(target: "client", "Banning node for sending invalid block headers");
            Err(ReasonForBan::BadBlockHeader)
        }
    }
}

impl Handler<SpanWrapped<BlockApproval>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<BlockApproval>) {
        let BlockApproval(approval, peer_id) = msg.span_unwrap();
        debug!(target: "client", "Receive approval {:?} from peer {:?}", approval, peer_id);
        self.client.collect_block_approval(&approval, ApprovalType::PeerApproval(peer_id));
    }
}

/// StateResponse is used during StateSync and catchup.
/// It contains either StateSync header information (that tells us how many parts there are etc) or a single part.
impl Handler<SpanWrapped<StateResponseReceived>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<StateResponseReceived>) {
        let StateResponseReceived { peer_id, state_response_info } = msg.span_unwrap();
        let shard_id = state_response_info.shard_id();
        let hash = state_response_info.sync_hash();
        let state_response = state_response_info.take_state_response();

        trace!(target: "sync", "Received state response shard_id: {} sync_hash: {:?} part(id/size): {:?}",
               shard_id,
               hash,
               state_response.part().as_ref().map(|(part_id, data)| (part_id, data.len()))
        );
        // Get the download that matches the shard_id and hash

        // ... It could be that the state was requested by the state sync
        if let SyncStatus::StateSync(StateSyncStatus { sync_hash, .. }) =
            &mut self.client.sync_handler.sync_status
        {
            if hash == *sync_hash {
                if let Err(err) = self.client.sync_handler.state_sync.apply_peer_message(
                    peer_id,
                    shard_id,
                    *sync_hash,
                    state_response,
                ) {
                    tracing::error!(?err, "Error applying state sync response");
                }
                return;
            }
        }

        // ... Or one of the catchups
        if let Some(CatchupState { state_sync, .. }) =
            self.client.catchup_state_syncs.get_mut(&hash)
        {
            if let Err(err) = state_sync.apply_peer_message(peer_id, shard_id, hash, state_response)
            {
                tracing::error!(?err, "Error applying catchup state sync response");
            }
            return;
        }

        error!(target: "sync", "State sync received hash {} that we're not expecting, potential malicious peer or a very delayed response.", hash);
    }
}

impl Handler<SpanWrapped<SetNetworkInfo>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<SetNetworkInfo>) {
        let msg = msg.span_unwrap();
        // SetNetworkInfo is a large message. Avoid printing it at the `debug` verbosity.
        self.network_info = msg.0;
    }
}

#[cfg(feature = "sandbox")]
impl
    Handler<
        near_client_primitives::types::SandboxMessage,
        near_client_primitives::types::SandboxResponse,
    > for ClientActorInner
{
    fn handle(
        &mut self,
        msg: near_client_primitives::types::SandboxMessage,
    ) -> near_client_primitives::types::SandboxResponse {
        match msg {
            near_client_primitives::types::SandboxMessage::SandboxPatchState(state) => {
                self.client.chain.patch_state(
                    near_primitives::sandbox::state_patch::SandboxStatePatch::new(state),
                );
                near_client_primitives::types::SandboxResponse::SandboxNoResponse
            }
            near_client_primitives::types::SandboxMessage::SandboxPatchStateStatus => {
                near_client_primitives::types::SandboxResponse::SandboxPatchStateFinished(
                    !self.client.chain.patch_state_in_progress(),
                )
            }
            near_client_primitives::types::SandboxMessage::SandboxFastForward(delta_height) => {
                if self.fastforward_delta > 0 {
                    return near_client_primitives::types::SandboxResponse::SandboxFastForwardFailed(
                        "Consecutive fast_forward requests cannot be made while a current one is going on.".to_string());
                }

                self.fastforward_delta = delta_height;
                near_client_primitives::types::SandboxResponse::SandboxNoResponse
            }
            near_client_primitives::types::SandboxMessage::SandboxFastForwardStatus => {
                near_client_primitives::types::SandboxResponse::SandboxFastForwardFinished(
                    self.fastforward_delta == 0,
                )
            }
        }
    }
}

impl Handler<SpanWrapped<Status>, Result<StatusResponse, StatusError>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<Status>) -> Result<StatusResponse, StatusError> {
        let msg = msg.span_unwrap();
        let head = self.client.chain.head()?;
        let head_header = self.client.chain.get_block_header(&head.last_block_hash)?;
        let latest_block_time = head_header.raw_timestamp();
        let latest_state_root = *head_header.prev_state_root();
        if msg.is_health_check {
            let now = self.clock.now_utc();
            let block_timestamp =
                Utc::from_unix_timestamp_nanos(latest_block_time as i128).unwrap();
            if now > block_timestamp {
                let elapsed = now - block_timestamp;
                if elapsed
                    > self.client.config.max_block_production_delay * STATUS_WAIT_TIME_MULTIPLIER
                {
                    return Err(StatusError::NoNewBlocks { elapsed });
                }
            }

            if self.client.sync_handler.sync_status.is_syncing() {
                return Err(StatusError::NodeIsSyncing);
            }
        }
        let validators: Vec<ValidatorInfo> = self
            .client
            .epoch_manager
            .get_epoch_block_producers_ordered(&head.epoch_id)
            .into_chain_error()?
            .into_iter()
            .map(|validator_stake| ValidatorInfo { account_id: validator_stake.take_account_id() })
            .collect();

        let epoch_start_height =
            self.client.epoch_manager.get_epoch_start_height(&head.last_block_hash).ok();

        let protocol_version = self
            .client
            .epoch_manager
            .get_epoch_protocol_version(&head.epoch_id)
            .into_chain_error()?;

        let node_public_key = self.node_id.public_key().clone();
        let (validator_account_id, validator_public_key) = match &self.client.validator_signer.get()
        {
            Some(vs) => (Some(vs.validator_id().clone()), Some(vs.public_key())),
            None => (None, None),
        };
        let node_key = validator_public_key.clone();

        let mut earliest_block_hash = None;
        let mut earliest_block_height = None;
        let mut earliest_block_time = None;
        if let Some(earliest_block_hash_value) = self.client.chain.get_earliest_block_hash()? {
            earliest_block_hash = Some(earliest_block_hash_value);
            if let Ok(earliest_block) =
                self.client.chain.get_block_header(&earliest_block_hash_value)
            {
                earliest_block_height = Some(earliest_block.height());
                earliest_block_time = Some(earliest_block.timestamp());
            }
        }
        // Provide more detailed information about the current state of chain.
        // For now - provide info about last 50 blocks.
        let detailed_debug_status = if msg.detailed {
            let head = self.client.chain.head()?;
            let header_head = self.client.chain.header_head()?;
            Some(DetailedDebugStatus {
                network_info: new_network_info_view(&self.client.chain, &self.network_info),
                sync_status: format!(
                    "{} ({})",
                    self.client.sync_handler.sync_status.as_variant_name(),
                    display_sync_status(
                        &self.client.sync_handler.sync_status,
                        &head,
                        &self.client.config.state_sync.sync,
                    ),
                ),
                catchup_status: self.client.get_catchup_status()?,
                current_head_status: head.as_ref().into(),
                current_header_head_status: header_head.as_ref().into(),
                block_production_delay_millis: self
                    .client
                    .config
                    .min_block_production_delay
                    .whole_milliseconds() as u64,
            })
        } else {
            None
        };
        let uptime_sec = self.clock.now_utc().unix_timestamp() - self.info_helper.boot_time_seconds;
        Ok(StatusResponse {
            version: self.client.config.version.clone(),
            protocol_version,
            latest_protocol_version: PROTOCOL_VERSION,
            chain_id: self.client.config.chain_id.clone(),
            rpc_addr: self.client.config.rpc_addr.clone(),
            validators,
            sync_info: StatusSyncInfo {
                latest_block_hash: head.last_block_hash,
                latest_block_height: head.height,
                latest_state_root,
                latest_block_time: Utc::from_unix_timestamp_nanos(latest_block_time as i128)
                    .unwrap(),
                syncing: self.client.sync_handler.sync_status.is_syncing(),
                earliest_block_hash,
                earliest_block_height,
                earliest_block_time,
                epoch_id: Some(head.epoch_id),
                epoch_start_height,
            },
            validator_account_id,
            validator_public_key,
            node_public_key,
            node_key,
            uptime_sec,
            genesis_hash: *self.client.chain.genesis().hash(),
            detailed_debug_status,
        })
    }
}

/// Private to public API conversion.
fn make_peer_info(from: near_network::types::PeerInfo) -> near_client_primitives::types::PeerInfo {
    near_client_primitives::types::PeerInfo {
        id: from.id,
        addr: from.addr,
        account_id: from.account_id,
    }
}

/// Private to public API conversion.
fn make_known_producer(
    from: near_network::types::KnownProducer,
) -> near_client_primitives::types::KnownProducer {
    near_client_primitives::types::KnownProducer {
        peer_id: from.peer_id,
        account_id: from.account_id,
        addr: from.addr,
        next_hops: from.next_hops,
    }
}

impl Handler<SpanWrapped<GetNetworkInfo>, Result<NetworkInfoResponse, String>>
    for ClientActorInner
{
    fn handle(&mut self, _msg: SpanWrapped<GetNetworkInfo>) -> Result<NetworkInfoResponse, String> {
        Ok(NetworkInfoResponse {
            connected_peers: (self.network_info.connected_peers.iter())
                .map(|fpi| make_peer_info(fpi.full_peer_info.peer_info.clone()))
                .collect(),
            num_connected_peers: self.network_info.num_connected_peers,
            peer_max_count: self.network_info.peer_max_count,
            sent_bytes_per_sec: self.network_info.sent_bytes_per_sec,
            received_bytes_per_sec: self.network_info.received_bytes_per_sec,
            known_producers: self
                .network_info
                .known_producers
                .iter()
                .map(|p| make_known_producer(p.clone()))
                .collect(),
        })
    }
}

impl Handler<SpanWrapped<ApplyChunksDoneMessage>> for ClientActorInner {
    fn handle(&mut self, _msg: SpanWrapped<ApplyChunksDoneMessage>) {
        self.try_process_unfinished_blocks();
    }
}

#[derive(Debug)]
enum SyncRequirement {
    SyncNeeded { peer_id: PeerId, highest_height: BlockHeight, head: Tip },
    AlreadyCaughtUp { peer_id: PeerId, highest_height: BlockHeight, head: Tip },
    NoPeers,
    AdvHeaderSyncDisabled,
}

impl SyncRequirement {
    fn sync_needed(&self) -> bool {
        matches!(self, Self::SyncNeeded { .. })
    }

    fn to_metrics_string(&self) -> String {
        match self {
            Self::SyncNeeded { .. } => "SyncNeeded",
            Self::AlreadyCaughtUp { .. } => "AlreadyCaughtUp",
            Self::NoPeers => "NoPeers",
            Self::AdvHeaderSyncDisabled { .. } => "AdvHeaderSyncDisabled",
        }
        .to_string()
    }
}

impl fmt::Display for SyncRequirement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SyncNeeded { peer_id, highest_height, head: my_head } => write!(
                f,
                "sync needed at #{} [{}]. highest height peer: {} at #{}",
                my_head.height,
                format_hash(my_head.last_block_hash),
                peer_id,
                highest_height
            ),
            Self::AlreadyCaughtUp { peer_id, highest_height, head: my_head } => write!(
                f,
                "synced at #{} [{}]. highest height peer: {} at #{}",
                my_head.height,
                format_hash(my_head.last_block_hash),
                peer_id,
                highest_height
            ),
            Self::NoPeers => write!(f, "no available peers"),
            Self::AdvHeaderSyncDisabled => {
                write!(f, "syncing disabled via adv_disable_header_sync")
            }
        }
    }
}

impl ClientActorInner {
    pub fn start(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        // Start syncing job.
        self.start_sync(ctx);

        // Start triggers
        self.schedule_triggers(ctx);

        // Start catchup job.
        self.catchup(ctx);

        if let Err(err) = self.client.send_network_chain_info() {
            tracing::error!(target: "client", ?err, "Failed to update network chain info");
        }
    }

    /// Check if client Account Id should be sent and send it.
    /// Account Id is sent when is not current a validator but are becoming a validator soon.
    fn check_send_announce_account(&mut self, prev_block_hash: CryptoHash) {
        // If no peers, there is no one to announce to.
        if self.network_info.num_connected_peers == 0 {
            debug!(target: "client", "No peers: skip account announce");
            return;
        }

        // First check that we currently have an AccountId
        let signer = match self.client.validator_signer.get() {
            None => return,
            Some(signer) => signer,
        };

        let now = self.clock.now();
        // Check that we haven't announced it too recently
        if let Some(last_validator_announce_time) = self.last_validator_announce_time {
            // Don't make announcement if have passed less than half of the time in which other peers
            // should remove our Account Id from their Routing Tables.
            if 2 * (now - last_validator_announce_time) < self.client.config.ttl_account_id_router {
                return;
            }
        }

        debug!(target: "client", "Check announce account for {}, last announce time {:?}", signer.validator_id(), self.last_validator_announce_time);

        // Announce AccountId if client is becoming a validator soon.
        let next_epoch_id = unwrap_or_return!(
            self.client.epoch_manager.get_next_epoch_id_from_prev_block(&prev_block_hash)
        );

        // Check client is part of the futures validators
        if self.client.is_validator(&next_epoch_id) {
            debug!(target: "client", "Sending announce account for {}", signer.validator_id());
            self.last_validator_announce_time = Some(now);

            let announce_account =
                AnnounceAccount::new(signer.as_ref(), self.node_id.clone(), next_epoch_id);
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::AnnounceAccount(announce_account),
            ));
        }
    }

    /// Process the sandbox fast forward request. If the change in block height is past an epoch,
    /// we fast forward to just right before the epoch, produce some blocks to get past and into
    /// a new epoch, then we continue on with the residual amount to fast forward.
    #[cfg(feature = "sandbox")]
    fn sandbox_process_fast_forward(
        &mut self,
        block_height: BlockHeight,
    ) -> Result<Option<near_chain::types::LatestKnown>, Error> {
        let mut delta_height = std::mem::replace(&mut self.fastforward_delta, 0);
        if delta_height == 0 {
            return Ok(None);
        }

        let epoch_length = self.client.config.epoch_length;
        if epoch_length <= 3 {
            return Err(Error::Other(
                "Unsupported: fast_forward with an epoch length of 3 or less".to_string(),
            ));
        }

        // Check if we are at epoch boundary. If we are, do not fast forward until new
        // epoch is here. Decrement the fast_forward count by 1 when a block is produced
        // during this period of waiting
        let block_height_wrt_epoch = block_height % epoch_length;
        if epoch_length - block_height_wrt_epoch <= 3 || block_height_wrt_epoch == 0 {
            // wait for doomslug to call into produce block
            self.fastforward_delta = delta_height;
            return Ok(None);
        }

        let delta_height = if block_height_wrt_epoch + delta_height >= epoch_length {
            // fast forward to just right before epoch boundary to have epoch_manager
            // handle the epoch_height updates as normal. `- 3` since this is being
            // done 3 blocks before the epoch ends.
            let right_before_epoch_update = epoch_length - block_height_wrt_epoch - 3;

            delta_height -= right_before_epoch_update;
            self.fastforward_delta = delta_height;
            right_before_epoch_update
        } else {
            delta_height
        };

        self.client.accrued_fastforward_delta += delta_height;
        let delta_time = self.client.sandbox_delta_time();
        let new_latest_known = near_chain::types::LatestKnown {
            height: block_height + delta_height,
            seen: (self.clock.now_utc() + delta_time).unix_timestamp_nanos() as u64,
        };

        Ok(Some(new_latest_known))
    }

    #[allow(clippy::needless_pass_by_ref_mut)] // &mut self is needed with the sandbox feature
    fn pre_block_production(&mut self) -> Result<(), Error> {
        #[cfg(feature = "sandbox")]
        {
            let latest_known = self.client.chain.mut_chain_store().get_latest_known()?;
            if let Some(new_latest_known) =
                self.sandbox_process_fast_forward(latest_known.height)?
            {
                self.client.chain.mut_chain_store().save_latest_known(new_latest_known)?;
                self.client.sandbox_update_tip(new_latest_known.height)?;
            }
        }
        Ok(())
    }

    #[allow(clippy::needless_pass_by_ref_mut)] // &mut self is needed with the sandbox feature
    fn post_block_production(&mut self) {
        #[cfg(feature = "sandbox")]
        if self.fastforward_delta > 0 {
            // Decrease the delta_height by 1 since we've produced a single block. This
            // ensures that we advanced the right amount of blocks when fast forwarding
            // and fast forwarding triggers regular block production in the case of
            // stepping between epoch boundaries.
            self.fastforward_delta -= 1;
        }
    }

    /// Retrieves latest height, and checks if must produce next block.
    /// Otherwise wait for block arrival or suggest to skip after timeout.
    fn handle_block_production(&mut self) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "client", "handle_block_production").entered();
        // If syncing, don't try to produce blocks.
        if self.client.sync_handler.sync_status.is_syncing() {
            debug!(target:"client", sync_status=format!("{:#?}", self.client.sync_handler.sync_status), "Syncing - block production disabled");
            return Ok(());
        }

        let _ = self.client.check_and_update_doomslug_tip();

        self.pre_block_production()?;
        let head = self.client.chain.head()?;
        let latest_known = self.client.chain.chain_store().get_latest_known()?;

        assert!(
            head.height <= latest_known.height,
            "Latest known height is invalid {} vs {}",
            head.height,
            latest_known.height
        );

        let epoch_id =
            self.client.epoch_manager.get_epoch_id_from_prev_block(&head.last_block_hash)?;
        let log_block_production_info =
            if self.client.epoch_manager.is_next_block_epoch_start(&head.last_block_hash)? {
                true
            } else {
                // the next block is still the same epoch
                let epoch_start_height =
                    self.client.epoch_manager.get_epoch_start_height(&head.last_block_hash)?;
                latest_known.height - epoch_start_height < EPOCH_START_INFO_BLOCKS
            };

        // We try to produce block for multiple heights (up to the highest height for which we've seen 2/3 of approvals).
        if latest_known.height + 1 <= self.client.doomslug.get_largest_height_crossing_threshold() {
            debug!(target: "client", "Considering blocks for production between {} and {} ", latest_known.height + 1, self.client.doomslug.get_largest_height_crossing_threshold());
        } else {
            debug!(target: "client", "Cannot produce any block: not enough approvals beyond {}", latest_known.height);
        }

        let me = if let Some(me) = self.client.validator_signer.get() {
            me.validator_id().clone()
        } else {
            return Ok(());
        };

        // For debug purpose, we record the approvals we have seen so far to the future blocks
        for height in latest_known.height + 1..=self.client.doomslug.get_largest_approval_height() {
            let next_block_producer_account =
                self.client.epoch_manager.get_block_producer(&epoch_id, height)?;

            if me == next_block_producer_account {
                self.client.block_production_info.record_approvals(
                    height,
                    self.client.doomslug.approval_status_at_height(&height),
                );
            }
        }

        let prev_block_hash = &head.last_block_hash;
        let chunks_readiness = self.client.prepare_chunk_headers(prev_block_hash, &epoch_id)?;
        for height in
            latest_known.height + 1..=self.client.doomslug.get_largest_height_crossing_threshold()
        {
            let next_block_producer_account =
                self.client.epoch_manager.get_block_producer(&epoch_id, height)?;

            if me != next_block_producer_account {
                continue;
            }

            if self.client.doomslug.ready_to_produce_block(
                height,
                chunks_readiness,
                log_block_production_info,
            ) {
                let shard_ids = self.client.epoch_manager.shard_ids(&epoch_id)?;
                self.client
                    .chunk_inclusion_tracker
                    .record_endorsement_metrics(prev_block_hash, &shard_ids);
                if let Err(err) = self.produce_block(height) {
                    // If there is an error, report it and let it retry on the next loop step.
                    error!(target: "client", height, "Block production failed: {}", err);
                } else {
                    self.post_block_production();
                }
            }
        }

        let protocol_version = self.client.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        if !ProtocolFeature::ProduceOptimisticBlock.enabled(protocol_version) {
            return Ok(());
        }
        let optimistic_block_height = self.client.doomslug.get_timer_height();
        if me != self.client.epoch_manager.get_block_producer(&epoch_id, optimistic_block_height)? {
            return Ok(());
        }
        if let Err(err) = self.produce_optimistic_block(optimistic_block_height) {
            // If there is an error, report it and let it retry.
            error!(target: "client", optimistic_block_height, ?err, "Optimistic block production failed!");
        }

        Ok(())
    }

    fn schedule_triggers(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        let wait = self.check_triggers(ctx);

        ctx.run_later("ClientActor schedule_triggers", wait, move |act, ctx| {
            act.schedule_triggers(ctx);
        });
    }

    /// Check if the scheduled time of any "triggers" has passed, and if so, call the trigger.
    /// Triggers are important functions of client, like running single step of state sync or
    /// checking if we can produce a block.
    ///
    /// It is called before processing Actix message and also in schedule_triggers.
    /// This is to ensure all triggers enjoy higher priority than any actix message.
    /// Otherwise due to a bug in Actix library Actix prioritizes processing messages
    /// while there are messages in mailbox. Because of that we handle scheduling
    /// triggers with custom `run_timer` function instead of `run_later` in Actix.
    ///
    /// Returns the delay before the next time `check_triggers` should be called, which is
    /// min(time until the closest trigger, 1 second).
    fn check_triggers(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) -> Duration {
        let _span = tracing::debug_span!(target: "client", "check_triggers").entered();
        if let Some(config_updater) = &mut self.config_updater {
            let update_result = config_updater.try_update(
                &|updatable_client_config| {
                    self.client.update_client_config(updatable_client_config)
                },
                &|validator_signer| self.client.update_validator_signer(validator_signer),
            );

            if update_result.validator_signer_updated {
                if let Some(validator_signer) = self.client.validator_signer.get() {
                    check_validator_tracked_shards(&self.client, validator_signer.validator_id())
                        .expect("Could not check validator tracked shards");
                }

                // Request PeerManager to advertise tier1 proxies.
                // It is needed to advertise that our validator key changed.
                self.network_adapter.send(PeerManagerMessageRequest::AdvertiseTier1Proxies);
            }
        }

        // Check block height to trigger expected shutdown
        if let Ok(head) = self.client.chain.head() {
            if let Some(block_height_to_shutdown) = self.client.config.expected_shutdown.get() {
                if head.height >= block_height_to_shutdown {
                    info!(target: "client", "Expected shutdown triggered: head block({}) >= ({:?})", head.height, block_height_to_shutdown);
                    if let Some(tx) = self.shutdown_signal.take() {
                        let _ = tx.send(()); // Ignore send signal fail, it will send again in next trigger
                    }
                }
            }
        }

        self.try_process_unfinished_blocks();

        let mut delay = near_async::time::Duration::seconds(1);
        let now = self.clock.now_utc();

        let timer = metrics::CHECK_TRIGGERS_TIME.start_timer();
        if self.sync_started {
            self.sync_timer_next_attempt = self.run_timer(
                self.sync_wait_period(),
                self.sync_timer_next_attempt,
                ctx,
                |act, _| act.run_sync_step(),
                "sync",
            );

            delay = std::cmp::min(delay, self.sync_timer_next_attempt - now);

            self.doomslug_timer_next_attempt = self.run_timer(
                self.client.config.doomslug_step_period,
                self.doomslug_timer_next_attempt,
                ctx,
                |act, _| act.try_doomslug_timer(),
                "doomslug",
            );
            delay = core::cmp::min(delay, self.doomslug_timer_next_attempt - now)
        }

        let validator_signer = self.client.validator_signer.get();
        if validator_signer.is_some() {
            self.block_production_next_attempt = self.run_timer(
                self.client.config.block_production_tracking_delay,
                self.block_production_next_attempt,
                ctx,
                |act, _ctx| act.try_handle_block_production(),
                "block_production",
            );

            let _ = self.client.check_head_progress_stalled(
                self.client.config.max_block_production_delay * HEAD_STALL_MULTIPLIER,
            );

            delay = core::cmp::min(delay, self.block_production_next_attempt - now)
        }

        self.log_summary_timer_next_attempt = self.run_timer(
            self.client.config.log_summary_period,
            self.log_summary_timer_next_attempt,
            ctx,
            |act, _ctx| act.log_summary(),
            "log_summary",
        );
        delay = core::cmp::min(delay, self.log_summary_timer_next_attempt - now);
        timer.observe_duration();
        delay
    }

    /// "Unfinished" blocks means that blocks that client has started the processing and haven't
    /// finished because it was waiting for applying chunks to be done. This function checks
    /// if there are any "unfinished" blocks that are ready to be processed again and finish processing
    /// these blocks.
    /// This function is called at two places, upon receiving ApplyChunkDoneMessage and `check_triggers`.
    /// The job that executes applying chunks will send an ApplyChunkDoneMessage to ClientActor after
    /// applying chunks is done, so when receiving ApplyChunkDoneMessage messages, ClientActor
    /// calls this function to finish processing the unfinished blocks. ClientActor also calls
    /// this function in `check_triggers`, because the actix queue may be blocked by other messages
    /// and we want to prioritize block processing.
    fn try_process_unfinished_blocks(&mut self) {
        let _span = debug_span!(target: "client", "try_process_unfinished_blocks").entered();
        let (accepted_blocks, errors) = self.client.postprocess_ready_blocks(
            Some(self.client.myself_sender.apply_chunks_done.clone()),
            true,
        );
        if !errors.is_empty() {
            error!(target: "client", ?errors, "try_process_unfinished_blocks got errors");
        }
        self.process_accepted_blocks(accepted_blocks);
    }

    fn try_handle_block_production(&mut self) {
        let _span = debug_span!(target: "client", "try_handle_block_production").entered();
        if let Err(err) = self.handle_block_production() {
            tracing::error!(target: "client", ?err, "Handle block production failed")
        }
    }

    fn try_doomslug_timer(&mut self) {
        let _span = tracing::debug_span!(target: "client", "try_doomslug_timer").entered();
        let _ = self.client.check_and_update_doomslug_tip();
        let signer = self.client.validator_signer.get();
        let approvals = self.client.doomslug.process_timer(&signer);

        // Important to save the largest approval target height before sending approvals, so
        // that if the node crashes in the meantime, we cannot get slashed on recovery
        let mut chain_store_update = self.client.chain.mut_chain_store().store_update();
        chain_store_update
            .save_largest_target_height(self.client.doomslug.get_largest_target_height());

        match chain_store_update.commit() {
            Ok(_) => {
                let head = unwrap_or_return!(self.client.chain.head());
                if self.client.is_validator(&head.epoch_id)
                    || self.client.is_validator(&head.next_epoch_id)
                {
                    for approval in approvals {
                        if let Err(e) = self
                            .client
                            .send_block_approval(&self.client.doomslug.get_tip().0, approval)
                        {
                            error!("Error while sending an approval {:?}", e);
                        }
                    }
                }
            }
            Err(e) => error!("Error while committing largest skipped height {:?}", e),
        };
    }

    /// Produce block if we are block producer for given `next_height` height.
    /// Can return error, should be called with `produce_block` to handle errors and reschedule.
    fn produce_block(&mut self, next_height: BlockHeight) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "client", "produce_block", next_height).entered();
        let Some(block) = self.client.produce_block_on_head(next_height, false)? else {
            return Ok(());
        };

        // If we produced the block, send it out before we apply the block.
        self.client.chain.blocks_delay_tracker.mark_block_received(&block);
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::Block { block: Arc::clone(&block) },
        ));
        // We've produced the block so that counts as validated block.
        let block = MaybeValidated::from_validated(block);
        let res = self.client.start_process_block(
            block,
            Provenance::PRODUCED,
            Some(self.client.myself_sender.apply_chunks_done.clone()),
        );
        let Err(error) = res else {
            return Ok(());
        };

        match error {
            near_chain::Error::ChunksMissing(_) => {
                debug!(target: "client", "chunks missing");
                // If block is missing chunks, it will be processed in
                // `check_blocks_with_missing_chunks`.
                Ok(())
            }
            near_chain::Error::BlockPendingOptimisticExecution => {
                debug!(target: "client", "block pending optimistic execution");
                // If block is pending optimistic execution, it will be
                // processed in `postprocess_optimistic_block`.
                Ok(())
            }
            _ => {
                error!(target: "client", ?error, "Failed to process freshly produced block");
                byzantine_assert!(false);
                Err(error.into())
            }
        }
    }

    /// Produce optimistic block if we are block producer for given `next_height` height.
    fn produce_optimistic_block(&mut self, next_height: BlockHeight) -> Result<(), Error> {
        // Check if optimistic block is already produced
        if self.client.is_optimistic_block_done(next_height) {
            return Ok(());
        }

        let Some(optimistic_block) = self.client.produce_optimistic_block_on_head(next_height)?
        else {
            return Ok(());
        };

        // If we produced the optimistic block, send it out before we save it.
        let tip = self.client.chain.head()?;
        let targets = self.client.get_optimistic_block_targets(&tip)?;
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::OptimisticBlock {
                chunk_producers: targets,
                optimistic_block: optimistic_block.clone(),
            },
        ));

        // We've produced the optimistic block, mark it as done so we don't produce it again.
        self.client.save_optimistic_block(&optimistic_block);
        self.client.chain.optimistic_block_chunks.add_block(optimistic_block);

        self.client.chain.maybe_process_optimistic_block(Some(
            self.client.myself_sender.apply_chunks_done.clone(),
        ));

        Ok(())
    }

    fn send_chunks_metrics(&self, block: &Block) {
        for (chunk, &included) in block.chunks().iter().zip(block.header().chunk_mask().iter()) {
            if included {
                self.info_helper.chunk_processed(
                    chunk.shard_id(),
                    chunk.prev_gas_used(),
                    chunk.prev_balance_burnt(),
                );
            } else {
                self.info_helper.chunk_skipped(chunk.shard_id());
            }
        }
    }

    fn send_block_metrics(&mut self, block: &Block) {
        let chunks_in_block = block.header().chunk_mask().iter().filter(|&&m| m).count();
        let gas_used = block.chunks().compute_gas_used();

        let last_final_hash = block.header().last_final_block();
        let last_final_ds_hash = block.header().last_ds_final_block();
        let last_final_block_height = self
            .client
            .chain
            .get_block(&last_final_hash)
            .map_or(0, |block| block.header().height());
        let last_final_ds_block_height = self
            .client
            .chain
            .get_block(&last_final_ds_hash)
            .map_or(0, |block| block.header().height());

        let epoch_height =
            self.client.epoch_manager.get_epoch_height_from_prev_block(block.hash()).unwrap_or(0);
        let epoch_start_height = self
            .client
            .epoch_manager
            .get_epoch_start_height(&last_final_hash)
            .unwrap_or(last_final_block_height);
        let last_final_block_height_in_epoch =
            last_final_block_height.checked_sub(epoch_start_height);

        self.info_helper.block_processed(
            gas_used,
            chunks_in_block as u64,
            block.header().next_gas_price(),
            block.header().total_supply(),
            last_final_block_height,
            last_final_ds_block_height,
            epoch_height,
            last_final_block_height_in_epoch,
        );
    }

    /// Process all blocks that were accepted by calling other relevant services.
    fn process_accepted_blocks(&mut self, accepted_blocks: Vec<CryptoHash>) {
        let _span = tracing::debug_span!(
            target: "client",
            "process_accepted_blocks",
            num_blocks = accepted_blocks.len())
        .entered();
        for accepted_block in accepted_blocks {
            let block = self.client.chain.get_block(&accepted_block).unwrap().clone();
            debug!(target: "client", height=block.header().height(), "process_accepted_block");
            self.send_chunks_metrics(&block);
            self.send_block_metrics(&block);
            self.check_send_announce_account(*block.header().last_final_block());
            self.chunk_executor_sender.send(ProcessedBlock { block_hash: accepted_block });
            self.spice_chunk_validator_sender.send(ProcessedBlock { block_hash: accepted_block });
        }
    }

    fn receive_headers(&mut self, headers: Vec<Arc<BlockHeader>>, peer_id: PeerId) -> bool {
        let _span =
            debug_span!(target: "client", "receive_headers", num_headers = headers.len(), ?peer_id)
                .entered();
        if headers.is_empty() {
            info!(target: "client", "Received an empty set of block headers");
            return true;
        }
        match self.client.sync_block_headers(headers) {
            Ok(_) => true,
            Err(err) => {
                if err.is_bad_data() {
                    error!(target: "client", ?err, "Error processing sync blocks");
                    false
                } else {
                    debug!(target: "client", ?err, "Block headers refused by chain");
                    true
                }
            }
        }
    }

    /// Check whether need to (continue) sync.
    /// Also return higher height with known peers at that height.
    fn syncing_info(&self) -> Result<SyncRequirement, near_chain::Error> {
        if self.adv.disable_header_sync() {
            return Ok(SyncRequirement::AdvHeaderSyncDisabled);
        }

        let head = self.client.chain.head()?;
        let is_syncing = self.client.sync_handler.sync_status.is_syncing();

        // Only consider peers whose latest block is not invalid blocks
        let eligible_peers: Vec<_> = self
            .network_info
            .highest_height_peers
            .iter()
            .filter(|p| !self.client.chain.is_block_invalid(&p.highest_block_hash))
            .collect();
        metrics::PEERS_WITH_INVALID_HASH
            .set(self.network_info.highest_height_peers.len() as i64 - eligible_peers.len() as i64);
        let peer_info = if let Some(peer_info) = eligible_peers.choose(&mut thread_rng()) {
            peer_info
        } else {
            return Ok(SyncRequirement::NoPeers);
        };

        let peer_id = peer_info.peer_info.id.clone();
        let shutdown_height = self.client.config.expected_shutdown.get().unwrap_or(u64::MAX);
        let highest_height = peer_info.highest_block_height.min(shutdown_height);
        let head = Tip::clone(&head);

        if is_syncing {
            if highest_height <= head.height {
                Ok(SyncRequirement::AlreadyCaughtUp { peer_id, highest_height, head })
            } else {
                Ok(SyncRequirement::SyncNeeded { peer_id, highest_height, head })
            }
        } else {
            if highest_height > head.height + self.client.config.sync_height_threshold {
                Ok(SyncRequirement::SyncNeeded { peer_id, highest_height, head })
            } else {
                Ok(SyncRequirement::AlreadyCaughtUp { peer_id, highest_height, head })
            }
        }
    }

    /// Starts syncing and then switches to either syncing or regular mode.
    fn start_sync(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        // Wait for connections reach at least minimum peers unless skipping sync.
        if self.network_info.num_connected_peers < self.client.config.min_num_peers
            && !self.client.config.skip_sync_wait
        {
            ctx.run_later(
                "ClientActor start_sync",
                self.client.config.sync_step_period,
                move |act, ctx| {
                    act.start_sync(ctx);
                },
            );
            return;
        }
        self.sync_started = true;

        // Sync loop will be started by check_triggers.
    }

    /// Runs catchup on repeat, if this client is a validator.
    /// Schedules itself again if it was not ran as response to state parts job result
    fn catchup(&mut self, ctx: &mut dyn DelayedActionRunner<Self>) {
        {
            // An extra scope to limit the lifetime of the span.
            let _span = tracing::debug_span!(target: "client", "catchup").entered();
            if let Err(err) = self.client.run_catchup(
                &self.sync_jobs_sender.block_catch_up,
                Some(self.client.myself_sender.apply_chunks_done.clone()),
            ) {
                error!(target: "client", "Error occurred during catchup for the next epoch: {:?}", err);
            }
        }

        ctx.run_later(
            "ClientActor catchup",
            self.client.config.catchup_step_period,
            move |act, ctx| {
                act.catchup(ctx);
            },
        );
    }

    /// Runs given callback if the time now is at least `next_attempt`.
    /// Returns time for next run which should be made based on given `delay` between runs.
    fn run_timer<F>(
        &mut self,
        delay: Duration,
        next_attempt: Utc,
        ctx: &mut dyn DelayedActionRunner<Self>,
        f: F,
        timer_label: &str,
    ) -> Utc
    where
        F: FnOnce(&mut Self, &mut dyn DelayedActionRunner<Self>) + 'static,
    {
        let now = self.clock.now_utc();
        if now < next_attempt {
            return next_attempt;
        }

        let timer =
            metrics::CLIENT_TRIGGER_TIME_BY_TYPE.with_label_values(&[timer_label]).start_timer();
        f(self, ctx);
        timer.observe_duration();

        now + delay
    }

    fn sync_wait_period(&self) -> Duration {
        if let Ok(sync) = self.syncing_info() {
            if !sync.sync_needed() {
                // If we don't need syncing - retry the sync call rarely.
                self.client.config.sync_check_period
            } else {
                // If we need syncing - retry the sync call often.
                self.client.config.sync_step_period
            }
        } else {
            self.client.config.sync_step_period
        }
    }

    /// Main syncing job responsible for syncing client with other peers.
    /// Runs itself iff it was not ran as reaction for message with results of
    /// finishing state part job
    fn run_sync_step(&mut self) {
        let _span = tracing::debug_span!(target: "client", "run_sync_step").entered();

        let currently_syncing = self.client.sync_handler.sync_status.is_syncing();
        let sync = match self.syncing_info() {
            Ok(sync) => sync,
            Err(err) => {
                tracing::error!(target: "sync", "Sync: Unexpected error: {}", err);
                return;
            }
        };
        self.info_helper.update_sync_requirements_metrics(sync.to_metrics_string());

        match sync {
            SyncRequirement::AlreadyCaughtUp { .. }
            | SyncRequirement::NoPeers
            | SyncRequirement::AdvHeaderSyncDisabled => {
                if currently_syncing {
                    // Initial transition out of "syncing" state.
                    debug!(target: "sync", prev_sync_status = ?self.client.sync_handler.sync_status, "disabling sync");
                    self.client.sync_handler.sync_status.update(SyncStatus::NoSync);
                    // Announce this client's account id if their epoch is coming up.
                    let head = match self.client.chain.head() {
                        Ok(v) => v,
                        Err(err) => {
                            tracing::error!(target: "sync", "Sync: Unexpected error: {}", err);
                            return;
                        }
                    };
                    self.check_send_announce_account(head.prev_block_hash);
                }
            }

            SyncRequirement::SyncNeeded { highest_height, .. } => {
                if !currently_syncing {
                    info!(target: "client", ?sync, "enabling sync");
                }

                self.handle_sync_needed(highest_height);
            }
        }
    }

    /// Handle the SyncRequirement::SyncNeeded.
    ///
    /// This method performs whatever syncing technique is needed (epoch sync, header sync,
    /// state sync, block sync) to make progress towards bring the node up to date.
    fn handle_sync_needed(&mut self, highest_height: u64) {
        let sync_step_result = self.client.sync_handler.handle_sync_needed(
            &mut self.client.chain,
            &self.client.shard_tracker,
            highest_height,
            &self.network_info.highest_height_peers,
            Some(self.client.myself_sender.apply_chunks_done.clone()),
        );
        let Some(sync_step_result) = sync_step_result else {
            return;
        };
        match sync_step_result {
            SyncHandlerRequest::NeedRequestBlocks(blocks_to_request) => {
                for (block_hash, peer_id) in blocks_to_request {
                    self.client.request_block(block_hash, peer_id);
                }
            }
            // This is the last step of state sync that is not in handle_sync_needed because it
            // needs access to the client.
            SyncHandlerRequest::NeedProcessBlockArtifact(block_processing_artifacts) => {
                self.client.process_block_processing_artifact(block_processing_artifacts);
            }
        }
    }

    /// Print current summary.
    fn log_summary(&mut self) {
        let _span = tracing::debug_span!(target: "client", "log_summary").entered();
        let signer = self.client.validator_signer.get();
        self.info_helper.log_summary(
            &self.client,
            &self.node_id,
            &self.network_info,
            &self.config_updater,
            &signer,
        )
    }

    /// Checks if the node is syncing its State and applies special logic in
    /// that case. A node usually ignores blocks that are too far ahead, but in
    /// case of a node syncing its state it is looking for specific blocks:
    ///
    /// - The sync hash block
    /// - The prev block
    /// - Extra blocks before the prev block needed for incoming receipts
    ///
    /// Returns whether the node is syncing its state.
    fn maybe_receive_state_sync_blocks(&mut self, block: Arc<Block>) -> bool {
        let SyncStatus::StateSync(StateSyncStatus { sync_hash, .. }) =
            self.client.sync_handler.sync_status
        else {
            return false;
        };

        let Ok(header) = self.client.chain.get_block_header(&sync_hash) else {
            return true;
        };

        let block: MaybeValidated<Arc<Block>> = Arc::clone(&block).into();
        let block_hash = *block.hash();

        // Notice that the blocks are saved differently:
        // * save_orphan() for the sync hash block
        // * save_block() for the prev block and all the extra blocks
        //
        // The sync hash block is saved to the orphan pool where it will
        // wait to be processed after state sync is completed.
        //
        // The other blocks do not need to be processed and are saved
        // directly to storage.

        if block_hash == sync_hash {
            // The first block of the new epoch.
            if let Err(err) = self.client.chain.validate_block(&block) {
                byzantine_assert!(false);
                error!(target: "client", ?err, ?block_hash, "Received an invalid block during state sync");
            }
            tracing::debug!(target: "sync", block_hash=?block.hash(), "maybe_receive_state_sync_blocks - save sync hash block");
            self.client.chain.save_orphan(block, Provenance::NONE, false);
            return true;
        }

        if &block_hash == header.prev_hash() {
            // The last block of the previous epoch.
            if let Err(err) = self.client.chain.validate_block(&block) {
                byzantine_assert!(false);
                error!(target: "client", ?err, ?block_hash, "Received an invalid block during state sync");
            }
            tracing::debug!(target: "sync", block_hash=?block.hash(), "maybe_receive_state_sync_blocks - save prev hash block");
            // Prev sync block will have its refcount increased later when processing sync block.
            if let Err(err) = self.client.chain.save_block(block) {
                error!(target: "client", ?err, ?block_hash, "Failed to save a block during state sync");
            }
            return true;
        }

        let extra_block_hashes = self.client.chain.get_extra_sync_block_hashes(&header.prev_hash());
        tracing::trace!(target: "sync", ?extra_block_hashes, "maybe_receive_state_sync_blocks: Extra block hashes for state sync");

        if extra_block_hashes.contains(&block_hash) {
            if let Err(err) = self.client.chain.validate_block(&block) {
                byzantine_assert!(false);
                error!(target: "client", ?err, ?block_hash, "Received an invalid block during state sync");
            }
            // Extra blocks needed when there are missing chunks.
            tracing::debug!(target: "sync", block_hash=?block.hash(), "maybe_receive_state_sync_blocks - save extra block");
            if let Err(err) = self.client.chain.save_block(block) {
                error!(target: "client", ?err, ?block_hash, "Failed to save a block during state sync");
            } else {
                // save_block() does not increase refcount, and for extra blocks we need to increase the refcount manually.
                let mut store_update = self.client.chain.mut_chain_store().store_update();
                store_update.inc_block_refcount(&block_hash).unwrap();
                store_update.commit().unwrap();
            }
            return true;
        }
        true
    }

    /// Produces `num_blocks` number of blocks.
    ///
    /// The parameter `height_selection` governs the produced blocks' heights and what base block
    /// height they are placed.
    #[cfg(feature = "test_features")]
    pub fn adv_produce_blocks_on(
        &mut self,
        num_blocks: BlockHeight,
        only_valid: bool,
        height_selection: AdvProduceBlockHeightSelection,
    ) {
        use AdvProduceBlockHeightSelection::*;

        info!(target: "adversary", num_blocks, "Starting adversary blocks production");
        if only_valid {
            self.client.adv_produce_blocks = Some(AdvProduceBlocksMode::OnlyValid);
        } else {
            self.client.adv_produce_blocks = Some(AdvProduceBlocksMode::All);
        }
        let (start_height, prev_block_height) = match height_selection {
            NextHeightOnLatestKnown => {
                let latest_height =
                    self.client.chain.mut_chain_store().get_latest_known().unwrap().height;
                (latest_height + 1, latest_height)
            }
            SelectedHeightOnLatestKnown { produced_block_height } => (
                produced_block_height,
                self.client.chain.mut_chain_store().get_latest_known().unwrap().height,
            ),
            NextHeightOnCurrentHead => {
                let head_height = self.client.chain.mut_chain_store().head().unwrap().height;
                (head_height + 1, head_height)
            }
            SelectedHeightOnCurrentHead { produced_block_height } => {
                (produced_block_height, self.client.chain.mut_chain_store().head().unwrap().height)
            }
            NextHeightOnSelectedBlock { base_block_height } => {
                (base_block_height + 1, base_block_height)
            }
            SelectedHeightOnSelectedBlock { produced_block_height, base_block_height } => {
                (produced_block_height, base_block_height)
            }
        };
        let is_based_on_current_head =
            prev_block_height == self.client.chain.mut_chain_store().head().unwrap().height;
        let mut blocks_produced = 0;
        for height in start_height.. {
            let block = if is_based_on_current_head {
                self.client.produce_block(height).expect("block should be produced")
            } else {
                let prev_block_hash = self
                    .client
                    .chain
                    .chain_store()
                    .get_block_hash_by_height(prev_block_height)
                    .expect("prev block should exist");
                self.client
                    .produce_block_on(height, prev_block_hash)
                    .expect("block should be produced")
            };
            if only_valid && block == None {
                continue;
            }
            let block = block.expect("block should exist after produced");
            info!(target: "adversary", blocks_produced, num_blocks, height, "Producing adversary block");
            self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
                NetworkRequests::Block { block: block.clone() },
            ));
            let _ = self.client.start_process_block(
                block.into(),
                Provenance::PRODUCED,
                Some(self.client.myself_sender.apply_chunks_done.clone()),
            );
            blocks_produced += 1;
            if blocks_produced == num_blocks {
                break;
            }
        }
    }
}

impl Handler<SpanWrapped<BlockCatchUpResponse>> for ClientActorInner {
    fn handle(&mut self, msg: SpanWrapped<BlockCatchUpResponse>) {
        let msg = msg.span_unwrap();
        debug!(target: "client", ?msg);
        if let Some(CatchupState { catchup, .. }) =
            self.client.catchup_state_syncs.get_mut(&msg.sync_hash)
        {
            assert!(catchup.scheduled_blocks.remove(&msg.block_hash));
            catchup.processed_blocks.insert(
                msg.block_hash,
                msg.results.into_iter().map(|res| res.1).collect::<Vec<_>>(),
            );
        } else {
            panic!("block catch up processing result from unknown sync hash");
        }
    }
}

impl Handler<SpanWrapped<ShardsManagerResponse>> for ClientActorInner {
    #[perf]
    fn handle(&mut self, msg: SpanWrapped<ShardsManagerResponse>) {
        let msg = msg.span_unwrap();
        match msg {
            ShardsManagerResponse::ChunkCompleted { partial_chunk, shard_chunk } => {
                self.client.on_chunk_completed(
                    partial_chunk,
                    shard_chunk,
                    Some(self.client.myself_sender.apply_chunks_done.clone()),
                );
            }
            ShardsManagerResponse::InvalidChunk(encoded_chunk) => {
                self.client.on_invalid_chunk(encoded_chunk);
            }
            ShardsManagerResponse::ChunkHeaderReadyForInclusion {
                chunk_header,
                chunk_producer,
            } => {
                self.client
                    .chunk_inclusion_tracker
                    .mark_chunk_header_ready_for_inclusion(chunk_header, chunk_producer);
            }
        }
    }
}

impl Handler<SpanWrapped<GetClientConfig>, Result<ClientConfig, GetClientConfigError>>
    for ClientActorInner
{
    fn handle(
        &mut self,
        msg: SpanWrapped<GetClientConfig>,
    ) -> Result<ClientConfig, GetClientConfigError> {
        debug!(target: "client", ?msg);
        Ok(self.client.config.clone())
    }
}

impl Handler<SpanWrapped<StateHeaderValidationRequest>, Result<(), near_chain::Error>>
    for ClientActorInner
{
    #[perf]
    fn handle(
        &mut self,
        msg: SpanWrapped<StateHeaderValidationRequest>,
    ) -> Result<(), near_chain::Error> {
        let msg = msg.span_unwrap();
        self.client.chain.state_sync_adapter.set_state_header(
            msg.shard_id,
            msg.sync_hash,
            msg.header,
        )
    }
}

impl Handler<SpanWrapped<ChainFinalizationRequest>, Result<(), near_chain::Error>>
    for ClientActorInner
{
    #[perf]
    fn handle(
        &mut self,
        msg: SpanWrapped<ChainFinalizationRequest>,
    ) -> Result<(), near_chain::Error> {
        let msg = msg.span_unwrap();
        self.client.chain.set_state_finalize(msg.shard_id, msg.sync_hash)
    }
}
