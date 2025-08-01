use std::sync::Arc;

use near_async::messaging::{IntoMultiSender, IntoSender, LateBoundSender, noop};
use near_async::test_loop::TestLoopV2;
use near_async::time::Duration;
use near_chain::resharding::resharding_actor::ReshardingActor;
use near_chain::runtime::NightshadeRuntime;
use near_chain::spice_core::CoreStatementsProcessor;
use near_chain::state_snapshot_actor::{
    SnapshotCallbacks, StateSnapshotActor, get_delete_snapshot_callback, get_make_snapshot_callback,
};
use near_chain::types::RuntimeAdapter;
use near_chain::{ApplyChunksSpawner, ChainGenesis};
use near_chain_configs::{MutableConfigValue, ReshardingHandle};
use near_chunks::shards_manager_actor::ShardsManagerActor;
use near_client::chunk_executor_actor::ChunkExecutorActor;
use near_client::client_actor::ClientActorInner;
use near_client::gc_actor::GCActor;
use near_client::spice_chunk_validator_actor::SpiceChunkValidatorActor;
use near_client::sync_jobs_actor::SyncJobsActor;
use near_client::{
    AsyncComputationMultiSpawner, Client, PartialWitnessActor, RpcHandler, RpcHandlerConfig,
    StateRequestActor, ViewClientActorInner,
};
use near_client::{
    ChunkValidationActorInner, ChunkValidationSender, ChunkValidationSenderForPartialWitness,
};
use near_epoch_manager::EpochManager;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::genesis::GenesisId;
use near_primitives::network::PeerId;
use near_primitives::test_utils::create_test_signer;
use near_store::adapter::StoreAdapter;
use near_store::{StoreConfig, TrieConfig};
use near_vm_runner::{ContractRuntimeCache, FilesystemContractRuntimeCache};
use nearcore::state_sync::StateSyncDumper;

use crate::utils::peer_manager_actor::TestLoopPeerManagerActor;

use super::drop_condition::ClientToShardsManagerSender;
use super::state::{NodeExecutionData, NodeSetupState, SharedState};

#[allow(clippy::large_stack_frames)]
pub fn setup_client(
    identifier: &str,
    test_loop: &mut TestLoopV2,
    node_state: NodeSetupState,
    shared_state: &SharedState,
) -> NodeExecutionData {
    let NodeSetupState { account_id, client_config, store, split_store } = node_state;
    let SharedState {
        genesis,
        tempdir,
        epoch_config_store,
        runtime_config_store,
        network_shared_state,
        upgrade_schedule,
        chunks_storage,
        drop_conditions,
        load_memtries_for_tracked_shards,
        ..
    } = shared_state;

    let client_adapter = LateBoundSender::new();
    let rpc_handler_adapter = LateBoundSender::new();
    let network_adapter = LateBoundSender::new();
    let state_snapshot_adapter = LateBoundSender::new();
    let partial_witness_adapter = LateBoundSender::new();
    let sync_jobs_adapter = LateBoundSender::new();
    let resharding_sender = LateBoundSender::new();
    let chunk_executor_adapter = LateBoundSender::new();
    let spice_chunk_validator_adapter = LateBoundSender::new();

    let homedir = tempdir.path().join(format!("{}", identifier));
    std::fs::create_dir_all(&homedir).expect("Unable to create homedir");

    let store_config = StoreConfig {
        path: Some(homedir.clone()),
        load_memtries_for_tracked_shards: *load_memtries_for_tracked_shards,
        ..Default::default()
    };

    let sync_jobs_actor = SyncJobsActor::new(client_adapter.as_multi_sender());
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
        store.clone(),
        &genesis.config,
        epoch_config_store.clone(),
    );

    let contract_cache = FilesystemContractRuntimeCache::test().expect("filesystem contract cache");
    let runtime_adapter = NightshadeRuntime::test_with_trie_config(
        &homedir,
        store.clone(),
        ContractRuntimeCache::handle(&contract_cache),
        &genesis.config,
        epoch_manager.clone(),
        runtime_config_store.clone(),
        TrieConfig::from_store_config(&store_config),
        client_config.gc.gc_num_epochs_to_keep,
    );

    let state_snapshot = StateSnapshotActor::new(
        runtime_adapter.get_flat_storage_manager(),
        network_adapter.as_multi_sender(),
        runtime_adapter.get_tries(),
    );

    let delete_snapshot_callback =
        get_delete_snapshot_callback(state_snapshot_adapter.as_multi_sender());
    let make_snapshot_callback = get_make_snapshot_callback(
        state_snapshot_adapter.as_multi_sender(),
        runtime_adapter.get_flat_storage_manager(),
    );
    let snapshot_callbacks = SnapshotCallbacks { make_snapshot_callback, delete_snapshot_callback };

    let validator_signer = MutableConfigValue::new(
        Some(Arc::new(create_test_signer(account_id.as_str()))),
        "validator_signer",
    );
    let shard_tracker = ShardTracker::new(
        client_config.tracked_shards_config.clone(),
        epoch_manager.clone(),
        validator_signer.clone(),
    );

    let shards_manager_adapter = LateBoundSender::new();
    let client_to_shards_manager_sender = Arc::new(ClientToShardsManagerSender {
        sender: shards_manager_adapter.clone(),
        chunks_storage: chunks_storage.clone(),
    });

    // Generate a PeerId. This is used to identify the client in the network.
    // Make sure this is the same as the account_id of the client to redirect the network messages properly.
    let peer_id = PeerId::new(create_test_signer(account_id.as_str()).public_key());

    let multi_spawner = AsyncComputationMultiSpawner::all_custom(Arc::new(
        test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80)),
    ));

    let chunk_validation_client_sender = LateBoundSender::<ChunkValidationSender>::new();

    let spice_core_processor = CoreStatementsProcessor::new(
        runtime_adapter.store().chain_store(),
        epoch_manager.clone(),
        chunk_executor_adapter.as_sender(),
        spice_chunk_validator_adapter.as_sender(),
    );
    let client = Client::new(
        test_loop.clock(),
        client_config.clone(),
        chain_genesis.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        runtime_adapter.clone(),
        network_adapter.as_multi_sender(),
        client_to_shards_manager_sender.as_sender(),
        validator_signer.clone(),
        true,
        [0; 32],
        Some(snapshot_callbacks),
        multi_spawner,
        partial_witness_adapter.as_multi_sender(),
        resharding_sender.as_multi_sender(),
        Arc::new(test_loop.future_spawner(identifier)),
        client_adapter.as_multi_sender(),
        client_adapter.as_multi_sender(),
        chunk_validation_client_sender.as_multi_sender(),
        upgrade_schedule.clone(),
        spice_core_processor.clone(),
    )
    .unwrap();

    // If this is an archival node and split storage is initialized, then create view-specific
    // versions of EpochManager, ShardTracker and RuntimeAdapter and use them to initialize the
    // ViewClientActorInner. Otherwise, we use the regular versions created above.
    let (view_epoch_manager, view_shard_tracker, view_runtime_adapter) =
        if let Some(split_store) = &split_store {
            let view_epoch_manager = EpochManager::new_arc_handle_from_epoch_config_store(
                split_store.clone(),
                &genesis.config,
                epoch_config_store.clone(),
            );
            let view_shard_tracker = ShardTracker::new(
                client_config.tracked_shards_config.clone(),
                view_epoch_manager.clone(),
                validator_signer.clone(),
            );
            let view_runtime_adapter = NightshadeRuntime::test_with_trie_config(
                &homedir,
                split_store.clone(),
                ContractRuntimeCache::handle(&contract_cache),
                &genesis.config,
                view_epoch_manager.clone(),
                runtime_config_store.clone(),
                TrieConfig::from_store_config(&store_config),
                client_config.gc.gc_num_epochs_to_keep,
            );
            (view_epoch_manager, view_shard_tracker, view_runtime_adapter)
        } else {
            (epoch_manager.clone(), shard_tracker.clone(), runtime_adapter.clone())
        };
    let view_client_actor = ViewClientActorInner::new(
        test_loop.clock(),
        chain_genesis.clone(),
        view_epoch_manager.clone(),
        view_shard_tracker,
        view_runtime_adapter,
        network_adapter.as_multi_sender(),
        client_config.clone(),
        near_client::adversarial::Controls::default(),
        validator_signer.clone(),
    )
    .unwrap();
    let state_request_actor = StateRequestActor::new(
        test_loop.clock(),
        runtime_adapter.clone(),
        epoch_manager.clone(),
        *view_client_actor.chain.genesis().hash(),
        client_config.state_request_throttle_period,
        client_config.state_requests_per_throttle_period,
    );

    let head = client.chain.head().unwrap();
    let header_head = client.chain.header_head().unwrap();
    let shards_manager = ShardsManagerActor::new(
        test_loop.clock(),
        validator_signer.clone(),
        epoch_manager.clone(),
        view_epoch_manager,
        shard_tracker.clone(),
        network_adapter.as_sender(),
        client_adapter.as_sender(),
        store.chunk_store(),
        <_>::clone(&head),
        <_>::clone(&header_head),
        Duration::milliseconds(100),
    );

    let genesis_block = client.chain.genesis_block();
    let chunk_validation_actor = ChunkValidationActorInner::new(
        client.chain.chain_store().clone(),
        genesis_block,
        epoch_manager.clone(),
        runtime_adapter.clone(),
        network_adapter.as_sender(),
        validator_signer.clone(),
        client_config.save_latest_witnesses,
        client_config.save_invalid_witnesses,
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80))),
        client_config.orphan_state_witness_pool_size,
        client_config.orphan_state_witness_max_size.as_u64(),
    );
    let chunk_executor_sender = if cfg!(feature = "protocol_feature_spice") {
        chunk_executor_adapter.as_sender()
    } else {
        noop().into_sender()
    };
    let spice_chunk_validator_sender = if cfg!(feature = "protocol_feature_spice") {
        spice_chunk_validator_adapter.as_sender()
    } else {
        noop().into_sender()
    };
    let client_actor = ClientActorInner::new(
        test_loop.clock(),
        client,
        peer_id.clone(),
        network_adapter.as_multi_sender(),
        noop().into_sender(),
        None,
        Default::default(),
        None,
        sync_jobs_adapter.as_multi_sender(),
        chunk_executor_sender,
        spice_chunk_validator_sender,
    )
    .unwrap();

    let rpc_handler_config = RpcHandlerConfig {
        handler_threads: client_config.transaction_request_handler_threads,
        tx_routing_height_horizon: client_config.tx_routing_height_horizon,
        epoch_length: client_config.epoch_length,
        transaction_validity_period: genesis.config.transaction_validity_period,
    };
    let rpc_handler = RpcHandler::new(
        rpc_handler_config,
        client_actor.client.chunk_producer.sharded_tx_pool.clone(),
        client_actor.client.chunk_endorsement_tracker.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        validator_signer.clone(),
        runtime_adapter.clone(),
        network_adapter.as_multi_sender(),
    );

    let chunk_validation_adapter = LateBoundSender::<ChunkValidationSenderForPartialWitness>::new();

    let partial_witness_actor = PartialWitnessActor::new(
        test_loop.clock(),
        network_adapter.as_multi_sender(),
        chunk_validation_adapter.as_multi_sender(),
        validator_signer.clone(),
        epoch_manager.clone(),
        runtime_adapter.clone(),
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80))), // Heavy contract compilation
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(10))),
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(10))),
    );

    let peer_manager_actor = TestLoopPeerManagerActor::new(
        test_loop.clock(),
        &account_id,
        network_shared_state,
        client_adapter.as_multi_sender(),
        GenesisId {
            chain_id: client_config.chain_id.clone(),
            hash: *client_actor.client.chain.genesis().hash(),
        },
        Arc::new(test_loop.future_spawner(identifier)),
    );

    let gc_actor = GCActor::new(
        runtime_adapter.store().clone(),
        &chain_genesis,
        runtime_adapter.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        client_config.gc.clone(),
        client_config.archive,
    );
    // We don't send messages to `GCActor` so adapter is not needed.
    test_loop.data.register_actor(identifier, gc_actor, None);

    let resharding_actor = ReshardingActor::new(
        epoch_manager.clone(),
        runtime_adapter.clone(),
        ReshardingHandle::new(),
        client_config.resharding_config.clone(),
    );

    let chunk_executor_actor = ChunkExecutorActor::new(
        runtime_adapter.store().clone(),
        &chain_genesis,
        *client_actor.client.chain.genesis().hash(),
        runtime_adapter.clone(),
        epoch_manager.clone(),
        shard_tracker.clone(),
        network_adapter.as_multi_sender(),
        validator_signer.clone(),
        spice_core_processor.clone(),
        client_actor.client.chunk_endorsement_tracker.clone(),
        Arc::new(test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80))),
        chunk_executor_adapter.as_sender(),
        client_config.clone(),
    );

    let chunk_executor_sender = test_loop.data.register_actor(
        identifier,
        chunk_executor_actor,
        Some(chunk_executor_adapter),
    );

    let spice_chunk_validator_actor = SpiceChunkValidatorActor::new(
        runtime_adapter.store().clone(),
        &chain_genesis,
        runtime_adapter.clone(),
        epoch_manager.clone(),
        network_adapter.as_multi_sender(),
        validator_signer.clone(),
        spice_core_processor,
        client_actor.client.chunk_endorsement_tracker.clone(),
        ApplyChunksSpawner::Custom(Arc::new(
            test_loop.async_computation_spawner(identifier, |_| Duration::milliseconds(80)),
        )),
        client_config.clone(),
    );

    let spice_chunk_validator_sender = test_loop.data.register_actor(
        identifier,
        spice_chunk_validator_actor,
        Some(spice_chunk_validator_adapter),
    );

    let state_sync_dumper = StateSyncDumper {
        clock: test_loop.clock(),
        client_config,
        chain_genesis,
        epoch_manager,
        shard_tracker,
        runtime: runtime_adapter,
        validator: validator_signer,
        future_spawner: Arc::new(test_loop.future_spawner(identifier)),
        handle: None,
    };
    let state_sync_dumper_handle = test_loop.data.register_data(state_sync_dumper);

    let client_sender =
        test_loop.data.register_actor(identifier, client_actor, Some(client_adapter));
    let view_client_sender = test_loop.data.register_actor(identifier, view_client_actor, None);
    let state_request_sender = test_loop.data.register_actor(identifier, state_request_actor, None);
    let rpc_handler_sender =
        test_loop.data.register_actor(identifier, rpc_handler, Some(rpc_handler_adapter));
    let shards_manager_sender =
        test_loop.data.register_actor(identifier, shards_manager, Some(shards_manager_adapter));
    let partial_witness_sender = test_loop.data.register_actor(
        identifier,
        partial_witness_actor,
        Some(partial_witness_adapter),
    );
    test_loop.data.register_actor(identifier, sync_jobs_actor, Some(sync_jobs_adapter));
    test_loop.data.register_actor(identifier, state_snapshot, Some(state_snapshot_adapter));
    let resharding_sender =
        test_loop.data.register_actor(identifier, resharding_actor, Some(resharding_sender));

    let chunk_validation_sender = test_loop.data.register_actor(
        identifier,
        chunk_validation_actor,
        None, // No adapter needed, we'll bind directly
    );

    let chunk_validation_multi_sender = ChunkValidationSender {
        chunk_state_witness: chunk_validation_sender.clone().into_sender(),
        block_notification: chunk_validation_sender.into_sender(),
    };

    chunk_validation_client_sender.bind(chunk_validation_multi_sender.clone());

    chunk_validation_adapter.bind(ChunkValidationSenderForPartialWitness {
        chunk_state_witness: chunk_validation_multi_sender.chunk_state_witness,
    });

    // State sync dumper is not an Actor, handle starting separately.
    let state_sync_dumper_handle_clone = state_sync_dumper_handle.clone();
    test_loop.send_adhoc_event("start_state_sync_dumper".to_owned(), move |test_loop_data| {
        test_loop_data.get_mut(&state_sync_dumper_handle_clone).start().unwrap();
    });

    let peer_manager_sender =
        test_loop.data.register_actor(identifier, peer_manager_actor, Some(network_adapter));

    let node_data = NodeExecutionData {
        identifier: identifier.to_string(),
        account_id,
        peer_id,
        client_sender,
        view_client_sender,
        state_request_sender,
        rpc_handler_sender,
        shards_manager_sender,
        partial_witness_sender,
        peer_manager_sender,
        resharding_sender,
        state_sync_dumper_handle,
        chunk_executor_sender,
        spice_chunk_validator_sender,
    };

    // Add the client to the network shared state before returning data
    // Note that this can potentially overwrite an existing client with the same account_id
    // and all new messages would be redirected to the new client.
    network_shared_state.add_client(&node_data);

    // Register all accumulated drop conditions
    for condition in drop_conditions {
        node_data.register_drop_condition(&mut test_loop.data, chunks_storage.clone(), condition);
    }

    node_data
}
