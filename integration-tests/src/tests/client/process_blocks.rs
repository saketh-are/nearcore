use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::setup::setup_mock;
use crate::env::test_env::TestEnv;
use crate::env::test_env_builder::TestEnvBuilder;
use crate::utils::process_blocks::{
    deploy_test_contract, prepare_env_with_congestion, set_block_protocol_version,
};
use actix::System;
use assert_matches::assert_matches;
use futures::{FutureExt, future};
use itertools::Itertools;
use near_actix_test_utils::run_actix;
use near_async::time::{Clock, Duration};
use near_chain::types::{LatestKnown, RuntimeAdapter};
use near_chain::validate::validate_chunk_with_chunk_extra;
use near_chain::{Block, BlockProcessingArtifact, ChainStoreAccess, Error, Provenance};
use near_chain::{ChainStore, MerkleProofAccess};
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_chain_configs::{DEFAULT_GC_NUM_EPOCHS_TO_KEEP, Genesis, NEAR_BASE};
use near_client::test_utils::create_chunk_on_height;
use near_client::{GetBlockWithMerkleTree, ProcessTxResponse, ProduceChunkResult};
use near_crypto::{InMemorySigner, KeyType, Signature};
use near_network::client::{BlockApproval, BlockResponse, SetNetworkInfo};
use near_network::test_utils::{MockPeerManagerAdapter, wait_or_panic};
use near_network::types::{
    BlockInfo, ConnectedPeerInfo, HighestHeightPeerInfo, NetworkInfo, PeerChainInfo,
    PeerManagerMessageRequest, PeerManagerMessageResponse, PeerType,
};
use near_network::types::{FullPeerInfo, NetworkRequests, NetworkResponses};
use near_network::types::{PeerInfo, ReasonForBan};
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::{init_integration_logger, init_test_logger};
use near_parameters::{ActionCosts, ExtCosts};
use near_parameters::{RuntimeConfig, RuntimeConfigStore};
use near_primitives::block::{Approval, Chunks};
use near_primitives::errors::TxExecutionError;
use near_primitives::errors::{ActionError, ActionErrorKind, InvalidTxError};
use near_primitives::genesis::GenesisId;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::{PartialMerkleTree, verify_hash};
use near_primitives::receipt::DelayedReceiptIndices;
use near_primitives::shard_layout::{ShardUId, get_block_shard_uid};
use near_primitives::sharding::{
    ShardChunkHeader, ShardChunkHeaderInner, ShardChunkHeaderV3, ShardChunkWithEncoding,
};
use near_primitives::state_part::PartId;
use near_primitives::state_sync::StatePartKey;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use near_primitives::test_utils::TestBlockBuilder;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::{
    Action, DeployContractAction, ExecutionStatus, FunctionCallAction, SignedTransaction,
    Transaction, TransactionV0,
};
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, BlockHeight, EpochId, NumBlocks};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{FinalExecutionStatus, QueryRequest, QueryResponseKind};
use near_primitives_core::num_rational::Ratio;
use near_store::NodeStorage;
use near_store::adapter::StoreUpdateAdapter;
use near_store::archive::cold_storage::{update_cold_db, update_cold_head};
use near_store::db::metadata::{DB_VERSION, DbKind};
use near_store::test_utils::create_test_node_storage_with_cold;
use near_store::{DBCol, TrieChanges, get};
use parking_lot::RwLock;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Runs block producing client and stops after network mock received two blocks.
#[test]
fn produce_two_blocks() {
    init_test_logger();
    run_actix(async {
        let count = Arc::new(AtomicUsize::new(0));
        setup_mock(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "test".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _, _| {
                if let NetworkRequests::Block { .. } = msg.as_network_requests_ref() {
                    count.fetch_add(1, Ordering::Relaxed);
                    if count.load(Ordering::Relaxed) >= 2 {
                        System::current().stop();
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        near_network::test_utils::wait_or_panic(5000);
    });
}

/// Runs client that receives a block from network and announces header to the network with approval.
/// Need 3 block producers, to receive approval.
#[test]
fn receive_network_block() {
    init_test_logger();
    run_actix(async {
        // The first header announce will be when the block is received. We don't immediately endorse
        // it. The second header announce will happen with the endorsement a little later.
        let first_header_announce = Arc::new(RwLock::new(true));
        let actor_handles = setup_mock(
            Clock::real(),
            vec!["test2".parse().unwrap(), "test1".parse().unwrap(), "test3".parse().unwrap()],
            "test2".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _, _| {
                if let NetworkRequests::Approval { .. } = msg.as_network_requests_ref() {
                    let mut first_header_announce = first_header_announce.write();
                    if *first_header_announce {
                        *first_header_announce = false;
                    } else {
                        System::current().stop();
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        let actor = actor_handles.view_client_actor.send(GetBlockWithMerkleTree::latest());
        let actor = actor.then(move |res| {
            let (last_block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            block_merkle_tree.insert(last_block.header.hash);
            let signer = create_test_signer("test1");
            let next_block_ordinal = last_block.header.block_ordinal.unwrap() + 1;
            let block = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                next_block_ordinal,
                last_block.chunks.iter().cloned().map(Into::into).collect(),
                vec![vec![]; last_block.chunks.len()],
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id)
                },
                None,
                vec![],
                Ratio::from_integer(0),
                0,
                100,
                None,
                &signer,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
                Clock::real(),
                None,
                None,
                vec![],
            );
            actor_handles.client_actor.do_send(
                BlockResponse {
                    block: block.into(),
                    peer_id: PeerInfo::random().id,
                    was_requested: false,
                }
                .span_wrap(),
            );
            future::ready(())
        });
        actix::spawn(actor);
        near_network::test_utils::wait_or_panic(5000);
    });
}

/// Include approvals to the next block in newly produced block.
#[test]
fn produce_block_with_approvals() {
    init_test_logger();
    let validators: Vec<_> =
        (1..=10).map(|i| AccountId::try_from(format!("test{}", i)).unwrap()).collect();

    // Which of validators is test producer depends on deterministic rng setup for the test.
    let block_producer = "test3";
    run_actix(async {
        let actor_handles = setup_mock(
            Clock::real(),
            validators.clone(),
            block_producer.parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _, _| {
                if let NetworkRequests::Block { block } = msg.as_network_requests_ref() {
                    // Below we send approvals from all the block producers except for test1 and test2
                    // test1 will only create their approval for height 10 after their doomslug timer
                    // runs 10 iterations, which is way further in the future than them producing the
                    // block
                    if block.header().num_approvals() == validators.len() as u64 - 2 {
                        System::current().stop();
                    } else if block.header().height() == 10 {
                        println!("{}", block.header().height());
                        println!(
                            "{} != {} -2 (height: {})",
                            block.header().num_approvals(),
                            validators.len(),
                            block.header().height()
                        );

                        assert!(false);
                    }
                }
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        let actor = actor_handles.view_client_actor.send(GetBlockWithMerkleTree::latest());
        let actor = actor.then(move |res| {
            let (last_block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            block_merkle_tree.insert(last_block.header.hash);
            let signer1 = create_test_signer(&block_producer);
            let next_block_ordinal = last_block.header.block_ordinal.unwrap() + 1;

            let chunks = last_block.chunks.into_iter().map(Into::into).collect_vec();
            let chunk_endorsements = vec![vec![]; chunks.len()];
            let block = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                next_block_ordinal,
                chunks,
                chunk_endorsements,
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id)
                },
                None,
                vec![],
                Ratio::from_integer(0),
                0,
                100,
                Some(0),
                &signer1,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
                Clock::real(),
                None,
                None,
                vec![],
            );
            actor_handles.client_actor.do_send(
                BlockResponse {
                    block: block.clone().into(),
                    peer_id: PeerInfo::random().id,
                    was_requested: false,
                }
                .span_wrap(),
            );

            for i in 3..11 {
                let s = AccountId::try_from(if i > 10 {
                    "test1".to_string()
                } else {
                    format!("test{}", i)
                })
                .unwrap();
                let signer = create_test_signer(s.as_str());
                let approval = Approval::new(
                    *block.hash(),
                    block.header().height(),
                    10, // the height at which "test1" is producing
                    &signer,
                );
                actor_handles
                    .client_actor
                    .do_send(BlockApproval(approval, PeerInfo::random().id).span_wrap());
            }

            future::ready(())
        });
        actix::spawn(actor);
        near_network::test_utils::wait_or_panic(5000);
    });
}

/// Sends one invalid block followed by one valid block, and checks that client announces only valid block.
/// and that the node bans the peer for invalid block header.
fn invalid_blocks_common(is_requested: bool) {
    init_test_logger();
    run_actix(async move {
        let mut ban_counter = 0;
        let actor_handles = setup_mock(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _client_actor, _rpc_handler_actor| {
                match msg.as_network_requests_ref() {
                    NetworkRequests::Block { block } => {
                        if is_requested {
                            panic!("rebroadcasting requested block");
                        } else {
                            assert_eq!(block.header().height(), 1);
                            assert_eq!(block.header().chunk_mask().len(), 1);
                            assert_eq!(block.header().latest_protocol_version(), PROTOCOL_VERSION);
                            assert_eq!(ban_counter, 3);
                            System::current().stop();
                        }
                    }
                    NetworkRequests::BanPeer { ban_reason, .. } => {
                        assert_eq!(ban_reason, &ReasonForBan::BadBlockHeader);
                        ban_counter += 1;
                        let expected_ban_counter = 4;
                        if ban_counter == expected_ban_counter && is_requested {
                            System::current().stop();
                        }
                    }
                    _ => {}
                };
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        let actor = actor_handles.view_client_actor.send(GetBlockWithMerkleTree::latest());
        let actor = actor.then(move |res| {
            let (last_block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            block_merkle_tree.insert(last_block.header.hash);
            let signer = create_test_signer("test");
            let next_block_ordinal = last_block.header.block_ordinal.unwrap() + 1;
            let valid_block = Block::produce(
                PROTOCOL_VERSION,
                &last_block.header.clone().into(),
                last_block.header.height + 1,
                next_block_ordinal,
                last_block.chunks.iter().cloned().map(Into::into).collect(),
                vec![vec![]; last_block.chunks.len()],
                EpochId::default(),
                if last_block.header.prev_hash == CryptoHash::default() {
                    EpochId(last_block.header.hash)
                } else {
                    EpochId(last_block.header.next_epoch_id)
                },
                None,
                vec![],
                Ratio::from_integer(0),
                0,
                100,
                Some(0),
                &signer,
                last_block.header.next_bp_hash,
                block_merkle_tree.root(),
                Clock::real(),
                None,
                None,
                vec![],
            );
            // Send block with invalid chunk mask
            let mut block = valid_block.clone();
            block.mut_header().set_chunk_mask(vec![]);
            block.mut_header().init();
            actor_handles.client_actor.do_send(
                BlockResponse {
                    block: block.clone().into(),
                    peer_id: PeerInfo::random().id,
                    was_requested: is_requested,
                }
                .span_wrap(),
            );

            // Send blocks with invalid protocol version
            let mut block = valid_block.clone();
            block.mut_header().set_latest_protocol_version(PROTOCOL_VERSION - 1);
            block.mut_header().init();
            actor_handles.client_actor.do_send(
                BlockResponse {
                    block: block.clone().into(),
                    peer_id: PeerInfo::random().id,
                    was_requested: is_requested,
                }
                .span_wrap(),
            );

            // Send block with invalid chunk signature
            let mut block = valid_block.clone();
            let mut chunks: Vec<_> = block.chunks().iter_raw().cloned().collect();
            let some_signature = Signature::from_parts(KeyType::ED25519, &[1; 64]).unwrap();
            match &mut chunks[0] {
                ShardChunkHeader::V1(chunk) => {
                    chunk.signature = some_signature;
                }
                ShardChunkHeader::V2(chunk) => {
                    chunk.signature = some_signature;
                }
                ShardChunkHeader::V3(chunk) => {
                    chunk.signature = some_signature;
                }
            };
            block.set_chunks(chunks);
            actor_handles.client_actor.do_send(
                BlockResponse {
                    block: block.clone().into(),
                    peer_id: PeerInfo::random().id,
                    was_requested: is_requested,
                }
                .span_wrap(),
            );

            // Send proper block.
            let block2 = valid_block;
            actor_handles.client_actor.do_send(
                BlockResponse {
                    block: block2.clone().into(),
                    peer_id: PeerInfo::random().id,
                    was_requested: is_requested,
                }
                .span_wrap(),
            );
            if is_requested {
                let mut block3 = block2;
                block3.mut_header().set_chunk_headers_root(hash(&[1]));
                block3.mut_header().init();
                actor_handles.client_actor.do_send(
                    BlockResponse {
                        block: block3.clone().into(),
                        peer_id: PeerInfo::random().id,
                        was_requested: is_requested,
                    }
                    .span_wrap(),
                );
            }
            future::ready(())
        });
        actix::spawn(actor);
        near_network::test_utils::wait_or_panic(5000);
    });
}

#[test]
fn test_invalid_blocks_not_requested() {
    invalid_blocks_common(false);
}

#[test]
fn test_invalid_blocks_requested() {
    invalid_blocks_common(true);
}

/// Runs two validators runtime with only one validator online.
/// Present validator produces blocks on it's height after deadline.
#[test]
fn skip_block_production() {
    init_test_logger();
    run_actix(async {
        setup_mock(
            Clock::real(),
            vec!["test1".parse().unwrap(), "test2".parse().unwrap()],
            "test2".parse().unwrap(),
            true,
            false,
            Box::new(move |msg, _ctx, _client_actor, _rpc_handler_actor| {
                match msg.as_network_requests_ref() {
                    NetworkRequests::Block { block } => {
                        if block.header().height() > 3 {
                            System::current().stop();
                        }
                    }
                    _ => {}
                };
                PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
            }),
        );
        wait_or_panic(10000);
    });
}

/// Runs client that requests syncing headers from peers.
#[test]
fn client_sync_headers() {
    init_test_logger();
    run_actix(async {
        let peer_info1 = PeerInfo::random();
        let peer_info2 = peer_info1.clone();
        let actor_handles = setup_mock(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            false,
            false,
            Box::new(move |msg, _ctx, _client_actor, _rpc_handler_actor| {
                match msg.as_network_requests_ref() {
                    NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                        assert_eq!(*peer_id, peer_info1.id);
                        assert_eq!(hashes.len(), 1);
                        // TODO: check it requests correct hashes.
                        System::current().stop();

                        PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse)
                    }
                    _ => PeerManagerMessageResponse::NetworkResponses(NetworkResponses::NoResponse),
                }
            }),
        );
        actor_handles.client_actor.do_send(
            SetNetworkInfo(NetworkInfo {
                connected_peers: vec![ConnectedPeerInfo {
                    full_peer_info: FullPeerInfo {
                        peer_info: peer_info2.clone(),
                        chain_info: PeerChainInfo {
                            genesis_id: Default::default(),
                            last_block: Some(BlockInfo { height: 5, hash: hash(&[5]) }),
                            tracked_shards: vec![],
                            archival: false,
                        },
                    },
                    received_bytes_per_sec: 0,
                    sent_bytes_per_sec: 0,
                    last_time_peer_requested: near_async::time::Instant::now(),
                    last_time_received_message: near_async::time::Instant::now(),
                    connection_established_time: near_async::time::Instant::now(),
                    peer_type: PeerType::Outbound,
                    nonce: 1,
                }],
                num_connected_peers: 1,
                peer_max_count: 1,
                highest_height_peers: vec![HighestHeightPeerInfo {
                    peer_info: peer_info2,
                    genesis_id: Default::default(),
                    highest_block_height: 5,
                    highest_block_hash: hash(&[5]),
                    tracked_shards: vec![],
                    archival: false,
                }],
                sent_bytes_per_sec: 0,
                received_bytes_per_sec: 0,
                known_producers: vec![],
                tier1_connections: vec![],
                tier1_accounts_keys: vec![],
                tier1_accounts_data: vec![],
            })
            .span_wrap(),
        );
        wait_or_panic(2000);
    });
}

#[test]
fn test_process_invalid_tx() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    genesis.config.transaction_validity_period = 10;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction::V0(TransactionV0 {
            signer_id: "test".parse().unwrap(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "test".parse().unwrap(),
            block_hash: *env.clients[0].chain.genesis().hash(),
            actions: vec![],
        }),
    );
    for i in 1..12 {
        env.produce_block(0, i);
    }
    assert_eq!(
        env.rpc_handlers[0].process_tx(tx, false, false),
        ProcessTxResponse::InvalidTx(InvalidTxError::Expired)
    );
    let tx2 = SignedTransaction::new(
        Signature::empty(KeyType::ED25519),
        Transaction::V0(TransactionV0 {
            signer_id: "test".parse().unwrap(),
            public_key: signer.public_key(),
            nonce: 0,
            receiver_id: "test".parse().unwrap(),
            block_hash: hash(&[1]),
            actions: vec![],
        }),
    );
    assert_eq!(
        env.rpc_handlers[0].process_tx(tx2, false, false),
        ProcessTxResponse::InvalidTx(InvalidTxError::Expired)
    );
}

/// If someone produce a block with Utc::now() + 1 min, we should produce a block with valid timestamp
#[test]
fn test_time_attack() {
    init_test_logger();
    let mut env = TestEnv::default_builder().clients_count(1).build();
    let client = &mut env.clients[0];
    let signer = client.validator_signer.get().unwrap();
    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
    let timestamp = b1.header().timestamp();
    Arc::make_mut(&mut b1)
        .mut_header()
        .set_timestamp((timestamp + Duration::seconds(60)).unix_timestamp_nanos() as u64);
    Arc::make_mut(&mut b1).mut_header().resign(signer.as_ref());

    let _ = client.process_block_test(b1.into(), Provenance::NONE).unwrap();

    let b2 = client.produce_block(2).unwrap().unwrap();
    let _ = client.process_block_test(b2.into(), Provenance::PRODUCED).unwrap();
}

#[test]
fn test_no_double_sign() {
    let mut env = TestEnv::default_builder().build();
    let _ = env.clients[0].produce_block(1).unwrap().unwrap();
    // Second time producing with the same height should fail.
    assert_eq!(env.clients[0].produce_block(1).unwrap(), None);
}

#[test]
fn test_invalid_gas_price() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    genesis.config.min_gas_price = 100;
    let mut env = TestEnv::builder_from_genesis(&genesis).clients_count(1).build();
    let client = &mut env.clients[0];
    let signer = client.validator_signer.get().unwrap();

    let genesis = client.chain.get_block_by_height(0).unwrap();
    let mut b1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
    Arc::make_mut(&mut b1).mut_header().set_next_gas_price(0);
    Arc::make_mut(&mut b1).mut_header().resign(signer.as_ref());

    let res = client.process_block_test(b1.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::InvalidGasPrice);
}

#[test]
fn test_invalid_height_too_large() {
    let mut env = TestEnv::default_builder().build();
    let b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    let _ = env.clients[0].process_block_test(b1.clone().into(), Provenance::PRODUCED).unwrap();
    let signer = Arc::new(create_test_signer("test0"));
    let b2 = TestBlockBuilder::new(Clock::real(), &b1, signer).height(u64::MAX).build();
    let res = env.clients[0].process_block_test(b2.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::InvalidBlockHeight(_));
}

/// Check that if block height is 5 epochs behind the head, it is not processed.
#[test]
fn test_invalid_height_too_old() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    genesis.config.epoch_length = 5;
    let mut env = TestEnv::builder_from_genesis(&genesis).build();
    let unprocessed_height = 6;
    for i in 1..unprocessed_height {
        env.produce_block(0, i);
    }
    let block = env.clients[0].produce_block(unprocessed_height).unwrap().unwrap();
    for i in (unprocessed_height + 1)..28 {
        env.produce_block(0, i);
    }
    let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
    // GC for canonical chain cleans up previous blocks for a given height. So in this case
    // block at height 5 will not be cleaned up by GC and we wouldn't get Orphan error here,
    // although GC would clean up the respective epoch.
    assert_matches!(res.unwrap_err(), Error::InvalidBlockHeight(_));
}

#[test]
fn test_bad_orphan() {
    let mut env = TestEnv::default_builder().build();
    for i in 1..4 {
        env.produce_block(0, i);
    }
    let block = env.clients[0].produce_block(5).unwrap().unwrap();
    let signer = env.clients[0].validator_signer.get().unwrap();
    {
        // Orphan block with unknown epoch
        let mut block = env.clients[0].produce_block(6).unwrap().unwrap();
        let mut_block = Arc::make_mut(&mut block);
        mut_block.mut_header().set_epoch_id(EpochId(CryptoHash([1; 32])));
        mut_block.mut_header().set_prev_hash(CryptoHash([1; 32]));
        mut_block.mut_header().resign(&*signer);
        let res = env.clients[0].process_block_test(block.clone().into(), Provenance::NONE);
        match res {
            Err(Error::EpochOutOfBounds(epoch_id)) => {
                assert_eq!(&epoch_id, block.header().epoch_id())
            }
            _ => panic!("expected EpochOutOfBounds error, got {res:?}"),
        }
    }
    {
        // Orphan block with invalid signature
        let mut block = env.clients[0].produce_block(7).unwrap().unwrap();
        let mut_block = Arc::make_mut(&mut block);
        mut_block.mut_header().set_prev_hash(CryptoHash([1; 32]));
        mut_block.mut_header().init();
        let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidSignature);
    }
    {
        // Orphan block with a valid header, but garbage in body
        let mut block = env.clients[0].produce_block(8).unwrap().unwrap();
        let mut_block = Arc::make_mut(&mut block);
        {
            // Change the chunk in any way, chunk_headers_root won't match
            let chunk = mut_block.mut_chunks()[0].get_mut();

            match &mut chunk.inner {
                ShardChunkHeaderInner::V1(inner) => inner.prev_block_hash = CryptoHash([1; 32]),
                ShardChunkHeaderInner::V2(inner) => inner.prev_block_hash = CryptoHash([1; 32]),
                ShardChunkHeaderInner::V3(inner) => inner.prev_block_hash = CryptoHash([1; 32]),
                ShardChunkHeaderInner::V4(inner) => inner.prev_block_hash = CryptoHash([1; 32]),
                ShardChunkHeaderInner::V5(inner) => inner.prev_block_hash = CryptoHash([1; 32]),
            }
            chunk.hash = ShardChunkHeaderV3::compute_hash(&chunk.inner);
        }
        mut_block.mut_header().set_prev_hash(CryptoHash([3; 32]));
        mut_block.mut_header().resign(&*signer);
        let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidSignature);
    }
    {
        // Orphan block with invalid approvals. Allowed for now.
        let mut block = env.clients[0].produce_block(9).unwrap().unwrap();
        let mut_block = Arc::make_mut(&mut block);
        let some_signature = Signature::from_parts(KeyType::ED25519, &[1; 64]).unwrap();
        mut_block.mut_header().set_approvals(vec![Some(Box::new(some_signature))]);
        mut_block.mut_header().set_prev_hash(CryptoHash([3; 32]));
        mut_block.mut_header().resign(&*signer);
        let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);

        assert_matches!(res.unwrap_err(), Error::Orphan);
    }
    {
        // Orphan block with no chunk signatures. Allowed for now.
        let mut block = env.clients[0].produce_block(10).unwrap().unwrap();
        let mut_block = Arc::make_mut(&mut block);
        let some_signature = Signature::from_parts(KeyType::ED25519, &[1; 64]).unwrap();
        {
            // Change the chunk in any way, chunk_headers_root won't match
            let chunk = mut_block.mut_chunks()[0].get_mut();
            chunk.signature = some_signature;
            chunk.hash = ShardChunkHeaderV3::compute_hash(&chunk.inner);
        }
        mut_block.mut_header().set_prev_hash(CryptoHash([4; 32]));
        mut_block.mut_header().resign(&*signer);
        let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidSignature);
    }
    {
        // Orphan block that's too far ahead: 20 * epoch_length
        let mut block = block.clone();
        let mut_block = Arc::make_mut(&mut block);
        mut_block.mut_header().set_prev_hash(CryptoHash([3; 32]));
        let height = mut_block.header().height();
        mut_block.mut_header().set_height(height + 2000);
        mut_block.mut_header().resign(&*signer);
        let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), Error::InvalidBlockHeight(_));
    }
    env.clients[0].process_block_test(block.into(), Provenance::NONE).unwrap();
}

#[test]
fn test_bad_chunk_mask() {
    init_test_logger();

    // Create a TestEnv with two shards and two validators who track both shards
    let account0: AccountId = "test0.near".parse().unwrap();
    let account1: AccountId = "test1.near".parse().unwrap();
    let accounts = vec![account0.clone(), account1.clone()];
    let num_validators: u64 = accounts.len().try_into().unwrap();
    let genesis = Genesis::test_sharded(
        Clock::real(),
        accounts.clone(),
        num_validators,
        vec![num_validators; 2],
    );
    let mut env = TestEnv::builder(&genesis.config)
        .clients(accounts)
        .nightshade_runtimes(&genesis)
        .track_all_shards()
        .build();

    // The test never goes past the first epoch, so EpochId(11111...) can be used for all calculations
    let first_epoch_id = &EpochId::default();

    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&first_epoch_id).unwrap();
    tracing::info!(target: "test", ?shard_layout, "shard layout");

    let [s0, s1] = shard_layout.shard_ids().collect_vec()[..] else { panic!("Expected 2 shards") };
    assert_eq!(s0, shard_layout.account_id_to_shard_id(&account0));
    assert_eq!(s1, shard_layout.account_id_to_shard_id(&account1));

    // Generate 4 blocks
    let shard_id = s0;

    let tip = env.clients[0].chain.get_block_by_height(0).unwrap();
    for chunk_header in tip.chunks().iter() {
        tracing::info!(target: "test", ?chunk_header, "chunk header");
    }

    for height in 1..5 {
        let chunk_producer = env.clients[0]
            .epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                epoch_id: *first_epoch_id,
                height_created: height,
                shard_id,
            })
            .unwrap()
            .take_account_id();
        let block_producer =
            env.clients[0].epoch_manager.get_block_producer(&first_epoch_id, height).unwrap();

        // Manually produce a single chunk on shard s0, chunk for s1 is always missing.
        let shard_chunk = env.client(&chunk_producer).produce_one_chunk(height, shard_id);
        env.process_partial_encoded_chunks();
        for i in 0..env.clients.len() {
            env.process_shards_manager_responses(i);
        }
        env.propagate_chunk_state_witnesses_and_endorsements(false);

        // Produce a block with a chunk on shard s0. On shard s1 the chunk is missing. chunk_mask should be [true, false]
        let mut block = env.client(&block_producer).produce_block(height).unwrap().unwrap();
        {
            let mut chunk_header = shard_chunk.cloned_header();
            *chunk_header.height_included_mut() = height;
            let mut chunk_headers: Vec<_> = block.chunks().iter_raw().cloned().collect();
            chunk_headers[0] = chunk_header;
            let mut_block = Arc::make_mut(&mut block);
            mut_block.set_chunks(chunk_headers.clone());
            let chunks = Chunks::from_chunk_headers(&chunk_headers, mut_block.header().height());
            mut_block.mut_header().set_chunk_headers_root(chunks.compute_chunk_headers_root().0);
            mut_block.mut_header().set_chunk_tx_root(chunks.compute_chunk_tx_root());
            mut_block.mut_header().set_prev_chunk_outgoing_receipts_root(
                chunks.compute_chunk_prev_outgoing_receipts_root(),
            );
            mut_block.mut_header().set_prev_state_root(chunks.compute_state_root());
            mut_block.mut_header().set_chunk_mask(vec![true, false]);
            let mess_with_chunk_mask = height == 4;
            if mess_with_chunk_mask {
                // On height 4 set the chunk_mask to an invalid value.
                mut_block.mut_header().set_chunk_mask(vec![false, true]);
                // The original test made sure that block_producer is different from chunk_producer,
                // so let's make sure that this is still the case using an assert.
                assert_ne!(block_producer, chunk_producer);
            }
            mut_block
                .mut_header()
                .resign(&*env.client(&block_producer).validator_signer.get().unwrap().clone());

            for client in &mut env.clients {
                let res = client
                    .process_block_test_no_produce_chunk(block.clone().into(), Provenance::NONE);
                if !mess_with_chunk_mask {
                    res.unwrap();
                } else {
                    // Processing a block with an invalid chunk_mask should fail.
                    res.unwrap_err();
                }
            }
        }
    }
}

#[test]
fn test_minimum_gas_price() {
    let min_gas_price = 100;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_price_adjustment_rate = Ratio::new(1, 10);
    let mut env = TestEnv::builder_from_genesis(&genesis).build();
    for i in 1..=100 {
        env.produce_block(0, i);
    }
    let block = env.clients[0].chain.get_block_by_height(100).unwrap();
    assert!(block.header().next_gas_price() >= min_gas_price);
}

fn test_gc_with_epoch_length_common(epoch_length: NumBlocks) {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let mut blocks = vec![];
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    blocks.push(genesis_block);
    for i in 1..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    for i in 0..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        println!("height = {}", i);
        if i < epoch_length {
            let block_hash = *blocks[i as usize].hash();
            assert_matches!(
                env.clients[0].chain.get_block(&block_hash).unwrap_err(),
                Error::DBNotFoundErr(missing_block_hash) if missing_block_hash == format!("BLOCK: {}", block_hash)
            );
            assert_matches!(
                env.clients[0].chain.get_block_by_height(i).unwrap_err(),
                Error::DBNotFoundErr(missing_block_hash) if missing_block_hash == format!("BLOCK: {}", block_hash)
            );
            assert!(
                env.clients[0]
                    .chain
                    .mut_chain_store()
                    .get_all_block_hashes_by_height(i as BlockHeight)
                    .unwrap()
                    .is_empty()
            );
        } else {
            assert!(env.clients[0].chain.get_block(blocks[i as usize].hash()).is_ok());
            assert!(env.clients[0].chain.get_block_by_height(i).is_ok());
            assert!(
                !env.clients[0]
                    .chain
                    .mut_chain_store()
                    .get_all_block_hashes_by_height(i as BlockHeight)
                    .unwrap()
                    .is_empty()
            );
        }
    }
    assert_eq!(env.clients[0].chain.chain_store().chunk_tail().unwrap(), epoch_length - 1);
}

#[test]
fn test_gc_with_epoch_length() {
    for i in 3..20 {
        test_gc_with_epoch_length_common(i);
    }
}

/// When an epoch is very long there should not be anything garbage collected unexpectedly
#[test]
fn test_gc_long_epoch() {
    test_gc_with_epoch_length_common(200);
}

/// Test that producing blocks works in archival mode with save_trie_changes enabled.
/// In that case garbage collection should not happen but trie changes should be saved to the store.
#[test]
fn test_archival_save_trie_changes() {
    let epoch_length = 10;

    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.total_supply = 1_000_000_000;
    let mut env = TestEnv::builder(&genesis.config)
        .nightshade_runtimes(&genesis)
        .archive(true)
        .save_trie_changes(true)
        .build();

    env.clients[0].chain.chain_store().store().set_db_kind(DbKind::Archive).unwrap();

    let mut blocks = vec![];
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    blocks.push(genesis_block);
    for i in 1..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    // Go through all of the blocks and verify that the block are stored and that the trie changes were stored too.
    for i in 0..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let client = &env.clients[0];
        let chain = &client.chain;
        let store = chain.chain_store();
        let block = &blocks[i as usize];
        let header = block.header();
        let epoch_id = header.epoch_id();
        let shard_layout = client.epoch_manager.get_shard_layout(epoch_id).unwrap();

        assert!(chain.get_block(block.hash()).is_ok());
        assert!(chain.get_block_by_height(i).is_ok());
        assert!(
            !chain
                .chain_store()
                .get_all_block_hashes_by_height(i as BlockHeight)
                .unwrap()
                .is_empty()
        );

        // The genesis block does not contain trie changes.
        if i == 0 {
            continue;
        }

        // Go through chunks and test that trie changes were correctly saved to the store.
        let chunks = block.chunks();
        let version = shard_layout.version();
        for chunk in chunks.iter() {
            let shard_id = chunk.shard_id();
            let shard_uid = ShardUId::new(version, shard_id);

            let key = get_block_shard_uid(&block.hash(), &shard_uid);
            let trie_changes: Option<TrieChanges> =
                store.store().get_ser(DBCol::TrieChanges, &key).unwrap();

            if let Some(trie_changes) = trie_changes {
                // After BandwidthScheduler there's a state change at every height, even when there are no transactions
                assert!(trie_changes.old_root != trie_changes.new_root);
            }
        }
    }
}

fn test_archival_gc_common(
    storage: NodeStorage,
    epoch_length: u64,
    max_height: BlockHeight,
    max_cold_head_height: BlockHeight,
    legacy: bool,
) {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;

    let hot_store = &storage.get_hot_store();
    let mut env = TestEnv::builder(&genesis.config)
        .stores(vec![hot_store.clone()])
        .nightshade_runtimes(&genesis)
        .archive(true)
        .save_trie_changes(true)
        .build();

    let mut blocks = vec![];
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    blocks.push(genesis_block);

    for i in 1..=max_height {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);

        let header = block.header();
        let epoch_id = header.epoch_id();
        let shard_layout = env.clients[0].epoch_manager.get_shard_layout(epoch_id).unwrap();
        let tracked_shards = shard_layout.shard_uids().collect();
        let is_resharding_boundary =
            env.clients[0].epoch_manager.is_resharding_boundary(header.prev_hash()).unwrap();

        blocks.push(block);

        if i <= max_cold_head_height {
            update_cold_db(
                storage.cold_db().unwrap(),
                hot_store,
                &shard_layout,
                &tracked_shards,
                &i,
                is_resharding_boundary,
                1,
            )
            .unwrap();
            update_cold_head(storage.cold_db().unwrap(), &hot_store, &i).unwrap();
        }
    }

    // All blocks up until max_gc_height, exclusively, should be garbage collected.
    // In the '_current' test this will be max_height - 5 epochs
    // In the '_behind' test this will be the cold head height.
    // In the '_migration' test this will be 0.
    let mut max_gc_height = 0;
    if !legacy {
        max_gc_height = std::cmp::min(
            max_height - epoch_length * DEFAULT_GC_NUM_EPOCHS_TO_KEEP,
            // Nice little way to round down to latest epoch start
            max_cold_head_height / epoch_length * epoch_length,
        );
    };

    for i in 0..=max_height {
        let client = &env.clients[0];
        let chain = &client.chain;
        let block = &blocks[i as usize];

        if i < max_gc_height {
            assert!(chain.get_block(block.hash()).is_err());
            assert!(chain.get_block_by_height(i).is_err());
        } else {
            assert!(chain.get_block(block.hash()).is_ok());
            assert!(chain.get_block_by_height(i).is_ok());
            assert!(
                !chain
                    .chain_store()
                    .get_all_block_hashes_by_height(i as BlockHeight)
                    .unwrap()
                    .is_empty()
            );
        }
    }
}

/// This test verifies that archival node in split storage mode that is up to
/// date on the hot -> cold block copying is correctly garbage collecting
/// blocks older than 5 epochs.
#[test]
fn test_archival_gc_migration() {
    // Split storage in the middle of migration has hot store kind set to archive.
    let (storage, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Archive);

    let epoch_length = 10;
    let max_height = epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 2);
    let max_cold_head_height = 5;

    test_archival_gc_common(storage, epoch_length, max_height, max_cold_head_height, true);
}

/// This test verifies that archival node in split storage mode that is up to
/// date on the hot -> cold block copying is correctly garbage collecting
/// blocks older than 5 epochs.
#[test]
fn test_archival_gc_split_storage_current() {
    // Fully migrated split storage has each store configured with kind = temperature.
    let (storage, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);

    let epoch_length = 10;
    let max_height = epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 2);
    let max_cold_head_height = max_height - 2 * epoch_length;

    test_archival_gc_common(storage, epoch_length, max_height, max_cold_head_height, false);
}

/// This test verifies that archival node in split storage mode that is behind
/// on the hot -> cold block copying is correctly garbage collecting blocks
/// older than the cold head.
#[test]
fn test_archival_gc_split_storage_behind() {
    // Fully migrated split storage has each store configured with kind = temperature.
    let (storage, ..) = create_test_node_storage_with_cold(DB_VERSION, DbKind::Hot);

    let epoch_length = 10;
    let max_height = epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 2);
    let max_cold_head_height = 5;

    test_archival_gc_common(storage, epoch_length, max_height, max_cold_head_height, false);
}

#[test]
fn test_gc_block_skips() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    genesis.config.epoch_length = 5;
    let mut env = TestEnv::builder_from_genesis(&genesis).build();
    for i in 1..=1000 {
        if i % 2 == 0 {
            env.produce_block(0, i);
        }
    }
    let mut env = TestEnv::builder_from_genesis(&genesis).build();
    for i in 1..=1000 {
        if i % 2 == 1 {
            env.produce_block(0, i);
        }
    }
    // Epoch skips
    let mut env = TestEnv::builder_from_genesis(&genesis).build();
    for i in 1..=1000 {
        if i % 9 == 7 {
            env.produce_block(0, i);
        }
    }
}

#[test]
fn test_gc_chunk_tail() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let epoch_length = 100;
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder_from_genesis(&genesis).build();
    let mut chunk_tail = 0;
    for i in (1..10).chain(101..epoch_length * 6) {
        env.produce_block(0, i);
        let cur_chunk_tail = env.clients[0].chain.chain_store().chunk_tail().unwrap();
        assert!(cur_chunk_tail >= chunk_tail);
        chunk_tail = cur_chunk_tail;
    }
}

#[test]
fn test_gc_execution_outcome() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer,
        100,
        genesis_hash,
    );
    let tx_hash = tx.get_hash();

    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    for i in 1..epoch_length {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].chain.get_final_transaction_result(&tx_hash).is_ok());

    for i in epoch_length..=epoch_length * 6 + 1 {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].chain.get_final_transaction_result(&tx_hash).is_err());
}

#[test]
fn slow_test_gc_after_state_sync() {
    let epoch_length = 1024;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env =
        TestEnv::builder(&genesis.config).clients_count(2).nightshade_runtimes(&genesis).build();
    for i in 1..epoch_length * 4 + 2 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block, Provenance::NONE);
    }
    let sync_height = epoch_length * 4 + 1;
    let sync_block = env.clients[0].chain.get_block_by_height(sync_height).unwrap();
    let sync_hash = *sync_block.hash();
    let prev_block_hash = *sync_block.header().prev_hash();
    // reset cache
    for i in epoch_length * 3 - 1..sync_height - 1 {
        let block_hash = *env.clients[0].chain.get_block_by_height(i).unwrap().hash();
        assert!(env.clients[1].chain.epoch_manager.get_epoch_start_height(&block_hash).is_ok());
    }
    env.clients[1].chain.reset_data_pre_state_sync(sync_hash).unwrap();
    assert_eq!(env.clients[1].runtime_adapter.get_gc_stop_height(&sync_hash), 0);
    // mimic what we do in possible_targets
    assert!(env.clients[1].epoch_manager.get_epoch_id_from_prev_block(&prev_block_hash).is_ok());
    env.clients[1].chain.clear_data(&Default::default()).unwrap();
}

#[test]
fn slow_test_process_block_after_state_sync() {
    let epoch_length = 1024;
    // test with shard_version > 0
    let mut genesis = Genesis::test_sharded_new_version(
        vec!["test0".parse().unwrap(), "test1".parse().unwrap()],
        1,
        vec![1],
    );
    genesis.config.epoch_length = epoch_length;

    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(1)
        .use_state_snapshots()
        .real_stores()
        .nightshade_runtimes(&genesis)
        .build();

    let mut sync_hash_attempts = 0;
    let mut next_height = 1;
    let sync_hash = loop {
        let block = env.clients[0].produce_block(next_height).unwrap().unwrap();
        let block_hash = *block.hash();
        let prev_hash = *block.header().prev_hash();
        env.process_block(0, block, Provenance::PRODUCED);
        next_height += 1;

        let epoch_height =
            env.clients[0].epoch_manager.get_epoch_height_from_prev_block(&prev_hash).unwrap();
        if epoch_height < 4 {
            continue;
        }
        let Some(sync_hash) = env.clients[0].chain.get_sync_hash(&block_hash).unwrap() else {
            sync_hash_attempts += 1;
            // Sync hash should be available in 4 blocks
            assert!(sync_hash_attempts <= 5, "sync_hash_attempts: {}", sync_hash_attempts);
            continue;
        };
        // Produce one more block after the sync hash is found so that the snapshot will be created
        if sync_hash != block_hash {
            break sync_hash;
        }
    };
    let sync_block = env.clients[0].chain.get_block(&sync_hash).unwrap();
    let shard_layout =
        env.clients[0].epoch_manager.get_shard_layout(sync_block.header().epoch_id()).unwrap();
    let shard_id = shard_layout.shard_ids().next().unwrap();

    let header = env.clients[0]
        .chain
        .state_sync_adapter
        .compute_state_response_header(shard_id, sync_hash)
        .unwrap();
    let state_root = header.chunk_prev_state_root();
    let sync_prev_header = env.clients[0].chain.get_previous_header(sync_block.header()).unwrap();
    let sync_prev_prev_hash = sync_prev_header.prev_hash();

    let state_part = env.clients[0]
        .runtime_adapter
        .obtain_state_part(shard_id, &sync_prev_prev_hash, &state_root, PartId::new(0, 1))
        .unwrap();
    // reset cache
    for i in epoch_length * 3 - 1..sync_block.header().height() - 1 {
        let block_hash = *env.clients[0].chain.get_block_by_height(i).unwrap().hash();
        assert!(env.clients[0].chain.epoch_manager.get_epoch_start_height(&block_hash).is_ok());
    }
    env.clients[0].chain.reset_data_pre_state_sync(sync_hash).unwrap();
    let epoch_id = *env.clients[0].chain.get_block_header(&sync_hash).unwrap().epoch_id();
    env.clients[0]
        .runtime_adapter
        .apply_state_part(shard_id, &state_root, PartId::new(0, 1), &state_part, &epoch_id)
        .unwrap();
    let block = env.clients[0].produce_block(next_height).unwrap().unwrap();
    env.clients[0].process_block_test(block.into(), Provenance::PRODUCED).unwrap();
}

#[test]
fn test_gc_fork_tail() {
    let epoch_length = 101;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env =
        TestEnv::builder(&genesis.config).clients_count(2).nightshade_runtimes(&genesis).build();
    let b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    for i in 0..2 {
        env.process_block(i, b1.clone(), Provenance::NONE);
    }
    // create 100 forks
    for i in 2..102 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(1, block, Provenance::NONE);
    }

    let mut second_epoch_start = None;
    for i in 102..epoch_length * DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 5 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        for j in 0..2 {
            env.process_block(j, block.clone(), Provenance::NONE);
        }
        if second_epoch_start.is_none() && block.header().epoch_id() != &EpochId::default() {
            second_epoch_start = Some(i);
        }
    }
    let head = env.clients[1].chain.head().unwrap();
    assert!(
        env.clients[1].runtime_adapter.get_gc_stop_height(&head.last_block_hash) > epoch_length
    );
    let tail = env.clients[1].chain.chain_store().tail().unwrap();
    let fork_tail = env.clients[1].chain.chain_store().fork_tail().unwrap();
    assert!(tail <= fork_tail && fork_tail < second_epoch_start.unwrap());
}

#[test]
fn test_tx_forwarding() {
    let mut genesis = Genesis::test(TestEnvBuilder::make_accounts(50), 50);
    genesis.config.epoch_length = 100;
    let mut env =
        TestEnv::builder_from_genesis(&genesis).clients_count(50).validator_seats(50).build();

    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = env.tx_from_actions(vec![], &signer, signer.get_account_id());

    // client_index is picked so that with epoch_info rng tx wouldn't need to be forwarded to
    // client itself.
    let client_index = 1;
    assert_eq!(
        env.rpc_handlers[client_index].process_tx(tx, false, false),
        ProcessTxResponse::RequestRouted
    );
    assert_eq!(
        env.network_adapters[client_index].requests.read().len(),
        env.clients[client_index].config.tx_routing_height_horizon as usize
    );
}

#[test]
fn test_tx_forwarding_no_double_forwarding() {
    let mut genesis = Genesis::test(TestEnvBuilder::make_accounts(50), 50);
    genesis.config.epoch_length = 100;
    let mut env =
        TestEnv::builder_from_genesis(&genesis).clients_count(50).validator_seats(50).build();

    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = env.tx_from_actions(vec![], &signer, signer.get_account_id());
    // The transaction has already been forwarded, so it won't be forwarded again.
    let is_forwarded = true;
    assert_eq!(
        env.rpc_handlers[0].process_tx(tx, is_forwarded, false),
        ProcessTxResponse::NoResponse
    );
    assert!(env.network_adapters[0].requests.read().is_empty());
}

#[test]
fn test_tx_forward_around_epoch_boundary() {
    let epoch_length = 4;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.num_block_producer_seats = 2;
    genesis.config.num_block_producer_seats_per_shard = vec![2];
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(3)
        .validator_seats(2)
        .nightshade_runtimes(&genesis)
        .build();
    let genesis_hash = *env.clients[0].chain.genesis().hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::stake(
        1,
        "test1".parse().unwrap(),
        &signer,
        TESTING_INIT_STAKE,
        signer.public_key(),
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    for i in 1..epoch_length * 2 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        for j in 0..3 {
            if j != 1 {
                let provenance = if j == 0 { Provenance::PRODUCED } else { Provenance::NONE };
                env.process_block(j, block.clone(), provenance);
            }
        }
    }
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        1,
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[2].process_tx(tx, false, false), ProcessTxResponse::RequestRouted);
    let mut accounts_to_forward = HashSet::new();
    for request in env.network_adapters[2].requests.read().iter() {
        if let PeerManagerMessageRequest::NetworkRequests(NetworkRequests::ForwardTx(
            account_id,
            _,
        )) = request
        {
            accounts_to_forward.insert(account_id.clone());
        }
    }
    assert_eq!(
        accounts_to_forward,
        HashSet::from_iter(vec!["test0".parse().unwrap(), "test1".parse().unwrap()])
    );
}

/// Blocks that have already been gc'ed should not be accepted again.
#[test]
fn test_not_resync_old_blocks() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let mut blocks = vec![];
    for i in 1..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    for i in 2..epoch_length {
        let block = blocks[i as usize - 1].clone();
        assert!(env.clients[0].chain.get_block(block.hash()).is_err());
        let res = env.clients[0].process_block_test(block.into(), Provenance::NONE);
        assert_matches!(res, Err(x) if matches!(x, Error::Orphan));
        assert_eq!(env.clients[0].chain.orphans_len(), 0);
    }
}

#[test]
fn test_reject_block_headers_during_epoch_sync() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 2;
    genesis.config.epoch_length = epoch_length;
    let mut env =
        TestEnv::builder(&genesis.config).clients_count(2).nightshade_runtimes(&genesis).build();

    let mut blocks = vec![];
    for i in 1..=epoch_length + 1 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }

    let sync_client = &mut env.clients[1];
    let status = &mut sync_client.sync_handler.sync_status;
    let chain = &sync_client.chain;
    let highest_height = sync_client.config.epoch_sync.epoch_sync_horizon + 1;
    let highest_height_peers = vec![HighestHeightPeerInfo {
        archival: false,
        genesis_id: GenesisId::default(),
        highest_block_hash: *blocks.last().unwrap().hash(),
        highest_block_height: blocks.len() as u64,
        tracked_shards: vec![],
        peer_info: PeerInfo::random(),
    }];

    // Running epoch sync, sets SyncStatus::EpochSync
    assert_matches!(
        sync_client.sync_handler.epoch_sync.run(
            status,
            chain,
            highest_height,
            &highest_height_peers
        ),
        Ok(()),
        "Epoch sync failure"
    );

    let headers = blocks.iter().map(|b| b.header().clone().into()).collect::<Vec<_>>();
    // actual attempt to sync headers during ongoing epoch sync
    assert_matches!(
        sync_client.sync_block_headers(headers),
        Err(_),
        "Block headers accepted during epoch sync"
    );
}

#[test]
fn test_gc_tail_update() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let epoch_length = 2;
    genesis.config.epoch_length = epoch_length;
    let mut env =
        TestEnv::builder(&genesis.config).clients_count(2).nightshade_runtimes(&genesis).build();
    let mut blocks = vec![];
    for i in 1..=epoch_length * (DEFAULT_GC_NUM_EPOCHS_TO_KEEP + 1) {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        blocks.push(block);
    }
    let headers = blocks.iter().map(|b| b.header().clone().into()).collect::<Vec<_>>();
    env.clients[1].sync_block_headers(headers).unwrap();
    // simulate save sync hash block
    let prev_prev_sync_block = blocks[blocks.len() - 4].clone();
    let prev_prev_sync_hash = *prev_prev_sync_block.hash();
    let prev_sync_block = blocks[blocks.len() - 3].clone();
    let prev_sync_hash = *prev_sync_block.hash();
    let prev_sync_height = prev_sync_block.header().height();
    let sync_block = blocks[blocks.len() - 2].clone();
    env.clients[1].chain.reset_data_pre_state_sync(*sync_block.hash()).unwrap();
    env.clients[1].chain.save_block(prev_prev_sync_block.into()).unwrap();
    env.clients[1].chain.save_block(prev_sync_block.into()).unwrap();
    let mut store_update = env.clients[1].chain.mut_chain_store().store_update();
    store_update.inc_block_refcount(&prev_prev_sync_hash).unwrap();
    store_update.inc_block_refcount(&prev_sync_hash).unwrap();
    store_update.save_block(sync_block.clone());
    store_update.commit().unwrap();
    env.clients[1]
        .chain
        .reset_heads_post_state_sync(
            *sync_block.hash(),
            &mut BlockProcessingArtifact::default(),
            None,
        )
        .unwrap();
    env.process_block(1, blocks.pop().unwrap(), Provenance::NONE);
    assert_eq!(env.clients[1].chain.chain_store().tail().unwrap(), prev_sync_height);
}

/// Test that transaction does not become invalid when there is some gas price change.
#[test]
fn test_gas_price_change() {
    init_test_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let target_num_tokens_left = NEAR_BASE / 10 + 1;
    let transaction_costs = RuntimeConfig::test().fees;

    let send_money_total_gas = transaction_costs.fee(ActionCosts::transfer).send_fee(false)
        + transaction_costs.fee(ActionCosts::new_action_receipt).send_fee(false)
        + transaction_costs.fee(ActionCosts::transfer).exec_fee()
        + transaction_costs.fee(ActionCosts::new_action_receipt).exec_fee();
    let min_gas_price = target_num_tokens_left / send_money_total_gas as u128;
    let gas_limit = 1000000000000;
    let gas_price_adjustment_rate = Ratio::new(1, 10);

    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = gas_limit;
    genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        TESTING_INIT_BALANCE
            - target_num_tokens_left
            - send_money_total_gas as u128 * min_gas_price,
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    env.produce_block(0, 1);
    let tx = SignedTransaction::send_money(
        2,
        "test1".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        1,
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    for i in 2..=4 {
        env.produce_block(0, i);
    }
}

#[test]
fn test_gas_price_overflow() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let min_gas_price = 1000000;
    let max_gas_price = 10_u128.pow(20);
    let gas_limit = 450000000000;
    let gas_price_adjustment_rate = Ratio::from_integer(1);
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = gas_limit;
    genesis.config.gas_price_adjustment_rate = gas_price_adjustment_rate;
    genesis.config.transaction_validity_period = 100000;
    genesis.config.epoch_length = 43200;
    genesis.config.max_gas_price = max_gas_price;

    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let signer = InMemorySigner::test_signer(&"test1".parse().unwrap());
    for i in 1..100 {
        let tx = SignedTransaction::send_money(
            i,
            "test1".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            1,
            genesis_hash,
        );
        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        assert!(block.header().next_gas_price() <= max_gas_price);
        env.process_block(0, block, Provenance::PRODUCED);
    }
}

#[test]
fn test_invalid_block_root() {
    let mut env = TestEnv::default_builder().build();
    let mut b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    let signer = create_test_signer("test0");
    Arc::make_mut(&mut b1).mut_header().set_block_merkle_root(CryptoHash::default());
    Arc::make_mut(&mut b1).mut_header().resign(&signer);
    let res = env.clients[0].process_block_test(b1.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::InvalidBlockMerkleRoot);
}

#[test]
fn test_incorrect_validator_key_produce_block() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 2);
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(1)
        .nightshade_runtimes(&genesis)
        .track_all_shards()
        .build();

    let res = env.clients[0].produce_block(1);
    assert_matches!(res, Ok(None));
}

fn test_block_merkle_proof_with_len(n: NumBlocks, rng: &mut StdRng) {
    let mut env = TestEnv::default_builder().build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let mut blocks = vec![genesis_block.clone()];
    let mut cur_height = genesis_block.header().height() + 1;
    while cur_height < n {
        let should_fork = rng.gen_bool(0.5);
        if should_fork {
            let block = env.clients[0].produce_block(cur_height).unwrap().unwrap();
            let fork_block = env.clients[0].produce_block(cur_height + 1).unwrap().unwrap();
            env.process_block(0, block.clone(), Provenance::PRODUCED);
            let next_block = env.clients[0].produce_block(cur_height + 2).unwrap().unwrap();
            assert_eq!(next_block.header().prev_hash(), block.hash());
            // simulate blocks arriving in random order
            if rng.gen_bool(0.5) {
                env.process_block(0, fork_block, Provenance::PRODUCED);
                env.process_block(0, next_block.clone(), Provenance::PRODUCED);
            } else {
                env.process_block(0, next_block.clone(), Provenance::PRODUCED);
                env.process_block(0, fork_block, Provenance::PRODUCED);
            }
            blocks.push(block);
            blocks.push(next_block);
            cur_height += 3;
        } else {
            let block = env.clients[0].produce_block(cur_height).unwrap().unwrap();
            blocks.push(block.clone());
            env.process_block(0, block, Provenance::PRODUCED);
            cur_height += 1;
        }
    }

    let head = blocks.pop().unwrap();
    let root = head.header().block_merkle_root();
    // verify that the mapping from block ordinal to block hash is correct
    for h in 0..head.header().height() {
        if let Ok(block) = env.clients[0].chain.get_block_by_height(h) {
            let block_hash = *block.hash();
            let block_ordinal = env.clients[0]
                .chain
                .mut_chain_store()
                .get_block_merkle_tree(&block_hash)
                .unwrap()
                .size();
            let block_hash1 = env.clients[0]
                .chain
                .mut_chain_store()
                .get_block_hash_from_ordinal(block_ordinal)
                .unwrap();
            assert_eq!(block_hash, block_hash1);
        }
    }
    for block in blocks {
        let proof = env.clients[0]
            .chain
            .compute_past_block_proof_in_merkle_tree_of_later_block(block.hash(), head.hash())
            .unwrap();
        assert!(verify_hash(*root, &proof, *block.hash()));
    }
}

#[test]
fn test_block_merkle_proof() {
    let mut rng = StdRng::seed_from_u64(0);
    for i in 0..50 {
        test_block_merkle_proof_with_len(i, &mut rng);
    }
}

#[test]
fn test_block_merkle_proof_same_hash() {
    let env = TestEnv::default_builder().build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let proof = env.clients[0]
        .chain
        .compute_past_block_proof_in_merkle_tree_of_later_block(
            genesis_block.hash(),
            genesis_block.hash(),
        )
        .unwrap();
    assert!(proof.is_empty());
}

#[test]
fn test_data_reset_before_state_sync() {
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let epoch_length = 5;
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();
    let tx = SignedTransaction::create_account(
        1,
        "test0".parse().unwrap(),
        "test_account".parse().unwrap(),
        NEAR_BASE,
        signer.public_key(),
        &signer,
        genesis_hash,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    for i in 1..5 {
        env.produce_block(0, i);
    }
    // check that the new account exists
    let head = env.clients[0].chain.head().unwrap();
    let head_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let response = env.clients[0]
        .runtime_adapter
        .query(
            ShardUId::single_shard(),
            &head_block.chunks()[0].prev_state_root(),
            head.height,
            0,
            &head.prev_block_hash,
            &head.last_block_hash,
            head_block.header().epoch_id(),
            &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
        )
        .unwrap();
    assert_matches!(response.kind, QueryResponseKind::ViewAccount(_));
    env.clients[0].chain.reset_data_pre_state_sync(*head_block.hash()).unwrap();
    // account should not exist after clearing state
    let response = env.clients[0].runtime_adapter.query(
        ShardUId::single_shard(),
        &head_block.chunks()[0].prev_state_root(),
        head.height,
        0,
        &head.prev_block_hash,
        &head.last_block_hash,
        head_block.header().epoch_id(),
        &QueryRequest::ViewAccount { account_id: "test_account".parse().unwrap() },
    );
    // TODO(#3742): ViewClient still has data in cache by current design.
    assert!(response.is_ok());
}

#[test]
fn test_sync_hash_validity() {
    init_test_logger();
    let epoch_length = 8;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let mut height = 1;

    for _num_epochs in 0..4 {
        let epoch_start = height;
        for _ in 0..epoch_length {
            env.produce_block(0, height);
            height += 1;
        }

        // This assumes that all shards have new chunks in every block, which should be true
        // with TestEnv::produce_block()
        let expected_sync_height = epoch_start + 3;

        for h in epoch_start..height {
            let header = env.clients[0].chain.get_block_header_by_height(h).unwrap();
            let valid = env.clients[0].chain.check_sync_hash_validity(header.hash()).unwrap();

            println!("height {} -> {}", header.height(), valid);
            assert_eq!(valid, header.height() == expected_sync_height);
        }
    }

    let bad_hash = CryptoHash::from_str("7tkzFg8RHBmMw1ncRJZCCZAizgq4rwCftTKYLce8RU8t").unwrap();
    let res = env.clients[0].chain.check_sync_hash_validity(&bad_hash);
    println!("bad hash -> {:?}", res.is_ok());
    match res {
        Ok(_) => assert!(false),
        Err(e) => match e {
            Error::DBNotFoundErr(_) => { /* the only expected error */ }
            _ => assert!(false),
        },
    }
}

#[test]
fn test_block_height_processed_orphan() {
    let mut env = TestEnv::default_builder().build();
    let block = env.clients[0].produce_block(1).unwrap().unwrap();
    let mut orphan_block = block;
    let validator_signer = create_test_signer("test0");
    Arc::make_mut(&mut orphan_block).mut_header().set_prev_hash(hash(&[1]));
    Arc::make_mut(&mut orphan_block).mut_header().resign(&validator_signer);
    let block_height = orphan_block.header().height();
    let res = env.clients[0].process_block_test(orphan_block.into(), Provenance::NONE);
    assert_matches!(res.unwrap_err(), Error::Orphan);
    assert!(env.clients[0].chain.mut_chain_store().is_height_processed(block_height).unwrap());
}

#[test]
fn test_validate_chunk_extra() {
    let mut capture = near_o11y::testonly::TracingCapture::enable();

    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = 0;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = SignedTransaction::from_actions(
        1,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::DeployContract(DeployContractAction {
            code: near_test_contracts::rs_contract().to_vec(),
        })],
        *genesis_block.hash(),
        0,
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    let mut last_block = genesis_block;
    for i in 1..3 {
        last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    }

    // Construct a chunk that such when the receipts generated by this chunk are included
    // in blocks of different heights, the state transitions are different.
    let function_call_tx = SignedTransaction::from_actions(
        2,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "write_block_height".to_string(),
            args: vec![],
            gas: 100000000000000,
            deposit: 0,
        }))],
        *last_block.hash(),
        0,
    );
    assert_eq!(
        env.rpc_handlers[0].process_tx(function_call_tx, false, false),
        ProcessTxResponse::ValidTx
    );
    for i in 3..5 {
        last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        if i == 3 {
            env.process_block(0, last_block.clone(), Provenance::PRODUCED);
        } else {
            let _ = env.clients[0]
                .process_block_test_no_produce_chunk(last_block.clone().into(), Provenance::NONE)
                .unwrap();
        }
    }

    // Construct two blocks that contain the same chunk and make the chunk unavailable.
    let validator_signer = create_test_signer("test0");
    let next_height = last_block.header().height() + 1;
    let ProduceChunkResult { chunk, encoded_chunk_parts_paths: merkle_paths, receipts, .. } =
        create_chunk_on_height(&mut env.clients[0], next_height);
    let encoded_chunk = chunk.into_parts().1;
    let mut block1 = env.clients[0].produce_block(next_height).unwrap().unwrap();
    let mut block2 = env.clients[0].produce_block(next_height + 1).unwrap().unwrap();

    // Process two blocks on two different forks that contain the same chunk.
    for (i, block) in vec![&mut block1, &mut block2].into_iter().enumerate() {
        let mut chunk_header = encoded_chunk.cloned_header();
        *chunk_header.height_included_mut() = i as BlockHeight + next_height;
        let chunk_headers = vec![chunk_header.clone()];
        let mut_block = Arc::make_mut(block);
        mut_block.set_chunks(chunk_headers.clone());
        let chunks = Chunks::from_chunk_headers(&chunk_headers, mut_block.header().height());
        mut_block.mut_header().set_chunk_headers_root(chunks.compute_chunk_headers_root().0);
        mut_block.mut_header().set_chunk_tx_root(chunks.compute_chunk_tx_root());
        mut_block.mut_header().set_prev_chunk_outgoing_receipts_root(
            chunks.compute_chunk_prev_outgoing_receipts_root(),
        );
        mut_block.mut_header().set_prev_state_root(chunks.compute_state_root());
        mut_block.mut_header().set_chunk_mask(vec![true]);
        mut_block
            .mut_header()
            .set_chunk_endorsements(ChunkEndorsementsBitmap::from_endorsements(vec![vec![true]]));
        mut_block.mut_header().set_prev_outcome_root(chunks.compute_outcome_root());
        let endorsement =
            ChunkEndorsement::new(EpochId::default(), &chunk_header, &validator_signer);
        mut_block.set_chunk_endorsements(vec![vec![Some(Box::new(endorsement.signature()))]]);
        let body_hash = mut_block.compute_block_body_hash().unwrap();
        mut_block.mut_header().set_block_body_hash(body_hash);
        mut_block.mut_header().resign(&validator_signer);
        let res = env.clients[0].process_block_test(block.clone().into(), Provenance::NONE);
        assert_matches!(res.unwrap_err(), near_chain::Error::ChunksMissing(_));
    }

    // Process the previously unavailable chunk. This causes two blocks to be
    // accepted. Technically, the blocks are accepted concurrently, so we can
    // observe either `block1 -> block2` reorg or `block2, block1` fork. We want
    // to try to produce chunks on top of block1, so we force the reorg case
    // using `capture`

    env.pause_block_processing(&mut capture, block2.hash());
    let mut chain_store = ChainStore::new(
        env.clients[0].chain.chain_store().store(),
        true,
        genesis.config.transaction_validity_period,
    );
    let chunk_header = encoded_chunk.cloned_header();
    let signer = env.clients[0].validator_signer.get();
    let validator_id = signer.as_ref().unwrap().validator_id().clone();
    let chunk = ShardChunkWithEncoding::from_encoded_shard_chunk(encoded_chunk).unwrap();
    env.clients[0]
        .persist_and_distribute_encoded_chunk(chunk, merkle_paths, receipts, validator_id)
        .unwrap();
    env.clients[0].chain.blocks_with_missing_chunks.accept_chunk(&chunk_header.chunk_hash());
    env.clients[0].process_blocks_with_missing_chunks(None);
    let accepted_blocks = env.clients[0].finish_block_in_processing(block1.hash());
    assert_eq!(accepted_blocks.len(), 1);
    env.resume_block_processing(block2.hash());
    let accepted_blocks = env.clients[0].finish_block_in_processing(block2.hash());
    env.propagate_chunk_state_witnesses_and_endorsements(false);
    assert_eq!(accepted_blocks.len(), 1);

    // Produce a block on top of block1.
    // Validate that result of chunk execution in `block1` is legit.
    let client = &mut env.clients[0];
    client
        .chunk_inclusion_tracker
        .prepare_chunk_headers_ready_for_inclusion(block1.hash(), &client.chunk_endorsement_tracker)
        .unwrap();
    let block = client.produce_block_on(next_height + 2, *block1.hash()).unwrap().unwrap();
    let epoch_id = *block.header().epoch_id();
    client.process_block_test(block.into(), Provenance::PRODUCED).unwrap();
    env.propagate_chunk_state_witnesses_and_endorsements(false);
    let client = &mut env.clients[0];
    let shard_layout = client.epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_uid = shard_layout.shard_uids().next().unwrap();
    let shard_id = shard_uid.shard_id();
    let chunks = client
        .chunk_inclusion_tracker
        .get_chunk_headers_ready_for_inclusion(block1.header().epoch_id(), &block1.hash());
    let (chunk_header, _) = client
        .chunk_inclusion_tracker
        .get_chunk_header_and_endorsements(chunks.get(&shard_id).unwrap())
        .unwrap();
    let chunk_extra = client.chain.get_chunk_extra(block1.hash(), &shard_uid).unwrap();
    assert!(
        validate_chunk_with_chunk_extra(
            &mut chain_store,
            client.epoch_manager.as_ref(),
            block1.hash(),
            &chunk_extra,
            block1.chunks()[0].height_included(),
            &chunk_header,
        )
        .is_ok()
    );
}

#[test]
fn slow_test_catchup_gas_price_change() {
    init_test_logger();
    let epoch_length = 8;
    let min_gas_price = 10000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = 1000000000000;

    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(2)
        .use_state_snapshots()
        .real_stores()
        .nightshade_runtimes(&genesis)
        .build();

    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let mut blocks = vec![];
    for i in 1..3 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        env.process_block(1, block, Provenance::NONE);
    }
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    for i in 0..3 {
        let tx = SignedTransaction::send_money(
            i + 1,
            "test0".parse().unwrap(),
            "test1".parse().unwrap(),
            &signer,
            1,
            *genesis_block.hash(),
        );

        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }
    // We go up to the end of the next epoch because we want at least 3 more blocks from the start
    // (plus one more for nodes to create snapshots) if syncing to the current epoch's state
    for i in 3..=epoch_length * 2 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        tracing::error!("process_block:{i}:0");
        env.process_block(1, block, Provenance::NONE);
        tracing::error!("process_block:{i}:1");
    }

    assert_ne!(blocks[3].header().next_gas_price(), blocks[4].header().next_gas_price());
    assert!(
        env.clients[1].chain.get_chunk_extra(blocks[4].hash(), &ShardUId::single_shard()).is_err()
    );

    // Simulate state sync

    let sync_hash =
        env.clients[0].chain.get_sync_hash(blocks.last().unwrap().hash()).unwrap().unwrap();
    let sync_block_idx = blocks
        .iter()
        .position(|b| *b.hash() == sync_hash)
        .expect("block with hash matching sync hash not found");
    if sync_block_idx == 0 {
        panic!("sync block should not be the first block produced");
    }
    let sync_prev_block = &blocks[sync_block_idx - 1];

    assert!(env.clients[0].chain.check_sync_hash_validity(&sync_hash).unwrap());

    let epoch_id = sync_prev_block.header().epoch_id();
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&epoch_id).unwrap();
    let shard_id = shard_layout.shard_uids().next().unwrap().shard_id();

    let state_sync_header = env.clients[0]
        .chain
        .state_sync_adapter
        .get_state_response_header(shard_id, sync_hash)
        .unwrap();
    let num_parts = state_sync_header.num_state_parts();
    let state_sync_parts = (0..num_parts)
        .map(|i| {
            env.clients[0]
                .chain
                .state_sync_adapter
                .get_state_response_part(shard_id, i, sync_hash)
                .unwrap()
        })
        .collect::<Vec<_>>();

    env.clients[1]
        .chain
        .state_sync_adapter
        .set_state_header(shard_id, sync_hash, state_sync_header.clone())
        .unwrap();
    for i in 0..num_parts {
        env.clients[1]
            .chain
            .state_sync_adapter
            .set_state_part(
                shard_id,
                sync_hash,
                PartId::new(i, num_parts),
                &state_sync_parts[i as usize],
            )
            .unwrap();
    }
    {
        let store = env.clients[1].runtime_adapter.store();
        let mut store_update = store.store_update();
        assert!(
            env.clients[1]
                .runtime_adapter
                .get_flat_storage_manager()
                .remove_flat_storage_for_shard(
                    ShardUId::single_shard(),
                    &mut store_update.flat_store_update()
                )
                .unwrap()
        );
        store_update.commit().unwrap();
        for part_id in 0..num_parts {
            let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id)).unwrap();
            let part = store.get(DBCol::StateParts, &key).unwrap().unwrap();
            env.clients[1]
                .runtime_adapter
                .apply_state_part(
                    shard_id,
                    &state_sync_header.chunk_prev_state_root(),
                    PartId::new(part_id, num_parts),
                    &part,
                    blocks[5].header().epoch_id(),
                )
                .unwrap();
        }
    }
    env.clients[1].chain.set_state_finalize(shard_id, sync_hash).unwrap();
    let chunk_extra_after_sync = env.clients[1]
        .chain
        .get_chunk_extra(sync_prev_block.hash(), &ShardUId::single_shard())
        .unwrap();
    let expected_chunk_extra = env.clients[0]
        .chain
        .get_chunk_extra(sync_prev_block.hash(), &ShardUId::single_shard())
        .unwrap();
    // The chunk extra of the prev block of sync block should be the same as the node that it is syncing from
    assert_eq!(chunk_extra_after_sync, expected_chunk_extra);
}

#[test]
fn test_block_execution_outcomes() {
    init_test_logger();

    let epoch_length = 5;
    let min_gas_price = 10000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = 1000000000000;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let mut tx_hashes = vec![];
    for i in 0..3 {
        // send transaction to the same account to generate local receipts
        let tx = SignedTransaction::send_money(
            i + 1,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            1,
            *genesis_block.hash(),
        );
        tx_hashes.push(tx.get_hash());
        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }
    for i in 1..4 {
        env.produce_block(0, i);
    }

    let mut expected_outcome_ids = HashSet::new();
    let mut delayed_receipt_id = vec![];
    // Due to gas limit, the first two transactions will create local receipts and they get executed
    // in the same block. The last local receipt will become delayed receipt
    for (i, id) in tx_hashes.into_iter().enumerate() {
        let execution_outcome = env.clients[0].chain.get_execution_outcome(&id).unwrap();
        assert_eq!(execution_outcome.outcome_with_id.outcome.receipt_ids.len(), 1);
        expected_outcome_ids.insert(id);
        if i < 2 {
            expected_outcome_ids.insert(execution_outcome.outcome_with_id.outcome.receipt_ids[0]);
        } else {
            delayed_receipt_id.push(execution_outcome.outcome_with_id.outcome.receipt_ids[0])
        }
    }
    let block = env.clients[0].chain.get_block_by_height(2).unwrap();
    let chunk = env.clients[0].chain.get_chunk(&block.chunks()[0].chunk_hash()).unwrap();
    let shard_id = chunk.shard_id();
    assert_eq!(chunk.to_transactions().len(), 3);
    let execution_outcomes_from_block = env.clients[0]
        .chain
        .chain_store()
        .get_block_execution_outcomes(block.hash())
        .unwrap()
        .remove(&shard_id)
        .unwrap();
    assert_eq!(execution_outcomes_from_block.len(), 5);
    assert_eq!(
        execution_outcomes_from_block
            .into_iter()
            .map(|execution_outcome| execution_outcome.outcome_with_id.id)
            .collect::<HashSet<_>>(),
        expected_outcome_ids
    );

    // Make sure the chunk outcomes contain the outcome from the delayed receipt.
    let next_block = env.clients[0].chain.get_block_by_height(3).unwrap();
    let next_chunk = env.clients[0].chain.get_chunk(&next_block.chunks()[0].chunk_hash()).unwrap();
    let shard_id = next_chunk.shard_id();
    assert!(next_chunk.to_transactions().is_empty());
    assert!(next_chunk.prev_outgoing_receipts().is_empty());
    let execution_outcomes_from_block = env.clients[0]
        .chain
        .chain_store()
        .get_block_execution_outcomes(next_block.hash())
        .unwrap()
        .remove(&shard_id)
        .unwrap();
    assert_eq!(execution_outcomes_from_block.len(), 1);
    assert!(execution_outcomes_from_block[0].outcome_with_id.id == delayed_receipt_id[0]);
}

#[test]
fn test_save_tx_outcomes_false() {
    init_test_logger();

    let epoch_length = 5;
    let min_gas_price = 10000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = min_gas_price;
    genesis.config.gas_limit = 1000000000000;
    let mut env = TestEnv::builder(&genesis.config)
        .save_tx_outcomes(false)
        .nightshade_runtimes(&genesis)
        .build();

    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let mut tx_hashes = vec![];
    for i in 0..3 {
        // send transaction to the same account to generate local receipts
        let tx = SignedTransaction::send_money(
            i + 1,
            "test0".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            1,
            *genesis_block.hash(),
        );
        tx_hashes.push(tx.get_hash());
        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }
    for i in 1..4 {
        env.produce_block(0, i);
    }

    // Check that the execution outcomes are not saved in the chain store.
    for id in tx_hashes {
        let execution_outcome = env.clients[0].chain.get_execution_outcome(&id);
        assert_matches!(execution_outcome, Err(Error::DBNotFoundErr(_)));
    }
}

// This test verifies that gas consumed for processing refund receipts is taken into account
// for the purpose of limiting the size of the chunk.
#[test]
fn test_refund_receipts_processing() {
    init_test_logger();

    let epoch_length = 5;
    let min_gas_price = 10000;
    let mut genesis = Genesis::test_sharded_new_version(
        vec!["test0".parse().unwrap(), "test1".parse().unwrap()],
        1,
        vec![1],
    );
    genesis.config.epoch_length = epoch_length;
    genesis.config.min_gas_price = min_gas_price;
    // Set gas limit to be small enough to produce some delayed receipts, but large enough for
    // transactions to get through.
    genesis.config.gas_limit = 100_000_000;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let mut tx_hashes = vec![];
    // Send transactions to a non-existing account to generate refunds.
    for i in 0..3 {
        // Send transaction from the same account to generate local receipts.
        let tx = SignedTransaction::send_money(
            i + 1,
            "test0".parse().unwrap(),
            "random_account".parse().unwrap(),
            &signer,
            1,
            *genesis_block.hash(),
        );
        tx_hashes.push(tx.get_hash());
        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    }

    // Make sure all transactions are processed.
    for i in 1..16 {
        env.produce_block(0, i);
    }

    let test_shard_uid = ShardUId { version: 1, shard_id: 0 };
    let test_shard_id = test_shard_uid.shard_id();
    for tx_hash in tx_hashes {
        let tx_outcome = env.clients[0].chain.get_execution_outcome(&tx_hash).unwrap();
        assert_eq!(tx_outcome.outcome_with_id.outcome.receipt_ids.len(), 1);
        if let ExecutionStatus::SuccessReceiptId(id) = tx_outcome.outcome_with_id.outcome.status {
            let receipt_outcome = env.clients[0].chain.get_execution_outcome(&id).unwrap();
            assert_matches!(
                receipt_outcome.outcome_with_id.outcome.status,
                ExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
                    kind: ActionErrorKind::AccountDoesNotExist { .. },
                    ..
                }))
            );
            let execution_outcomes_from_block = env.clients[0]
                .chain
                .chain_store()
                .get_block_execution_outcomes(&receipt_outcome.block_hash)
                .unwrap()
                .remove(&test_shard_id)
                .unwrap();
            assert_eq!(execution_outcomes_from_block.len(), 1);
            let chunk_extra = env.clients[0]
                .chain
                .get_chunk_extra(&receipt_outcome.block_hash, &test_shard_uid)
                .unwrap()
                .clone();
            assert!(chunk_extra.gas_used() >= chunk_extra.gas_limit());
        } else {
            unreachable!("Transaction must succeed");
        }
    }
}

#[test]
fn test_execution_metadata() {
    // Prepare TestEnv with a very simple WASM contract.
    let wasm_code = wat::parse_str(
        r#"
(module
    (import "env" "block_index" (func $block_index (result i64)))
    (func (export "main")
        (call $block_index)
        drop
    )
)"#,
    )
    .unwrap();

    let mut env = {
        let epoch_length = 5;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = epoch_length;
        let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

        deploy_test_contract(&mut env, "test0".parse().unwrap(), &wasm_code, epoch_length, 1);
        env
    };

    // Call the contract and get the execution outcome.
    let execution_outcome = env.call_main(&"test0".parse().unwrap());

    // Now, let's assert that we get the cost breakdown we expect.
    let config = RuntimeConfigStore::test().get_config(PROTOCOL_VERSION).clone();

    // Total costs for creating a function call receipt.
    let expected_receipt_cost = config.fees.fee(ActionCosts::new_action_receipt).execution
        + config.fees.fee(ActionCosts::function_call_base).exec_fee()
        + config.fees.fee(ActionCosts::function_call_byte).exec_fee() * "main".len() as u64;

    // We spend two wasm instructions (call & drop), plus 8 ops for initializing function
    // operand stack (8 bytes worth to hold the return value.)
    let expected_wasm_ops = 10;

    // Profile for what's happening *inside* wasm vm during function call.
    let expected_profile = serde_json::json!([
      // Inside the contract, we called one host function.
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "BASE",
        "gas_used": config.wasm_config.ext_costs.gas_cost(ExtCosts::base).to_string()
      },
      // We include compilation costs into running the function.
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "CONTRACT_LOADING_BASE",
        "gas_used": config.wasm_config.ext_costs.gas_cost(ExtCosts::contract_loading_base).to_string()
      },
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "CONTRACT_LOADING_BYTES",
        "gas_used": "92590075"
      },
      {
        "cost_category": "WASM_HOST_COST",
        "cost": "WASM_INSTRUCTION",
        "gas_used": (config.wasm_config.regular_op_cost as u64 * expected_wasm_ops).to_string()
      }
    ]);
    let outcome = &execution_outcome.receipts_outcome[0].outcome;
    let metadata = &outcome.metadata;

    let actual_profile = serde_json::to_value(&metadata.gas_profile).unwrap();
    assert_eq!(expected_profile, actual_profile);

    let actual_receipt_cost = outcome.gas_burnt
        - metadata
            .gas_profile
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|it| it.gas_used)
            .sum::<u64>();

    assert_eq!(expected_receipt_cost, actual_receipt_cost)
}

#[test]
fn test_epoch_protocol_version_change() {
    init_test_logger();
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 2);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = PROTOCOL_VERSION - 1;
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(2)
        .validator_seats(2)
        .nightshade_runtimes(&genesis)
        .build();
    for i in 1..=16 {
        let head = env.clients[0].chain.head().unwrap();
        let epoch_id = env.clients[0]
            .epoch_manager
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .unwrap();

        produce_chunks(&mut env, &epoch_id, i);

        let epoch_id = env.clients[0]
            .epoch_manager
            .get_epoch_id_from_prev_block(&head.last_block_hash)
            .unwrap();
        let block_producer = env.clients[0].epoch_manager.get_block_producer(&epoch_id, i).unwrap();
        let index = if block_producer == "test0" { 0 } else { 1 };
        let mut block = env.clients[index].produce_block(i).unwrap().unwrap();
        // upgrade to new protocol version but in the second epoch one node vote for the old version.
        if i != 10 {
            set_block_protocol_version(&mut block, block_producer.clone(), PROTOCOL_VERSION);
        }
        for j in 0..2 {
            env.clients[j].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
        }
    }
    let last_block = env.clients[0].chain.get_block_by_height(16).unwrap();
    let protocol_version = env.clients[0]
        .epoch_manager
        .get_epoch_protocol_version(last_block.header().epoch_id())
        .unwrap();
    assert_eq!(protocol_version, PROTOCOL_VERSION);
}

#[test]
fn test_epoch_multi_protocol_version_change() {
    init_test_logger();

    let v0 = PROTOCOL_VERSION - 2;
    let v1 = PROTOCOL_VERSION - 1;
    let v2 = PROTOCOL_VERSION;

    // produce blocks roughly every 100ms
    // one epoch is roughly 500ms
    // at least two epochs are needed for one protocol upgrade
    // schedule the first protocol upgrade voting at now +1s
    // schedule the second protocol upgrade voting at now +3s

    let start_time = chrono::Utc::now();
    let end_time = start_time + chrono::Duration::seconds(6);

    let v1_upgrade_time = start_time + chrono::Duration::seconds(1);
    let v2_upgrade_time = start_time + chrono::Duration::seconds(3);

    let v1_upgrade_time = v1_upgrade_time.format("%Y-%m-%d %H:%M:%S").to_string();
    let v2_upgrade_time = v2_upgrade_time.format("%Y-%m-%d %H:%M:%S").to_string();

    let protocol_version_override =
        format!("{}={},{}={}", v1_upgrade_time, v1, v2_upgrade_time, v2);

    tracing::debug!(target: "test", ?protocol_version_override, "setting the protocol_version_override");
    unsafe { std::env::set_var("NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE", protocol_version_override) };

    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 2);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = v0;
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(2)
        .validator_seats(2)
        .nightshade_runtimes(&genesis)
        .build();

    let mut seen_v0 = false;
    let mut seen_v1 = false;
    let mut seen_v2 = false;

    let mut height = 1;
    while chrono::Utc::now() < end_time && (!seen_v0 || !seen_v1 || !seen_v2) {
        let (epoch_id, protocol_version) = get_epoch_id_and_protocol_version(&env);

        produce_chunks(&mut env, &epoch_id, height);

        produce_block(&mut env, &epoch_id, height);

        if protocol_version == v0 {
            seen_v0 = true;
        }
        if protocol_version == v1 {
            seen_v1 = true;
        }
        if protocol_version == v2 {
            seen_v2 = true;
        }

        tracing::debug!(target: "test", ?height, ?protocol_version, "loop iter finished");

        height += 1;
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    assert!(seen_v0);
    assert!(seen_v1);
    assert!(seen_v2);
}

#[test]
fn slow_test_epoch_multi_protocol_version_change_epoch_overlap() {
    init_test_logger();

    let v0 = PROTOCOL_VERSION - 2;
    let v1 = PROTOCOL_VERSION - 1;
    let v2 = PROTOCOL_VERSION;

    // produce blocks roughly every 500ms
    // one epoch is roughly 2500ms
    // at least two epochs are needed for one protocol upgrade
    // schedule the first protocol upgrade voting at now +1s
    // arrange the second protocol upgrade voting so that it falls
    // on the same epoch as the first (now +1s)
    // assert two protocol upgrades should never happen on the same epoch

    let start_time = chrono::Utc::now();
    let end_time = start_time + chrono::Duration::seconds(25);

    let v1_upgrade_time = start_time + chrono::Duration::seconds(1);
    let v2_upgrade_time = start_time + chrono::Duration::seconds(2);

    let v1_upgrade_time = v1_upgrade_time.format("%Y-%m-%d %H:%M:%S").to_string();
    let v2_upgrade_time = v2_upgrade_time.format("%Y-%m-%d %H:%M:%S").to_string();

    let protocol_version_override =
        format!("{}={},{}={}", v1_upgrade_time, v1, v2_upgrade_time, v2);

    tracing::debug!(target: "test", ?protocol_version_override, "setting the protocol_version_override");
    unsafe { std::env::set_var("NEAR_TESTS_PROTOCOL_UPGRADE_OVERRIDE", protocol_version_override) };

    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 2);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = v0;
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(2)
        .validator_seats(2)
        .nightshade_runtimes(&genesis)
        .build();

    let mut seen_v0 = false;
    let mut seen_v1 = false;
    let mut seen_v2 = false;

    let mut height = 1;
    let (mut current_epoch_id, mut current_protocol_version) =
        get_epoch_id_and_protocol_version(&env);
    while chrono::Utc::now() < end_time && (!seen_v0 || !seen_v1 || !seen_v2) {
        let (epoch_id, protocol_version) = get_epoch_id_and_protocol_version(&env);

        produce_chunks(&mut env, &epoch_id, height);

        produce_block(&mut env, &epoch_id, height);

        if protocol_version == v0 {
            seen_v0 = true;
        }
        if protocol_version == v1 {
            seen_v1 = true;
        }
        if protocol_version == v2 {
            seen_v2 = true;
        }

        assert!(
            protocol_version - current_protocol_version <= 1,
            "protocol version should never increase twice in one iteration ({current_protocol_version} -> {protocol_version})"
        );
        if epoch_id == current_epoch_id {
            assert_eq!(
                current_protocol_version, protocol_version,
                "protocol version shouldn't change during the same epoch"
            );
        }

        tracing::debug!(target: "test", ?height, ?protocol_version, "loop iter finished");

        height += 1;
        current_epoch_id = epoch_id;
        current_protocol_version = protocol_version;
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    assert!(seen_v0);
    assert!(seen_v1);
    assert!(seen_v2);
}

// helper for test_epoch_multi_protocol_version_change* class of tests
fn produce_block(env: &mut TestEnv, epoch_id: &EpochId, height: u64) {
    let block_producer = env.clients[0].epoch_manager.get_block_producer(epoch_id, height).unwrap();
    let index = if block_producer == "test0" { 0 } else { 1 };
    let block = env.clients[index].produce_block(height).unwrap().unwrap();
    for client in &mut env.clients {
        client.process_block_test(block.clone().into(), Provenance::NONE).unwrap();
    }
}

// helper for test_epoch_multi_protocol_version_change* class of tests
fn produce_chunks(env: &mut TestEnv, epoch_id: &EpochId, height: u64) {
    let shard_layout = env.clients[0].epoch_manager.get_shard_layout(epoch_id).unwrap();

    for shard_id in shard_layout.shard_ids() {
        let chunk_producer = env.clients[0]
            .epoch_manager
            .get_chunk_producer_info(&ChunkProductionKey {
                epoch_id: *epoch_id,
                height_created: height,
                shard_id,
            })
            .unwrap()
            .take_account_id();

        let produce_chunk_result = create_chunk_on_height(env.client(&chunk_producer), height);
        let ProduceChunkResult { chunk, encoded_chunk_parts_paths, receipts } =
            produce_chunk_result;
        for client in &mut env.clients {
            let validator_id = client.validator_signer.get().unwrap().validator_id().clone();
            client
                .persist_and_distribute_encoded_chunk(
                    chunk.clone(),
                    encoded_chunk_parts_paths.clone(),
                    receipts.clone(),
                    validator_id,
                )
                .unwrap();
        }
    }
}

// helper for test_epoch_multi_protocol_version_change* class of tests
fn get_epoch_id_and_protocol_version(env: &TestEnv) -> (EpochId, u32) {
    let head = env.clients[0].chain.head().unwrap();
    let epoch_id =
        env.clients[0].epoch_manager.get_epoch_id_from_prev_block(&head.last_block_hash).unwrap();
    let protocol_version =
        env.clients[0].epoch_manager.get_epoch_protocol_version(&epoch_id).unwrap();
    (epoch_id, protocol_version)
}

#[test]
fn test_discard_non_finalizable_block() {
    let genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();

    let first_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, first_block.clone(), Provenance::PRODUCED);
    // Produce, but not process test block on top of block (1).
    let non_finalizable_block = env.clients[0].produce_block(6).unwrap().unwrap();
    env.clients[0]
        .chain
        .mut_chain_store()
        .save_latest_known(LatestKnown {
            height: first_block.header().height(),
            seen: first_block.header().raw_timestamp(),
        })
        .unwrap();

    let second_block = env.clients[0].produce_block(2).unwrap().unwrap();
    env.process_block(0, second_block.clone(), Provenance::PRODUCED);
    // Produce, but not process test block on top of block (2).
    let finalizable_block = env.clients[0].produce_block(7).unwrap().unwrap();
    env.clients[0]
        .chain
        .mut_chain_store()
        .save_latest_known(LatestKnown {
            height: second_block.header().height(),
            seen: second_block.header().raw_timestamp(),
        })
        .unwrap();

    // Produce and process two more blocks.
    for i in 3..5 {
        env.produce_block(0, i);
    }

    assert_eq!(env.clients[0].chain.final_head().unwrap().height, 2);
    // Check that the first test block can't be finalized, because it is produced behind final head.
    assert_matches!(
        env.clients[0]
            .process_block_test(non_finalizable_block.into(), Provenance::NONE)
            .unwrap_err(),
        Error::CannotBeFinalized
    );
    // Check that the second test block still can be finalized.
    assert_matches!(
        env.clients[0].process_block_test(finalizable_block.into(), Provenance::NONE),
        Ok(_)
    );
}

/// Final state should be consistent when a node switches between forks in the following scenario
///                      /-----------h+2
/// h-2 ---- h-1 ------ h
///                      \------h+1
/// even though from the perspective of h+2 the last final block is h-2.
#[test]
fn test_query_final_state() {
    let epoch_length = 10;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer,
        100,
        *genesis_block.hash(),
    );
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);

    let mut blocks = vec![];

    for i in 1..5 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
    }

    let query_final_state =
        |chain: &mut near_chain::Chain, runtime: Arc<dyn RuntimeAdapter>, account_id: AccountId| {
            let final_head = chain.chain_store().final_head().unwrap();
            let last_final_block = chain.get_block(&final_head.last_block_hash).unwrap();
            let response = runtime
                .query(
                    ShardUId::single_shard(),
                    &last_final_block.chunks()[0].prev_state_root(),
                    last_final_block.header().height(),
                    last_final_block.header().raw_timestamp(),
                    &final_head.prev_block_hash,
                    last_final_block.hash(),
                    last_final_block.header().epoch_id(),
                    &QueryRequest::ViewAccount { account_id },
                )
                .unwrap();
            match response.kind {
                QueryResponseKind::ViewAccount(account_view) => account_view,
                _ => panic!("Wrong return value"),
            }
        };

    let fork1_block = env.clients[0].produce_block(5).unwrap().unwrap();
    env.clients[0]
        .chain
        .mut_chain_store()
        .save_latest_known(LatestKnown {
            height: blocks.last().unwrap().header().height(),
            seen: blocks.last().unwrap().header().raw_timestamp(),
        })
        .unwrap();
    let fork2_block = env.clients[0].produce_block(6).unwrap().unwrap();
    assert_eq!(fork1_block.header().prev_hash(), fork2_block.header().prev_hash());
    env.process_block(0, fork1_block, Provenance::NONE);
    assert_eq!(env.clients[0].chain.head().unwrap().height, 5);

    let runtime = env.clients[0].runtime_adapter.clone();
    let account_state1 =
        query_final_state(&mut env.clients[0].chain, runtime.clone(), "test0".parse().unwrap());

    env.process_block(0, fork2_block, Provenance::NONE);
    assert_eq!(env.clients[0].chain.head().unwrap().height, 6);

    let runtime = env.clients[0].runtime_adapter.clone();
    let account_state2 =
        query_final_state(&mut env.clients[0].chain, runtime.clone(), "test0".parse().unwrap());

    assert_eq!(account_state1, account_state2);
    assert!(account_state1.amount < TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
}

// Check that if the same receipt is executed twice in forked chain, both outcomes are recorded
// but child receipt ids are different.
#[test]
fn test_fork_receipt_ids() {
    let (mut env, tx_hash) = prepare_env_with_transaction();

    let produced_block = env.clients[0].produce_block(1).unwrap().unwrap();
    env.process_block(0, produced_block.clone(), Provenance::PRODUCED);

    // Construct two blocks that contain the same chunk and make the chunk unavailable.
    let validator_signer = create_test_signer("test0");
    let last_height = produced_block.header().height();
    let ProduceChunkResult { chunk, .. } =
        create_chunk_on_height(&mut env.clients[0], last_height + 1);
    let encoded_chunk = chunk.into_parts().1;
    let mut block1 = env.clients[0].produce_block(last_height + 1).unwrap().unwrap();
    let mut block2 = env.clients[0].produce_block(last_height + 2).unwrap().unwrap();

    // Process two blocks on two different forks that contain the same chunk.
    for block in [&mut block2, &mut block1] {
        let mut chunk_header = encoded_chunk.cloned_header();
        *chunk_header.height_included_mut() = block.header().height();
        let chunk_headers = vec![chunk_header];
        let mut_block = Arc::make_mut(block);
        mut_block.set_chunks(chunk_headers.clone());
        let chunks = Chunks::from_chunk_headers(&chunk_headers, mut_block.header().height());
        mut_block.mut_header().set_chunk_headers_root(chunks.compute_chunk_headers_root().0);
        mut_block.mut_header().set_chunk_tx_root(chunks.compute_chunk_tx_root());
        mut_block.mut_header().set_prev_chunk_outgoing_receipts_root(
            chunks.compute_chunk_prev_outgoing_receipts_root(),
        );
        mut_block.mut_header().set_prev_state_root(chunks.compute_state_root());
        mut_block.mut_header().set_chunk_mask(vec![true]);
        mut_block.mut_header().resign(&validator_signer);
        env.clients[0].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
    }

    // Ensure that in stateless validation protocol receipts in fork blocks are executed.
    let block3 = env.clients[0].produce_block_on(last_height + 3, *block1.hash()).unwrap().unwrap();
    let block4 = env.clients[0].produce_block_on(last_height + 4, *block2.hash()).unwrap().unwrap();
    env.clients[0].process_block_test(block3.into(), Provenance::NONE).unwrap();
    env.clients[0].process_block_test(block4.into(), Provenance::NONE).unwrap();

    let transaction_execution_outcome =
        env.clients[0].chain.mut_chain_store().get_outcomes_by_id(&tx_hash).unwrap();
    assert_eq!(transaction_execution_outcome.len(), 2);
    let receipt_id0 = transaction_execution_outcome[0].outcome_with_id.outcome.receipt_ids[0];
    let receipt_id1 = transaction_execution_outcome[1].outcome_with_id.outcome.receipt_ids[0];
    assert_ne!(receipt_id0, receipt_id1);
}

// Check that in if receipt is executed twice in different forks, two execution
// outcomes are recorded, canonical chain outcome is correct and GC cleanups
// all outcomes.
#[test]
fn test_fork_execution_outcome() {
    init_test_logger();

    let (mut env, tx_hash) = prepare_env_with_transaction();

    let mut last_height = 0;
    for i in 1..3 {
        let last_block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, last_block.clone(), Provenance::PRODUCED);
        last_height = last_block.header().height();
    }

    // Construct two blocks that contain the same chunk and make the chunk unavailable.
    let validator_signer = create_test_signer("test0");
    let next_height = last_height + 1;
    let ProduceChunkResult { chunk, .. } = create_chunk_on_height(&mut env.clients[0], next_height);
    let encoded_chunk = chunk.into_parts().1;
    let mut block1 = env.clients[0].produce_block(last_height + 1).unwrap().unwrap();
    let mut block2 = env.clients[0].produce_block(last_height + 2).unwrap().unwrap();

    // Process two blocks on two different forks that contain the same chunk.
    for block in [&mut block2, &mut block1] {
        let mut chunk_header = encoded_chunk.cloned_header();
        *chunk_header.height_included_mut() = block.header().height();
        let chunk_headers = vec![chunk_header];
        let mut_block = Arc::make_mut(block);
        mut_block.set_chunks(chunk_headers.clone());
        let chunks = Chunks::from_chunk_headers(&chunk_headers, mut_block.header().height());
        mut_block.mut_header().set_chunk_headers_root(chunks.compute_chunk_headers_root().0);
        mut_block.mut_header().set_chunk_tx_root(chunks.compute_chunk_tx_root());
        mut_block.mut_header().set_prev_chunk_outgoing_receipts_root(
            chunks.compute_chunk_prev_outgoing_receipts_root(),
        );
        mut_block.mut_header().set_prev_state_root(chunks.compute_state_root());
        mut_block.mut_header().set_chunk_mask(vec![true]);
        mut_block.mut_header().resign(&validator_signer);
        env.clients[0].process_block_test(block.clone().into(), Provenance::NONE).unwrap();
    }

    let block3 = env.clients[0].produce_block_on(last_height + 3, *block1.hash()).unwrap().unwrap();
    let block4 = env.clients[0].produce_block_on(last_height + 4, *block2.hash()).unwrap().unwrap();
    env.clients[0].process_block_test(block3.into(), Provenance::NONE).unwrap();
    env.clients[0].process_block_test(block4.into(), Provenance::NONE).unwrap();

    let transaction_execution_outcome =
        env.clients[0].chain.mut_chain_store().get_outcomes_by_id(&tx_hash).unwrap();
    assert_eq!(transaction_execution_outcome.len(), 1);
    let receipt_id = transaction_execution_outcome[0].outcome_with_id.outcome.receipt_ids[0];
    let receipt_execution_outcomes =
        env.clients[0].chain.mut_chain_store().get_outcomes_by_id(&receipt_id).unwrap();
    assert_eq!(receipt_execution_outcomes.len(), 2);
    let canonical_chain_outcome = env.clients[0].chain.get_execution_outcome(&receipt_id).unwrap();
    assert_eq!(canonical_chain_outcome.block_hash, *block2.hash());

    // Make sure that GC cleanups execution outcomes.
    let epoch_length = env.clients[0].config.epoch_length;
    for i in last_height + 5..last_height + 5 + epoch_length * 6 {
        env.produce_block(0, i);
    }
    let transaction_execution_outcome =
        env.clients[0].chain.chain_store().get_outcomes_by_id(&tx_hash).unwrap();
    assert!(transaction_execution_outcome.is_empty());
    let receipt_execution_outcomes =
        env.clients[0].chain.chain_store().get_outcomes_by_id(&receipt_id).unwrap();
    assert!(receipt_execution_outcomes.is_empty());
}

fn prepare_env_with_transaction() -> (TestEnv, CryptoHash) {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let tx = SignedTransaction::send_money(
        1,
        "test0".parse().unwrap(),
        "test1".parse().unwrap(),
        &signer,
        100,
        *genesis_block.hash(),
    );
    let tx_hash = tx.get_hash();
    assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
    (env, tx_hash)
}

#[test]
fn test_not_broadcast_block_on_accept() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let network_adapter = Arc::new(MockPeerManagerAdapter::default());
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(2)
        .nightshade_runtimes(&genesis)
        .network_adapters(vec![
            Arc::new(MockPeerManagerAdapter::default()),
            network_adapter.clone(),
        ])
        .build();
    let b1 = env.clients[0].produce_block(1).unwrap().unwrap();
    for i in 0..2 {
        env.process_block(i, b1.clone(), Provenance::NONE);
    }
    assert!(network_adapter.requests.read().is_empty());
}

#[test]
#[should_panic(
    expected = "The client protocol version is older than the protocol version of the network"
)]
fn test_node_shutdown_with_old_protocol_version() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let validator_signer = create_test_signer("test0");
    for i in 1..=5 {
        let mut block = env.clients[0].produce_block(i).unwrap().unwrap();
        Arc::make_mut(&mut block).mut_header().set_latest_protocol_version(PROTOCOL_VERSION + 1);
        Arc::make_mut(&mut block).mut_header().resign(&validator_signer);
        env.process_block(0, block, Provenance::NONE);
    }
    for i in 6..=10 {
        env.produce_block(0, i);
    }
    env.produce_block(0, 11);
}

#[test]
fn test_block_ordinal() {
    // Ensure that GC will not happen.
    let epoch_length = 200;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder_from_genesis(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    assert_eq!(genesis_block.header().block_ordinal(), 1);
    let mut ordinal = 1;

    // Test no skips
    for i in 1..=5 {
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        ordinal += 1;
        assert_eq!(block.header().block_ordinal(), ordinal);
    }

    // Test skips
    for i in 1..=5 {
        let block = env.clients[0].produce_block(i * 10).unwrap().unwrap();
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        ordinal += 1;
        assert_eq!(block.header().block_ordinal(), ordinal);
    }

    // Test forks
    let last_block = env.clients[0].produce_block(99).unwrap().unwrap();
    env.process_block(0, last_block.clone(), Provenance::PRODUCED);
    ordinal += 1;
    assert_eq!(last_block.header().block_ordinal(), ordinal);
    let fork1_block = env.clients[0].produce_block(100).unwrap().unwrap();
    env.clients[0]
        .chain
        .mut_chain_store()
        .save_latest_known(LatestKnown {
            height: last_block.header().height(),
            seen: last_block.header().raw_timestamp(),
        })
        .unwrap();
    let fork2_block = env.clients[0].produce_block(101).unwrap().unwrap();
    assert_eq!(fork1_block.header().prev_hash(), fork2_block.header().prev_hash());
    env.process_block(0, fork1_block.clone(), Provenance::NONE);
    let next_block = env.clients[0].produce_block(102).unwrap().unwrap();
    assert_eq!(next_block.header().prev_hash(), fork1_block.header().hash());
    env.process_block(0, fork2_block.clone(), Provenance::NONE);
    ordinal += 1;
    let fork_ordinal = ordinal - 1;
    assert_eq!(fork1_block.header().block_ordinal(), ordinal);
    assert_eq!(fork2_block.header().block_ordinal(), ordinal);
    assert_eq!(env.clients[0].chain.head().unwrap().height, fork2_block.header().height());
    // Next block on top of fork
    env.process_block(0, next_block.clone(), Provenance::PRODUCED);
    ordinal += 1;
    assert_eq!(env.clients[0].chain.head().unwrap().height, next_block.header().height());
    assert_eq!(next_block.header().block_ordinal(), ordinal);

    // make sure that the old ordinal maps to what is on the canonical chain
    let fork_ordinal_block_hash =
        env.clients[0].chain.mut_chain_store().get_block_hash_from_ordinal(fork_ordinal).unwrap();
    assert_eq!(fork_ordinal_block_hash, *fork1_block.hash());
}

#[test]
fn test_congestion_receipt_execution() {
    let (mut env, tx_hashes) = prepare_env_with_congestion(PROTOCOL_VERSION, None, 3);

    // Produce block with no new chunk.
    env.produce_block(0, 3);
    let height = 4;
    env.produce_block(0, height);
    let prev_block = env.clients[0].chain.get_block_by_height(height).unwrap();
    let chunk_extra =
        env.clients[0].chain.get_chunk_extra(prev_block.hash(), &ShardUId::single_shard()).unwrap();
    assert!(chunk_extra.gas_used() >= chunk_extra.gas_limit());
    let state_update = env.clients[0]
        .runtime_adapter
        .get_tries()
        .new_trie_update(ShardUId::single_shard(), *chunk_extra.state_root());
    let delayed_indices: DelayedReceiptIndices =
        get(&state_update, &TrieKey::DelayedReceiptIndices).unwrap().unwrap();
    assert!(delayed_indices.next_available_index > 0);
    let mut block = env.clients[0].produce_block(height + 1).unwrap().unwrap();
    testlib::process_blocks::set_no_chunk_in_block(Arc::make_mut(&mut block), &prev_block);
    env.process_block(0, block.clone(), Provenance::NONE);

    // let all receipts finish
    for i in height + 2..height + 7 {
        env.produce_block(0, i);
    }

    for tx_hash in &tx_hashes {
        let final_outcome = env.clients[0].chain.get_final_transaction_result(tx_hash).unwrap();
        assert_matches!(final_outcome.status, FinalExecutionStatus::SuccessValue(_));

        // Check that all receipt ids have corresponding execution outcomes. This means that all receipts generated are executed.
        let transaction_outcome = env.clients[0].chain.get_execution_outcome(tx_hash).unwrap();
        let mut receipt_ids: VecDeque<_> =
            transaction_outcome.outcome_with_id.outcome.receipt_ids.into();
        while !receipt_ids.is_empty() {
            let receipt_id = receipt_ids.pop_front().unwrap();
            let receipt_outcome = env.clients[0].chain.get_execution_outcome(&receipt_id).unwrap();
            match receipt_outcome.outcome_with_id.outcome.status {
                ExecutionStatus::SuccessValue(_) | ExecutionStatus::SuccessReceiptId(_) => {}
                ExecutionStatus::Failure(_) | ExecutionStatus::Unknown => {
                    panic!("unexpected receipt execution outcome")
                }
            }
            receipt_ids.extend(receipt_outcome.outcome_with_id.outcome.receipt_ids);
        }
    }
}

#[test]
fn test_validator_stake_host_function() {
    init_test_logger();
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    let mut env = TestEnv::builder(&genesis.config).nightshade_runtimes(&genesis).build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let block_height = deploy_test_contract(
        &mut env,
        "test0".parse().unwrap(),
        near_test_contracts::rs_contract(),
        epoch_length,
        1,
    );
    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let signed_transaction = SignedTransaction::from_actions(
        10,
        "test0".parse().unwrap(),
        "test0".parse().unwrap(),
        &signer,
        vec![Action::FunctionCall(Box::new(FunctionCallAction {
            method_name: "ext_validator_stake".to_string(),
            args: b"test0".to_vec(),
            gas: 100_000_000_000_000,
            deposit: 0,
        }))],
        *genesis_block.hash(),
        0,
    );
    assert_eq!(
        env.rpc_handlers[0].process_tx(signed_transaction, false, false),
        ProcessTxResponse::ValidTx
    );
    for i in 0..3 {
        env.produce_block(0, block_height + i);
    }
}

#[test]
/// Test that if a node's shard assignment will not change in the next epoch, the node
/// does not need to catch up.
fn test_catchup_no_sharding_change() {
    init_integration_logger();
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    genesis.config.epoch_length = 5;
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(1)
        .validator_seats(1)
        .nightshade_runtimes(&genesis)
        .build();
    // run the chain to a few epochs and make sure no catch up is triggered and the chain still
    // functions
    for h in 1..20 {
        let block = env.clients[0].produce_block(h).unwrap().unwrap();
        let _ =
            env.clients[0].process_block_test(block.clone().into(), Provenance::PRODUCED).unwrap();
        assert_eq!(env.clients[0].chain.chain_store().iterate_state_sync_infos().unwrap(), vec![]);
        assert_eq!(
            env.clients[0]
                .chain
                .chain_store()
                .get_blocks_to_catchup(block.header().prev_hash())
                .unwrap(),
            vec![]
        );
    }
}

/// cspell:words aarch
/// These tests fail on aarch because the WasmtimeVM::precompile method doesn't populate the cache.
mod contract_precompilation_tests {
    use crate::utils::process_blocks::produce_blocks_from_height;

    use super::*;
    use near_primitives::test_utils::MockEpochInfoProvider;
    use near_store::TrieUpdate;
    use near_vm_runner::{
        ContractCode, ContractRuntimeCache, FilesystemContractRuntimeCache, get_contract_cache_key,
    };
    use node_runtime::state_viewer::TrieViewer;
    use node_runtime::state_viewer::ViewApplyState;

    const EPOCH_LENGTH: u64 = 25;

    fn state_sync_on_height(env: &TestEnv, height: BlockHeight) {
        let sync_block = env.clients[0].chain.get_block_by_height(height).unwrap();
        let sync_hash = *sync_block.hash();
        let epoch_id = *env.clients[0].chain.get_block_header(&sync_hash).unwrap().epoch_id();
        let shard_layout = env.clients[0].epoch_manager.get_shard_layout(&epoch_id).unwrap();
        let shard_id = shard_layout.shard_ids().next().unwrap();
        let state_sync_header = env.clients[0]
            .chain
            .state_sync_adapter
            .get_state_response_header(shard_id, sync_hash)
            .unwrap();
        let state_root = state_sync_header.chunk_prev_state_root();
        let sync_prev_header =
            env.clients[0].chain.get_previous_header(sync_block.header()).unwrap();
        let sync_prev_prev_hash = sync_prev_header.prev_hash();
        let state_part = env.clients[0]
            .runtime_adapter
            .obtain_state_part(shard_id, &sync_prev_prev_hash, &state_root, PartId::new(0, 1))
            .unwrap();
        env.clients[1]
            .runtime_adapter
            .apply_state_part(shard_id, &state_root, PartId::new(0, 1), &state_part, &epoch_id)
            .unwrap();
    }

    #[test]
    fn slow_test_sync_and_call_cached_contract() {
        init_integration_logger();
        let num_clients = 2;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = EPOCH_LENGTH;
        let mut caches: Vec<FilesystemContractRuntimeCache> =
            (0..num_clients).map(|_| FilesystemContractRuntimeCache::test().unwrap()).collect();
        let mut env = TestEnv::builder(&genesis.config)
            .clients_count(num_clients)
            .use_state_snapshots()
            .contract_caches(&caches)
            .real_stores()
            .nightshade_runtimes(&genesis)
            .build();

        let start_height = 1;

        // Process test contract deployment on the first client.
        let wasm_code = near_test_contracts::rs_contract().to_vec();
        let height = deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &wasm_code,
            EPOCH_LENGTH + 1,
            start_height,
        );

        // `height` is one more than the start of the epoch. Produce two more blocks with chunks,
        // and then one more than that so the node will generate the needed snapshot.
        let sync_height = produce_blocks_from_height(&mut env, 4, height) - 2;

        // Perform state sync for the second client.
        state_sync_on_height(&mut env, sync_height);

        // Check existence of contract in both caches.
        let contract_code = ContractCode::new(wasm_code.clone(), None);
        let epoch_id =
            *env.clients[0].chain.get_block_by_height(height - 1).unwrap().header().epoch_id();
        let runtime_config = env.get_runtime_config(0, epoch_id);
        let key = get_contract_cache_key(*contract_code.hash(), &runtime_config.wasm_config);
        for i in 0..num_clients {
            caches[i]
                .get(&key)
                .unwrap_or_else(|_| panic!("Failed to get cached result for client {}", i))
                .unwrap_or_else(|| {
                    panic!("Compilation result should be non-empty for client {}", i)
                });
        }

        // Check that contract function may be successfully called on the second client.
        // Note that we can't test that behavior is the same on two clients, because
        // compile_module_cached_wasmer0 is cached by contract key via macro.
        let block = env.clients[0].chain.get_block_by_height(sync_height - 1).unwrap();
        let shard_uid = ShardUId::single_shard();
        let shard_id = shard_uid.shard_id();
        let chunk_extra =
            env.clients[0].chain.get_chunk_extra(block.header().prev_hash(), &shard_uid).unwrap();
        let state_root = *chunk_extra.state_root();

        let viewer = TrieViewer::default();
        // TODO (#7327): set use_flat_storage to true when we implement support for state sync for FlatStorage

        let trie = env.clients[1]
            .runtime_adapter
            .get_trie_for_shard(shard_id, block.header().prev_hash(), state_root, false)
            .unwrap();
        let state_update = TrieUpdate::new(trie);

        let mut logs = vec![];
        let view_state = ViewApplyState {
            block_height: EPOCH_LENGTH,
            prev_block_hash: *block.header().prev_hash(),
            shard_id,
            epoch_id: *block.header().epoch_id(),
            epoch_height: 1,
            block_timestamp: block.header().raw_timestamp(),
            current_protocol_version: PROTOCOL_VERSION,
            cache: Some(Box::new(caches.swap_remove(1))),
        };
        viewer
            .call_function(
                state_update,
                view_state,
                &"test0".parse().unwrap(),
                "log_something",
                &[],
                &mut logs,
                &MockEpochInfoProvider::default(),
            )
            .unwrap();
    }

    #[test]
    fn slow_test_two_deployments() {
        init_integration_logger();
        let num_clients = 2;
        let mut genesis =
            Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
        genesis.config.epoch_length = EPOCH_LENGTH;

        let caches: Vec<FilesystemContractRuntimeCache> =
            (0..num_clients).map(|_| FilesystemContractRuntimeCache::test().unwrap()).collect();
        let mut env = TestEnv::builder(&genesis.config)
            .clients_count(num_clients)
            .use_state_snapshots()
            .contract_caches(&caches)
            .real_stores()
            .nightshade_runtimes(&genesis)
            .build();

        let mut height = 1;

        // Process tiny contract deployment on the first client.
        let tiny_wasm_code = near_test_contracts::trivial_contract().to_vec();
        height = deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &tiny_wasm_code,
            EPOCH_LENGTH,
            height,
        );

        // Wait 3 epochs.
        height = produce_blocks_from_height(&mut env, 3 * EPOCH_LENGTH, height);

        // Process test contract deployment on the first client.
        let wasm_code = near_test_contracts::rs_contract().to_vec();
        height = deploy_test_contract(
            &mut env,
            "test0".parse().unwrap(),
            &wasm_code,
            EPOCH_LENGTH + 1,
            height,
        );

        // `height` is one more than the start of the epoch. Produce two more blocks with chunks,
        // and then one more than that so the node will generate the needed snapshot.
        let sync_height = produce_blocks_from_height(&mut env, 4, height) - 2;

        // Perform state sync for the second client on the last produced height.
        state_sync_on_height(&mut env, sync_height);

        let epoch_id =
            *env.clients[0].chain.get_block_by_height(sync_height).unwrap().header().epoch_id();
        let runtime_config = env.get_runtime_config(0, epoch_id);
        let tiny_contract_key = get_contract_cache_key(
            *ContractCode::new(tiny_wasm_code.clone(), None).hash(),
            &runtime_config.wasm_config,
        );
        let test_contract_key = get_contract_cache_key(
            *ContractCode::new(wasm_code.clone(), None).hash(),
            &runtime_config.wasm_config,
        );

        // Check that both deployed contracts are presented in cache for client 0.
        assert!(caches[0].get(&tiny_contract_key).unwrap().is_some());
        assert!(caches[0].get(&test_contract_key).unwrap().is_some());

        // Check that only last contract is presented in cache for client 1.
        assert!(caches[1].get(&tiny_contract_key).unwrap().is_none());
        assert!(caches[1].get(&test_contract_key).unwrap().is_some());
    }

    #[test]
    fn slow_test_sync_after_delete_account() {
        init_test_logger();
        let num_clients = 3;
        let mut genesis = Genesis::test(
            vec!["test0".parse().unwrap(), "test1".parse().unwrap(), "test2".parse().unwrap()],
            1,
        );
        genesis.config.epoch_length = EPOCH_LENGTH;
        let caches: Vec<FilesystemContractRuntimeCache> =
            (0..num_clients).map(|_| FilesystemContractRuntimeCache::test().unwrap()).collect();
        let mut env = TestEnv::builder(&genesis.config)
            .clients_count(num_clients)
            .use_state_snapshots()
            .contract_caches(&caches)
            .real_stores()
            .nightshade_runtimes(&genesis)
            .build();

        let mut height = 1;

        // Process test contract deployment on the first client.
        let wasm_code = near_test_contracts::rs_contract().to_vec();
        height = deploy_test_contract(
            &mut env,
            "test2".parse().unwrap(),
            &wasm_code,
            EPOCH_LENGTH,
            height,
        );

        // Delete account on which test contract is stored.
        let block = env.clients[0].chain.get_block_by_height(height - 1).unwrap();
        let signer = InMemorySigner::test_signer(&"test2".parse().unwrap());
        let delete_account_tx = SignedTransaction::delete_account(
            2,
            "test2".parse().unwrap(),
            "test2".parse().unwrap(),
            "test0".parse().unwrap(),
            &signer,
            *block.hash(),
        );
        assert_eq!(
            env.rpc_handlers[0].process_tx(delete_account_tx, false, false),
            ProcessTxResponse::ValidTx
        );
        // `height` is the first block of a new epoch (which has not been produced yet).
        // We produce two more than that, plus one more so that the node will generate the needed snapshot.
        let sync_height = produce_blocks_from_height(&mut env, EPOCH_LENGTH + 5, height) - 2;

        // Perform state sync for the second client.
        state_sync_on_height(&mut env, sync_height);

        let epoch_id =
            *env.clients[0].chain.get_block_by_height(sync_height).unwrap().header().epoch_id();
        let runtime_config = env.get_runtime_config(0, epoch_id);
        let contract_key = get_contract_cache_key(
            *ContractCode::new(wasm_code.clone(), None).hash(),
            &runtime_config.wasm_config,
        );

        // Check that contract is cached for client 0 despite account deletion.
        assert!(caches[0].get(&contract_key).unwrap().is_some());

        // Check that contract is not cached for client 1 because of late state sync.
        assert!(caches[1].get(&contract_key).unwrap().is_none());
    }
}
