use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use actix::{Actor, Addr, System};
use futures::{FutureExt, future};
use near_chain_configs::test_utils::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_primitives::num_rational::Ratio;
use rand::Rng;

use crate::utils::genesis_helpers::genesis_hash;
use crate::utils::test_helpers::heavy_test;
use near_actix_test_utils::run_actix;
use near_chain_configs::{Genesis, NEAR_BASE, TrackedShardsConfig};
use near_client::{
    ClientActor, GetBlock, ProcessTxRequest, Query, RpcHandlerActor, ViewClientActor,
};
use near_crypto::{InMemorySigner, Signer};
use near_network::tcp;
use near_network::test_utils::{WaitOrTimeoutActor, convert_boot_nodes};
use near_o11y::testonly::init_integration_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeightDelta, BlockReference, NumSeats};
use near_primitives::views::{QueryRequest, QueryResponseKind, ValidatorInfo};
use nearcore::{NearConfig, load_test_config, start_with_config};

use near_client_primitives::types::Status;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use {near_primitives::types::BlockId, primitive_types::U256};

#[derive(Clone)]
struct TestNode {
    account_id: AccountId,
    signer: Arc<Signer>,
    config: NearConfig,
    client: Addr<ClientActor>,
    view_client: Addr<ViewClientActor>,
    tx_processor: Addr<RpcHandlerActor>,
    genesis_hash: CryptoHash,
}

fn init_test_staking(
    paths: Vec<&Path>,
    num_node_seats: NumSeats,
    num_validator_seats: NumSeats,
    epoch_length: BlockHeightDelta,
    enable_rewards: bool,
    minimum_stake_divisor: u64,
    track_all_shards: bool,
) -> Vec<TestNode> {
    init_integration_logger();

    let seeds = (0..num_node_seats).map(|i| format!("near.{}", i)).collect::<Vec<_>>();
    let mut genesis =
        Genesis::test(seeds.iter().map(|s| s.parse().unwrap()).collect(), num_validator_seats);
    genesis.config.epoch_length = epoch_length;
    genesis.config.num_block_producer_seats = num_node_seats;
    genesis.config.block_producer_kickout_threshold = 20;
    genesis.config.chunk_producer_kickout_threshold = 20;
    genesis.config.chunk_validator_only_kickout_threshold = 20;
    genesis.config.minimum_stake_divisor = minimum_stake_divisor;
    if !enable_rewards {
        genesis.config.max_inflation_rate = Ratio::from_integer(0);
        genesis.config.min_gas_price = 0;
    }
    let first_node = tcp::ListenerAddr::reserve_for_test();

    let configs = (0..num_node_seats).map(|i| {
        let mut config = load_test_config(
            &format!("near.{}", i),
            if i == 0 { first_node } else { tcp::ListenerAddr::reserve_for_test() },
            genesis.clone(),
        );
        if track_all_shards {
            config.config.tracked_shards_config = Some(TrackedShardsConfig::AllShards);
            config.client_config.tracked_shards_config = TrackedShardsConfig::AllShards;
        }
        if i != 0 {
            config.network_config.peer_store.boot_nodes =
                convert_boot_nodes(vec![("near.0", *first_node)]);
        }
        config.client_config.min_num_peers = num_node_seats as usize - 1;
        config
    });
    configs
        .enumerate()
        .map(|(i, config)| {
            let genesis_hash = genesis_hash(&config.genesis);
            let nearcore::NearNode { client, view_client, rpc_handler: tx_processor, .. } =
                start_with_config(paths[i], config.clone()).expect("start_with_config");
            let account_id = format!("near.{}", i).parse::<AccountId>().unwrap();
            let signer = Arc::new(InMemorySigner::test_signer(&account_id));
            TestNode { account_id, signer, config, client, view_client, tx_processor, genesis_hash }
        })
        .collect()
}

/// Runs one validator network, sends staking transaction for the second node and
/// waits until it becomes a validator.
#[test]
fn slow_test_stake_nodes() {
    heavy_test(|| {
        let num_nodes = 2;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("stake_node_{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        run_actix(async {
            let test_nodes = init_test_staking(
                dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
                num_nodes,
                1,
                10,
                false,
                10,
                false,
            );

            let tx = SignedTransaction::stake(
                1,
                test_nodes[1].account_id.clone(),
                // &*test_nodes[1].config.block_producer.as_ref().unwrap().signer,
                &(*test_nodes[1].signer),
                TESTING_INIT_STAKE,
                test_nodes[1].config.validator_signer.get().unwrap().public_key(),
                test_nodes[1].genesis_hash,
            );
            actix::spawn(
                test_nodes[0]
                    .tx_processor
                    .send(ProcessTxRequest {
                        transaction: tx,
                        is_forwarded: false,
                        check_only: false,
                    })
                    .map(drop),
            );

            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let actor = test_nodes[0]
                        .client
                        .send(Status { is_health_check: false, detailed: false }.span_wrap());
                    let actor = actor.then(|res| {
                        let res = res.unwrap();
                        if res.is_err() {
                            return future::ready(());
                        }
                        let mut validators = res.unwrap().validators;
                        validators.sort_unstable_by(|a, b| a.account_id.cmp(&b.account_id));
                        if validators
                            == vec![
                                ValidatorInfo { account_id: "near.0".parse().unwrap() },
                                ValidatorInfo { account_id: "near.1".parse().unwrap() },
                            ]
                        {
                            System::current().stop();
                        }
                        future::ready(())
                    });
                    actix::spawn(actor);
                }),
                100,
                40000,
            )
            .start();
        });
    });
}

#[test]
fn slow_test_validator_kickout() {
    heavy_test(|| {
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new()
                    .prefix(&format!("validator_kickout_{}", i))
                    .tempdir()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        run_actix(async {
            let test_nodes = init_test_staking(
                dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
                num_nodes,
                4,
                15,
                false,
                (TESTING_INIT_STAKE / NEAR_BASE) as u64 + 1,
                false,
            );
            let mut rng = rand::thread_rng();
            let stakes = (0..num_nodes / 2).map(|_| NEAR_BASE + rng.gen_range(1..100));
            let stake_transactions = stakes.enumerate().map(|(i, stake)| {
                let test_node = &test_nodes[i];
                let signer = Arc::new(InMemorySigner::test_signer(&test_node.account_id));
                SignedTransaction::stake(
                    1,
                    test_node.account_id.clone(),
                    &*signer,
                    stake,
                    test_node.config.validator_signer.get().unwrap().public_key(),
                    test_node.genesis_hash,
                )
            });

            for (i, stake_transaction) in stake_transactions.enumerate() {
                let test_node = &test_nodes[i];
                actix::spawn(
                    test_node
                        .tx_processor
                        .send(ProcessTxRequest {
                            transaction: stake_transaction,
                            is_forwarded: false,
                            check_only: false,
                        })
                        .map(drop),
                );
            }

            let finalized_mark: Arc<Vec<_>> =
                Arc::new((0..num_nodes).map(|_| Arc::new(AtomicBool::new(false))).collect());

            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let test_nodes = test_nodes.clone();
                    let test_node1 = test_nodes[(num_nodes / 2) as usize].clone();
                    let finalized_mark1 = finalized_mark.clone();

                    let actor = test_node1
                        .client
                        .send(Status { is_health_check: false, detailed: false }.span_wrap());
                    let actor = actor.then(move |res| {
                        let expected: Vec<_> = (num_nodes / 2..num_nodes)
                            .map(|i| ValidatorInfo {
                                account_id: AccountId::try_from(format!("near.{}", i)).unwrap(),
                            })
                            .collect();
                        let res = res.unwrap();
                        if res.is_err() {
                            return future::ready(());
                        }
                        if res.unwrap().validators == expected {
                            for i in 0..num_nodes / 2 {
                                let mark = finalized_mark1[i as usize].clone();
                                let actor = test_node1.view_client.send(Query::new(
                                    BlockReference::latest(),
                                    QueryRequest::ViewAccount {
                                        account_id: test_nodes[i as usize].account_id.clone(),
                                    },
                                ));
                                let actor =
                                    actor.then(move |res| match res.unwrap().unwrap().kind {
                                        QueryResponseKind::ViewAccount(result) => {
                                            if result.locked == 0
                                                || result.amount == TESTING_INIT_BALANCE
                                            {
                                                mark.store(true, Ordering::SeqCst);
                                            }
                                            future::ready(())
                                        }
                                        _ => panic!("wrong return result"),
                                    });
                                actix::spawn(actor);
                            }
                            for i in num_nodes / 2..num_nodes {
                                let mark = finalized_mark1[i as usize].clone();

                                let actor = test_node1.view_client.send(Query::new(
                                    BlockReference::latest(),
                                    QueryRequest::ViewAccount {
                                        account_id: test_nodes[i as usize].account_id.clone(),
                                    },
                                ));
                                let actor =
                                    actor.then(move |res| match res.unwrap().unwrap().kind {
                                        QueryResponseKind::ViewAccount(result) => {
                                            assert_eq!(result.locked, TESTING_INIT_STAKE);
                                            assert_eq!(
                                                result.amount,
                                                TESTING_INIT_BALANCE - TESTING_INIT_STAKE
                                            );
                                            mark.store(true, Ordering::SeqCst);
                                            future::ready(())
                                        }
                                        _ => panic!("wrong return result"),
                                    });
                                actix::spawn(actor);
                            }

                            if finalized_mark1.iter().all(|mark| mark.load(Ordering::SeqCst)) {
                                System::current().stop();
                            }
                        }
                        future::ready(())
                    });
                    actix::spawn(actor);
                }),
                100,
                70000,
            )
            .start();
        });
    })
}

/// Starts 4 nodes, genesis has 2 validator seats.
/// Node1 unstakes, Node2 stakes.
/// Submit the transactions via Node1 and Node2.
/// Poll `/status` until you see the change of validator assignments.
/// Afterwards check that `locked` amount on accounts Node1 and Node2 are 0 and TESTING_INIT_STAKE.
#[test]
fn ultra_slow_test_validator_join() {
    heavy_test(|| {
        let num_nodes = 4;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("validator_join_{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        run_actix(async {
            let test_nodes = init_test_staking(
                dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
                num_nodes,
                2,
                30,
                false,
                10,
                false,
            );
            let signer = Arc::new(InMemorySigner::test_signer(&test_nodes[1].account_id));
            let unstake_transaction = SignedTransaction::stake(
                1,
                test_nodes[1].account_id.clone(),
                &*signer,
                0,
                test_nodes[1].config.validator_signer.get().unwrap().public_key(),
                test_nodes[1].genesis_hash,
            );

            let signer = Arc::new(InMemorySigner::test_signer(&test_nodes[2].account_id));
            let stake_transaction = SignedTransaction::stake(
                1,
                test_nodes[2].account_id.clone(),
                &*signer,
                TESTING_INIT_STAKE,
                test_nodes[2].config.validator_signer.get().unwrap().public_key(),
                test_nodes[2].genesis_hash,
            );

            actix::spawn(
                test_nodes[1]
                    .tx_processor
                    .send(ProcessTxRequest {
                        transaction: unstake_transaction,
                        is_forwarded: false,
                        check_only: false,
                    })
                    .map(drop),
            );
            actix::spawn(
                test_nodes[0]
                    .tx_processor
                    .send(ProcessTxRequest {
                        transaction: stake_transaction,
                        is_forwarded: false,
                        check_only: false,
                    })
                    .map(drop),
            );

            let (done1, done2) =
                (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
            let (done1_copy1, done2_copy1) = (done1, done2);
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let test_nodes = test_nodes.clone();
                    let test_node1 = test_nodes[0].clone();
                    let (done1_copy2, done2_copy2) = (done1_copy1.clone(), done2_copy1.clone());
                    let actor = test_node1
                        .client
                        .send(Status { is_health_check: false, detailed: false }.span_wrap());
                    let actor = actor.then(move |res| {
                        let expected = vec![
                            ValidatorInfo { account_id: "near.0".parse().unwrap() },
                            ValidatorInfo { account_id: "near.2".parse().unwrap() },
                        ];
                        let res = res.unwrap();
                        if res.is_err() {
                            return future::ready(());
                        }
                        if res.unwrap().validators == expected {
                            let actor = test_node1.view_client.send(Query::new(
                                BlockReference::latest(),
                                QueryRequest::ViewAccount {
                                    account_id: test_nodes[1].account_id.clone(),
                                },
                            ));
                            let actor = actor.then(move |res| match res.unwrap().unwrap().kind {
                                QueryResponseKind::ViewAccount(result) => {
                                    if result.locked == 0 {
                                        done1_copy2.store(true, Ordering::SeqCst);
                                    }
                                    future::ready(())
                                }
                                _ => panic!("wrong return result"),
                            });
                            actix::spawn(actor);
                            let actor = test_node1.view_client.send(Query::new(
                                BlockReference::latest(),
                                QueryRequest::ViewAccount {
                                    account_id: test_nodes[2].account_id.clone(),
                                },
                            ));
                            let actor = actor.then(move |res| match res.unwrap().unwrap().kind {
                                QueryResponseKind::ViewAccount(result) => {
                                    if result.locked == TESTING_INIT_STAKE {
                                        done2_copy2.store(true, Ordering::SeqCst);
                                    }

                                    future::ready(())
                                }
                                _ => panic!("wrong return result"),
                            });
                            actix::spawn(actor);
                        }

                        future::ready(())
                    });
                    actix::spawn(actor);
                    if done1_copy1.load(Ordering::SeqCst) && done2_copy1.load(Ordering::SeqCst) {
                        System::current().stop();
                    }
                }),
                1000,
                60000,
            )
            .start();
        });
    });
}

/// Checks that during the first epoch, total_supply matches total_supply in genesis.
/// Checks that during the second epoch, total_supply matches the expected inflation rate.
#[test]
fn slow_test_inflation() {
    heavy_test(|| {
        let num_nodes = 1;
        let dirs = (0..num_nodes)
            .map(|i| {
                tempfile::Builder::new().prefix(&format!("stake_node_{}", i)).tempdir().unwrap()
            })
            .collect::<Vec<_>>();
        let epoch_length = 10;
        run_actix(async {
            let test_nodes = init_test_staking(
                dirs.iter().map(|dir| dir.path()).collect::<Vec<_>>(),
                num_nodes,
                1,
                epoch_length,
                true,
                10,
                false,
            );
            let initial_total_supply = test_nodes[0].config.genesis.config.total_supply;
            let max_inflation_rate = test_nodes[0].config.genesis.config.max_inflation_rate;

            let (done1, done2) =
                (Arc::new(AtomicBool::new(false)), Arc::new(AtomicBool::new(false)));
            let (done1_copy1, done2_copy1) = (done1, done2);
            WaitOrTimeoutActor::new(
                Box::new(move |_ctx| {
                    let (done1_copy2, done2_copy2) = (done1_copy1.clone(), done2_copy1.clone());
                    let actor =
                        test_nodes[0].view_client.send(GetBlock::latest());
                    let actor = actor.then(move |res| {
                        if let Ok(Ok(block)) = res {
                            if block.header.height >= 2 && block.header.height <= epoch_length {
                                tracing::info!(?block.header.total_supply, ?block.header.height, ?initial_total_supply, epoch_length, "Step1: epoch1");
                                if block.header.total_supply == initial_total_supply {
                                    done1_copy2.store(true, Ordering::SeqCst);
                                }
                            } else {
                                tracing::info!("Step1: not epoch1");
                            }
                        }
                        future::ready(())
                    });
                    actix::spawn(actor);
                    let view_client = test_nodes[0].view_client.clone();
                    actix::spawn(async move {
                        if let Ok(Ok(block)) =
                            view_client.send(GetBlock::latest()).await
                        {
                            if block.header.height > epoch_length
                                && block.header.height < epoch_length * 2
                            {
                                tracing::info!(?block.header.total_supply, ?block.header.height, ?initial_total_supply, epoch_length, "Step2: epoch2");
                                let base_reward = {
                                    let genesis_block_view = view_client
                                        .send(
                                            GetBlock(BlockReference::BlockId(BlockId::Height(0)))
                                                ,
                                        )
                                        .await
                                        .unwrap()
                                        .unwrap();
                                    let epoch_end_block_view = view_client
                                        .send(
                                            GetBlock(BlockReference::BlockId(BlockId::Height(
                                                epoch_length,
                                            )))
                                            ,
                                        )
                                        .await
                                        .unwrap()
                                        .unwrap();
                                    (U256::from(initial_total_supply)
                                        * U256::from(
                                            epoch_end_block_view.header.timestamp_nanosec
                                                - genesis_block_view.header.timestamp_nanosec,
                                        )
                                        * U256::from(*max_inflation_rate.numer() as u64)
                                        / (U256::from(10u64.pow(9) * 365 * 24 * 60 * 60)
                                            * U256::from(*max_inflation_rate.denom() as u64)))
                                    .as_u128()
                                };
                                // To match rounding, split into protocol reward and validator reward.
                                // Protocol reward is one tenth of the base reward, while validator reward is the remainder.
                                // There's only one validator so the second part of the computation is easier.
                                // The validator rewards depend on its uptime; in other words, the more blocks, chunks and endorsements
                                // it produces the bigger is the reward.
                                // In this test the validator produces 10 blocks out 10, 9 chunks out of 10 and 9 endorsements out of 10.
                                // Then there's a formula to translate 28/30 successes to a 10/27 reward multiplier
                                // (using min_online_threshold=9/10 and max_online_threshold=99/100).
                                //
                                // For additional details check: chain/epoch-manager/src/reward_calculator.rs or
                                // https://nomicon.io/Economics/Economic#validator-rewards-calculation
                                let protocol_reward = base_reward * 1 / 10;
                                let validator_reward = base_reward - protocol_reward;
                                // Chunk endorsement ratio 9/10 is mapped to 1 so the reward multiplier becomes 20/27.
                                let inflation = protocol_reward + validator_reward * 20 / 27;
                                tracing::info!(?block.header.total_supply, ?block.header.height, ?initial_total_supply, epoch_length, ?inflation, "Step2: epoch2");
                                if block.header.total_supply == initial_total_supply + inflation {
                                    done2_copy2.store(true, Ordering::SeqCst);
                                }
                            } else {
                                tracing::info!("Step2: not epoch2");
                            }
                        }
                    });
                    if done1_copy1.load(Ordering::SeqCst) && done2_copy1.load(Ordering::SeqCst) {
                        System::current().stop();
                    }
                }),
                100,
                20000,
            )
            .start();
        });
    });
}
