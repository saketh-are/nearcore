use crate::tests::nearcore::node_cluster::NodeCluster;
use crate::utils::genesis_helpers::genesis_block;
use actix::clock::sleep;
use actix::{Actor, System};
use assert_matches::assert_matches;

use futures::future::join_all;
use futures::{FutureExt, TryFutureExt, future};
use near_actix_test_utils::spawn_interruptible;
use near_client::{GetBlock, GetExecutionOutcome, GetValidatorInfo};
use near_crypto::InMemorySigner;
use near_jsonrpc::client::new_client;
use near_jsonrpc_primitives::types::transactions::{RpcTransactionStatusRequest, TransactionInfo};
use near_network::test_utils::WaitOrTimeoutActor;
use near_o11y::testonly::init_integration_logger;
use near_parameters::{RuntimeConfigStore, RuntimeConfigView};
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::merkle::{compute_root_from_path_and_item, verify_path};
use near_primitives::serialize::to_base64;
use near_primitives::transaction::{PartialExecutionStatus, SignedTransaction};
use near_primitives::types::{
    BlockId, BlockReference, EpochId, EpochReference, Finality, TransactionOrReceiptId,
};
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature, ProtocolVersion};
use near_primitives::views::{ExecutionOutcomeView, ExecutionStatusView, TxExecutionStatus};
use std::time::Duration;

#[test]
fn test_get_validator_info_rpc() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_, rpc_addrs, clients| async move {
        WaitOrTimeoutActor::new(
            Box::new(move |_ctx| {
                let rpc_addrs_copy = rpc_addrs.clone();
                let view_client = clients[0].1.clone();
                spawn_interruptible(async move {
                    let block_view = view_client.send(GetBlock::latest()).await.unwrap();
                    if let Err(err) = block_view {
                        println!("Failed to get the latest block: {:?}", err);
                        return;
                    }
                    let block_view = block_view.unwrap();
                    if block_view.header.height > 1 {
                        let client = new_client(&format!("http://{}", rpc_addrs_copy[0]));
                        let block_hash = block_view.header.hash;
                        let invalid_res = client
                            .validators(Some(EpochReference::BlockId(BlockId::Hash(block_hash))))
                            .await;
                        assert!(invalid_res.is_err());
                        let res = client.validators(None).await.unwrap();
                        assert_eq!(res.current_validators.len(), 1);
                        assert!(res.current_validators.iter().any(|r| r.account_id == "near.0"));
                        System::current().stop();
                    }
                });
            }),
            100,
            40000,
        )
        .start();
    });
}

fn outcome_view_to_hashes(outcome: &ExecutionOutcomeView) -> Vec<CryptoHash> {
    let status = match &outcome.status {
        ExecutionStatusView::Unknown => PartialExecutionStatus::Unknown,
        ExecutionStatusView::SuccessValue(s) => PartialExecutionStatus::SuccessValue(s.clone()),
        ExecutionStatusView::Failure(_) => PartialExecutionStatus::Failure,
        ExecutionStatusView::SuccessReceiptId(id) => PartialExecutionStatus::SuccessReceiptId(*id),
    };
    let mut result = vec![CryptoHash::hash_borsh((
        outcome.receipt_ids.clone(),
        outcome.gas_burnt,
        outcome.tokens_burnt,
        outcome.executor_id.clone(),
        status,
    ))];
    for log in &outcome.logs {
        result.push(hash(log.as_bytes()));
    }
    result
}

fn test_get_execution_outcome(is_tx_successful: bool) {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(1)
        .set_epoch_length(1000)
        .set_genesis_height(0)
        .set_save_tx_outcomes(true);

    cluster.exec_until_stop(|genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(&genesis).hash();
        let signer = InMemorySigner::test_signer(&"near.0".parse().unwrap());
        let transaction = if is_tx_successful {
            SignedTransaction::send_money(
                1,
                "near.0".parse().unwrap(),
                "near.1".parse().unwrap(),
                &signer,
                10000,
                genesis_hash,
            )
        } else {
            SignedTransaction::create_account(
                1,
                "near.0".parse().unwrap(),
                "near.1".parse().unwrap(),
                10,
                signer.public_key(),
                &signer,
                genesis_hash,
            )
        };

        WaitOrTimeoutActor::new(
            Box::new(move |_ctx| {
                let client = new_client(&format!("http://{}", rpc_addrs[0]));
                let bytes = borsh::to_vec(&transaction).unwrap();
                let view_client1 = view_client.clone();
                spawn_interruptible(client.broadcast_tx_commit(to_base64(&bytes)).then(
                    move |res| {
                        let final_transaction_outcome = match res {
                            Ok(outcome) => outcome.final_execution_outcome.unwrap().into_outcome(),
                            Err(_) => return future::ready(()),
                        };
                        spawn_interruptible(sleep(Duration::from_secs(1)).then(move |_| {
                            let mut futures = vec![];
                            for id in vec![TransactionOrReceiptId::Transaction {
                                transaction_hash: final_transaction_outcome.transaction_outcome.id,
                                sender_id: "near.0".parse().unwrap(),
                            }]
                            .into_iter()
                            .chain(
                                final_transaction_outcome.receipts_outcome.into_iter().map(|r| {
                                    TransactionOrReceiptId::Receipt {
                                        receipt_id: r.id,
                                        receiver_id: "near.1".parse().unwrap(),
                                    }
                                }),
                            ) {
                                let view_client2 = view_client1.clone();
                                let fut = view_client1.send(GetExecutionOutcome { id });
                                let fut = fut.then(move |res| {
                                    let execution_outcome_response = res.unwrap().unwrap();
                                    view_client2
                                        .send(GetBlock(BlockReference::BlockId(BlockId::Hash(
                                            execution_outcome_response.outcome_proof.block_hash,
                                        ))))
                                        .then(move |res| {
                                            let res = res.unwrap().unwrap();
                                            let mut outcome_with_id_to_hash =
                                                vec![execution_outcome_response.outcome_proof.id];
                                            outcome_with_id_to_hash.extend(outcome_view_to_hashes(
                                                &execution_outcome_response.outcome_proof.outcome,
                                            ));
                                            let chunk_outcome_root =
                                                compute_root_from_path_and_item(
                                                    &execution_outcome_response.outcome_proof.proof,
                                                    &outcome_with_id_to_hash,
                                                );
                                            assert!(verify_path(
                                                res.header.outcome_root,
                                                &execution_outcome_response.outcome_root_proof,
                                                &chunk_outcome_root
                                            ));
                                            future::ready(())
                                        })
                                });
                                futures.push(fut);
                            }
                            spawn_interruptible(join_all(futures).then(|_| {
                                System::current().stop();
                                future::ready(())
                            }));
                            future::ready(())
                        }));

                        future::ready(())
                    },
                ));
            }),
            100,
            40000,
        )
        .start();
    });
}

#[test]
fn ultra_slow_test_get_execution_outcome_tx_success() {
    test_get_execution_outcome(true);
}

#[test]
fn ultra_slow_test_get_execution_outcome_tx_failure() {
    test_get_execution_outcome(false);
}

#[test]
fn test_protocol_config_rpc() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_, rpc_addrs, _| async move {
        let client = new_client(&format!("http://{}", rpc_addrs[0]));
        let config_response = client
            .EXPERIMENTAL_protocol_config(
                near_jsonrpc_primitives::types::config::RpcProtocolConfigRequest {
                    block_reference: near_primitives::types::BlockReference::Finality(
                        Finality::None,
                    ),
                },
            )
            .await
            .unwrap();

        let runtime_config_store = RuntimeConfigStore::new(None);
        let initial_runtime_config = runtime_config_store.get_config(ProtocolVersion::MIN);
        let latest_runtime_config =
            runtime_config_store.get_config(near_primitives::version::PROTOCOL_VERSION);
        assert_ne!(
            config_response.config_view.runtime_config.storage_amount_per_byte,
            initial_runtime_config.storage_amount_per_byte()
        );
        // compare JSON view
        assert_eq!(
            serde_json::json!(config_response.config_view.runtime_config),
            serde_json::json!(RuntimeConfigView::from(latest_runtime_config.as_ref().clone()))
        );
        System::current().stop();
    });
}

#[test]
fn test_query_rpc_account_view_must_succeed() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_, rpc_addrs, _| async move {
        let client = new_client(&format!("http://{}", rpc_addrs[0]));
        let query_response = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: near_primitives::types::BlockReference::Finality(Finality::Final),
                request: near_primitives::views::QueryRequest::ViewAccount {
                    account_id: "near.0".parse().unwrap(),
                },
            })
            .await
            .unwrap();
        let account =
            if let near_jsonrpc_primitives::types::query::QueryResponseKind::ViewAccount(account) =
                query_response.kind
            {
                account
            } else {
                panic!(
                    "expected a account view result, but received something else: {:?}",
                    query_response.kind
                );
            };
        assert_matches!(account, near_primitives::views::AccountView { .. });
        System::current().stop();
    });
}

#[test]
fn test_query_rpc_account_view_account_does_not_exist_must_return_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    // cspell:ignore accountdoesntexist
    cluster.exec_until_stop(|_, rpc_addrs, _| async move {
        let client = new_client(&format!("http://{}", rpc_addrs[0]));
        let error_message = loop {
            let query_response = client
                .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                    block_reference: near_primitives::types::BlockReference::Finality(Finality::Final),
                    request: near_primitives::views::QueryRequest::ViewAccount {
                        account_id: "accountdoesntexist.0".parse().unwrap(),
                    },
                })
                .await;

            break match query_response {
                Ok(result) => panic!("expected error but received Ok: {:?}", result.kind),
                Err(err) => {
                    let value = *err.data.unwrap();
                    if value == serde_json::to_value("Block either has never been observed on the node or has been garbage collected: Finality(Final)").unwrap() {
                                println!("No blocks are produced yet, retry.");
                                sleep(std::time::Duration::from_millis(100)).await;
                                continue;
                    }
                    value
                }
            };
        };

        assert!(
            error_message
                .to_string()
                .contains("account accountdoesntexist.0 does not exist while viewing"),
            "{}",
            error_message
        );

        System::current().stop();
    });
}

#[test]
fn slow_test_tx_not_enough_balance_must_return_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(2)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(&genesis).hash();
        let signer = InMemorySigner::test_signer(&"near.0".parse().unwrap());
        let transaction = SignedTransaction::send_money(
            1,
            "near.0".parse().unwrap(),
            "near.1".parse().unwrap(),
            &signer,
            1100000000000000000000000000000000,
            genesis_hash,
        );

        let client = new_client(&format!("http://{}", rpc_addrs[0]));
        let bytes = borsh::to_vec(&transaction).unwrap();

        spawn_interruptible(async move {
            loop {
                let res = view_client.send(GetBlock::latest()).await;
                if let Ok(Ok(block)) = res {
                    if block.header.height > 10 {
                        break;
                    }
                }
                sleep(std::time::Duration::from_millis(500)).await;
            }
            let expected_cost = if ProtocolFeature::ReducedGasRefunds.enabled(PROTOCOL_VERSION) {
                "1100000000000044636512500000000000"
            } else {
                "1100000000000045306060187500000000"
            };
            let _ = client
                .broadcast_tx_commit(to_base64(&bytes))
                .map_err(|err| {
                    println!("testing: {:?}", err.data);
                    assert_eq!(
                        *err.data.unwrap(),
                        serde_json::json!({"TxExecutionError": {
                            "InvalidTxError": {
                                "NotEnoughBalance": {
                                    "signer_id": "near.0",
                                    "balance": "950000000000000000000000000000000", // If something changes in setup just update this value
                                    "cost": expected_cost,
                                }
                            }
                        }})
                    );
                    System::current().stop();
                })
                .map_ok(|_| panic!("Transaction must not succeed"))
                .await;
        });
    });
}

#[test]
fn slow_test_check_unknown_tx_must_return_error() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_nodes(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(&genesis).hash();
        let signer = InMemorySigner::test_signer(&"near.0".parse().unwrap());
        let transaction = SignedTransaction::send_money(
            1,
            "near.0".parse().unwrap(),
            "near.0".parse().unwrap(),
            &signer,
            10000,
            genesis_hash,
        );

        let client = new_client(&format!("http://{}", rpc_addrs[0]));
        let tx_hash = transaction.get_hash();

        spawn_interruptible(async move {
            loop {
                let res = view_client.send(GetBlock::latest()).await;
                if let Ok(Ok(block)) = res {
                    if block.header.height > 10 {
                        let _ = client
                            .EXPERIMENTAL_tx_status(RpcTransactionStatusRequest {
                                transaction_info: TransactionInfo::TransactionId {
                                    tx_hash,
                                    sender_account_id: transaction.transaction.signer_id().clone(),
                                },
                                wait_until: TxExecutionStatus::None,
                            })
                            .map_err(|err| {
                                assert_eq!(
                                    *err.data.unwrap(),
                                    serde_json::json!(format!(
                                        "Transaction {} doesn't exist",
                                        tx_hash
                                    ))
                                );
                                System::current().stop();
                            })
                            .map_ok(|_| panic!("Transaction must be unknown"))
                            .await;
                        break;
                    }
                }
                sleep(std::time::Duration::from_millis(500)).await;
            }
        });
    });
}

#[test]
#[ignore = "Need to implement forwarding and fix the test"]
fn test_tx_status_on_lightclient_must_return_does_not_track_shard() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(1)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|genesis, rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        let genesis_hash = *genesis_block(&genesis).hash();
        let signer = InMemorySigner::test_signer(&"near.1".parse().unwrap());
        let transaction = SignedTransaction::send_money(
            1,
            "near.1".parse().unwrap(),
            "near.1".parse().unwrap(),
            &signer,
            10000,
            genesis_hash,
        );

        let client = new_client(&format!("http://{}", rpc_addrs[1]));

        spawn_interruptible(async move {
            loop {
                let res = view_client.send(GetBlock::latest()).await;
                if let Ok(Ok(block)) = res {
                    if block.header.height > 10 {
                        let request = RpcTransactionStatusRequest {
                            transaction_info: TransactionInfo::from_signed_tx(transaction),
                            wait_until: TxExecutionStatus::None,
                        };
                        let _ = client
                            .tx(request)
                            .map_err(|err| {
                                assert_eq!(
                                    *err.data.unwrap(),
                                    serde_json::json!("Node doesn't track this shard. Cannot determine whether the transaction is valid")
                                );
                                System::current().stop();
                            })
                            .map_ok(|_| panic!("Must not track shard"))
                            .await;
                        break;
                    }
                }
                sleep(std::time::Duration::from_millis(500)).await;
            }
        });
    });
}

#[test]
fn test_validators_by_epoch_id_current_epoch_not_fails() {
    init_integration_logger();

    let cluster = NodeCluster::default()
        .set_num_shards(1)
        .set_num_validator_seats(1)
        .set_num_lightclients(0)
        .set_epoch_length(10)
        .set_genesis_height(0);

    cluster.exec_until_stop(|_genesis, _rpc_addrs, clients| async move {
        let view_client = clients[0].1.clone();

        spawn_interruptible(async move {
            let final_block = loop {
                let res = view_client.send(GetBlock::latest()).await;
                if let Ok(Ok(block)) = res {
                    if block.header.height > 1 {
                        break block;
                    }
                }
            };

            let res = view_client
                .send(GetValidatorInfo {
                    epoch_reference: EpochReference::EpochId(EpochId(final_block.header.epoch_id)),
                })
                .await;

            match res {
                Ok(Ok(validators)) => {
                    assert_eq!(validators.current_validators.len(), 1);
                    System::current().stop();
                }
                err => panic!("Validators list by EpochId must succeed: {:?}", err),
            }
        });
    });
}
