use std::ops::ControlFlow;
use std::str::FromStr;

use actix::System;
use awc::http::StatusCode;
use futures::{FutureExt, future};
use near_chain_configs::test_utils::TESTING_INIT_BALANCE;
use near_primitives::action::GlobalContractDeployMode;
use near_primitives::transaction::SignedTransaction;
use serde_json::json;

use near_actix_test_utils::run_actix;
use near_crypto::{InMemorySigner, Signature};
use near_jsonrpc::client::{ChunkId, JsonRpcClient, new_client};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_jsonrpc_primitives::types::validator::RpcValidatorsOrderedRequest;
use near_network::test_utils::wait_or_timeout;
use near_o11y::testonly::init_test_logger;
use near_primitives::account::{AccessKey, AccessKeyPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, BlockId, BlockReference, EpochId, ShardId, SyncCheckpoint,
};
use near_primitives::views::{FinalExecutionStatus, QueryRequest};
use near_time::Clock;

use near_jsonrpc_tests::{self as test_utils, test_with_client};

/// Retrieve blocks via json rpc
#[test]
fn test_block_by_id_height() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client.block_by_id(BlockId::Height(0)).await.unwrap();
        assert_eq!(block.author, "test1");
        assert_eq!(block.header.height, 0);
        assert_eq!(block.header.epoch_id.0.as_ref(), &[0; 32]);
        assert_eq!(block.header.hash.0.as_ref().len(), 32);
        assert_eq!(block.header.prev_hash.0.as_ref(), &[0; 32]);
        assert_eq!(
            block.header.prev_state_root,
            CryptoHash::from_str("CfKJ4CZqCCtLAESUk1RnWSrXvwenMVooWYrvoMsDrCAH").unwrap()
        );
        assert!(block.header.timestamp > 0);
        assert_eq!(block.header.validator_proposals.len(), 0);
    });
}

/// Retrieve blocks via json rpc
#[test]
fn test_block_by_id_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client.block_by_id(BlockId::Height(0)).await.unwrap();
        let same_block = client.block_by_id(BlockId::Hash(block.header.hash)).await.unwrap();
        assert_eq!(block.header.height, 0);
        assert_eq!(same_block.header.height, 0);
    });
}

/// Retrieve blocks via json rpc
#[test]
fn test_block_query() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block_response1 =
            client.block(BlockReference::BlockId(BlockId::Height(0))).await.unwrap();
        let block_response2 = client
            .block(BlockReference::BlockId(BlockId::Hash(block_response1.header.hash)))
            .await
            .unwrap();
        let block_response3 = client.block(BlockReference::latest()).await.unwrap();
        let block_response4 =
            client.block(BlockReference::SyncCheckpoint(SyncCheckpoint::Genesis)).await.unwrap();
        let block_response5 = client
            .block(BlockReference::SyncCheckpoint(SyncCheckpoint::EarliestAvailable))
            .await
            .unwrap();
        for block in
            &[block_response1, block_response2, block_response3, block_response4, block_response5]
        {
            assert_eq!(block.author, "test1");
            assert_eq!(block.header.height, 0);
            assert_eq!(block.header.epoch_id.as_ref(), &[0; 32]);
            assert_eq!(block.header.hash.as_ref().len(), 32);
            assert_eq!(block.header.prev_hash.as_ref(), &[0; 32]);
            assert_eq!(
                block.header.prev_state_root,
                CryptoHash::from_str("CfKJ4CZqCCtLAESUk1RnWSrXvwenMVooWYrvoMsDrCAH").unwrap()
            );
            assert!(block.header.timestamp > 0);
            assert_eq!(block.header.validator_proposals.len(), 0);
        }
    });
}

/// Retrieve chunk via json rpc
#[test]
fn test_chunk_by_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let chunk =
            client.chunk(ChunkId::BlockShardId(BlockId::Height(0), ShardId::new(0))).await.unwrap();
        assert_eq!(chunk.author, "test1");
        assert_eq!(chunk.header.balance_burnt, 0);
        assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.encoded_length, 8);
        assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
        assert_eq!(chunk.header.gas_limit, 1000000);
        assert_eq!(chunk.header.gas_used, 0);
        assert_eq!(chunk.header.height_created, 0);
        assert_eq!(chunk.header.height_included, 0);
        assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
        assert_eq!(chunk.header.rent_paid, 0);
        assert_eq!(chunk.header.shard_id, ShardId::new(0));
        assert!(if let Signature::ED25519(_) = chunk.header.signature { true } else { false });
        assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
        assert_eq!(chunk.header.validator_proposals, vec![]);
        assert_eq!(chunk.header.validator_reward, 0);
        let same_chunk = client.chunk(ChunkId::Hash(chunk.header.chunk_hash)).await.unwrap();
        assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
    });
}

/// Retrieve chunk via json rpc
#[test]
fn test_chunk_invalid_shard_id() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let chunk =
            client.chunk(ChunkId::BlockShardId(BlockId::Height(0), ShardId::new(100))).await;
        match chunk {
            Ok(_) => panic!("should result in an error"),
            Err(e) => {
                let s = serde_json::to_string(&e.data.unwrap()).unwrap();
                assert!(s.starts_with("\"Shard id 100 does not exist"));
            }
        }
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_account() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.status().await.unwrap();
        let block_hash = status.sync_info.latest_block_hash;
        let query_response =
            client.query_by_path("account/test".to_string(), "".to_string()).await.unwrap();
        assert_eq!(query_response.block_height, 0);
        assert_eq!(query_response.block_hash, block_hash);
        let account_info = if let QueryResponseKind::ViewAccount(account) = query_response.kind {
            account
        } else {
            panic!("queried account, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(account_info.amount, TESTING_INIT_BALANCE);
        assert_eq!(account_info.code_hash, CryptoHash::default());
        assert_eq!(account_info.locked, 0);
        assert_eq!(account_info.storage_paid_at, 0);
        assert_eq!(account_info.global_contract_hash, None);
        assert_eq!(account_info.global_contract_account_id, None);
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_account() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.status().await.unwrap();
        let block_hash = status.sync_info.latest_block_hash;
        let query_response_1 = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        let query_response_2 = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Height(0)),
                request: QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        let query_response_3 = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::BlockId(BlockId::Hash(block_hash)),
                request: QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        for query_response in &[query_response_1, query_response_2, query_response_3] {
            assert_eq!(query_response.block_height, 0);
            assert_eq!(query_response.block_hash, block_hash);
            let account_info = if let QueryResponseKind::ViewAccount(ref account) =
                query_response.kind
            {
                account
            } else {
                panic!("queried account, but received something else: {:?}", query_response.kind);
            };
            assert_eq!(account_info.amount, TESTING_INIT_BALANCE);
            assert_eq!(account_info.code_hash, CryptoHash::default());
            assert_eq!(account_info.locked, 0);
            assert_eq!(account_info.storage_paid_at, 0);
            assert_eq!(account_info.global_contract_hash, None);
            assert_eq!(account_info.global_contract_account_id, None);
        }
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_access_keys() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let account = "test".parse().unwrap();
        let signer = InMemorySigner::test_signer(&account);
        let query_response =
            client.query_by_path("access_key/test".to_string(), "".to_string()).await.unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind
        {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_keys.keys.len(), 1);
        assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
        assert_eq!(access_keys.keys[0].public_key, signer.public_key());
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_access_keys() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKeyList { account_id: "test".parse().unwrap() },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_keys = if let QueryResponseKind::AccessKeyList(access_keys) = query_response.kind
        {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        let signer = InMemorySigner::test_signer(&"test".parse().unwrap());
        assert_eq!(access_keys.keys.len(), 1);
        assert_eq!(access_keys.keys[0].access_key, AccessKey::full_access().into());
        assert_eq!(access_keys.keys[0].public_key, signer.public_key());
    });
}

/// Connect to json rpc and query account info with soft-deprecated query API.
#[test]
fn test_query_by_path_access_key() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let account = "test".parse().unwrap();
        let signer = InMemorySigner::test_signer(&account);
        let query_response = client
            .query_by_path(format!("access_key/test/{}", signer.public_key()), "".to_string())
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_key.nonce, 0);
        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
    });
}

/// Connect to json rpc and query account info.
#[test]
fn test_query_access_key() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let account = "test".parse().unwrap();
        let signer = InMemorySigner::test_signer(&account);
        let query_response = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewAccessKey {
                    account_id: account.clone(),
                    public_key: signer.public_key(),
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let access_key = if let QueryResponseKind::AccessKey(access_keys) = query_response.kind {
            access_keys
        } else {
            panic!("queried access keys, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(access_key.nonce, 0);
        assert_eq!(access_key.permission, AccessKeyPermission::FullAccess.into());
    });
}

/// Connect to json rpc and query state.
#[test]
fn test_query_state() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let query_response = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewState {
                    account_id: "test".parse().unwrap(),
                    prefix: vec![].into(),
                    include_proof: false,
                },
            })
            .await
            .unwrap();
        assert_eq!(query_response.block_height, 0);
        let state = if let QueryResponseKind::ViewState(state) = query_response.kind {
            state
        } else {
            panic!("queried state, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(state.values.len(), 0);
    });
}

/// Connect to json rpc and call function
#[test]
fn test_query_call_function() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let account = "test".parse().unwrap();
        let code = near_test_contracts::rs_contract().to_vec();
        deploy_contract(&client, &account, code.clone()).await;

        let query_response = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::CallFunction {
                    account_id: "test".parse().unwrap(),
                    method_name: "run_test".to_string(),
                    args: vec![].into(),
                },
            })
            .await
            .unwrap();
        let call_result = if let QueryResponseKind::CallResult(call_result) = query_response.kind {
            call_result
        } else {
            panic!(
                "expected a call function result, but received something else: {:?}",
                query_response.kind
            );
        };
        assert_eq!(call_result.result, 10i32.to_le_bytes());
        assert_eq!(call_result.logs.len(), 0);
    });
}

/// query contract code
#[test]
fn test_query_contract_code() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let account = "test".parse().unwrap();
        let code = near_test_contracts::rs_contract().to_vec();
        deploy_contract(&client, &account, code.clone()).await;

        let query_response = client
            .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                block_reference: BlockReference::latest(),
                request: QueryRequest::ViewCode { account_id: account.clone() },
            })
            .await
            .unwrap();
        let response_code = if let QueryResponseKind::ViewCode(code) = query_response.kind {
            code
        } else {
            panic!("queried code, but received something else: {:?}", query_response.kind);
        };
        assert_eq!(response_code.code, code);
        assert_eq!(response_code.hash, CryptoHash::hash_bytes(&code));
    });
}

async fn deploy_contract(client: &JsonRpcClient, account: &AccountId, code: Vec<u8>) {
    let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
    let signer = InMemorySigner::test_signer(&account);
    let tx = SignedTransaction::deploy_contract(1, &account, code, &signer, block_hash);
    let bytes = borsh::to_vec(&tx).unwrap();
    let result =
        client.broadcast_tx_commit(near_primitives::serialize::to_base64(&bytes)).await.unwrap();
    assert_eq!(
        result.final_execution_outcome.unwrap().into_outcome().status,
        FinalExecutionStatus::SuccessValue(Vec::new())
    );
}

/// Retrieve client status via JSON RPC.
#[test]
fn test_status() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let status = client.status().await.unwrap();
        assert_eq!(status.chain_id, "unittest");
        assert_eq!(status.sync_info.latest_block_height, 0);
        assert_eq!(status.sync_info.syncing, false);
        assert_eq!(status.sync_info.epoch_id, Some(EpochId::default()));
        assert_eq!(status.sync_info.epoch_start_height, Some(0));
    });
}

/// Retrieve client status failed.
#[test]
fn test_status_fail() {
    init_test_logger();

    run_actix(async {
        let (_, addr, _runtime_temp_dir) =
            test_utils::start_all(Clock::real(), test_utils::NodeType::NonValidator);

        let client = new_client(&format!("http://{}", addr));
        wait_or_timeout(100, 10000, || async {
            let res = client.health().await;
            if res.is_err() {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        System::current().stop()
    });
}

/// Check health fails when node is absent.
#[test]
fn test_health_fail() {
    init_test_logger();

    run_actix(async {
        let client = new_client("http://127.0.0.1:12322/health");
        actix::spawn(client.health().then(|res| {
            assert!(res.is_err());
            System::current().stop();
            future::ready(())
        }));
    });
}

/// Health fails when node doesn't produce block for period of time.
#[test]
fn test_health_fail_no_blocks() {
    init_test_logger();

    run_actix(async {
        let (_, addr, _runtime_temp_dir) =
            test_utils::start_all(Clock::real(), test_utils::NodeType::NonValidator);

        let client = new_client(&format!("http://{}", addr));
        wait_or_timeout(300, 10000, || async {
            let res = client.health().await;
            if res.is_err() {
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        })
        .await
        .unwrap();
        System::current().stop()
    });
}

/// Retrieve client health.
#[test]
fn test_health_ok() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let health = client.health().await;
        assert_eq!(health, Ok(()));
    });
}

#[test]
fn test_validators_ordered() {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let validators = client
            .EXPERIMENTAL_validators_ordered(RpcValidatorsOrderedRequest { block_id: None })
            .await
            .unwrap();
        assert_eq!(
            validators.into_iter().map(|v| v.take_account_id()).collect::<Vec<_>>(),
            vec!["test1"]
        )
    });
}

/// Retrieve genesis config via JSON RPC.
/// WARNING: Be mindful about changing genesis structure as it is part of the public protocol!
#[test]
fn test_genesis_config() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let genesis_config = client.genesis_config().await.unwrap();
        if !cfg!(feature = "nightly") {
            assert_eq!(
                genesis_config["protocol_version"].as_u64().unwrap(),
                near_primitives::version::PROTOCOL_VERSION as u64
            );
        }
        assert!(!genesis_config["chain_id"].as_str().unwrap().is_empty());
        assert!(!genesis_config.as_object().unwrap().contains_key("records"));
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price_by_height() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let gas_price = client.gas_price(Some(BlockId::Height(0))).await.unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price_by_hash() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let block = client.block(BlockReference::BlockId(BlockId::Height(0))).await.unwrap();
        let gas_price = client.gas_price(Some(BlockId::Hash(block.header.hash))).await.unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

/// Retrieve gas price
#[test]
fn test_gas_price() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let gas_price = client.gas_price(None).await.unwrap();
        assert!(gas_price.gas_price > 0);
    });
}

#[test]
fn test_invalid_methods() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let method_names = vec![
            serde_json::json!(
                "\u{0}\u{0}\u{0}k\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}\u{0}SRP"
            ),
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!(0),
            serde_json::json!(""),
        ];

        for method_name in method_names {
            let json = serde_json::json!({
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": &method_name,
                "params": serde_json::json!([]),
            });
            let response = &mut client
                .client
                .post(&client.server_addr)
                .insert_header(("Content-Type", "application/json"))
                .send_json(&json)
                .await
                .unwrap();

            assert_eq!(response.status(), StatusCode::BAD_REQUEST);

            let response =
                serde_json::from_value::<serde_json::Value>(response.json().await.unwrap())
                    .unwrap();

            assert!(
                response["error"] != serde_json::json!(null),
                "Invalid method {:?} must return error",
                method_name
            );
        }
    });
}

#[test]
fn test_parse_error_status_code() {
    // cspell:ignore badtx frolik
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let json = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "tx",
            "params": serde_json::json!({
                "tx": "badtx",
                "sender_account_id": "frolik.near"
            }),
        });

        let response = &mut client
            .client
            .post(&client.server_addr)
            .insert_header(("Content-Type", "application/json"))
            .send_json(&json)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    });
}

#[test]
fn slow_test_bad_handler_error_status_code() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let json = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "tx",
            "params": serde_json::json!({
                "tx_hash": CryptoHash::new().to_string(),
                "sender_account_id": "frolik.near"
            }),
        });

        let response = &mut client
            .client
            .post(&client.server_addr)
            .insert_header(("Content-Type", "application/json"))
            .send_json(&json)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::REQUEST_TIMEOUT);
    });
}

#[test]
fn test_good_handler_error_status_code() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let json = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "EXPERIMENTAL_receipt",
            "params": serde_json::json!({"receipt_id": CryptoHash::new().to_string()})
        });

        let response = &mut client
            .client
            .post(&client.server_addr)
            .insert_header(("Content-Type", "application/json"))
            .send_json(&json)
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    });
}

#[test]
fn test_get_chunk_with_object_in_params() {
    test_with_client!(test_utils::NodeType::NonValidator, client, async move {
        let chunk: near_primitives::views::ChunkView = test_utils::call_method(
            &client.client,
            &client.server_addr,
            "chunk",
            json!({
                "block_id": 0u64,
                "shard_id": 0u64,
            }),
        )
        .await
        .unwrap();
        assert_eq!(chunk.author, "test1");
        assert_eq!(chunk.header.balance_burnt, 0);
        assert_eq!(chunk.header.chunk_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.encoded_length, 8);
        assert_eq!(chunk.header.encoded_merkle_root.as_ref().len(), 32);
        assert_eq!(chunk.header.gas_limit, 1000000);
        assert_eq!(chunk.header.gas_used, 0);
        assert_eq!(chunk.header.height_created, 0);
        assert_eq!(chunk.header.height_included, 0);
        assert_eq!(chunk.header.outgoing_receipts_root.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_block_hash.as_ref().len(), 32);
        assert_eq!(chunk.header.prev_state_root.as_ref().len(), 32);
        assert_eq!(chunk.header.rent_paid, 0);
        assert_eq!(chunk.header.shard_id, ShardId::new(0));
        assert!(if let Signature::ED25519(_) = chunk.header.signature { true } else { false });
        assert_eq!(chunk.header.tx_root.as_ref(), &[0; 32]);
        assert_eq!(chunk.header.validator_proposals, vec![]);
        assert_eq!(chunk.header.validator_reward, 0);
        let same_chunk = client.chunk(ChunkId::Hash(chunk.header.chunk_hash)).await.unwrap();
        assert_eq!(chunk.header.chunk_hash, same_chunk.header.chunk_hash);
    });
}

#[test]
fn test_query_global_contract_code_by_hash() {
    test_query_global_contract_code(GlobalContractDeployMode::CodeHash);
}

#[test]
fn test_query_global_contract_code_by_account_id() {
    test_query_global_contract_code(GlobalContractDeployMode::AccountId);
}

fn test_query_global_contract_code(deploy_mode: GlobalContractDeployMode) {
    test_with_client!(test_utils::NodeType::Validator, client, async move {
        let account = "test".parse().unwrap();
        let code = near_test_contracts::rs_contract().to_vec();
        let code_hash = CryptoHash::hash_bytes(&code);
        deploy_global_contract(&client, &account, code.clone(), deploy_mode.clone()).await;

        // Global contract distribution takes time, so we might not be able to query the contract
        // immediately after broadcast_tx_commit.
        wait_or_timeout(100, 10000, || async {
            let request = match deploy_mode {
                GlobalContractDeployMode::CodeHash => {
                    QueryRequest::ViewGlobalContractCode { code_hash }
                }
                GlobalContractDeployMode::AccountId => {
                    QueryRequest::ViewGlobalContractCodeByAccountId { account_id: account.clone() }
                }
            };
            let query_res = client
                .query(near_jsonrpc_primitives::types::query::RpcQueryRequest {
                    block_reference: BlockReference::latest(),
                    request,
                })
                .await;

            let Ok(query_response) = query_res else {
                return ControlFlow::Continue(());
            };

            let response_code = if let QueryResponseKind::ViewCode(code) = query_response.kind {
                code
            } else {
                panic!("queried code, but received something else: {:?}", query_response.kind);
            };
            assert_eq!(response_code.code, code);
            assert_eq!(response_code.hash, code_hash);
            ControlFlow::Break(())
        })
        .await
        .unwrap();
    });
}

async fn deploy_global_contract(
    client: &JsonRpcClient,
    account: &AccountId,
    code: Vec<u8>,
    deploy_mode: GlobalContractDeployMode,
) {
    let block_hash = client.block(BlockReference::latest()).await.unwrap().header.hash;
    let signer = InMemorySigner::test_signer(&account);
    let tx = SignedTransaction::deploy_global_contract(
        1,
        account.clone(),
        code,
        &signer,
        block_hash,
        deploy_mode,
    );
    let bytes = borsh::to_vec(&tx).unwrap();
    let result =
        client.broadcast_tx_commit(near_primitives::serialize::to_base64(&bytes)).await.unwrap();
    assert_eq!(
        result.final_execution_outcome.unwrap().into_outcome().status,
        FinalExecutionStatus::SuccessValue(Vec::new())
    );
}
