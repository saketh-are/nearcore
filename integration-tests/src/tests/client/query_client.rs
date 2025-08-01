use crate::env::setup::setup_no_network;
use actix::System;
use futures::{FutureExt, future};
use near_actix_test_utils::run_actix;
use near_async::time::{Clock, Duration};
use near_client::{
    GetBlock, GetBlockWithMerkleTree, GetExecutionOutcomesForBlock, Query, TxStatus,
};
use near_client_primitives::types::Status;
use near_crypto::InMemorySigner;
use near_network::client::{BlockResponse, ProcessTxRequest, ProcessTxResponse};
use near_network::types::PeerInfo;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_o11y::testonly::init_test_logger;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::merkle::PartialMerkleTree;
use near_primitives::test_utils::create_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{BlockReference, EpochId, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{QueryRequest, QueryResponseKind};
use num_rational::Ratio;

/// Query account from view client
#[test]
fn query_client() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            true,
        );
        let actor = actor_handles.view_client_actor.send(Query::new(
            BlockReference::latest(),
            QueryRequest::ViewAccount { account_id: "test".parse().unwrap() },
        ));
        let actor = actor.then(|res| {
            match res.unwrap().unwrap().kind {
                QueryResponseKind::ViewAccount(_) => (),
                _ => panic!("Invalid response"),
            }
            System::current().stop();
            future::ready(())
        });
        actix::spawn(actor);
    });
}

/// When we receive health check and the latest block's timestamp is in the future, the client
/// should not crash.
#[test]
fn query_status_not_crash() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "other".parse().unwrap(),
            true,
            false,
        );
        let signer = create_test_signer("test");
        let actor = actor_handles.view_client_actor.send(GetBlockWithMerkleTree::latest());
        let actor = actor.then(move |res| {
            let (block, block_merkle_tree) = res.unwrap().unwrap();
            let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);
            let header: BlockHeader = block.header.clone().into();
            block_merkle_tree.insert(*header.hash());
            let mut next_block = Block::produce(
                PROTOCOL_VERSION,
                &header,
                block.header.height + 1,
                header.block_ordinal() + 1,
                block.chunks.iter().cloned().map(|c| c.into()).collect(),
                vec![vec![]; block.chunks.len()],
                EpochId(block.header.next_epoch_id),
                EpochId(block.header.hash),
                None,
                vec![],
                Ratio::from_integer(0),
                0,
                100,
                None,
                &signer,
                block.header.next_bp_hash,
                block_merkle_tree.root(),
                Clock::real(),
                None,
                None,
                vec![],
            );
            let timestamp = next_block.header().timestamp();
            next_block
                .mut_header()
                .set_timestamp((timestamp + Duration::seconds(60)).unix_timestamp_nanos() as u64);
            next_block.mut_header().resign(&signer);

            actix::spawn(
                actor_handles
                    .client_actor
                    .send(
                        BlockResponse {
                            block: next_block.into(),
                            peer_id: PeerInfo::random().id,
                            was_requested: false,
                        }
                        .span_wrap(),
                    )
                    .then(move |_| {
                        actix::spawn(
                            actor_handles
                                .client_actor
                                .send(Status { is_health_check: true, detailed: false }.span_wrap())
                                .then(move |_| {
                                    System::current().stop();
                                    future::ready(())
                                }),
                        );
                        future::ready(())
                    }),
            );
            future::ready(())
        });
        actix::spawn(actor);
        near_network::test_utils::wait_or_panic(5000);
    });
}

#[test]
fn test_execution_outcome_for_chunk() {
    init_test_logger();
    run_actix(async {
        let actor_handles = setup_no_network(
            Clock::real(),
            vec!["test".parse().unwrap()],
            "test".parse().unwrap(),
            true,
            false,
        );
        let signer = InMemorySigner::test_signer(&"test".parse().unwrap());

        actix::spawn(async move {
            let block_hash = actor_handles
                .view_client_actor
                .send(GetBlock::latest())
                .await
                .unwrap()
                .unwrap()
                .header
                .hash;

            let transaction = SignedTransaction::send_money(
                1,
                "test".parse().unwrap(),
                "near".parse().unwrap(),
                &signer,
                10,
                block_hash,
            );
            let tx_hash = transaction.get_hash();
            let res = actor_handles
                .rpc_handler_actor
                .send(ProcessTxRequest { transaction, is_forwarded: false, check_only: false })
                .await
                .unwrap();
            assert!(matches!(res, ProcessTxResponse::ValidTx));

            actix::clock::sleep(std::time::Duration::from_millis(500)).await;
            let block_hash = actor_handles
                .view_client_actor
                .send(TxStatus {
                    tx_hash,
                    signer_account_id: "test".parse().unwrap(),
                    fetch_receipt: false,
                })
                .await
                .unwrap()
                .unwrap()
                .into_outcome()
                .unwrap()
                .transaction_outcome
                .block_hash;

            let mut execution_outcomes_in_block = actor_handles
                .view_client_actor
                .send(GetExecutionOutcomesForBlock { block_hash })
                .await
                .unwrap()
                .unwrap();
            assert_eq!(execution_outcomes_in_block.len(), 1);
            let outcomes = execution_outcomes_in_block.remove(&ShardId::new(0)).unwrap();
            assert_eq!(outcomes[0].id, tx_hash);
            System::current().stop();
        });
        near_network::test_utils::wait_or_panic(5000);
    });
}
