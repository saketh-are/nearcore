// FIXME(nagisa): Is there a good reason we're triggering this? Luckily though this is just test
// code so we're in the clear.
#![allow(clippy::arc_with_non_send_sync)]

use crate::Client;
use crate::chunk_producer::ProduceChunkResult;
use crate::client::CatchupState;
use actix_rt::System;
use itertools::Itertools;
use near_async::messaging::Sender;
use near_chain::chain::{BlockCatchUpRequest, do_apply_chunks};
use near_chain::test_utils::{wait_for_all_blocks_in_processing, wait_for_block_in_processing};
use near_chain::{ChainStoreAccess, Provenance};
use near_client_primitives::types::Error;
use near_primitives::bandwidth_scheduler::BandwidthRequests;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::merkle::{PartialMerkleTree, merklize};
use near_primitives::optimistic_block::BlockToApply;
use near_primitives::sharding::{EncodedShardChunk, ShardChunk, ShardChunkWithEncoding};
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::transaction::ValidatedTransaction;
use near_primitives::types::{BlockHeight, EpochId, ShardId};
use near_primitives::utils::MaybeValidated;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::ShardUId;
use num_rational::Ratio;
use parking_lot::RwLock;
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::mem::swap;
use std::sync::Arc;

impl Client {
    /// Unlike Client::start_process_block, which returns before the block finishes processing
    /// This function waits until the block is processed.
    /// `should_produce_chunk`: Normally, if a block is accepted, client will try to produce
    ///                         chunks for the next block if it is the chunk producer.
    ///                         If `should_produce_chunk` is set to false, client will skip the
    ///                         chunk production. This is useful in tests that need to tweak
    ///                         the produced chunk content.
    fn process_block_sync_with_produce_chunk_options(
        &mut self,
        block: MaybeValidated<Arc<Block>>,
        provenance: Provenance,
        should_produce_chunk: bool,
        allow_errors: bool,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.start_process_block(block, provenance, None)?;
        wait_for_all_blocks_in_processing(&mut self.chain);
        let (accepted_blocks, errors) = self.postprocess_ready_blocks(None, should_produce_chunk);
        if !allow_errors {
            assert!(errors.is_empty(), "unexpected errors when processing blocks: {errors:#?}");
        }
        Ok(accepted_blocks)
    }

    pub fn process_block_test(
        &mut self,
        block: MaybeValidated<Arc<Block>>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, true, false)
    }

    pub fn process_block_test_no_produce_chunk(
        &mut self,
        block: MaybeValidated<Arc<Block>>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, false, false)
    }

    pub fn process_block_test_no_produce_chunk_allow_errors(
        &mut self,
        block: MaybeValidated<Arc<Block>>,
        provenance: Provenance,
    ) -> Result<Vec<CryptoHash>, near_chain::Error> {
        self.process_block_sync_with_produce_chunk_options(block, provenance, false, true)
    }

    /// This function finishes processing all blocks that started being processed.
    pub fn finish_blocks_in_processing(&mut self) -> Vec<CryptoHash> {
        let mut accepted_blocks = vec![];
        while wait_for_all_blocks_in_processing(&mut self.chain) {
            accepted_blocks.extend(self.postprocess_ready_blocks(None, true).0);
        }
        accepted_blocks
    }

    /// This function finishes processing block with hash `hash`, if the processing of that block
    /// has started.
    pub fn finish_block_in_processing(&mut self, hash: &CryptoHash) -> Vec<CryptoHash> {
        if let Ok(()) = wait_for_block_in_processing(&mut self.chain, hash) {
            let (accepted_blocks, _) = self.postprocess_ready_blocks(None, true);
            return accepted_blocks;
        }
        vec![]
    }

    /// Manually produce a single chunk on the given shard and send out the corresponding network messages
    pub fn produce_one_chunk(&mut self, height: BlockHeight, shard_id: ShardId) -> ShardChunk {
        let ProduceChunkResult { chunk, encoded_chunk_parts_paths: merkle_paths, receipts } =
            create_chunk_on_height_for_shard(self, height, shard_id);
        let shard_chunk = chunk.to_shard_chunk().clone();
        let signer = self.validator_signer.get();
        self.persist_and_distribute_encoded_chunk(
            chunk,
            merkle_paths,
            receipts,
            signer.as_ref().unwrap().validator_id().clone(),
        )
        .unwrap();
        let prev_block = self.chain.get_block(shard_chunk.prev_block()).unwrap();
        let prev_chunk_header =
            self.epoch_manager.get_prev_chunk_header(&prev_block, shard_chunk.shard_id()).unwrap();
        self.send_chunk_state_witness_to_chunk_validators(
            &self.epoch_manager.get_epoch_id_from_prev_block(shard_chunk.prev_block()).unwrap(),
            prev_block.header(),
            &prev_chunk_header,
            &shard_chunk,
        )
        .unwrap();
        shard_chunk
    }
}

fn create_chunk_on_height_for_shard(
    client: &mut Client,
    next_height: BlockHeight,
    shard_id: ShardId,
) -> ProduceChunkResult {
    let last_block_hash = client.chain.head().unwrap().last_block_hash;
    let last_block = client.chain.get_block(&last_block_hash).unwrap();
    let signer = client.validator_signer.get().unwrap();
    client
        .chunk_producer
        .produce_chunk(
            &last_block,
            &client.epoch_manager.get_epoch_id_from_prev_block(&last_block_hash).unwrap(),
            client.epoch_manager.get_prev_chunk_header(&last_block, shard_id).unwrap(),
            next_height,
            shard_id,
            &signer,
            &client.chain.transaction_validity_check(last_block.header().clone().into()),
        )
        .unwrap()
        .unwrap()
}

pub fn create_chunk_on_height(client: &mut Client, next_height: BlockHeight) -> ProduceChunkResult {
    create_chunk_on_height_for_shard(client, next_height, ShardUId::single_shard().shard_id())
}

/// Create a chunk with specified transactions and possibly a new state root.
pub fn create_chunk(
    client: &mut Client,
    validated_txs: Vec<ValidatedTransaction>,
) -> (ProduceChunkResult, Arc<Block>) {
    let last_block = client.chain.get_block_by_height(client.chain.head().unwrap().height).unwrap();
    let next_height = last_block.header().height() + 1;
    let signer = client.validator_signer.get().unwrap();
    let ProduceChunkResult { chunk, encoded_chunk_parts_paths: mut merkle_paths, receipts } =
        client
            .chunk_producer
            .produce_chunk(
                &last_block,
                last_block.header().epoch_id(),
                last_block.chunks()[0].clone(),
                next_height,
                ShardId::new(0),
                &signer,
                &client.chain.transaction_validity_check(last_block.header().clone().into()),
            )
            .unwrap()
            .unwrap();
    let mut encoded_chunk = chunk.into_parts().1;
    let signed_txs = validated_txs
        .iter()
        .cloned()
        .map(|validated_tx| validated_tx.into_signed_tx())
        .collect::<Vec<_>>();
    let tx_root = merklize(&signed_txs).0;

    // reconstruct the chunk with changes
    {
        // The best way it to decode chunk, replace transactions and then recreate encoded chunk.
        let total_parts = client.chain.epoch_manager.num_total_parts();
        let data_parts = client.chain.epoch_manager.num_data_parts();
        let decoded_chunk = encoded_chunk.decode_chunk().unwrap();
        let parity_parts = total_parts - data_parts;
        let rs = ReedSolomon::new(data_parts, parity_parts).unwrap();

        let header = encoded_chunk.cloned_header();
        let (new_chunk, mut new_merkle_paths) = ShardChunkWithEncoding::new(
            *header.prev_block_hash(),
            header.prev_state_root(),
            *header.prev_outcome_root(),
            header.height_created(),
            header.shard_id(),
            header.prev_gas_used(),
            header.gas_limit(),
            header.prev_balance_burnt(),
            header.prev_validator_proposals().collect(),
            validated_txs,
            decoded_chunk.prev_outgoing_receipts().to_vec(),
            *header.prev_outgoing_receipts_root(),
            tx_root,
            header.congestion_info(),
            header.bandwidth_requests().cloned().unwrap_or_else(BandwidthRequests::empty),
            &*signer,
            &rs,
        );
        let mut new_encoded_chunk = new_chunk.into_parts().1;
        swap(&mut encoded_chunk, &mut new_encoded_chunk);
        swap(&mut merkle_paths, &mut new_merkle_paths);
    }
    match &mut encoded_chunk {
        EncodedShardChunk::V1(chunk) => {
            chunk.header.height_included = next_height;
        }
        EncodedShardChunk::V2(chunk) => {
            *chunk.header.height_included_mut() = next_height;
        }
    }
    let block_merkle_tree =
        client.chain.chain_store().get_block_merkle_tree(last_block.hash()).unwrap();
    let mut block_merkle_tree = PartialMerkleTree::clone(&block_merkle_tree);

    let signer = client.validator_signer.get().unwrap();
    let endorsement =
        ChunkEndorsement::new(EpochId::default(), &encoded_chunk.cloned_header(), signer.as_ref());
    block_merkle_tree.insert(*last_block.hash());
    let block = Arc::new(Block::produce(
        PROTOCOL_VERSION,
        last_block.header(),
        next_height,
        last_block.header().block_ordinal() + 1,
        vec![encoded_chunk.cloned_header()],
        vec![vec![Some(Box::new(endorsement.signature()))]],
        *last_block.header().epoch_id(),
        *last_block.header().next_epoch_id(),
        None,
        vec![],
        Ratio::new(0, 1),
        0,
        100,
        None,
        &*client.validator_signer.get().unwrap(),
        *last_block.header().next_bp_hash(),
        block_merkle_tree.root(),
        client.clock.clone(),
        None,
        None,
        vec![],
    ));
    let chunk = ShardChunkWithEncoding::from_encoded_shard_chunk(encoded_chunk).unwrap();
    (ProduceChunkResult { chunk, encoded_chunk_parts_paths: merkle_paths, receipts }, block)
}

/// Keep running catchup until there is no more catchup work that can be done
/// Note that this function does not necessarily mean that all blocks are caught up.
/// It's possible that some blocks that need to be caught up are still being processed
/// and the catchup process can't catch up on these blocks yet.
pub fn run_catchup(client: &mut Client) -> Result<(), Error> {
    let block_messages = Arc::new(RwLock::new(vec![]));
    let block_inside_messages = block_messages.clone();
    let block_catch_up = Sender::from_fn(move |msg: BlockCatchUpRequest| {
        block_inside_messages.write().push(msg);
    });
    let _ = System::new();
    loop {
        client.run_catchup(&block_catch_up, None)?;
        let mut catchup_done = true;
        for msg in block_messages.write().drain(..) {
            let results =
                do_apply_chunks(BlockToApply::Normal(msg.block_hash), msg.block_height, msg.work)
                    .into_iter()
                    .map(|res| res.2)
                    .collect_vec();
            if let Some(CatchupState { catchup, .. }) =
                client.catchup_state_syncs.get_mut(&msg.sync_hash)
            {
                assert!(catchup.scheduled_blocks.remove(&msg.block_hash));
                catchup.processed_blocks.insert(msg.block_hash, results);
            } else {
                panic!("block catch up processing result from unknown sync hash");
            }
            catchup_done = false;
        }
        if catchup_done {
            break;
        }
    }
    Ok(())
}
