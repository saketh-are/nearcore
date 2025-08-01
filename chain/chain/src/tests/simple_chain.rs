use crate::near_chain_primitives::error::BlockKnownError;
use crate::test_utils::{setup, wait_for_all_blocks_in_processing};
use crate::{Block, BlockProcessingArtifact, ChainStoreAccess, Error};
use assert_matches::assert_matches;
use near_async::time::{Clock, Duration, FakeClock, Utc};
use near_o11y::testonly::init_test_logger;
#[cfg(feature = "test_features")]
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::{hash::CryptoHash, test_utils::TestBlockBuilder, version::PROTOCOL_VERSION};
use num_rational::Ratio;
use std::sync::Arc;

#[test]
fn build_chain() {
    init_test_logger();
    let clock = FakeClock::new(Utc::from_unix_timestamp(1601510400).unwrap()); // 2020-10-01 00:00:00
    clock.advance(Duration::milliseconds(3444));
    let (mut chain, _, _, signer) = setup(clock.clock());
    assert_eq!(chain.head().unwrap().height, 0);

    // The hashes here will have to be modified after changes to the protocol.
    // In particular if you update protocol version or add new protocol
    // features.  If this assert is failing without you adding any new or
    // stabilizing any existing protocol features, this indicates bug in your
    // code which unexpectedly changes the protocol.
    //
    // To update the hashes you can use cargo-insta.  Note that you'll need to
    // run the test twice: once with default features and once with
    // 'nightly' feature enabled:
    //
    //     cargo install cargo-insta
    //     cargo insta test --accept -p near-chain                    -- tests::simple_chain::build_chain
    //     cargo insta test --accept -p near-chain --features nightly -- tests::simple_chain::build_chain
    let hash = chain.head().unwrap().last_block_hash;
    if cfg!(feature = "nightly") {
        // cspell:disable-next-line
        insta::assert_snapshot!(hash, @"24ZC3eGVvtFdTEok4wPGBzx3x61tWqQpves7nFvow2zf");
    } else {
        // cspell:disable-next-line
        insta::assert_snapshot!(hash, @"8786LxDz73XhJQroRrqBcxgDxAmntKR724w7vWPHPVq5");
    }

    for i in 1..5 {
        clock.advance(Duration::milliseconds(1));
        let prev_hash = *chain.head_header().unwrap().hash();
        let prev = chain.get_block(&prev_hash).unwrap();
        let block = TestBlockBuilder::new(clock.clock(), &prev, signer.clone()).build();
        chain.process_block_test(block).unwrap();
        assert_eq!(chain.head().unwrap().height, i as u64);
    }

    assert_eq!(chain.head().unwrap().height, 4);

    let hash = chain.head().unwrap().last_block_hash;
    if cfg!(feature = "nightly") {
        // cspell:disable-next-line
        insta::assert_snapshot!(hash, @"9enFQNcVUW65x3oW2iVdYSBxK9qFNETAixEQZLzXWeaQ");
    } else {
        // cspell:disable-next-line
        insta::assert_snapshot!(hash, @"6amtpQxcJYguKKEh4DUpgzrtJQ1JSJ9ivW8VQqf9Z2v2");
    }
}

#[test]
fn build_chain_with_orphans() {
    init_test_logger();
    let clock = Clock::real();
    let (mut chain, _, _, signer) = setup(clock.clone());
    let mut blocks = vec![chain.get_block(&chain.genesis().hash().clone()).unwrap()];
    for i in 1..4 {
        let block = TestBlockBuilder::new(clock.clone(), &blocks[i - 1], signer.clone()).build();
        blocks.push(block);
    }
    let last_block = &blocks[blocks.len() - 1];
    let block = Arc::new(Block::produce(
        PROTOCOL_VERSION,
        last_block.header(),
        10,
        last_block.header().block_ordinal() + 1,
        last_block.chunks().iter_raw().cloned().collect(),
        vec![vec![]; last_block.chunks().len()],
        *last_block.header().epoch_id(),
        *last_block.header().next_epoch_id(),
        None,
        vec![],
        Ratio::from_integer(0),
        0,
        100,
        Some(0),
        &*signer,
        *last_block.header().next_bp_hash(),
        CryptoHash::default(),
        clock,
        None,
        None,
        vec![],
    ));
    assert_matches!(chain.process_block_test(block).unwrap_err(), Error::Orphan);
    assert_matches!(chain.process_block_test(blocks.pop().unwrap()).unwrap_err(), Error::Orphan);
    assert_matches!(chain.process_block_test(blocks.pop().unwrap()).unwrap_err(), Error::Orphan);
    chain.process_block_test(blocks.pop().unwrap()).unwrap();
    while wait_for_all_blocks_in_processing(&mut chain) {
        chain.postprocess_ready_blocks(&mut BlockProcessingArtifact::default(), None);
    }
    assert_eq!(chain.head().unwrap().height, 10);
    assert_matches!(
        chain.process_block_test(blocks.pop().unwrap(),).unwrap_err(),
        Error::BlockKnown(BlockKnownError::KnownInStore)
    );
}

/// Checks that chain successfully processes blocks with skipped blocks and forks, but doesn't process block behind
/// final head.
#[test]
fn build_chain_with_skips_and_forks() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup(Clock::real());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let b1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
    let b2 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).height(2).build();
    let b3 = TestBlockBuilder::new(Clock::real(), &b1, signer.clone()).height(3).build();
    let b4 = TestBlockBuilder::new(Clock::real(), &b2, signer.clone()).height(4).build();
    let b5 = TestBlockBuilder::new(Clock::real(), &b4, signer.clone()).build();
    let b6 = TestBlockBuilder::new(Clock::real(), &b5, signer.clone()).build();
    assert!(chain.process_block_test(b1).is_ok());
    assert!(chain.process_block_test(b2).is_ok());
    assert!(chain.process_block_test(b3.clone()).is_ok());
    assert!(chain.process_block_test(b4).is_ok());
    assert!(chain.process_block_test(b5).is_ok());
    assert!(chain.process_block_test(b6).is_ok());
    assert!(chain.get_block_header_by_height(1).is_err());
    assert_eq!(chain.get_block_header_by_height(5).unwrap().height(), 5);
    assert_eq!(chain.get_block_header_by_height(6).unwrap().height(), 6);

    let c4 = TestBlockBuilder::new(Clock::real(), &b3, signer).height(4).build();
    assert_eq!(chain.final_head().unwrap().height, 4);
    assert_matches!(chain.process_block_test(c4), Err(Error::CannotBeFinalized));
}

/// Verifies that getting block by its height are updated correctly when blocks from different forks are
/// processed, especially when certain heights are skipped.
/// Chain looks as follows (variable name + height):
///
/// 0 -> b1 (c1) -> b2
///        |  \      \
///        |   \      -> d3 -------> d5 -> d6
///        |    \
///        |     ------> c3 -> c4
///        |
///        ------------------------------------> e7
///
/// Note that only block b1 is finalized, so all blocks here can be processed. But getting block by height should
/// return only blocks from the canonical chain.
#[test]
fn blocks_at_height() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup(Clock::real());
    let genesis = chain.get_block_by_height(0).unwrap();
    let b_1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).height(1).build();

    let b_2 = TestBlockBuilder::new(Clock::real(), &b_1, signer.clone()).height(2).build();

    let c_1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).height(1).build();
    let c_3 = TestBlockBuilder::new(Clock::real(), &c_1, signer.clone()).height(3).build();
    let c_4 = TestBlockBuilder::new(Clock::real(), &c_3, signer.clone()).height(4).build();

    let d_3 = TestBlockBuilder::new(Clock::real(), &b_2, signer.clone()).height(3).build();

    let d_5 = TestBlockBuilder::new(Clock::real(), &d_3, signer.clone()).height(5).build();
    let d_6 = TestBlockBuilder::new(Clock::real(), &d_5, signer.clone()).height(6).build();

    let e_7 = TestBlockBuilder::new(Clock::real(), &b_1, signer).height(7).build();

    let b_1_hash = *b_1.hash();
    let b_2_hash = *b_2.hash();

    let c_1_hash = *c_1.hash();
    let c_3_hash = *c_3.hash();
    let c_4_hash = *c_4.hash();

    let d_3_hash = *d_3.hash();
    let d_5_hash = *d_5.hash();
    let d_6_hash = *d_6.hash();

    let e_7_hash = *e_7.hash();

    assert_ne!(c_3_hash, d_3_hash);

    chain.process_block_test(b_1).unwrap();
    chain.process_block_test(b_2).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 2);

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &b_1_hash);
    assert_eq!(chain.get_block_header_by_height(2).unwrap().hash(), &b_2_hash);

    chain.process_block_test(c_1).unwrap();
    chain.process_block_test(c_3).unwrap();
    chain.process_block_test(c_4).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 4);

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &c_1_hash);
    assert!(chain.get_block_header_by_height(2).is_err());
    assert_eq!(chain.get_block_header_by_height(3).unwrap().hash(), &c_3_hash);
    assert_eq!(chain.get_block_header_by_height(4).unwrap().hash(), &c_4_hash);

    chain.process_block_test(d_3).unwrap();
    chain.process_block_test(d_5).unwrap();
    chain.process_block_test(d_6).unwrap();
    assert_eq!(chain.header_head().unwrap().height, 6);

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &b_1_hash);
    assert_eq!(chain.get_block_header_by_height(2).unwrap().hash(), &b_2_hash);
    assert_eq!(chain.get_block_header_by_height(3).unwrap().hash(), &d_3_hash);
    assert!(chain.get_block_header_by_height(4).is_err());
    assert_eq!(chain.get_block_header_by_height(5).unwrap().hash(), &d_5_hash);
    assert_eq!(chain.get_block_header_by_height(6).unwrap().hash(), &d_6_hash);

    chain.process_block_test(e_7).unwrap();

    assert_eq!(chain.get_block_header_by_height(1).unwrap().hash(), &b_1_hash);
    for h in 2..=5 {
        assert!(chain.get_block_header_by_height(h).is_err());
    }
    assert_eq!(chain.get_block_header_by_height(7).unwrap().hash(), &e_7_hash);
}

#[test]
fn next_blocks() {
    init_test_logger();
    let (mut chain, _, _, signer) = setup(Clock::real());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let b1 = TestBlockBuilder::new(Clock::real(), &genesis, signer.clone()).build();
    let b2 = TestBlockBuilder::new(Clock::real(), &b1, signer.clone()).height(2).build();
    let b3 = TestBlockBuilder::new(Clock::real(), &b1, signer.clone()).height(3).build();
    let b4 = TestBlockBuilder::new(Clock::real(), &b3, signer).height(4).build();
    let b1_hash = *b1.hash();
    let b2_hash = *b2.hash();
    let b3_hash = *b3.hash();
    let b4_hash = *b4.hash();
    assert!(chain.process_block_test(b1).is_ok());
    assert!(chain.process_block_test(b2).is_ok());
    assert_eq!(chain.mut_chain_store().get_next_block_hash(&b1_hash).unwrap(), b2_hash);
    assert!(chain.process_block_test(b3).is_ok());
    assert!(chain.process_block_test(b4).is_ok());
    assert_eq!(chain.mut_chain_store().get_next_block_hash(&b1_hash).unwrap(), b3_hash);
    assert_eq!(chain.mut_chain_store().get_next_block_hash(&b3_hash).unwrap(), b4_hash);
}

#[test]
fn block_chunk_headers_iter() {
    init_test_logger();
    let (chain, _, _, signer) = setup(Clock::real());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();
    let mut block = TestBlockBuilder::new(Clock::real(), &genesis, signer).build();
    let header = block.chunks().get(0).unwrap().clone();
    let mut fake_headers = vec![header; 16];

    // Make half of the headers have the same height as the block to appear as `New`
    for i in 0..fake_headers.len() / 2 {
        let fake_header = &mut fake_headers[i];
        *fake_header.height_included_mut() = block.header().height();
    }
    Arc::make_mut(&mut block).set_chunks(fake_headers);

    let chunks = block.chunks();
    let old_headers_count = chunks.iter_old().count();
    let new_headers_count = chunks.iter_new().count();
    let raw_headers_count = chunks.iter().count();

    assert_eq!(old_headers_count, 8);
    assert_eq!(new_headers_count, 8);
    assert_eq!(raw_headers_count, old_headers_count + new_headers_count);
}

/// Check that if block is processed while optimistic block is in processing,
/// it is marked as pending and can be processed later.
#[cfg(feature = "test_features")]
#[test]
fn test_pending_block() {
    init_test_logger();
    let clock = Clock::real();
    let (mut chain, _, _, signer) = setup(clock.clone());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();

    // Create block 1
    let block1 = TestBlockBuilder::new(clock.clone(), &genesis, signer.clone()).build();
    chain.process_block_test(block1.clone()).unwrap();

    // Create block 2 (but don't process it yet)
    let block2 = TestBlockBuilder::new(clock, &block1, signer.clone()).build();

    // Create optimistic block at height 2 based on block 1
    let optimistic_block = OptimisticBlock::adv_produce(
        &block1.header(),
        2,
        &*signer,
        block2.header().raw_timestamp(),
        None,
        near_primitives::optimistic_block::OptimisticBlockAdvType::Normal,
    );
    chain
        .process_optimistic_block(
            optimistic_block,
            block2.chunks().iter_raw().cloned().collect(),
            None,
        )
        .unwrap();

    let result = chain.start_process_block_async(
        block2.clone().into(),
        crate::Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
        None,
    );
    let Err(err) = &result else {
        panic!("Block processing should not succeed");
    };

    // Block must be rejected due to optimistic block in processing
    assert_matches!(err, Error::BlockPendingOptimisticExecution);

    // Verify the block is in the pending pool
    assert!(chain.blocks_pending_execution.contains_key(&block2.header().height()));

    // Process optimistic block
    let mut block_processing_artifact = BlockProcessingArtifact::default();
    while wait_for_all_blocks_in_processing(&mut chain) {
        chain.postprocess_ready_blocks(&mut block_processing_artifact, None);
    }

    // Verify the block is no longer in the pending pool
    assert!(!chain.blocks_pending_execution.contains_key(&block2.header().height()));

    // Wait for the pending block to be processed
    while wait_for_all_blocks_in_processing(&mut chain) {
        chain.postprocess_ready_blocks(&mut block_processing_artifact, None);
    }

    // Verify the block is now in the chain
    assert_eq!(chain.head().unwrap().height, 2);
}

/// Check chain behaviour on processing blocks on same height:
/// * If we receive the same block twice and it matches the optimistic block,
/// it should be marked as pending both times, in order not to trigger chunk
/// execution excessive in the regular flow.
/// * If we receive the a different block with the same height, it is a
/// malicious behaviour. We must process all these blocks anyway, because we
/// don't know which one gets finalized. To simplify optimistic logic, we
/// skip pending pool and process block right away.
#[cfg(feature = "test_features")]
#[test]
fn test_pending_block_same_height() {
    use near_crypto::{KeyType, Signature};

    init_test_logger();
    let clock = Clock::real();
    let (mut chain, _, _, signer) = setup(clock.clone());
    let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();

    // Create block 1
    let block1 = TestBlockBuilder::new(clock.clone(), &genesis, signer.clone()).build();
    chain.process_block_test(block1.clone()).unwrap();

    // Create block 2 and its copy
    let block2 = TestBlockBuilder::new(clock, &block1, signer.clone()).build();
    let block2a = block2.clone();

    // Create copy of block 2 with different hash.
    // The content still matches the optimistic execution, but the hash is
    // different.
    // Approvals can be set arbitrarily because we process blocks in
    // Provenance::PRODUCED mode.
    let mut block2b = block2.clone();
    let some_signature = Signature::from_parts(KeyType::ED25519, &[1; 64]).unwrap();
    Arc::make_mut(&mut block2b).mut_header().set_approvals(vec![Some(Box::new(some_signature))]);
    Arc::make_mut(&mut block2b).mut_header().resign(&*signer);
    assert!(block2a.hash() != block2b.hash());

    // Create an optimistic block at height 2
    let optimistic_block = OptimisticBlock::adv_produce(
        &block1.header(),
        2,
        &*signer,
        block2.header().raw_timestamp(),
        None,
        near_primitives::optimistic_block::OptimisticBlockAdvType::Normal,
    );

    // Process the optimistic block
    let chunk_headers = block2.chunks().iter_raw().cloned().collect();
    chain.process_optimistic_block(optimistic_block, chunk_headers, None).unwrap();

    // Check that processing the first copy is failed due to optimistic block
    // in processing.
    let result_a = chain.start_process_block_async(
        block2.into(),
        crate::Provenance::PRODUCED,
        &mut BlockProcessingArtifact::default(),
        None,
    );
    let Err(err) = &result_a else {
        panic!("Block processing should not succeed");
    };
    assert_matches!(err, Error::BlockPendingOptimisticExecution);

    // Check that the copy is also marked as pending.
    let result_b = chain.process_block_test(block2a);
    assert_matches!(result_b, Err(Error::BlockPendingOptimisticExecution));

    // Check that the copy with different hash is processed.
    let result_b = chain.process_block_test(block2b);
    assert_matches!(result_b, Ok(_));
    assert_eq!(chain.head().unwrap().height, 2);
}
