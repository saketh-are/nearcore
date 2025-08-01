use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::{fmt, io};

use itertools::Itertools;
use near_chain_configs::GCConfig;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_epoch_manager::shard_assignment::shard_id_to_uid;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::get_block_shard_uid;
use near_primitives::state_sync::{StateHeaderKey, StatePartKey};
use near_primitives::types::{BlockHeight, BlockHeightDelta, EpochId, NumBlocks, ShardId};
use near_primitives::utils::{
    get_block_shard_id, get_block_shard_id_rev, get_outcome_id_block_hash, get_receipt_proof_key,
    index_to_bytes,
};
use near_store::adapter::trie_store::get_shard_uid_mapping;
use near_store::adapter::{StoreAdapter, StoreUpdateAdapter};
use near_store::{DBCol, KeyForStateChanges, ShardTries, ShardUId};

use crate::types::RuntimeAdapter;
use crate::{Chain, ChainStore, ChainStoreAccess, ChainStoreUpdate, metrics};

#[derive(Clone)]
pub enum GCMode {
    Fork(ShardTries),
    Canonical(ShardTries),
    StateSync { clear_block_info: bool },
}

impl fmt::Debug for GCMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            GCMode::Fork(_) => write!(f, "GCMode::Fork"),
            GCMode::Canonical(_) => write!(f, "GCMode::Canonical"),
            GCMode::StateSync { .. } => write!(f, "GCMode::StateSync"),
        }
    }
}

/// Both functions here are only used for testing as they create convenient
/// wrappers that allow us to do correctness integration testing without having
/// to fully spin up GCActor
///
/// TODO - the reset_data_pre_state_sync function seems to also be used in
/// production code. It's used in update_sync_status <- handle_sync_needed <- run_sync_step
impl Chain {
    pub fn clear_data(&mut self, gc_config: &GCConfig) -> Result<(), Error> {
        let runtime_adapter = self.runtime_adapter.clone();
        let epoch_manager = self.epoch_manager.clone();
        let shard_tracker = self.shard_tracker.clone();
        self.mut_chain_store().clear_data(gc_config, runtime_adapter, epoch_manager, &shard_tracker)
    }

    pub fn reset_data_pre_state_sync(&mut self, sync_hash: CryptoHash) -> Result<(), Error> {
        let runtime_adapter = self.runtime_adapter.clone();
        let epoch_manager = self.epoch_manager.clone();
        self.mut_chain_store().reset_data_pre_state_sync(sync_hash, runtime_adapter, epoch_manager)
    }
}

impl ChainStore {
    // GC CONTRACT
    // ===
    //
    // Prerequisites, guaranteed by the System:
    // 1. Genesis block is available and should not be removed by GC.
    // 2. No block in storage except Genesis has height lower or equal to `genesis_height`.
    // 3. There is known lowest block height (Tail) came from Genesis or State Sync.
    //    a. Tail is always on the Canonical Chain.
    //    b. Only one Tail exists.
    //    c. Tail's height is higher than or equal to `genesis_height`,
    // 4. There is a known highest block height (Head).
    //    a. Head is always on the Canonical Chain.
    // 5. All blocks in the storage have heights in range [Tail; Head].
    //    a. All forks end up on height of Head or lower.
    // 6. If block A is ancestor of block B, height of A is strictly less then height of B.
    // 7. (Property 1). A block with the lowest height among all the blocks at which the fork has started,
    //    i.e. all the blocks with the outgoing degree 2 or more,
    //    has the least height among all blocks on the fork.
    // 8. (Property 2). The oldest block where the fork happened is never affected
    //    by Canonical Chain Switching and always stays on Canonical Chain.
    //
    // Overall:
    // 1. GC procedure is handled by `clear_old_blocks_data()` function.
    // 2. `clear_old_blocks_data()` runs GC process for all blocks from the Tail to GC Stop Height provided by Epoch Manager.
    // 3. `clear_old_blocks_data()` executes separately:
    //    a. Forks Clearing runs for each height from Tail up to GC Stop Height.
    //    b. Canonical Chain Clearing (CCC) from (Tail + 1) up to GC Stop Height.
    //       i) After CCC for the last block of an epoch, we check what shards tracked in the epoch qualify for trie State cleanup.
    //       ii) A shard qualify for trie State cleanup, if we did not care about it up to the Head,
    //           and we won't care about it in the next epoch after the Head.
    //       iii) `gc_state()` handles trie State cleanup, and it uses current tracking config (`shard_tracker` and optional validator ID),
    //            to determine what shards we care about at the Head or in the next epoch after the Head.
    // 4. Before actual clearing is started, Block Reference Map should be built.
    // 5. `clear_old_blocks_data()` executes every time when block at new height is added.
    // 6. In case of State Sync, State Sync Clearing happens.
    //
    // Forks Clearing:
    // 1. Any fork which ends up on height `height` INCLUSIVELY and earlier will be completely deleted
    //    from the Store with all its ancestors up to the ancestor block where fork is happened
    //    EXCLUDING the ancestor block where fork is happened.
    // 2. The oldest ancestor block always remains on the Canonical Chain by property 2.
    // 3. All forks which end up on height `height + 1` and further are protected from deletion and
    //    no their ancestor will be deleted (even with lowest heights).
    // 4. `clear_forks_data()` handles forks clearing for fixed height `height`.
    //
    // Canonical Chain Clearing:
    // 1. Blocks on the Canonical Chain with the only descendant (if no forks started from them)
    //    are unlocked for Canonical Chain Clearing.
    // 2. If Forks Clearing ended up on the Canonical Chain, the block may be unlocked
    //    for the Canonical Chain Clearing. There is no other reason to unlock the block exists.
    // 3. All the unlocked blocks will be completely deleted
    //    from the Tail up to GC Stop Height EXCLUSIVELY.
    // 4. (Property 3, GC invariant). Tail can be shifted safely to the height of the
    //    earliest existing block. There is always only one Tail (based on property 1)
    //    and it's always on the Canonical Chain (based on property 2).
    //
    // Example:
    //
    // height: 101   102   103   104
    // --------[A]---[B]---[C]---[D]
    //          \     \
    //           \     \---[E]
    //            \
    //             \-[F]---[G]
    //
    // 1. Let's define clearing height = 102. It this case fork A-F-G is protected from deletion
    //    because of G which is on height 103. Nothing will be deleted.
    // 2. Let's define clearing height = 103. It this case Fork Clearing will be executed for A
    //    to delete blocks G and F, then Fork Clearing will be executed for B to delete block E.
    //    Then Canonical Chain Clearing will delete blocks A and B as unlocked.
    //    Block C is the only block of height 103 remains on the Canonical Chain (invariant).
    //
    // State Sync Clearing:
    // 1. Executing State Sync means that no data in the storage is useful for block processing
    //    and should be removed completely.
    // 2. The Tail should be set to the block preceding Sync Block if there are
    //    no missing chunks or to a block before that such that all shards have
    //    at least one new chunk in the blocks leading to the Sync Block.
    // 3. All the data preceding new Tail is deleted in State Sync Clearing
    //    and the Trie is updated with having only Genesis data.
    // 4. State Sync Clearing happens in `reset_data_pre_state_sync()`.
    //
    pub fn clear_data(
        &mut self,
        gc_config: &GCConfig,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: &ShardTracker,
    ) -> Result<(), Error> {
        // We clear state transition data separately without respecting gc configs because it gets
        // accumulated too quickly for regular gc process.
        // If clearing state transition data fails there's no reason not to try cleaning old
        // blocks.
        let result = self.clear_state_transition_data(epoch_manager.as_ref());

        result.and(self.clear_old_blocks_data(
            gc_config,
            runtime_adapter,
            epoch_manager,
            shard_tracker,
        ))
    }

    fn clear_old_blocks_data(
        &mut self,
        gc_config: &GCConfig,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        shard_tracker: &ShardTracker,
    ) -> Result<(), Error> {
        let tries = runtime_adapter.get_tries();
        let head = self.head()?;
        if head.height == self.get_genesis_height() {
            // Nothing to do if head is at genesis. Return early because some of the later queries would fail.
            return Ok(());
        }
        let tail = self.tail()?;
        let gc_stop_height = runtime_adapter.get_gc_stop_height(&head.last_block_hash);
        if gc_stop_height > head.height {
            return Err(Error::GCError("gc_stop_height cannot be larger than head.height".into()));
        }
        let mut fork_tail = self.fork_tail()?;
        let chunk_tail = self.chain_store().chunk_tail()?;
        metrics::TAIL_HEIGHT.set(tail as i64);
        metrics::FORK_TAIL_HEIGHT.set(fork_tail as i64);
        metrics::CHUNK_TAIL_HEIGHT.set(chunk_tail as i64);
        metrics::GC_STOP_HEIGHT.set(gc_stop_height as i64);
        let last_known_gc_heigh = self.gc_stop_height()?;
        if last_known_gc_heigh != gc_stop_height {
            tracing::debug!(
                target: "garbage_collection",
                gc_stop_height,
                last_known_gc_heigh,
                "Update last known gc_stop_height"
            );
            let mut chain_store_update = self.store_update();
            chain_store_update.update_gc_stop_height(gc_stop_height);
            if fork_tail < gc_stop_height {
                tracing::debug!(
                    target: "garbage_collection",
                    fork_tail,
                    gc_stop_height,
                    "Update fork_tail"
                );
                chain_store_update.update_fork_tail(gc_stop_height);
                fork_tail = gc_stop_height;
            }
            chain_store_update.commit()?;
        }
        let mut gc_blocks_remaining = gc_config.gc_blocks_limit;
        let _span = tracing::debug_span!(
            target: "garbage_collection",
            "clear_old_blocks_data",
            tail,
            fork_tail,
            gc_stop_height,
            chunk_tail,
            gc_blocks_remaining,
        )
        .entered();

        let gc_fork_clean_step = gc_config.gc_fork_clean_step;
        let stop_height = tail.max(fork_tail.saturating_sub(gc_fork_clean_step));
        tracing::debug!(
            target: "garbage_collection",
            stop_height,
            gc_fork_clean_step,
            "Start Fork Cleaning"
        );
        for height in (stop_height..fork_tail).rev() {
            self.clear_forks_data(
                tries.clone(),
                height,
                &mut gc_blocks_remaining,
                epoch_manager.clone(),
            )?;
            if gc_blocks_remaining == 0 {
                return Ok(());
            }
            let mut chain_store_update = self.store_update();
            chain_store_update.update_fork_tail(height);
            chain_store_update.commit()?;
        }

        tracing::debug!(
            target: "garbage_collection",
            gc_blocks_remaining,
            "Start Canonical Chain Clearing"
        );
        for height in tail + 1..gc_stop_height {
            if gc_blocks_remaining == 0 {
                return Ok(());
            }
            let blocks_current_height = self
                .chain_store()
                .get_all_block_hashes_by_height(height)?
                .values()
                .flatten()
                .cloned()
                .collect_vec();
            let epoch_manager = epoch_manager.clone();
            let mut chain_store_update = self.store_update();
            if let Some(block_hash) = blocks_current_height.first() {
                let prev_hash = *chain_store_update.get_block_header(block_hash)?.prev_hash();
                let prev_block_refcount = chain_store_update.get_block_refcount(&prev_hash)?;
                if prev_block_refcount > 1 {
                    tracing::debug!(
                        target: "garbage_collection",
                        ?prev_hash,
                        height,
                        prev_block_refcount,
                        "Block of prev_hash starts a Fork, stopping"
                    );
                    break;
                }
                if prev_block_refcount < 1 {
                    return Err(Error::GCError(
                        "block on canonical chain shouldn't have refcount 0".into(),
                    ));
                }
                debug_assert_eq!(blocks_current_height.len(), 1);

                chain_store_update.clear_block_data(
                    epoch_manager.as_ref(),
                    *block_hash,
                    GCMode::Canonical(tries.clone()),
                )?;
                gc_parent_shard_after_resharding(
                    &mut chain_store_update,
                    epoch_manager.as_ref(),
                    block_hash,
                )?;
                gc_state(
                    &mut chain_store_update,
                    epoch_manager.as_ref(),
                    block_hash,
                    shard_tracker,
                )?;

                gc_blocks_remaining -= 1;
            }
            chain_store_update.update_tail(height)?;
            chain_store_update.commit()?;
        }

        Ok(())
    }

    fn clear_state_transition_data(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        let _metric_timer = metrics::STATE_TRANSITION_DATA_GC_TIME.start_timer();
        let _span =
            tracing::debug_span!(target: "garbage_collection", "clear_state_transition_data")
                .entered();

        let final_block_hash =
            *self.get_block_header(&self.head()?.last_block_hash)?.last_final_block();
        if final_block_hash == CryptoHash::default() {
            return Ok(());
        }
        let Ok(final_block) = self.get_block(&final_block_hash) else {
            // This can happen if the node just did state sync.
            tracing::debug!(target: "garbage_collection", ?final_block_hash, "Could not get final block");
            return Ok(());
        };
        let final_block_chunk_created_heights: HashMap<_, _> = final_block
            .chunks()
            .iter()
            .map(|chunk| (chunk.shard_id(), chunk.height_created()))
            .collect();

        let relevant_shards: HashSet<_> = {
            let shard_layout = epoch_manager
                .get_shard_layout(final_block.header().epoch_id())
                .expect("epoch id must exist");
            let next_epoch_shard_layout = epoch_manager
                .get_shard_layout(final_block.header().next_epoch_id())
                .expect("next epoch id must exist");
            shard_layout.shard_ids().chain(next_epoch_shard_layout.shard_ids()).collect()
        };

        let mut total_entries = 0;
        let mut entries_cleared = 0;
        let mut store_update = self.store().store_update();
        for res in self.store().iter(DBCol::StateTransitionData) {
            total_entries += 1;
            let key = &res?.0;
            let (block_hash, shard_id) = get_block_shard_id_rev(key).map_err(|err| {
                Error::StorageError(near_store::StorageError::StorageInconsistentState(format!(
                    "Invalid StateTransitionData key: {err:?}"
                )))
            })?;

            let Some(final_block_height) = final_block_chunk_created_heights.get(&shard_id) else {
                if !relevant_shards.contains(&shard_id) {
                    store_update.delete(DBCol::StateTransitionData, key);
                    entries_cleared += 1;
                }
                // StateTransitionData may correspond to the shard that is created in next epoch.
                continue;
            };

            let block_height = self.get_block_height(&block_hash)?;
            if block_height < *final_block_height {
                store_update.delete(DBCol::StateTransitionData, key);
                entries_cleared += 1;
            }
        }

        metrics::STATE_TRANSITION_DATA_GC_TOTAL_ENTRIES.set(total_entries);
        store_update.commit()?;
        metrics::STATE_TRANSITION_DATA_GC_CLEARED_ENTRIES.inc_by(entries_cleared);
        Ok(())
    }

    /// Garbage collect data which archival node doesn't need to keep.
    ///
    /// Normally, archival nodes keep all the data from the genesis block and
    /// don't run garbage collection.  On the other hand, for better performance
    /// the storage contains some data duplication, i.e. values in some of the
    /// columns can be recomputed from data in different columns.  To save on
    /// storage, archival nodes do garbage collect that data.
    ///
    /// `gc_height_limit` limits how many heights will the function process.
    pub fn clear_archive_data(
        &mut self,
        gc_height_limit: BlockHeightDelta,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
    ) -> Result<(), Error> {
        let _span =
            tracing::debug_span!(target: "chain", "clear_archive_data", gc_height_limit).entered();

        let head = self.head()?;
        let gc_stop_height = runtime_adapter.get_gc_stop_height(&head.last_block_hash);
        if gc_stop_height > head.height {
            return Err(Error::GCError("gc_stop_height cannot be larger than head.height".into()));
        }

        let mut chain_store_update = self.store_update();
        chain_store_update.clear_redundant_chunk_data(gc_stop_height, gc_height_limit)?;
        metrics::CHUNK_TAIL_HEIGHT.set(chain_store_update.chunk_tail()? as i64);
        metrics::GC_STOP_HEIGHT.set(gc_stop_height as i64);
        chain_store_update.commit()
    }

    fn clear_forks_data(
        &mut self,
        tries: ShardTries,
        height: BlockHeight,
        gc_blocks_remaining: &mut NumBlocks,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Result<(), Error> {
        let blocks_current_height = self
            .chain_store()
            .get_all_block_hashes_by_height(height)?
            .values()
            .flatten()
            .cloned()
            .collect_vec();
        for block_hash in &blocks_current_height {
            let mut current_hash = *block_hash;
            loop {
                if *gc_blocks_remaining == 0 {
                    return Ok(());
                }
                // Block `block_hash` is not on the Canonical Chain
                // because shorter chain cannot be Canonical one
                // and it may be safely deleted
                // and all its ancestors while there are no other sibling blocks rely on it.
                let epoch_manager = epoch_manager.clone();
                let mut chain_store_update = self.store_update();
                let current_block_refcount =
                    chain_store_update.get_block_refcount(&current_hash)?;
                if current_block_refcount == 0 {
                    let prev_hash =
                        *chain_store_update.get_block_header(&current_hash)?.prev_hash();

                    // It's safe to call `clear_block_data` for prev data because it clears fork only here
                    chain_store_update.clear_block_data(
                        epoch_manager.as_ref(),
                        current_hash,
                        GCMode::Fork(tries.clone()),
                    )?;
                    chain_store_update.commit()?;
                    *gc_blocks_remaining -= 1;

                    current_hash = prev_hash;
                } else {
                    tracing::debug!(
                        target: "garbage_collection",
                        ?current_hash,
                        height,
                        current_block_refcount,
                        "Block is an ancestor for some other blocks, stopping"
                    );
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn reset_data_pre_state_sync(
        &mut self,
        sync_hash: CryptoHash,
        runtime_adapter: Arc<dyn RuntimeAdapter>,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Result<(), Error> {
        let _span = tracing::debug_span!(target: "sync", "reset_data_pre_state_sync").entered();
        let head = self.head()?;
        if head.prev_block_hash == CryptoHash::default() {
            // This is genesis. It means we are state syncing right after epoch sync. Don't clear
            // anything at genesis, or else the node will never boot up again.
            return Ok(());
        }
        // Get header we were syncing into.
        let header = self.get_block_header(&sync_hash)?;
        let prev_hash = *header.prev_hash();
        let prev_header = self.get_block_header(&prev_hash)?;
        let sync_height = header.height();
        let prev_height = prev_header.height();

        // After state sync we may need a few additional blocks leading up to the sync prev block.
        // For simplicity we'll GC them and allow state sync to re-download exactly what it needs.
        let gc_height = std::cmp::min(head.height + 1, prev_height);

        // GC all the data from current tail up to `gc_height`. In case tail points to a height where
        // there is no block, we need to make sure that the last block before tail is cleaned.
        let tail = self.chain_store().tail()?;
        let mut tail_prev_block_cleaned = false;
        for height in tail..gc_height {
            let blocks_current_height = self
                .chain_store()
                .get_all_block_hashes_by_height(height)?
                .values()
                .flatten()
                .cloned()
                .collect_vec();
            for block_hash in blocks_current_height {
                let epoch_manager = epoch_manager.clone();
                let mut chain_store_update = self.store_update();
                if !tail_prev_block_cleaned {
                    let prev_block_hash =
                        *chain_store_update.get_block_header(&block_hash)?.prev_hash();
                    if chain_store_update.get_block(&prev_block_hash).is_ok() {
                        chain_store_update.clear_block_data(
                            epoch_manager.as_ref(),
                            prev_block_hash,
                            GCMode::StateSync { clear_block_info: true },
                        )?;
                    }
                    tail_prev_block_cleaned = true;
                }
                chain_store_update.clear_block_data(
                    epoch_manager.as_ref(),
                    block_hash,
                    GCMode::StateSync { clear_block_info: block_hash != prev_hash },
                )?;
                chain_store_update.commit()?;
            }
        }

        // Clear Chunks data
        let mut chain_store_update = self.store_update();
        // The largest height of chunk we have in storage is head.height + 1
        let chunk_height = std::cmp::min(head.height + 2, sync_height);
        chain_store_update.clear_chunk_data_and_headers(chunk_height)?;
        chain_store_update.commit()?;

        // clear all trie data
        let tries = runtime_adapter.get_tries();
        let mut chain_store_update = self.store_update();
        let mut store_update = tries.store_update();
        store_update.delete_all_state();
        chain_store_update.merge(store_update.into());

        // The reason to reset tail here is not to allow Tail be greater than Head
        chain_store_update.reset_tail();
        chain_store_update.commit()?;
        Ok(())
    }
}

impl<'a> ChainStoreUpdate<'a> {
    fn clear_header_data_for_heights(
        &mut self,
        start: BlockHeight,
        end: BlockHeight,
    ) -> Result<(), Error> {
        for height in start..=end {
            let header_hashes = self.chain_store().get_all_header_hashes_by_height(height)?;
            for header_hash in header_hashes {
                // Delete header_hash-indexed data: block header
                let mut store_update = self.store().store_update();
                let key: &[u8] = header_hash.as_bytes();
                store_update.delete(DBCol::BlockHeader, key);
                self.merge(store_update);
            }
            let key = index_to_bytes(height);
            self.gc_col(DBCol::HeaderHashesByHeight, &key);
        }
        Ok(())
    }

    fn clear_chunk_data_and_headers(&mut self, min_chunk_height: BlockHeight) -> Result<(), Error> {
        let chunk_tail = self.chunk_tail()?;
        for height in chunk_tail..min_chunk_height {
            let chunk_hashes = self.chain_store().get_all_chunk_hashes_by_height(height)?;
            for chunk_hash in chunk_hashes {
                // 1. Delete chunk-related data
                let chunk = self.get_chunk(&chunk_hash)?;
                debug_assert_eq!(chunk.height_created(), height);
                for transaction in chunk.to_transactions() {
                    self.gc_col(DBCol::Transactions, transaction.get_hash().as_bytes());
                }

                let partial_chunk = self.get_partial_chunk(&chunk_hash);
                if let Ok(partial_chunk) = partial_chunk {
                    for receipts in partial_chunk.prev_outgoing_receipts() {
                        for receipt in &receipts.0 {
                            self.gc_col(DBCol::Receipts, receipt.receipt_id().as_bytes());
                        }
                    }
                }

                // 2. Delete chunk_hash-indexed data
                let chunk_hash = chunk_hash.as_bytes();
                self.gc_col(DBCol::Chunks, chunk_hash);
                self.gc_col(DBCol::PartialChunks, chunk_hash);
                self.gc_col(DBCol::InvalidChunks, chunk_hash);
            }

            let header_hashes = self.chain_store().get_all_header_hashes_by_height(height)?;
            for _header_hash in header_hashes {
                // 3. Delete header_hash-indexed data
                // TODO #3488: enable
                //self.gc_col(DBCol::BlockHeader, header_hash.as_bytes());
            }

            // 4. Delete chunks_tail-related data
            let key = index_to_bytes(height);
            self.gc_col(DBCol::ChunkHashesByHeight, &key);
            self.gc_col(DBCol::HeaderHashesByHeight, &key);
        }
        self.update_chunk_tail(min_chunk_height);
        Ok(())
    }

    /// Clears chunk data which can be computed from other data in the storage.
    ///
    /// We are storing PartialEncodedChunk objects in the DBCol::PartialChunks in
    /// the storage.  However, those objects can be computed from data in
    /// DBCol::Chunks and as such are redundant.  For performance reasons we want to
    /// keep that data when operating at head of the chain but the data can be
    /// safely removed from archival storage.
    ///
    /// `gc_stop_height` indicates height starting from which no data should be
    /// garbage collected.  Roughly speaking this represents start of the 'hot'
    /// data that we want to keep.
    ///
    /// `gt_height_limit` indicates limit of how many non-empty heights to
    /// process.  This limit means that the method may stop garbage collection
    /// before reaching `gc_stop_height`.
    fn clear_redundant_chunk_data(
        &mut self,
        gc_stop_height: BlockHeight,
        gc_height_limit: BlockHeightDelta,
    ) -> Result<(), Error> {
        let mut height = self.chunk_tail()?;
        let mut remaining = gc_height_limit;
        while height < gc_stop_height && remaining > 0 {
            let chunk_hashes = self.chain_store().get_all_chunk_hashes_by_height(height)?;
            height += 1;
            if !chunk_hashes.is_empty() {
                remaining -= 1;
                for chunk_hash in chunk_hashes {
                    let chunk_hash = chunk_hash.as_bytes();
                    self.gc_col(DBCol::PartialChunks, chunk_hash);
                    // Data in DBCol::InvalidChunks isn't technically redundant (it
                    // cannot be calculated from other data) but it is data we
                    // don't need for anything so it can be deleted as well.
                    self.gc_col(DBCol::InvalidChunks, chunk_hash);
                }
            }
        }
        self.update_chunk_tail(height);
        Ok(())
    }

    fn get_shard_uids_to_gc(
        &self,
        epoch_manager: &dyn EpochManagerAdapter,
        block_hash: &CryptoHash,
    ) -> Vec<ShardUId> {
        let block_header = self.get_block_header(block_hash).expect("block header must exist");
        let shard_layout =
            epoch_manager.get_shard_layout(block_header.epoch_id()).expect("epoch info must exist");
        // gc shards in this epoch
        let mut shard_uids_to_gc: Vec<_> = shard_layout.shard_uids().collect();
        // gc shards in the shard layout in the next epoch if shards will change in the next epoch
        // Suppose shard changes at epoch T, we need to garbage collect the new shard layout
        // from the last block in epoch T-2 to the last block in epoch T-1
        // Because we need to gc the last block in epoch T-2, we can't simply use
        // block_header.epoch_id() as next_epoch_id
        let next_epoch_id = block_header.next_epoch_id();
        let next_shard_layout =
            epoch_manager.get_shard_layout(next_epoch_id).expect("epoch info must exist");
        if shard_layout != next_shard_layout {
            shard_uids_to_gc.extend(next_shard_layout.shard_uids());
        }
        shard_uids_to_gc.into_iter().unique().collect_vec()
    }

    // Clearing block data of `block_hash`, if on a fork.
    // Clearing block data of `block_hash.prev`, if on the Canonical Chain.
    pub fn clear_block_data(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
        mut block_hash: CryptoHash,
        gc_mode: GCMode,
    ) -> Result<(), Error> {
        let mut store_update = self.store().trie_store().store_update();

        tracing::debug!(target: "garbage_collection", ?gc_mode, ?block_hash, "GC block_hash");

        // 1. Garbage collect TrieChanges.
        self.gc_trie_changes(epoch_manager, block_hash, &gc_mode, &mut store_update)?;

        if matches!(gc_mode, GCMode::Canonical(_)) {
            // If you know why do we do this in case of canonical chain please add a comment here.
            block_hash = *self.get_block_header(&block_hash)?.prev_hash();
        }

        let block =
            self.get_block(&block_hash).expect("block data is not expected to be already cleaned");
        let height = block.header().height();
        let epoch_id = block.header().epoch_id();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).expect("epoch id must exist");

        // 2. Delete shard_id-indexed data (Receipts, State Headers and Parts, etc.)
        for shard_id in shard_layout.shard_ids() {
            let block_shard_id = get_block_shard_id(&block_hash, shard_id);
            self.gc_outgoing_receipts(&block_hash, shard_id);
            self.gc_col(DBCol::IncomingReceipts, &block_shard_id);
            self.gc_col(DBCol::ChunkApplyStats, &block_shard_id);

            if cfg!(feature = "protocol_feature_spice") {
                for to_shard_id in shard_layout.shard_ids() {
                    self.gc_col(
                        DBCol::receipt_proofs(),
                        &get_receipt_proof_key(&block_hash, shard_id, to_shard_id),
                    );
                }
            }

            // For incoming State Parts it's done in chain.clear_downloaded_parts()
            // The following code is mostly for outgoing State Parts.
            // However, if node crashes while State Syncing, it may never clear
            // downloaded State parts in `clear_downloaded_parts`.
            // We need to make sure all State Parts are removed.
            if let Ok(shard_state_header) =
                self.chain_store().get_state_header(shard_id, block_hash)
            {
                let state_num_parts = shard_state_header.num_state_parts();
                self.gc_col_state_parts(block_hash, shard_id, state_num_parts)?;
                let key = borsh::to_vec(&StateHeaderKey(shard_id, block_hash))?;
                self.gc_col(DBCol::StateHeaders, &key);
            }
        }
        // gc DBCol::ChunkExtra based on shard_uid since it's indexed by shard_uid in the storage
        for shard_uid in self.get_shard_uids_to_gc(epoch_manager, &block_hash) {
            let block_shard_uid = get_block_shard_uid(&block_hash, &shard_uid);
            self.gc_col(DBCol::ChunkExtra, &block_shard_uid);
        }

        // 3. Delete block_hash-indexed data
        self.gc_col(DBCol::Block, block_hash.as_bytes());
        self.gc_col(DBCol::NextBlockHashes, block_hash.as_bytes());
        if cfg!(feature = "protocol_feature_spice") {
            self.gc_col(DBCol::all_next_block_hashes(), block_hash.as_bytes());
        }
        self.gc_col(DBCol::ChallengedBlocks, block_hash.as_bytes());
        self.gc_col(DBCol::BlocksToCatchup, block_hash.as_bytes());
        let storage_key = KeyForStateChanges::for_block(&block_hash);
        let stored_state_changes: Vec<Box<[u8]>> = self
            .store()
            .iter_prefix(DBCol::StateChanges, storage_key.as_ref())
            .map(|item| item.map(|(key, _)| key))
            .collect::<io::Result<Vec<_>>>()?;
        for key in stored_state_changes {
            self.gc_col(DBCol::StateChanges, &key);
        }
        self.gc_col(DBCol::BlockRefCount, block_hash.as_bytes());
        self.gc_outcomes(&block)?;
        match gc_mode {
            GCMode::StateSync { clear_block_info: false } => {}
            _ => self.gc_col(DBCol::BlockInfo, block_hash.as_bytes()),
        }
        self.gc_col(DBCol::StateDlInfos, block_hash.as_bytes());

        // 4. Update or delete block_hash_per_height
        self.gc_col_block_per_height(&block_hash, height, block.header().epoch_id())?;

        match gc_mode {
            GCMode::Fork(_) => {
                // 5. Forks only clearing
                self.dec_block_refcount(block.header().prev_hash())?;
            }
            GCMode::Canonical(_) => {
                // 6. Canonical Chain only clearing
                // Delete chunks, chunk-indexed data and block headers
                let mut min_chunk_height = self.tail()?;
                for chunk_header in block.chunks().iter() {
                    if min_chunk_height > chunk_header.height_created() {
                        min_chunk_height = chunk_header.height_created();
                    }
                }
                self.clear_chunk_data_and_headers(min_chunk_height)?;
            }
            GCMode::StateSync { .. } => {
                // 7. State Sync clearing
                // Chunks deleted separately
            }
        };
        self.merge(store_update.into());
        Ok(())
    }

    fn gc_trie_changes(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
        block_hash: CryptoHash,
        gc_mode: &GCMode,
        store_update: &mut near_store::adapter::trie_store::TrieStoreUpdateAdapter<'_>,
    ) -> Result<(), Error> {
        let shard_uids_to_gc = self.get_shard_uids_to_gc(epoch_manager, &block_hash);
        for shard_uid in shard_uids_to_gc {
            let trie_changes_key = get_block_shard_uid(&block_hash, &shard_uid);
            let trie_changes = self.store().get_ser(DBCol::TrieChanges, &trie_changes_key)?;

            let Some(trie_changes) = trie_changes else {
                continue;
            };
            match gc_mode.clone() {
                GCMode::Fork(tries) => {
                    // If the block is on a fork, we delete the state that's the result of applying this block
                    tries.revert_insertions(&trie_changes, shard_uid, store_update);
                }
                GCMode::Canonical(tries) => {
                    // If the block is on canonical chain, we delete the state that's before applying this block
                    tries.apply_deletions(&trie_changes, shard_uid, store_update);
                }
                GCMode::StateSync { .. } => {
                    // Not apply the data from DBCol::TrieChanges
                }
            }

            self.gc_col(DBCol::TrieChanges, &trie_changes_key);
        }
        Ok(())
    }

    // Delete all data in rocksdb that are partially or wholly indexed and can be looked up by hash of the current head of the chain
    // and that indicates a link between current head and its prev block
    pub fn clear_head_block_data(
        &mut self,
        epoch_manager: &dyn EpochManagerAdapter,
    ) -> Result<(), Error> {
        let header_head = self.header_head().unwrap();
        let header_head_height = header_head.height;
        let block_hash = self.head().unwrap().last_block_hash;

        let block =
            self.get_block(&block_hash).expect("block data is not expected to be already cleaned");

        let epoch_id = block.header().epoch_id();
        let head_height = block.header().height();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).expect("epoch id must exist");

        // 1. Delete shard_id-indexed data (TrieChanges, Receipts, ChunkExtra, State Headers and Parts, FlatStorage data)
        for shard_id in shard_layout.shard_ids() {
            let shard_uid = shard_id_to_uid(epoch_manager, shard_id, epoch_id).unwrap();
            let block_shard_id = get_block_shard_uid(&block_hash, &shard_uid);

            // delete TrieChanges
            self.gc_col(DBCol::TrieChanges, &block_shard_id);

            // delete Receipts
            self.gc_outgoing_receipts(&block_hash, shard_id);
            self.gc_col(DBCol::IncomingReceipts, &block_shard_id);

            self.gc_col(DBCol::StateTransitionData, &block_shard_id);

            // delete DBCol::ChunkExtra based on shard_uid since it's indexed by shard_uid in the storage
            self.gc_col(DBCol::ChunkExtra, &block_shard_id);

            // delete state parts and state headers
            if let Ok(shard_state_header) =
                self.chain_store().get_state_header(shard_id, block_hash)
            {
                let state_num_parts = shard_state_header.num_state_parts();
                self.gc_col_state_parts(block_hash, shard_id, state_num_parts)?;
                let state_header_key = borsh::to_vec(&StateHeaderKey(shard_id, block_hash))?;
                self.gc_col(DBCol::StateHeaders, &state_header_key);
            }

            // delete flat storage columns: FlatStateChanges and FlatStateDeltaMetadata
            let mut store_update = self.store().store_update();
            store_update.flat_store_update().remove_delta(shard_uid, block_hash);
            self.merge(store_update);
        }

        // 2. Delete block_hash-indexed data
        self.gc_col(DBCol::Block, block_hash.as_bytes());
        self.gc_col(DBCol::NextBlockHashes, block_hash.as_bytes());
        self.gc_col(DBCol::ChallengedBlocks, block_hash.as_bytes());
        self.gc_col(DBCol::BlocksToCatchup, block_hash.as_bytes());
        let storage_key = KeyForStateChanges::for_block(&block_hash);
        let stored_state_changes: Vec<Box<[u8]>> = self
            .store()
            .iter_prefix(DBCol::StateChanges, storage_key.as_ref())
            .map(|item| item.map(|(key, _)| key))
            .collect::<io::Result<Vec<_>>>()?;
        for key in stored_state_changes {
            self.gc_col(DBCol::StateChanges, &key);
        }
        self.gc_col(DBCol::BlockRefCount, block_hash.as_bytes());
        self.gc_outcomes(&block)?;
        self.gc_col(DBCol::BlockInfo, block_hash.as_bytes());
        self.gc_col(DBCol::StateDlInfos, block_hash.as_bytes());
        self.gc_col(DBCol::StateSyncNewChunks, block_hash.as_bytes());

        // 3. update columns related to prev block (block refcount and NextBlockHashes)
        self.dec_block_refcount(block.header().prev_hash())?;
        self.gc_col(DBCol::NextBlockHashes, block.header().prev_hash().as_bytes());

        // 4. Update or delete block_hash_per_height
        self.gc_col_block_per_height(&block_hash, head_height, block.header().epoch_id())?;

        self.clear_chunk_data_at_height(head_height)?;

        self.clear_header_data_for_heights(head_height, header_head_height)?;

        Ok(())
    }

    fn clear_chunk_data_at_height(&mut self, height: BlockHeight) -> Result<(), Error> {
        let chunk_hashes = self.chain_store().get_all_chunk_hashes_by_height(height)?;
        for chunk_hash in chunk_hashes {
            // 1. Delete chunk-related data
            let chunk = self.get_chunk(&chunk_hash)?;
            debug_assert_eq!(chunk.height_created(), height);
            for transaction in chunk.to_transactions() {
                self.gc_col(DBCol::Transactions, transaction.get_hash().as_bytes());
            }

            let partial_chunk = self.get_partial_chunk(&chunk_hash);
            if let Ok(partial_chunk) = partial_chunk {
                for receipts in partial_chunk.prev_outgoing_receipts() {
                    for receipt in &receipts.0 {
                        self.gc_col(DBCol::Receipts, receipt.receipt_id().as_bytes());
                    }
                }
            }

            // 2. Delete chunk_hash-indexed data
            let chunk_hash = chunk_hash.as_bytes();
            self.gc_col(DBCol::Chunks, chunk_hash);
            self.gc_col(DBCol::PartialChunks, chunk_hash);
            self.gc_col(DBCol::InvalidChunks, chunk_hash);
        }

        // 4. Delete chunk hashes per height
        let key = index_to_bytes(height);
        self.gc_col(DBCol::ChunkHashesByHeight, &key);

        Ok(())
    }

    fn gc_col_block_per_height(
        &mut self,
        block_hash: &CryptoHash,
        height: BlockHeight,
        epoch_id: &EpochId,
    ) -> Result<(), Error> {
        let mut store_update = self.store().store_update();
        let mut epoch_to_hashes =
            HashMap::clone(self.chain_store().get_all_block_hashes_by_height(height)?.as_ref());
        let hashes = epoch_to_hashes.get_mut(epoch_id).ok_or_else(|| {
            near_chain_primitives::Error::Other("current epoch id should exist".into())
        })?;
        hashes.remove(block_hash);
        if hashes.is_empty() {
            epoch_to_hashes.remove(epoch_id);
        }
        let key = &index_to_bytes(height)[..];
        if epoch_to_hashes.is_empty() {
            store_update.delete(DBCol::BlockPerHeight, key);
        } else {
            store_update.set_ser(DBCol::BlockPerHeight, key, &epoch_to_hashes)?;
        }
        if self.is_height_processed(height)? {
            self.gc_col(DBCol::ProcessedBlockHeights, key);
        }
        self.merge(store_update);
        Ok(())
    }

    pub fn gc_col_state_parts(
        &mut self,
        sync_hash: CryptoHash,
        shard_id: ShardId,
        num_parts: u64,
    ) -> Result<(), Error> {
        for part_id in 0..num_parts {
            let key = borsh::to_vec(&StatePartKey(sync_hash, shard_id, part_id))?;
            self.gc_col(DBCol::StateParts, &key);
        }
        Ok(())
    }

    fn gc_outgoing_receipts(&mut self, block_hash: &CryptoHash, shard_id: ShardId) {
        let mut store_update = self.store().store_update();
        let key = get_block_shard_id(block_hash, shard_id);
        store_update.delete(DBCol::OutgoingReceipts, &key);
        self.merge(store_update);
    }

    fn gc_outcomes(&mut self, block: &Block) -> Result<(), Error> {
        let block_hash = block.hash();
        let store_update = self.store().store_update();
        for chunk_header in block.chunks().iter_new() {
            // It is ok to use the shard id from the header because it is a new
            // chunk. An old chunk may have the shard id from the parent shard.
            let shard_id = chunk_header.shard_id();
            let outcome_ids =
                self.chain_store().get_outcomes_by_block_hash_and_shard_id(block_hash, shard_id)?;
            for outcome_id in outcome_ids {
                self.gc_col(
                    DBCol::TransactionResultForBlock,
                    &get_outcome_id_block_hash(&outcome_id, block_hash),
                );
            }
            self.gc_col(DBCol::OutcomeIds, &get_block_shard_id(block_hash, shard_id));
        }
        self.merge(store_update);
        Ok(())
    }

    fn gc_col(&mut self, col: DBCol, key: &[u8]) {
        let mut store_update = self.store().store_update();
        match col {
            DBCol::OutgoingReceipts => {
                panic!("Outgoing receipts must be garbage collected by calling gc_outgoing_receipts");
            }
            DBCol::IncomingReceipts => {
                store_update.delete(col, key);
            }
            DBCol::StateHeaders => {
                store_update.delete(col, key);
            }
            DBCol::BlockHeader => {
                // TODO(#3488) At the moment header sync needs block headers.
                // However, we want to eventually garbage collect headers.
                // When that happens we should make sure that block headers is
                // copied to the cold storage.
                store_update.delete(col, key);
                unreachable!();
            }
            DBCol::Block => {
                store_update.delete(col, key);
            }
            DBCol::NextBlockHashes => {
                store_update.delete(col, key);
            }
            DBCol::ChallengedBlocks => {
                store_update.delete(col, key);
            }
            DBCol::BlocksToCatchup => {
                store_update.delete(col, key);
            }
            DBCol::StateChanges => {
                store_update.delete(col, key);
            }
            DBCol::BlockRefCount => {
                store_update.delete(col, key);
            }
            DBCol::Transactions => {
                store_update.decrement_refcount(col, key);
            }
            DBCol::Receipts => {
                store_update.decrement_refcount(col, key);
            }
            DBCol::Chunks => {
                store_update.delete(col, key);
            }
            DBCol::ChunkExtra => {
                store_update.delete(col, key);
            }
            DBCol::PartialChunks => {
                store_update.delete(col, key);
            }
            DBCol::InvalidChunks => {
                store_update.delete(col, key);
            }
            DBCol::ChunkHashesByHeight => {
                store_update.delete(col, key);
            }
            DBCol::StateParts => {
                store_update.delete(col, key);
            }
            DBCol::State => {
                panic!("Actual gc happens elsewhere, call inc_gc_col_state to increase gc count");
            }
            DBCol::TrieChanges => {
                store_update.delete(col, key);
            }
            DBCol::BlockPerHeight => {
                panic!("Must use gc_col_block_per_height method to gc DBCol::BlockPerHeight");
            }
            DBCol::TransactionResultForBlock => {
                store_update.delete(col, key);
            }
            DBCol::OutcomeIds => {
                store_update.delete(col, key);
            }
            DBCol::StateDlInfos => {
                store_update.delete(col, key);
            }
            DBCol::BlockInfo => {
                store_update.delete(col, key);
            }
            DBCol::ProcessedBlockHeights => {
                store_update.delete(col, key);
            }
            DBCol::HeaderHashesByHeight => {
                store_update.delete(col, key);
            }
            DBCol::StateTransitionData => {
                store_update.delete(col, key);
            }
            DBCol::LatestChunkStateWitnesses => {
                store_update.delete(col, key);
            }
            DBCol::LatestWitnessesByIndex => {
                store_update.delete(col, key);
            }
            DBCol::InvalidChunkStateWitnesses => {
                store_update.delete(col, key);
            }
            DBCol::InvalidWitnessesByIndex => {
                store_update.delete(col, key);
            }
            DBCol::StateSyncNewChunks => {
                store_update.delete(col, key);
            }
            DBCol::ChunkApplyStats => {
                store_update.delete(col, key);
            }
            #[cfg(feature = "protocol_feature_spice")]
            DBCol::ReceiptProofs => {
                store_update.delete(col, key);
            }
            #[cfg(feature = "protocol_feature_spice")]
            DBCol::AllNextBlockHashes => {
                store_update.delete(col, key);
            }
            DBCol::DbVersion
            | DBCol::BlockMisc
            | DBCol::_BlockExtra
            | DBCol::_GCCount
            | DBCol::BlockHeight  // block sync needs it + genesis should be accessible
            | DBCol::_Peers
            | DBCol::RecentOutboundConnections
            | DBCol::BlockMerkleTree
            | DBCol::AccountAnnouncements
            | DBCol::EpochLightClientBlocks
            | DBCol::PeerComponent
            | DBCol::LastComponentNonce
            | DBCol::ComponentEdges
            // https://github.com/nearprotocol/nearcore/pull/2952
            | DBCol::EpochInfo
            | DBCol::EpochStart
            | DBCol::EpochValidatorInfo
            | DBCol::BlockOrdinal
            | DBCol::_ChunkPerHeightShard
            | DBCol::_NextBlockWithNewChunk
            | DBCol::_LastBlockWithNewChunk
            | DBCol::_TransactionRefCount
            | DBCol::_TransactionResult
            | DBCol::StateChangesForSplitStates
            | DBCol::CachedContractCode
            | DBCol::FlatState
            | DBCol::FlatStateChanges
            | DBCol::FlatStateDeltaMetadata
            | DBCol::FlatStorageStatus
            | DBCol::EpochSyncProof
            | DBCol::Misc
            | DBCol::_ReceiptIdToShardId
            | DBCol::StateShardUIdMapping
            // Note that StateSyncHashes should not ever have too many keys in them
            // because we remove unneeded keys as we add new ones.
            | DBCol::StateSyncHashes
            => unreachable!(),
        }
        self.merge(store_update);
    }
}

/// If block_hash is the resharding_block, cleanup the state of shards that have been resharded.
///
/// When we are GC'ing the last block of the epoch where a resharding happened, we need to cleanup the
/// state of the parent shard. This is done by iterating through the new shard layout and deleting the
/// state of all parent shards.
fn gc_parent_shard_after_resharding(
    chain_store_update: &mut ChainStoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
) -> Result<(), Error> {
    // Clear out state for the parent shard. Note that this function is called at every epoch boundary,
    // even if there is no resharding.
    // It's fine to do that as after the first call to `trie_store_update.delete_shard_uid_prefixed_state`
    // the rest of the calls in future epochs are no-ops.
    if !epoch_manager.is_last_block_in_finished_epoch(block_hash)? {
        return Ok(());
    }

    tracing::debug!(target: "garbage_collection", ?block_hash, "Resharding state cleanup");
    // Given block_hash is the resharding block, shard_layout is the shard layout of the next epoch
    // Important: We are not allowed to call `epoch_manager.get_shard_layout_from_prev_block()` as
    // the function relies on `self.get_block_info(block_info.epoch_first_block())` but epoch_first_block
    // has already been cleaned up.
    // We instead need to rely on chain_store to get the next block hash and use the block_info to get
    // the next epoch id and shard layout.
    let store = chain_store_update.store();
    let next_block_hash = store.chain_store().get_next_block_hash(block_hash)?;
    let next_epoch_id = epoch_manager.get_epoch_id(&next_block_hash)?;
    let shard_layout = epoch_manager.get_shard_layout(&next_epoch_id)?;
    let mut trie_store_update = store.trie_store().store_update();
    for parent_shard_uid in shard_layout.get_split_parent_shard_uids() {
        // Check if any child shard still map to this parent shard
        let children_shards =
            shard_layout.get_children_shards_uids(parent_shard_uid.shard_id()).unwrap();
        let has_active_mapping = children_shards.into_iter().any(|child_shard_uid| {
            let mapped_shard_uid = get_shard_uid_mapping(&store, child_shard_uid);
            mapped_shard_uid == parent_shard_uid && mapped_shard_uid != child_shard_uid
        });
        if !has_active_mapping {
            // Delete the state of the parent shard
            tracing::debug!(target: "garbage_collection", ?parent_shard_uid, "Resharding state cleanup for shard");
            trie_store_update.delete_shard_uid_prefixed_state(parent_shard_uid);
        } else {
            tracing::debug!(target: "garbage_collection", ?parent_shard_uid, "Skipping parent shard cleanup - active mappings exist");
        }
    }

    chain_store_update.merge(trie_store_update.into());
    Ok(())
}

/// State cleanup for single shard tracking. Removes State of shards that are no longer in use.
///
/// Noop if block_hash is NOT the last block of the epoch we are cleaning up.
/// We start by listing all the shard_uids that belong to the epoch we are cleaning up.
/// We filter out the shard_uids that we are currently tracking.
/// We filter out any other shard_uids that we may be tracking from the cleanup epoch upto current epoch.
/// This is done by reverse iterating over the epochs from the chain head to cleanup epoch.
fn gc_state(
    chain_store_update: &mut ChainStoreUpdate,
    epoch_manager: &dyn EpochManagerAdapter,
    block_hash: &CryptoHash,
    shard_tracker: &ShardTracker,
) -> Result<(), Error> {
    // Return if we are not dealing with the last block of the epoch
    if !epoch_manager.is_last_block_in_finished_epoch(block_hash)? {
        return Ok(());
    }

    tracing::debug!(target: "garbage_collection", "GC state");
    let latest_block_hash = chain_store_update.head()?.last_block_hash;
    let last_block_hash_in_gc_epoch = block_hash;

    // Get all the shards that belong to the gc_epoch for shards_to_cleanup
    let block_info = epoch_manager.get_block_info(last_block_hash_in_gc_epoch)?;
    let mut shards_to_cleanup =
        epoch_manager.get_shard_layout(block_info.epoch_id())?.shard_uids().collect_vec();

    // Remove shards that we are currently tracking from shards_to_cleanup
    shards_to_cleanup.retain(|shard_uid| {
        !shard_tracker
            .cares_about_shard_this_or_next_epoch(&latest_block_hash, shard_uid.shard_id())
    });

    // reverse iterate over the epochs starting from epoch of latest_block_hash upto gc_epoch
    // The current_block_hash is the hash of the last block in the current iteration epoch.
    let store = chain_store_update.store();
    let mut current_block_hash = *epoch_manager.get_block_info(&latest_block_hash)?.hash();
    while &current_block_hash != last_block_hash_in_gc_epoch {
        shards_to_cleanup.retain(|shard_uid| {
            // If shard_uid exists in the TrieChanges column, it means we were tracking the shard_uid in this epoch.
            // We would like to remove shard_uid from shards_to_cleanup
            let trie_changes_key = get_block_shard_uid(&current_block_hash, shard_uid);
            !store.exists(DBCol::TrieChanges, &trie_changes_key).unwrap()
        });

        // Go to the previous epoch last_block_hash
        let epoch_block_info = epoch_manager.get_block_info(&current_block_hash)?;
        let epoch_first_block_hash = epoch_block_info.epoch_first_block();
        let epoch_first_block = store.chain_store().get_block_header(epoch_first_block_hash)?;
        current_block_hash = *epoch_first_block.prev_hash();
    }

    // Delete State of `shards_to_cleanup` and associated ShardUId mapping.
    tracing::debug!(target: "garbage_collection", ?shards_to_cleanup, "State shards cleanup");
    let mut trie_store_update = store.trie_store().store_update();
    for shard_uid_prefix in shards_to_cleanup {
        trie_store_update.delete_shard_uid_prefixed_state(shard_uid_prefix);
    }
    chain_store_update.merge(trie_store_update.into());
    Ok(())
}
