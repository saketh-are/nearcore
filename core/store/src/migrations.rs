use crate::db::metadata::{DbKind, KIND_KEY};
use crate::{DBCol, Store, StoreUpdate};
use anyhow::{Context, anyhow};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::epoch_manager::EpochSummary;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::{ChunkHash, StateSyncInfo, StateSyncInfoV0};
use near_primitives::state::FlatStateValue;
use near_primitives::state::PartialState;
use near_primitives::stateless_validation::contract_distribution::{CodeBytes, CodeHash};
use near_primitives::stateless_validation::stored_chunk_state_transition_data::{
    StoredChunkStateTransitionData, StoredChunkStateTransitionDataV1,
};
use near_primitives::transaction::{ExecutionOutcomeWithIdAndProof, ExecutionOutcomeWithProof};
use near_primitives::types::{
    AccountId, EpochId, ShardId, ValidatorId, ValidatorKickoutReason, ValidatorStats,
    validator_stake::ValidatorStake,
};
use near_primitives::types::{BlockChunkValidatorStats, ChunkStats};
use near_primitives::utils::{get_block_shard_id_rev, get_outcome_id_block_hash};
use near_primitives::version::ProtocolVersion;
use std::collections::{BTreeMap, HashMap};
use tracing::info;

pub struct BatchedStoreUpdate<'a> {
    batch_size_limit: usize,
    batch_size: usize,
    store: &'a Store,
    store_update: Option<StoreUpdate>,
    total_size_written: u64,
    printed_total_size_written: u64,
}

const PRINT_PROGRESS_EVERY_BYTES: u64 = bytesize::GIB;

impl<'a> BatchedStoreUpdate<'a> {
    pub fn new(store: &'a Store, batch_size_limit: usize) -> Self {
        Self {
            batch_size_limit,
            batch_size: 0,
            store,
            store_update: Some(store.store_update()),
            total_size_written: 0,
            printed_total_size_written: 0,
        }
    }

    fn commit(&mut self) -> std::io::Result<()> {
        let store_update = self.store_update.take().unwrap();
        store_update.commit()?;
        self.store_update = Some(self.store.store_update());
        self.batch_size = 0;
        Ok(())
    }

    fn set_or_insert_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
        insert: bool,
    ) -> std::io::Result<()> {
        let value_bytes = borsh::to_vec(&value)?;
        let entry_size = key.as_ref().len() + value_bytes.len() + 8;
        self.batch_size += entry_size;
        self.total_size_written += entry_size as u64;
        let update = self.store_update.as_mut().unwrap();
        if insert {
            update.insert(col, key.to_vec(), value_bytes);
        } else {
            update.set(col, key.as_ref(), &value_bytes);
        }

        if self.batch_size > self.batch_size_limit {
            self.commit()?;
        }
        if self.total_size_written - self.printed_total_size_written > PRINT_PROGRESS_EVERY_BYTES {
            info!(
                target: "migrations",
                "Migrations: {} written",
                bytesize::to_string(self.total_size_written, true)
            );
            self.printed_total_size_written = self.total_size_written;
        }

        Ok(())
    }

    pub fn set_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        self.set_or_insert_ser(col, key, value, false)
    }

    pub fn insert_ser<T: BorshSerialize>(
        &mut self,
        col: DBCol,
        key: &[u8],
        value: &T,
    ) -> std::io::Result<()> {
        self.set_or_insert_ser(col, key, value, true)
    }

    pub fn finish(mut self) -> std::io::Result<()> {
        if self.batch_size > 0 {
            self.commit()?;
        }

        Ok(())
    }
}

/// Migrates the database from version 32 to 33.
///
/// This removes the TransactionResult column and moves it to TransactionResultForBlock.
/// The new column removes the need for high-latency read-modify-write operations when committing
/// new blocks.
pub fn migrate_32_to_33(store: &Store) -> anyhow::Result<()> {
    let mut update = BatchedStoreUpdate::new(&store, 10_000_000);
    for row in store.iter_ser::<Vec<ExecutionOutcomeWithIdAndProof>>(DBCol::_TransactionResult) {
        let (_, mut outcomes) = row?;
        // It appears that it was possible that the same entry in the original column contained
        // duplicate outcomes. We remove them here to avoid panicking due to issuing a
        // self-overwriting transaction.
        outcomes.sort_by_key(|outcome| (*outcome.id(), outcome.block_hash));
        outcomes.dedup_by_key(|outcome| (*outcome.id(), outcome.block_hash));
        for outcome in outcomes {
            update.insert_ser(
                DBCol::TransactionResultForBlock,
                &get_outcome_id_block_hash(outcome.id(), &outcome.block_hash),
                &ExecutionOutcomeWithProof {
                    proof: outcome.proof,
                    outcome: outcome.outcome_with_id.outcome,
                },
            )?;
        }
    }
    update.finish()?;
    let mut delete_old_update = store.store_update();
    delete_old_update.delete_all(DBCol::_TransactionResult);
    delete_old_update.commit()?;
    Ok(())
}

/// Migrates the database from version 33 to 34.
///
/// Most importantly, this involves adding KIND entry to DbVersion column,
/// removing IS_ARCHIVAL from BlockMisc column.  Furthermore, migration deletes
/// GCCount column which is no longer used.
///
/// If the database has IS_ARCHIVAL key in BlockMisc column set to true, this
/// overrides value of is_node_archival argument.  Otherwise, the kind of the
/// resulting database is determined based on that argument.
pub fn migrate_33_to_34(store: &Store, mut is_node_archival: bool) -> anyhow::Result<()> {
    const IS_ARCHIVE_KEY: &[u8; 10] = b"IS_ARCHIVE";

    let is_store_archival =
        store.get_ser::<bool>(DBCol::BlockMisc, IS_ARCHIVE_KEY)?.unwrap_or_default();

    if is_store_archival != is_node_archival {
        if is_store_archival {
            tracing::info!(target: "migrations", "Opening an archival database.");
            tracing::warn!(target: "migrations", "Ignoring `archive` client configuration and setting database kind to Archive.");
        } else {
            tracing::info!(target: "migrations", "Running node in archival mode (as per `archive` client configuration).");
            tracing::info!(target: "migrations", "Setting database kind to Archive.");
            tracing::warn!(target: "migrations", "Starting node in non-archival mode will no longer be possible with this database.");
        }
        is_node_archival = true;
    }

    let mut update = store.store_update();
    if is_store_archival {
        update.delete(DBCol::BlockMisc, IS_ARCHIVE_KEY);
    }
    let kind = if is_node_archival { DbKind::Archive } else { DbKind::RPC };
    update.set(DBCol::DbVersion, KIND_KEY, <&str>::from(kind).as_bytes());
    update.delete_all(DBCol::_GCCount);
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 34 to 35.
///
/// This involves deleting contents of Peers column which is now
/// deprecated and no longer used.
pub fn migrate_34_to_35(store: &Store) -> anyhow::Result<()> {
    let mut update = store.store_update();
    update.delete_all(DBCol::_Peers);
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 36 to 37.
///
/// This involves rewriting all FlatStateChanges entries in the new format.
/// The size of that column should not exceed several dozens of entries.
pub fn migrate_36_to_37(store: &Store) -> anyhow::Result<()> {
    #[derive(borsh::BorshDeserialize)]
    struct LegacyFlatStateChanges(HashMap<Vec<u8>, Option<near_primitives::state::ValueRef>>);

    let mut update = store.store_update();
    update.delete_all(DBCol::FlatStateChanges);
    for result in store.iter(DBCol::FlatStateChanges) {
        let (key, old_value) = result?;
        let new_value = borsh::to_vec(&crate::flat::FlatStateChanges(
            LegacyFlatStateChanges::try_from_slice(&old_value)?
                .0
                .into_iter()
                .map(|(key, value_ref)| (key, value_ref.map(|v| FlatStateValue::Ref(v))))
                .collect(),
        ))?;
        update.set(DBCol::FlatStateChanges, &key, &new_value);
    }
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 37 to 38.
///
/// Rewrites FlatStateDeltaMetadata to add a bit to Metadata, `prev_block_with_changes`.
/// That bit is initialized with a `None` regardless of the corresponding flat state changes.
pub fn migrate_37_to_38(store: &Store) -> anyhow::Result<()> {
    #[derive(borsh::BorshDeserialize)]
    struct LegacyFlatStateDeltaMetadata {
        block: crate::flat::BlockInfo,
    }

    let mut update = store.store_update();
    update.delete_all(DBCol::FlatStateDeltaMetadata);
    for result in store.iter(DBCol::FlatStateDeltaMetadata) {
        let (key, old_value) = result?;
        let LegacyFlatStateDeltaMetadata { block } =
            LegacyFlatStateDeltaMetadata::try_from_slice(&old_value)?;
        let new_value =
            crate::flat::FlatStateDeltaMetadata { block, prev_block_with_changes: None };
        update.set(DBCol::FlatStateDeltaMetadata, &key, &borsh::to_vec(&new_value)?);
    }
    update.commit()?;
    Ok(())
}

/// `ValidatorKickoutReason` struct layout before DB version 38, included.
#[derive(BorshDeserialize)]
struct LegacyBlockChunkValidatorStatsV38 {
    pub block_stats: ValidatorStats,
    pub chunk_stats: ValidatorStats,
}

/// `ValidatorKickoutReason` struct layout before DB version 38, included.
#[derive(BorshDeserialize)]
struct LegacyEpochSummaryV38 {
    pub prev_epoch_last_block_hash: CryptoHash,
    /// Proposals from the epoch, only the latest one per account
    pub all_proposals: Vec<ValidatorStake>,
    /// Kickout set, includes slashed
    pub validator_kickout: HashMap<AccountId, ValidatorKickoutReason>,
    /// Only for validators who met the threshold and didn't get slashed
    pub validator_block_chunk_stats: HashMap<AccountId, LegacyBlockChunkValidatorStatsV38>,
    /// Protocol version for next epoch.
    pub next_version: ProtocolVersion,
}

/// Migrates the database from version 38 to 39.
///
/// Rewrites Epoch summary to include endorsement stats.
pub fn migrate_38_to_39(store: &Store) -> anyhow::Result<()> {
    #[derive(BorshSerialize, BorshDeserialize)]
    struct EpochInfoAggregator<T> {
        /// Map from validator index to (num_blocks_produced, num_blocks_expected) so far in the given epoch.
        pub block_tracker: HashMap<ValidatorId, ValidatorStats>,
        /// For each shard, a map of validator id to (num_chunks_produced, num_chunks_expected) so far in the given epoch.
        pub shard_tracker: HashMap<ShardId, HashMap<ValidatorId, T>>,
        /// Latest protocol version that each validator supports.
        pub version_tracker: HashMap<ValidatorId, ProtocolVersion>,
        /// All proposals in this epoch up to this block.
        pub all_proposals: BTreeMap<AccountId, ValidatorStake>,
        /// Id of the epoch that this aggregator is in.
        pub epoch_id: EpochId,
        /// Last block hash recorded.
        pub last_block_hash: CryptoHash,
    }

    type LegacyEpochInfoAggregator = EpochInfoAggregator<ValidatorStats>;
    type NewEpochInfoAggregator = EpochInfoAggregator<ChunkStats>;

    let mut update = store.store_update();

    // Update EpochInfoAggregator
    let maybe_legacy_aggregator: Option<LegacyEpochInfoAggregator> =
        store.get_ser(DBCol::EpochInfo, AGGREGATOR_KEY)?;
    if let Some(legacy_aggregator) = maybe_legacy_aggregator {
        let new_aggregator = NewEpochInfoAggregator {
            block_tracker: legacy_aggregator.block_tracker,
            shard_tracker: legacy_aggregator
                .shard_tracker
                .into_iter()
                .map(|(shard_id, legacy_stats)| {
                    let new_stats = legacy_stats
                        .into_iter()
                        .map(|(validator_id, stats)| {
                            (
                                validator_id,
                                ChunkStats::new_with_production(stats.produced, stats.expected),
                            )
                        })
                        .collect();
                    (shard_id, new_stats)
                })
                .collect(),
            version_tracker: legacy_aggregator.version_tracker,
            all_proposals: legacy_aggregator.all_proposals,
            epoch_id: legacy_aggregator.epoch_id,
            last_block_hash: legacy_aggregator.last_block_hash,
        };
        update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, &new_aggregator)?;
    }

    // Update EpochSummary
    for result in store.iter(DBCol::EpochValidatorInfo) {
        let (key, old_value) = result?;
        let legacy_summary = LegacyEpochSummaryV38::try_from_slice(&old_value)?;
        let new_value = EpochSummary {
            prev_epoch_last_block_hash: legacy_summary.prev_epoch_last_block_hash,
            all_proposals: legacy_summary.all_proposals,
            validator_kickout: legacy_summary.validator_kickout,
            validator_block_chunk_stats: legacy_summary
                .validator_block_chunk_stats
                .into_iter()
                .map(|(account_id, stats)| {
                    let new_stats = BlockChunkValidatorStats {
                        block_stats: stats.block_stats,
                        chunk_stats: ChunkStats::new_with_production(
                            stats.chunk_stats.produced,
                            stats.chunk_stats.expected,
                        ),
                    };
                    (account_id, new_stats)
                })
                .collect(),
            next_next_epoch_version: legacy_summary.next_version,
        };
        update.set(DBCol::EpochValidatorInfo, &key, &borsh::to_vec(&new_value)?);
    }

    update.commit()?;
    Ok(())
}

/// Migrates the database from version 39 to 40.
///
/// This involves deleting contents of _ReceiptIdToShardId column which is now
/// deprecated and no longer used.
pub fn migrate_39_to_40(store: &Store) -> anyhow::Result<()> {
    let _span =
        tracing::info_span!(target: "migrations", "Deleting contents of deprecated _ReceiptIdToShardId column").entered();
    let mut update = store.store_update();
    update.delete_all(DBCol::_ReceiptIdToShardId);
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 40 to 41.
///
/// The migration replaces non-enum StoredChunkStateTransitionData struct with its enum version V1.
/// NOTE: The data written by this migration is overridden by migrate_42_to_43 to a different format.
pub fn migrate_40_to_41(store: &Store) -> anyhow::Result<()> {
    #[derive(BorshDeserialize)]
    pub struct DeprecatedStoredChunkStateTransitionData {
        pub base_state: PartialState,
        pub receipts_hash: CryptoHash,
    }

    let _span =
        tracing::info_span!(target: "migrations", "Replacing StoredChunkStateTransitionData with its enum version V1").entered();
    let mut update = store.store_update();
    for result in store.iter(DBCol::StateTransitionData) {
        let (key, old_value) = result?;
        let DeprecatedStoredChunkStateTransitionData { base_state, receipts_hash } =
            DeprecatedStoredChunkStateTransitionData::try_from_slice(&old_value)?;
        let new_value = borsh::to_vec(&DeprecatedStoredChunkStateTransitionDataEnum::V1(
            DeprecatedStoredChunkStateTransitionDataV1 {
                base_state,
                receipts_hash,
                contract_accesses: Default::default(),
            },
        ))?;
        update.set(DBCol::StateTransitionData, &key, &new_value);
    }
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 41 to 42.
///
/// This rewrites the contents of the StateDlInfos column
pub fn migrate_41_to_42(store: &Store) -> anyhow::Result<()> {
    #[derive(BorshSerialize, BorshDeserialize)]
    struct LegacyShardInfo(ShardId, ChunkHash);

    #[derive(BorshSerialize, BorshDeserialize)]
    struct LegacyStateSyncInfo {
        sync_hash: CryptoHash,
        shards: Vec<LegacyShardInfo>,
    }

    let mut update = store.store_update();

    for row in store.iter_ser::<LegacyStateSyncInfo>(DBCol::StateDlInfos) {
        let (key, LegacyStateSyncInfo { sync_hash, shards }) =
            row.context("failed deserializing legacy StateSyncInfo in StateDlInfos")?;

        let epoch_first_block = CryptoHash::try_from_slice(&key)
            .context("failed deserializing CryptoHash key in StateDlInfos")?;

        if epoch_first_block != sync_hash {
            tracing::warn!(key = %epoch_first_block, %sync_hash, "sync_hash field of legacy StateSyncInfo not equal to the key. Something is wrong with this node's catchup info");
        }
        let shards =
            shards.into_iter().map(|LegacyShardInfo(shard_id, _chunk_hash)| shard_id).collect();
        let new_info = StateSyncInfo::V0(StateSyncInfoV0 { sync_hash, shards });
        update
            .set_ser(DBCol::StateDlInfos, &key, &new_info)
            .context("failed writing to StateDlInfos")?;
    }
    update.commit()?;
    Ok(())
}

#[derive(BorshSerialize, BorshDeserialize)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
enum DeprecatedStoredChunkStateTransitionDataEnum {
    V1(DeprecatedStoredChunkStateTransitionDataV1) = 0,
    V2(DeprecatedStoredChunkStateTransitionDataV2) = 1,
    V3(DeprecatedStoredChunkStateTransitionDataV3) = 2,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct DeprecatedStoredChunkStateTransitionDataV1 {
    base_state: PartialState,
    receipts_hash: CryptoHash,
    contract_accesses: Vec<CodeHash>,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct DeprecatedStoredChunkStateTransitionDataV2 {
    base_state: PartialState,
    receipts_hash: CryptoHash,
    contract_accesses: Vec<CodeHash>,
    // This field is ignored since it only contains code hashes.
    _contract_deploys: Vec<CodeHash>,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct DeprecatedStoredChunkStateTransitionDataV3 {
    base_state: PartialState,
    receipts_hash: CryptoHash,
    contract_accesses: Vec<CodeHash>,
    contract_deploys: Vec<CodeBytes>,
}

/// Migrates the database from version 42 to 43.
///
/// Merges versions V1-V3 of StoredChunkStateTransitionData into a single version.
pub fn migrate_42_to_43(store: &Store) -> anyhow::Result<()> {
    let _span =
        tracing::info_span!(target: "migrations", "Merging versions V1-V3 of StoredChunkStateTransitionData into single version").entered();
    let mut update = store.store_update();
    for result in store.iter(DBCol::StateTransitionData) {
        let (key, old_value) = result?;

        let old_data = DeprecatedStoredChunkStateTransitionDataEnum::try_from_slice(&old_value).map_err(|err| {
            if let Ok((block_hash, shard_id)) = get_block_shard_id_rev(&key) {
                anyhow!("Failed to parse StoredChunkStateTransitionData in DB. Block: {:?}, Shard: {:?}, Error: {:?}", block_hash, shard_id, err)
            } else {
                anyhow!("Failed to parse StoredChunkStateTransitionData in DB. Key: {:?}, Error: {:?}", key, err)
            }
        })?;
        let (base_state, receipts_hash, contract_accesses, contract_deploys) = match old_data {
            DeprecatedStoredChunkStateTransitionDataEnum::V1(
                DeprecatedStoredChunkStateTransitionDataV1 {
                    base_state,
                    receipts_hash,
                    contract_accesses,
                },
            )
            | DeprecatedStoredChunkStateTransitionDataEnum::V2(
                DeprecatedStoredChunkStateTransitionDataV2 {
                    base_state,
                    receipts_hash,
                    contract_accesses,
                    ..
                },
            ) => (base_state, receipts_hash, contract_accesses, vec![]),
            DeprecatedStoredChunkStateTransitionDataEnum::V3(
                DeprecatedStoredChunkStateTransitionDataV3 {
                    base_state,
                    receipts_hash,
                    contract_accesses,
                    contract_deploys,
                },
            ) => (base_state, receipts_hash, contract_accesses, contract_deploys),
        };
        let new_value =
            borsh::to_vec(&StoredChunkStateTransitionData::V1(StoredChunkStateTransitionDataV1 {
                base_state,
                receipts_hash,
                contract_accesses,
                contract_deploys,
            }))?;
        update.set(DBCol::StateTransitionData, &key, &new_value);
    }
    update.commit()?;
    Ok(())
}

/// Migrates the database from version 44 to 45.
///
/// Removes STATE_TRANSITION_START_HEIGHTS key from DBCol::Misc that is no longer needed.
pub fn migrate_44_to_45(store: &Store) -> anyhow::Result<()> {
    pub const STATE_TRANSITION_START_HEIGHTS: &[u8] = b"STATE_TRANSITION_START_HEIGHTS";

    let mut update = store.store_update();
    update.delete(DBCol::Misc, STATE_TRANSITION_START_HEIGHTS);
    update.commit()?;
    Ok(())
}
