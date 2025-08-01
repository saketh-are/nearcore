use crate::adapter::StoreUpdateAdapter;
use crate::adapter::trie_store::get_shard_uid_mapping;
use crate::columns::DBKeyType;
use crate::db::{COLD_HEAD_KEY, ColdDB, HEAD_KEY};
use crate::{DBCol, DBTransaction, Database, Store, TrieChanges, metrics};

use borsh::BorshDeserialize;
use near_primitives::block::{Block, BlockHeader, Tip};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::sharding::ShardChunk;
use near_primitives::types::{BlockHeight, ShardId};
use rayon::prelude::{IntoParallelIterator, ParallelIterator};
use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::Arc;
use strum::IntoEnumIterator;

type StoreKey = Vec<u8>;
type StoreValue = Option<Vec<u8>>;

/// This trait is used on top of Store to calculate cold loop specific metrics,
/// and implement conversion to errors for absent data.
pub trait ColdMigrationStore {
    fn iter_prefix_with_callback_for_cold(
        &self,
        col: DBCol,
        key_prefix: &[u8],
        callback: impl FnMut(Box<[u8]>),
    ) -> io::Result<()>;

    fn get_for_cold(&self, column: DBCol, key: &[u8]) -> io::Result<StoreValue>;

    fn get_ser_for_cold<T: BorshDeserialize>(
        &self,
        column: DBCol,
        key: &[u8],
    ) -> io::Result<Option<T>>;

    fn get_or_err_for_cold(&self, column: DBCol, key: &[u8]) -> io::Result<Vec<u8>>;

    fn get_ser_or_err_for_cold<T: BorshDeserialize>(
        &self,
        column: DBCol,
        key: &[u8],
    ) -> io::Result<T>;
}

/// The BatchTransaction can be used to write multiple set operations to the cold db in batches.
/// [`write`] is called every time `transaction_size` overgrows `threshold_transaction_size`.
/// [`write`] should also be called manually before dropping BatchTransaction to write any leftovers.
struct BatchTransaction {
    cold_db: Arc<ColdDB>,
    transaction: DBTransaction,
    /// Size of all values keys and values in `transaction` in bytes.
    transaction_size: usize,
    /// Minimum size, after which we write transaction
    threshold_transaction_size: usize,
}

/// Updates provided cold database from provided hot store with information about block at `height`.
/// Block at `height` has to be final and present in `hot_store`.
///
/// First, we read from hot store information necessary
/// to determine all the keys that need to be updated in cold db.
/// Then we write updates to cold db column by column.
///
/// This approach is used, because a key for db often combines several parts,
/// and many of those parts are reused across several cold columns (block hash, shard id, chunk hash, tx hash, ...).
/// Rather than manually combining those parts in the right order for every cold column,
/// we define `DBCol::key_type` to determine how a key for the column is formed,
/// `get_keys_from_store` to determine all possible keys only for needed key parts,
/// and `combine_keys` to generated all possible whole keys for the column based on order of those parts.
///
/// To add a new column to cold storage, we need to
/// 1. add it to `DBCol::is_cold` list
/// 2. define `DBCol::key_type` for it (if it isn't already defined)
/// 3. add new clause in `get_keys_from_store` for new key types used for this column (if there are any)
pub fn update_cold_db(
    cold_db: &ColdDB,
    hot_store: &Store,
    shard_layout: &ShardLayout,
    tracked_shards: &Vec<ShardUId>,
    height: &BlockHeight,
    is_resharding_boundary: bool,
    num_threads: usize,
) -> io::Result<()> {
    let _span = tracing::debug_span!(target: "cold_store", "update cold db", height = height);
    let _timer = metrics::COLD_COPY_DURATION.start_timer();

    let height_key = height.to_le_bytes();
    let block_hash_vec = hot_store.get_or_err_for_cold(DBCol::BlockHeight, &height_key)?;
    let block_hash_key = block_hash_vec.as_slice();

    let key_type_to_keys =
        get_keys_from_store(&hot_store, shard_layout, tracked_shards, &height_key, block_hash_key)?;
    let columns_to_update = DBCol::iter()
        .filter(|col| {
            // DBCol::StateShardUIdMapping is handled separately
            col.is_cold() && col != &DBCol::StateShardUIdMapping
        })
        .collect::<Vec<DBCol>>();

    // Create new thread pool with `num_threads`.
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build()
        .map_err(|_| io::Error::new(io::ErrorKind::Other, "Failed to create rayon pool"))?
        .install(|| {
            columns_to_update
                .into_par_iter() // Process cold columns to update as a separate task in thread pool in parallel.
                // Copy column to cold db.
                .map(|col: DBCol| -> io::Result<()> {
                    if col == DBCol::State {
                        if is_resharding_boundary {
                            update_state_shard_uid_mapping(cold_db, shard_layout)?;
                        }
                        copy_state_from_store(
                            shard_layout,
                            &tracked_shards,
                            block_hash_key,
                            cold_db,
                            &hot_store,
                        )
                    } else {
                        let keys = combine_keys(&key_type_to_keys, &col.key_type());
                        copy_from_store(cold_db, &hot_store, col, keys)
                    }
                })
                // Return first found error, or Ok(())
                .reduce(
                    || Ok(()), // Ok(()) by default
                    // First found Err, or Ok(())g
                    |left, right| -> io::Result<()> {
                        vec![left, right]
                            .into_iter()
                            .filter(|res| res.is_err())
                            .next()
                            .unwrap_or(Ok(()))
                    },
                )
        })?;
    Ok(())
}

// Correctly set the key and value on DBTransaction, taking reference counting
// into account. For non-rc columns it just sets the value. For rc columns it
// appends rc = 1 to the value and sets it.
fn rc_aware_set(
    transaction: &mut DBTransaction,
    col: DBCol,
    key: Vec<u8>,
    mut value: Vec<u8>,
) -> usize {
    const ONE: &[u8] = &1i64.to_le_bytes();
    match col.is_rc() {
        false => {
            let size = key.len() + value.len();
            transaction.set(col, key, value);
            return size;
        }
        true => {
            value.extend_from_slice(&ONE);
            let size = key.len() + value.len();
            transaction.update_refcount(col, key, value);
            return size;
        }
    };
}

/// Updates the shard_uid mapping for all children of split shards to point to
/// the same shard_uid as their parent. If a parent was already mapped, the
/// children will point to the grandparent.
/// This should be called once while processing the block at the resharding
/// boundary and before calling `copy_state_from_store`.
fn update_state_shard_uid_mapping(cold_db: &ColdDB, shard_layout: &ShardLayout) -> io::Result<()> {
    let _span = tracing::debug_span!(target: "cold_store", "update_state_shard_uid_mapping");
    let cold_store = cold_db.as_store();
    let mut update = cold_store.store_update();
    let split_parents = shard_layout.get_split_parent_shard_uids();
    for parent_shard_uid in split_parents {
        // Need to check if the parent itself was previously mapped.
        let mapped_shard_uid = get_shard_uid_mapping(&cold_store, parent_shard_uid);
        let children = shard_layout
            .get_children_shards_uids(parent_shard_uid.shard_id())
            .expect("get_children_shards_uids should not fail for split parents");
        for child_shard_uid in children {
            update.trie_store_update().set_shard_uid_mapping(child_shard_uid, mapped_shard_uid);
        }
    }
    update.commit()
}

// A specialized version of copy_from_store for the State column. Finds all the
// State nodes that were inserted at given height by reading TrieChanges from
// all shards and inserts them into the cold store. While `tracked_shards`
// determine what data must be present, we iterate over all shards as a
// precaution to avoid missing data due to unexpected issues, making the process
// more robust and future-proof.
//
// The generic implementation is not efficient for State because it would
// attempt to read every node from every shard. Here we know exactly what shard
// the node belongs to.
fn copy_state_from_store(
    shard_layout: &ShardLayout,
    tracked_shards: &Vec<ShardUId>,
    block_hash_key: &[u8],
    cold_db: &ColdDB,
    hot_store: &Store,
) -> io::Result<()> {
    let col = DBCol::State;
    let _span = tracing::debug_span!(target: "cold_store", "copy_state_from_store", %col);
    let instant = std::time::Instant::now();
    let cold_store = cold_db.as_store();

    let mut total_keys = 0;
    let mut total_size = 0;
    let mut transaction = DBTransaction::new();
    let mut copied_shards = HashSet::<ShardUId>::new();
    for shard_uid in shard_layout.shard_uids() {
        debug_assert_eq!(
            DBCol::TrieChanges.key_type(),
            &[DBKeyType::BlockHash, DBKeyType::ShardUId]
        );

        let shard_uid_key = shard_uid.to_bytes();
        let key = join_two_keys(&block_hash_key, &shard_uid_key);
        let trie_changes: Option<TrieChanges> =
            hot_store.get_ser::<TrieChanges>(DBCol::TrieChanges, &key)?;

        let Some(trie_changes) = trie_changes else { continue };
        copied_shards.insert(shard_uid);
        total_keys += trie_changes.insertions().len();
        let mapped_shard_uid_key = get_shard_uid_mapping(&cold_store, shard_uid).to_bytes();
        for op in trie_changes.insertions() {
            // TODO(resharding) Test it properly. Currently this path is not triggered in testloop.
            let key = join_two_keys(&mapped_shard_uid_key, op.hash().as_bytes());
            let value = op.payload().to_vec();

            total_size += value.len();
            tracing::trace!(target: "cold_store", pretty_key=?near_fmt::StorageKey(&key), "copying state node to colddb");
            rc_aware_set(&mut transaction, DBCol::State, key, value);
        }
    }
    for tracked_shard in tracked_shards {
        if !copied_shards.contains(tracked_shard) {
            let error_message = format!("TrieChanges for {tracked_shard} not present in hot store");
            return Err(io::Error::new(io::ErrorKind::NotFound, error_message));
        }
    }
    // We know that `copied_shards` includes all `tracked_shards`.
    // Emit a warning if `copied_shards` contains any unexpected extra shards.
    if copied_shards.len() > tracked_shards.len() {
        tracing::warn!(target: "cold_store", "Copied state for shards {:?} while tracking {:?}", copied_shards, tracked_shards);
    }

    let read_duration = instant.elapsed();

    let instant = std::time::Instant::now();
    cold_db.write(transaction)?;
    let write_duration = instant.elapsed();

    tracing::trace!(target: "cold_store", ?total_keys, ?total_size, ?read_duration, ?write_duration, "copy_state_from_store finished");

    Ok(())
}

/// Gets values for given keys in a column from provided hot_store.
/// Creates a transaction based on that values with set DBOp s.
/// Writes that transaction to cold_db.
fn copy_from_store(
    cold_db: &ColdDB,
    hot_store: &Store,
    col: DBCol,
    keys: Vec<StoreKey>,
) -> io::Result<()> {
    debug_assert!(col.is_cold());

    // note this function should only be used for state in tests where it's
    // needed to copy state records from genesis

    let _span = tracing::debug_span!(target: "cold_store", "copy_from_store", col = %col);
    let instant = std::time::Instant::now();

    let mut transaction = DBTransaction::new();
    let mut good_keys = 0;
    let mut total_size = 0;
    let total_keys = keys.len();
    for key in keys {
        // TODO: Look into using RocksDB's multi_key function.  It
        // might speed things up.  Currently our Database abstraction
        // doesn't offer interface for it so that would need to be
        // added.
        let data = hot_store.get_for_cold(col, &key)?;
        if let Some(value) = data {
            // TODO: As an optimization, we might consider breaking the
            // abstraction layer.  Since we're always writing to cold database,
            // rather than using `cold_db: &dyn Database` argument we could have
            // `cold_db: &ColdDB` and then some custom function which lets us
            // write raw bytes. This would also allow us to bypass stripping and
            // re-adding the reference count.

            good_keys += 1;
            total_size += value.len();
            rc_aware_set(&mut transaction, col, key, value);
        }
    }

    let read_duration = instant.elapsed();

    let instant = std::time::Instant::now();
    cold_db.write(transaction)?;
    let write_duration = instant.elapsed();

    tracing::trace!(target: "cold_store", ?col, ?good_keys, ?total_keys, ?total_size, ?read_duration, ?write_duration, "copy_from_store finished");

    return Ok(());
}

/// This function sets the cold head to the Tip that reflect provided height in two places:
/// - In cold storage in HEAD key in BlockMisc column.
/// - In hot storage in COLD_HEAD key in BlockMisc column.
/// This function should be used after all of the blocks from genesis to `height` inclusive had been copied.
///
/// This method relies on the fact that BlockHeight and BlockHeader are not garbage collectable.
/// (to construct the Tip we query hot_store for block hash and block header)
/// If this is to change, caller should be careful about `height` not being garbage collected in hot storage yet.
pub fn update_cold_head(
    cold_db: &ColdDB,
    hot_store: &Store,
    height: &BlockHeight,
) -> io::Result<()> {
    tracing::debug!(target: "cold_store", "update HEAD of cold db to {}", height);

    let height_key = height.to_le_bytes();
    let block_hash_key =
        hot_store.get_or_err_for_cold(DBCol::BlockHeight, &height_key)?.as_slice().to_vec();
    let tip_header =
        &hot_store.get_ser_or_err_for_cold::<BlockHeader>(DBCol::BlockHeader, &block_hash_key)?;
    let tip = Tip::from_header(tip_header);

    // Write HEAD to the cold db.
    {
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::BlockMisc, HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
        cold_db.write(transaction)?;
    }

    // Write COLD_HEAD_KEY to the cold db.
    {
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
        cold_db.write(transaction)?;
    }

    // Write COLD_HEAD to the hot db.
    {
        let mut transaction = DBTransaction::new();
        transaction.set(DBCol::BlockMisc, COLD_HEAD_KEY.to_vec(), borsh::to_vec(&tip)?);
        hot_store.database().write(transaction)?;

        crate::metrics::COLD_HEAD_HEIGHT.set(*height as i64);
    }

    return Ok(());
}

/// Reads the cold-head from the Cold DB.
pub fn get_cold_head(cold_db: &ColdDB) -> io::Result<Option<Tip>> {
    cold_db
        .get_raw_bytes(DBCol::BlockMisc, HEAD_KEY)?
        .as_deref()
        .map(Tip::try_from_slice)
        .transpose()
}

pub enum CopyAllDataToColdStatus {
    EverythingCopied,
    Interrupted,
}

/// Copies all contents of all cold columns from `hot_store` to `cold_db`.
/// Does it column by column, and because columns can be huge, writes in batches of ~`batch_size`.
pub fn copy_all_data_to_cold(
    cold_db: Arc<ColdDB>,
    hot_store: &Store,
    batch_size: usize,
    keep_going: &Arc<std::sync::atomic::AtomicBool>,
) -> io::Result<CopyAllDataToColdStatus> {
    for col in DBCol::iter() {
        if col.is_cold() {
            tracing::info!(target: "cold_store", ?col, "Started column migration");
            let mut transaction = BatchTransaction::new(cold_db.clone(), batch_size);
            for result in hot_store.iter(col) {
                if !keep_going.load(std::sync::atomic::Ordering::Relaxed) {
                    tracing::debug!(target: "cold_store", "stopping copy_all_data_to_cold");
                    return Ok(CopyAllDataToColdStatus::Interrupted);
                }
                let (key, value) = result?;
                transaction.set_and_write_if_full(col, key.to_vec(), value.to_vec())?;
            }
            transaction.write()?;
            tracing::info!(target: "cold_store", ?col, "Finished column migration");
        }
    }
    Ok(CopyAllDataToColdStatus::EverythingCopied)
}

// The copy_state_from_store function depends on the state nodes to be present
// in the trie changes. This isn't the case for genesis so instead this method
// can be used to copy the genesis records from hot to cold.
// TODO - How did copying from genesis worked in the prod migration to split storage?
pub fn test_cold_genesis_update(cold_db: &ColdDB, hot_store: &Store) -> io::Result<()> {
    for col in DBCol::iter() {
        if !col.is_cold() {
            continue;
        }

        // Note that we use the generic implementation of `copy_from_store` also
        // for the State column that otherwise should be copied using the
        // specialized `copy_state_from_store`.
        copy_from_store(
            cold_db,
            &hot_store,
            col,
            hot_store.iter(col).map(|x| x.unwrap().0.to_vec()).collect(),
        )?;
    }
    Ok(())
}

pub fn test_get_store_reads(column: DBCol) -> u64 {
    crate::metrics::COLD_MIGRATION_READS.with_label_values(&[<&str>::from(column)]).get()
}

pub fn test_get_store_initial_writes(column: DBCol) -> u64 {
    crate::metrics::COLD_STORE_MIGRATION_BATCH_WRITE_COUNT
        .with_label_values(&[<&str>::from(column)])
        .get()
}

/// Returns HashMap from DBKeyType to possible keys of that type for provided height.
/// Only constructs keys for key types that are used in cold columns.
/// The goal is to capture all changes to db made during production of the block at provided height.
/// So, for every KeyType we need to capture all the keys that are related to that block.
/// For BlockHash it is just one key -- block hash of that height.
/// But for TransactionHash, for example, it is all of the tx hashes in that block.
///
/// Although `tracked_shards` should be sufficient to determine which shards matter,
/// we process all shards as a precaution to ensure robustness and avoid
/// missing data in case `tracked_shards` are incomplete due to bugs or future
/// changes. This redundancy is acceptable because only the State column is
/// performance- and size-sensitive, and it is handled separately in `copy_state_from_store`.
fn get_keys_from_store(
    store: &Store,
    shard_layout: &ShardLayout,
    tracked_shards: &Vec<ShardUId>,
    height_key: &[u8],
    block_hash_key: &[u8],
) -> io::Result<HashMap<DBKeyType, Vec<StoreKey>>> {
    let mut key_type_to_keys = HashMap::new();
    let tracked_shards: HashSet<ShardId> =
        tracked_shards.iter().map(|shard_uid| shard_uid.shard_id()).collect();

    let block: Block = store.get_ser_or_err_for_cold(DBCol::Block, &block_hash_key)?;
    let mut chunk_hashes = vec![];
    let mut chunks = vec![];
    // TODO(cloud_archival): Maybe iterate over only new chunks?
    for chunk_header in block.chunks().iter() {
        let chunk_hash = chunk_header.chunk_hash();
        chunk_hashes.push(chunk_hash.clone());
        let chunk: Option<ShardChunk> =
            store.get_ser_for_cold(DBCol::Chunks, chunk_hash.as_bytes())?;
        let shard_id = chunk_header.shard_id();
        let Some(chunk) = chunk else {
            // TODO(cloud_archival): Uncomment the check below and cover it with test
            // if chunk_header.height_included() == block.header().height()
            //     && tracked_shards.contains(&shard_id)
            // {
            //     let error_message =
            //         format!("Chunk missing for shard {shard_id}, hash {chunk_hash:?}");
            //     return Err(io::Error::new(io::ErrorKind::NotFound, error_message));
            // }
            continue;
        };
        if !tracked_shards.contains(&shard_id) {
            tracing::warn!(target: "cold_store", "Copied chunk for shard {} which is not tracked at height {}", shard_id, block.header().height());
        }
        chunks.push(chunk);
    }
    for key_type in DBKeyType::iter() {
        if key_type == DBKeyType::TrieNodeOrValueHash {
            // The TrieNodeOrValueHash is only used in the State column, which is handled separately.
            continue;
        }

        key_type_to_keys.insert(
            key_type,
            match key_type {
                DBKeyType::TrieNodeOrValueHash => {
                    unreachable!();
                }
                DBKeyType::BlockHeight => vec![height_key.to_vec()],
                DBKeyType::BlockHash => vec![block_hash_key.to_vec()],
                DBKeyType::PreviousBlockHash => {
                    vec![block.header().prev_hash().as_bytes().to_vec()]
                }
                DBKeyType::ShardId => shard_layout
                    .shard_ids()
                    .map(|shard_id| shard_id.to_le_bytes().to_vec())
                    .collect(),
                DBKeyType::ShardUId => shard_layout
                    .shard_uids()
                    .map(|shard_uid| shard_uid.to_bytes().to_vec())
                    .collect(),
                // TODO: write StateChanges values to colddb directly, not to cache.
                DBKeyType::TrieKey => {
                    let mut keys = vec![];
                    store.iter_prefix_with_callback_for_cold(
                        DBCol::StateChanges,
                        &block_hash_key,
                        |full_key| {
                            let mut full_key = Vec::from(full_key);
                            full_key.drain(..block_hash_key.len());
                            keys.push(full_key);
                        },
                    )?;
                    keys
                }
                DBKeyType::TransactionHash => chunks
                    .iter()
                    .flat_map(|c| {
                        c.to_transactions().iter().map(|t| t.get_hash().as_bytes().to_vec())
                    })
                    .collect(),
                DBKeyType::ReceiptHash => chunks
                    .iter()
                    .flat_map(|c| {
                        c.prev_outgoing_receipts().iter().map(|r| r.get_hash().as_bytes().to_vec())
                    })
                    .collect(),
                DBKeyType::ChunkHash => {
                    chunk_hashes.iter().map(|chunk_hash| chunk_hash.as_bytes().to_vec()).collect()
                }
                DBKeyType::OutcomeId => {
                    debug_assert_eq!(
                        DBCol::OutcomeIds.key_type(),
                        &[DBKeyType::BlockHash, DBKeyType::ShardId]
                    );
                    shard_layout
                        .shard_ids()
                        .map(|shard_id| {
                            store.get_ser(
                                DBCol::OutcomeIds,
                                &join_two_keys(&block_hash_key, &shard_id.to_le_bytes()),
                            )
                        })
                        .collect::<io::Result<Vec<Option<Vec<CryptoHash>>>>>()?
                        .into_iter()
                        .flat_map(|hashes| {
                            hashes
                                .unwrap_or_default()
                                .into_iter()
                                .map(|hash| hash.as_bytes().to_vec())
                        })
                        .collect()
                }
                _ => {
                    vec![]
                }
            },
        );
    }

    Ok(key_type_to_keys)
}

pub fn join_two_keys(prefix_key: &[u8], suffix_key: &[u8]) -> StoreKey {
    [prefix_key, suffix_key].concat()
}

/// Returns all possible keys for a column with key represented by a specific sequence of key types.
/// `key_type_to_value` -- result of `get_keys_from_store`, mapping from KeyType to all possible keys of that type.
/// `key_types` -- description of a final key, what sequence of key types forms a key, result of `DBCol::key_type`.
/// Basically, returns all possible combinations of keys from `key_type_to_value` for given order of key types.
pub fn combine_keys(
    key_type_to_value: &HashMap<DBKeyType, Vec<StoreKey>>,
    key_types: &[DBKeyType],
) -> Vec<StoreKey> {
    combine_keys_with_stop(key_type_to_value, key_types, key_types.len())
}

/// Recursive method to create every combination of keys values for given order of key types.
/// stop: usize -- what length of key_types to consider.
/// first generates all the key combination for first stop - 1 key types
/// then adds every key value for the last key type to every key value generated by previous call.
fn combine_keys_with_stop(
    key_type_to_keys: &HashMap<DBKeyType, Vec<StoreKey>>,
    keys_order: &[DBKeyType],
    stop: usize,
) -> Vec<StoreKey> {
    // if no key types are provided, return one empty key value
    if stop == 0 {
        return vec![StoreKey::new()];
    }
    let last_kt = &keys_order[stop - 1];
    // if one of the key types has no keys, no need to calculate anything, the result is empty
    if key_type_to_keys[last_kt].is_empty() {
        return vec![];
    }
    let all_smaller_keys = combine_keys_with_stop(key_type_to_keys, keys_order, stop - 1);
    let mut result_keys = vec![];
    for prefix_key in &all_smaller_keys {
        for suffix_key in &key_type_to_keys[last_kt] {
            result_keys.push(join_two_keys(prefix_key, suffix_key));
        }
    }
    result_keys
}

fn option_to_not_found<T, F>(res: io::Result<Option<T>>, field_name: F) -> io::Result<T>
where
    F: std::string::ToString,
{
    match res {
        Ok(Some(o)) => Ok(o),
        Ok(None) => Err(io::Error::new(io::ErrorKind::NotFound, field_name.to_string())),
        Err(e) => Err(e),
    }
}

impl ColdMigrationStore for Store {
    fn iter_prefix_with_callback_for_cold(
        &self,
        col: DBCol,
        key_prefix: &[u8],
        mut callback: impl FnMut(Box<[u8]>),
    ) -> io::Result<()> {
        for iter_result in self.iter_prefix(col, key_prefix) {
            crate::metrics::COLD_MIGRATION_READS.with_label_values(&[<&str>::from(col)]).inc();
            let (key, _) = iter_result?;
            callback(key);
        }
        Ok(())
    }

    fn get_for_cold(&self, column: DBCol, key: &[u8]) -> io::Result<StoreValue> {
        crate::metrics::COLD_MIGRATION_READS.with_label_values(&[<&str>::from(column)]).inc();
        Ok(self.get(column, key)?.map(|x| x.as_slice().to_vec()))
    }

    fn get_ser_for_cold<T: BorshDeserialize>(
        &self,
        column: DBCol,
        key: &[u8],
    ) -> io::Result<Option<T>> {
        match self.get_for_cold(column, key)? {
            Some(bytes) => Ok(Some(T::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    fn get_or_err_for_cold(&self, column: DBCol, key: &[u8]) -> io::Result<Vec<u8>> {
        option_to_not_found(self.get_for_cold(column, key), format_args!("{:?}: {:?}", column, key))
    }

    fn get_ser_or_err_for_cold<T: BorshDeserialize>(
        &self,
        column: DBCol,
        key: &[u8],
    ) -> io::Result<T> {
        option_to_not_found(
            self.get_ser_for_cold(column, key),
            format_args!("{:?}: {:?}", column, key),
        )
    }
}

impl BatchTransaction {
    pub fn new(cold_db: Arc<ColdDB>, batch_size: usize) -> Self {
        Self {
            cold_db,
            transaction: DBTransaction::new(),
            transaction_size: 0,
            threshold_transaction_size: batch_size,
        }
    }

    /// Adds a set DBOp to `self.transaction`. Updates `self.transaction_size`.
    /// If `self.transaction_size` becomes too big, calls for write.
    pub fn set_and_write_if_full(
        &mut self,
        col: DBCol,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> io::Result<()> {
        let size = rc_aware_set(&mut self.transaction, col, key, value);
        self.transaction_size += size;

        if self.transaction_size > self.threshold_transaction_size {
            self.write()?;
        }
        Ok(())
    }

    /// Writes `self.transaction` and replaces it with new empty DBTransaction.
    /// Sets `self.transaction_size` to 0.
    fn write(&mut self) -> io::Result<()> {
        if self.transaction.ops.is_empty() {
            return Ok(());
        }

        let column_label = [<&str>::from(self.transaction.ops[0].col())];

        crate::metrics::COLD_STORE_MIGRATION_BATCH_WRITE_COUNT
            .with_label_values(&column_label)
            .inc();
        let _timer = crate::metrics::COLD_STORE_MIGRATION_BATCH_WRITE_TIME
            .with_label_values(&column_label)
            .start_timer();

        tracing::info!(
                target: "cold_store",
                ?column_label,
                tx_size_in_megabytes = self.transaction_size as f64 / 1e6,
                "Writing a Cold Store transaction");

        let transaction = std::mem::take(&mut self.transaction);
        self.cold_db.write(transaction)?;
        self.transaction_size = 0;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{StoreKey, combine_keys};
    use crate::columns::DBKeyType;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_combine_keys() {
        // What DBKeyType s are used here does not matter
        let key_type_to_keys = HashMap::from([
            (DBKeyType::BlockHash, vec![vec![1, 2, 3], vec![2, 3]]),
            (DBKeyType::BlockHeight, vec![vec![0, 1], vec![3, 4, 5]]),
            (DBKeyType::ShardId, vec![]),
        ]);

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &[DBKeyType::BlockHash, DBKeyType::BlockHeight]
            )),
            HashSet::<StoreKey>::from_iter(vec![
                vec![1, 2, 3, 0, 1],
                vec![1, 2, 3, 3, 4, 5],
                vec![2, 3, 0, 1],
                vec![2, 3, 3, 4, 5]
            ])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &[DBKeyType::BlockHeight, DBKeyType::BlockHash, DBKeyType::BlockHeight]
            )),
            HashSet::<StoreKey>::from_iter(vec![
                vec![0, 1, 1, 2, 3, 0, 1],
                vec![0, 1, 1, 2, 3, 3, 4, 5],
                vec![0, 1, 2, 3, 0, 1],
                vec![0, 1, 2, 3, 3, 4, 5],
                vec![3, 4, 5, 1, 2, 3, 0, 1],
                vec![3, 4, 5, 1, 2, 3, 3, 4, 5],
                vec![3, 4, 5, 2, 3, 0, 1],
                vec![3, 4, 5, 2, 3, 3, 4, 5]
            ])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &[DBKeyType::ShardId, DBKeyType::BlockHeight]
            )),
            HashSet::<StoreKey>::from_iter(vec![])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(
                &key_type_to_keys,
                &[DBKeyType::BlockHash, DBKeyType::ShardId]
            )),
            HashSet::<StoreKey>::from_iter(vec![])
        );

        assert_eq!(
            HashSet::<StoreKey>::from_iter(combine_keys(&key_type_to_keys, &[])),
            HashSet::<StoreKey>::from_iter(vec![vec![]])
        );
    }
}
