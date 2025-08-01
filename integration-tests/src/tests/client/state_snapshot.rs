use crate::env::nightshade_setup::TestEnvNightshadeSetupExt;
use crate::env::test_env::TestEnv;
use near_chain::{ChainStoreAccess, Provenance};
use near_chain_configs::{Genesis, NEAR_BASE};
use near_client::ProcessTxResponse;
use near_crypto::{InMemorySigner, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::SignedTransaction;
use near_store::adapter::StoreAdapter;
use near_store::config::{STATE_SNAPSHOT_DIR, StateSnapshotType};
use near_store::flat::FlatStorageManager;
use near_store::trie::state_snapshots_dir;
use near_store::{
    Mode, ShardTries, StateSnapshotConfig, StoreConfig, TrieConfig, config::TrieCacheConfig,
    test_utils::create_test_store,
};
use near_store::{NodeStorage, Store};
use std::path::PathBuf;

struct StateSnapshotTestEnv {
    state_snapshots_dir: PathBuf,
    shard_tries: ShardTries,
}

impl StateSnapshotTestEnv {
    fn new(
        state_snapshots_dir: PathBuf,
        state_snapshot_config: StateSnapshotConfig,
        store: &Store,
    ) -> Self {
        let trie_cache_config = TrieCacheConfig {
            default_max_bytes: bytesize::ByteSize::mb(50),
            per_shard_max_bytes: Default::default(),
            shard_cache_deletions_queue_capacity: 0,
        };
        let trie_config = TrieConfig {
            shard_cache_config: trie_cache_config.clone(),
            view_shard_cache_config: trie_cache_config,
            ..TrieConfig::default()
        };
        let flat_storage_manager = FlatStorageManager::new(store.flat_store());
        let shard_uids = [ShardUId::single_shard()];

        let shard_tries = ShardTries::new(
            store.trie_store(),
            trie_config,
            &shard_uids,
            flat_storage_manager,
            state_snapshot_config,
        );
        Self { state_snapshots_dir, shard_tries }
    }
}

fn set_up_test_env_for_state_snapshots(
    store: &Store,
    snapshot_type: StateSnapshotType,
) -> StateSnapshotTestEnv {
    let home_dir =
        tempfile::Builder::new().prefix("storage").tempdir().unwrap().path().to_path_buf();
    let state_snapshots_dir = state_snapshots_dir(&home_dir, "data", STATE_SNAPSHOT_DIR);
    let state_snapshot_config = match snapshot_type {
        StateSnapshotType::Enabled => {
            StateSnapshotConfig::Enabled { state_snapshots_dir: state_snapshots_dir.clone() }
        }
        StateSnapshotType::Disabled => StateSnapshotConfig::Disabled,
    };

    StateSnapshotTestEnv::new(state_snapshots_dir, state_snapshot_config, store)
}

#[test]
// there's no entry in rocksdb for STATE_SNAPSHOT_KEY, maybe_open_state_snapshot should return error instead of panic
fn test_maybe_open_state_snapshot_no_state_snapshot_key_entry() {
    init_test_logger();
    let store = create_test_store();
    let test_env = set_up_test_env_for_state_snapshots(&store, StateSnapshotType::Enabled);
    let result =
        test_env.shard_tries.maybe_open_state_snapshot(|_| Ok(vec![(0, ShardUId::single_shard())]));
    assert!(result.is_err());
}

#[test]
// there's no file present in the path for state snapshot, maybe_open_state_snapshot should return error instead of panic
fn test_maybe_open_state_snapshot_file_not_exist() {
    init_test_logger();
    let store = create_test_store();
    let test_env = set_up_test_env_for_state_snapshots(&store, StateSnapshotType::Enabled);
    let snapshot_hash = CryptoHash::new();
    let mut store_update = test_env.shard_tries.store_update();
    store_update.set_state_snapshot_hash(Some(snapshot_hash));
    store_update.commit().unwrap();
    let result =
        test_env.shard_tries.maybe_open_state_snapshot(|_| Ok(vec![(0, ShardUId::single_shard())]));
    assert!(result.is_err());
}

#[test]
// there's garbage in the path for state snapshot, maybe_open_state_snapshot should return error instead of panic
fn test_maybe_open_state_snapshot_garbage_snapshot() {
    use std::fs::{File, create_dir_all};
    use std::io::Write;
    use std::path::Path;
    init_test_logger();
    let store = create_test_store();
    let test_env = set_up_test_env_for_state_snapshots(&store, StateSnapshotType::Enabled);
    let snapshot_hash = CryptoHash::new();
    let mut store_update = test_env.shard_tries.store_update();
    store_update.set_state_snapshot_hash(Some(snapshot_hash));
    store_update.commit().unwrap();
    let snapshot_path =
        ShardTries::get_state_snapshot_base_dir(&snapshot_hash, &test_env.state_snapshots_dir);
    if let Some(parent) = Path::new(&snapshot_path).parent() {
        create_dir_all(parent).unwrap();
    }
    let mut file = File::create(snapshot_path).unwrap();
    // write some garbage
    let data: Vec<u8> = vec![1, 2, 3, 4];
    file.write_all(&data).unwrap();

    let result =
        test_env.shard_tries.maybe_open_state_snapshot(|_| Ok(vec![(0, ShardUId::single_shard())]));
    assert!(result.is_err());
}

#[test]
fn test_state_snapshot_disabled() -> anyhow::Result<()> {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let env = TestEnv::builder(&genesis.config)
        .clients_count(1)
        .real_stores()
        .nightshade_runtimes(&genesis)
        .build();

    let genesis_block = env.clients[0].chain.get_block_by_height(0)?;

    let store = env.clients[0].chain.chain_store().store();
    let state_snapshot_test_env =
        set_up_test_env_for_state_snapshots(&store, StateSnapshotType::Disabled);

    if std::fs::exists(&state_snapshot_test_env.state_snapshots_dir)? {
        std::fs::remove_dir_all(&state_snapshot_test_env.state_snapshots_dir)?;
    }

    state_snapshot_test_env.shard_tries.create_state_snapshot(
        CryptoHash::default(),
        &[(0, ShardUId::single_shard())],
        &genesis_block,
    )?;

    anyhow::ensure!(
        !std::fs::exists(&state_snapshot_test_env.state_snapshots_dir)?,
        "state snapshots directory should not exist"
    );

    Ok(())
}

fn verify_make_snapshot(
    state_snapshot_test_env: &StateSnapshotTestEnv,
    block_hash: CryptoHash,
    block: &Block,
) -> Result<(), anyhow::Error> {
    state_snapshot_test_env.shard_tries.delete_state_snapshot();
    state_snapshot_test_env.shard_tries.create_state_snapshot(
        block_hash,
        &[(0, ShardUId::single_shard())],
        block,
    )?;
    // check that make_state_snapshot does not panic or err out
    // assert!(res.is_ok());
    let snapshot_path = ShardTries::get_state_snapshot_base_dir(
        &block_hash,
        &state_snapshot_test_env.state_snapshots_dir,
    );
    // check that the snapshot just made can be opened
    state_snapshot_test_env
        .shard_tries
        .maybe_open_state_snapshot(|_| Ok(vec![(0, ShardUId::single_shard())]))?;
    // check that the entry of STATE_SNAPSHOT_KEY is the latest block hash
    let db_state_snapshot_hash =
        state_snapshot_test_env.shard_tries.store().get_state_snapshot_hash()?;
    if db_state_snapshot_hash != block_hash {
        return Err(anyhow::Error::msg(
            "the entry of STATE_SNAPSHOT_KEY does not equal to the prev block hash",
        ));
    }
    // check that the stored snapshot in file system is an actual snapshot
    let store_config = StoreConfig::default();
    let opener = NodeStorage::opener(&snapshot_path, &store_config, None, None);
    let _storage = opener.open_in_mode(Mode::ReadOnly)?;
    // check that there's only one snapshot at the parent directory of snapshot path
    let parent_path = snapshot_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("{snapshot_path:?} needs to have a parent dir"))?;
    let parent_path_result = std::fs::read_dir(parent_path)?;
    if vec![parent_path_result.filter_map(Result::ok)].len() > 1 {
        return Err(anyhow::Error::msg(
            "there are more than 1 snapshot file in the snapshot parent directory",
        ));
    }
    return Ok(());
}

fn delete_content_at_path(path: &str) -> std::io::Result<()> {
    let metadata = std::fs::metadata(path)?;
    if metadata.is_dir() {
        std::fs::remove_dir_all(path)?;
    } else {
        std::fs::remove_file(path)?;
    }
    Ok(())
}

#[test]
// Runs a validator node.
// Makes a state snapshot after processing every block. Each block contains a
// transaction creating an account.
fn slow_test_make_state_snapshot() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let mut env = TestEnv::builder(&genesis.config)
        .clients_count(1)
        .use_state_snapshots()
        .real_stores()
        .nightshade_runtimes(&genesis)
        .build();

    let signer: Signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();
    let genesis_hash = *genesis_block.hash();

    let mut blocks = vec![];

    let store = env.clients[0].chain.chain_store().store();
    let state_snapshot_test_env =
        set_up_test_env_for_state_snapshots(&store, StateSnapshotType::Enabled);

    for i in 1..=5 {
        let new_account_id = format!("test_account_{i}");
        let nonce = i;
        let tx = SignedTransaction::create_account(
            nonce,
            "test0".parse().unwrap(),
            new_account_id.parse().unwrap(),
            NEAR_BASE,
            signer.public_key(),
            &signer,
            genesis_hash,
        );
        assert_eq!(env.rpc_handlers[0].process_tx(tx, false, false), ProcessTxResponse::ValidTx);
        let block = env.clients[0].produce_block(i).unwrap().unwrap();
        blocks.push(block.clone());
        env.process_block(0, block.clone(), Provenance::PRODUCED);
        assert_eq!(
            format!("{:?}", Ok::<(), anyhow::Error>(())),
            format!("{:?}", verify_make_snapshot(&state_snapshot_test_env, *block.hash(), &block))
        );
    }

    // check that if the entry in DBCol::STATE_SNAPSHOT_KEY was missing while snapshot file exists, an overwrite of snapshot can succeed
    let mut store_update = state_snapshot_test_env.shard_tries.store_update();
    store_update.set_state_snapshot_hash(None);
    store_update.commit().unwrap();
    let head = env.clients[0].chain.head().unwrap();
    let head_block_hash = head.last_block_hash;
    let head_block = env.clients[0].chain.get_block(&head_block_hash).unwrap();
    assert_eq!(
        format!("{:?}", Ok::<(), anyhow::Error>(())),
        format!(
            "{:?}",
            verify_make_snapshot(&state_snapshot_test_env, head_block_hash, &head_block)
        )
    );

    // check that if the snapshot is deleted from file system while there's entry in DBCol::STATE_SNAPSHOT_KEY
    // recreating the snapshot will succeed
    let snapshot_hash = head.last_block_hash;
    let snapshot_path = ShardTries::get_state_snapshot_base_dir(
        &snapshot_hash,
        &state_snapshot_test_env.state_snapshots_dir,
    );
    delete_content_at_path(snapshot_path.to_str().unwrap()).unwrap();
    assert_eq!(
        format!("{:?}", Ok::<(), anyhow::Error>(())),
        format!(
            "{:?}",
            verify_make_snapshot(&state_snapshot_test_env, head.last_block_hash, &head_block)
        )
    );
}
