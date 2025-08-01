use near_chain_configs::{
    BLOCK_PRODUCER_KICKOUT_THRESHOLD, CHUNK_PRODUCER_KICKOUT_THRESHOLD,
    CHUNK_VALIDATOR_ONLY_KICKOUT_THRESHOLD, EXPECTED_EPOCH_LENGTH, FISHERMEN_THRESHOLD,
    GAS_PRICE_ADJUSTMENT_RATE, GENESIS_CONFIG_FILENAME, Genesis, GenesisConfig, INITIAL_GAS_LIMIT,
    MAX_INFLATION_RATE, MIN_GAS_PRICE, NEAR_BASE, NUM_BLOCK_PRODUCER_SEATS, NUM_BLOCKS_PER_YEAR,
    PROTOCOL_REWARD_RATE, PROTOCOL_UPGRADE_STAKE_THRESHOLD, TRANSACTION_VALIDITY_PERIOD,
    TrackedShardsConfig,
};
use near_primitives::types::{Balance, NumShards, ShardId};
use near_primitives::utils::get_num_seats_per_shard;
use near_primitives::version::PROTOCOL_VERSION;
use nearcore::config::{CONFIG_FILENAME, Config, NODE_KEY_FILE};
use std::collections::HashSet;
use std::fs::File;
use std::path::Path;

const ACCOUNTS_FILE: &str = "accounts.csv";
const SHARDS: &'static [ShardId] = &[
    ShardId::new(0),
    ShardId::new(1),
    ShardId::new(2),
    ShardId::new(3),
    ShardId::new(4),
    ShardId::new(5),
    ShardId::new(6),
    ShardId::new(7),
];

fn verify_total_supply(total_supply: Balance, chain_id: &str) {
    if chain_id == near_primitives::chains::MAINNET {
        assert_eq!(
            total_supply,
            1_000_000_000 * NEAR_BASE,
            "Total supply should be exactly 1 billion"
        );
    } else if total_supply > 10_000_000_000 * NEAR_BASE
        && chain_id == near_primitives::chains::TESTNET
    {
        panic!("Total supply should not be more than 10 billion");
    }
}

/// Generates `config.json` and `genesis.config` from csv files.
/// Verifies that `validator_key.json`, and `node_key.json` are present.
pub fn csv_to_json_configs(home: &Path, chain_id: String, tracked_shards: Vec<ShardId>) {
    // Verify that key files exist.
    assert!(home.join(NODE_KEY_FILE).as_path().exists(), "Node key file should exist");

    let shards_set: HashSet<_> = SHARDS.iter().collect();
    if tracked_shards.iter().any(|shard_id| !shards_set.contains(shard_id)) {
        panic!("Trying to track a shard that does not exist");
    }

    // Construct `config.json`.
    let mut config = Config::default();
    // TODO(cloud_archival): Revisit this file, `tracked_shards` likely does not make sense here.
    // Perhaps it will make sense if used together with `TrackedShardsConfig::Shards` when it is added.
    let tracked_shards_config = if tracked_shards.is_empty() {
        TrackedShardsConfig::NoShards
    } else {
        TrackedShardsConfig::AllShards
    };
    config.tracked_shards_config = Some(tracked_shards_config);

    // Construct genesis config.
    let (records, validators, peer_info, treasury, genesis_time) =
        crate::csv_parser::keys_to_state_records(
            File::open(home.join(ACCOUNTS_FILE)).expect("Error opening accounts file."),
            MIN_GAS_PRICE,
        )
        .expect("Error parsing accounts file.");
    config.network.boot_nodes =
        peer_info.into_iter().map(|x| x.to_string()).collect::<Vec<_>>().join(",");
    let genesis_config = GenesisConfig {
        protocol_version: PROTOCOL_VERSION,
        genesis_time,
        chain_id: chain_id.clone(),
        num_block_producer_seats: NUM_BLOCK_PRODUCER_SEATS,
        num_block_producer_seats_per_shard: get_num_seats_per_shard(
            SHARDS.len() as NumShards,
            NUM_BLOCK_PRODUCER_SEATS,
        ),
        avg_hidden_validator_seats_per_shard: SHARDS.iter().map(|_| 0).collect(),
        dynamic_resharding: false,
        protocol_upgrade_stake_threshold: PROTOCOL_UPGRADE_STAKE_THRESHOLD,
        epoch_length: EXPECTED_EPOCH_LENGTH,
        gas_limit: INITIAL_GAS_LIMIT,
        gas_price_adjustment_rate: GAS_PRICE_ADJUSTMENT_RATE,
        block_producer_kickout_threshold: BLOCK_PRODUCER_KICKOUT_THRESHOLD,
        chunk_producer_kickout_threshold: CHUNK_PRODUCER_KICKOUT_THRESHOLD,
        chunk_validator_only_kickout_threshold: CHUNK_VALIDATOR_ONLY_KICKOUT_THRESHOLD,
        validators,
        transaction_validity_period: TRANSACTION_VALIDITY_PERIOD,
        protocol_reward_rate: PROTOCOL_REWARD_RATE,
        max_inflation_rate: MAX_INFLATION_RATE,
        num_blocks_per_year: NUM_BLOCKS_PER_YEAR,
        protocol_treasury_account: treasury,
        min_gas_price: MIN_GAS_PRICE,
        fishermen_threshold: FISHERMEN_THRESHOLD,
        ..Default::default()
    };
    let genesis = Genesis::new(genesis_config, records.into()).unwrap();
    verify_total_supply(genesis.config.total_supply, &chain_id);

    // Write all configs to files.
    config.write_to_file(&home.join(CONFIG_FILENAME)).expect("Error writing config");
    genesis.to_file(&home.join(GENESIS_CONFIG_FILENAME));
}
