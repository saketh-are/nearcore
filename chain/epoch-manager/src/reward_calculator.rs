use std::collections::HashMap;

use num_rational::Rational32;
use primitive_types::{U256, U512};

use near_chain_configs::GenesisConfig;
use near_primitives::types::{AccountId, Balance, BlockChunkValidatorStats};
use near_primitives::version::ProtocolVersion;

use crate::validator_stats::get_validator_online_ratio;

pub(crate) const NUM_NS_IN_SECOND: u64 = 1_000_000_000;
pub const NUM_SECONDS_IN_A_YEAR: u64 = 24 * 60 * 60 * 365;

/// Contains online thresholds for validators.
#[derive(Clone, Debug)]
pub struct ValidatorOnlineThresholds {
    /// Online minimum threshold below which validator doesn't receive reward.
    pub online_min_threshold: Rational32,
    /// Online maximum threshold above which validator gets full reward.
    pub online_max_threshold: Rational32,
    /// If set, contains a number between 0 and 100 (percentage), and endorsement ratio
    /// below this threshold will be treated 0, and otherwise be treated 1,
    /// before calculating the average uptime ratio of the validator.
    /// If not set, endorsement ratio will be used as is.
    pub endorsement_cutoff_threshold: Option<u8>,
}

#[derive(Clone, Debug)]
pub struct RewardCalculator {
    pub max_inflation_rate: Rational32,
    pub num_blocks_per_year: u64,
    pub epoch_length: u64,
    pub protocol_reward_rate: Rational32,
    pub protocol_treasury_account: AccountId,
    pub num_seconds_per_year: u64,
    pub genesis_protocol_version: ProtocolVersion,
}

impl RewardCalculator {
    pub fn new(config: &GenesisConfig, epoch_length: u64) -> Self {
        RewardCalculator {
            max_inflation_rate: config.max_inflation_rate,
            num_blocks_per_year: config.num_blocks_per_year,
            epoch_length,
            protocol_reward_rate: config.protocol_reward_rate,
            protocol_treasury_account: config.protocol_treasury_account.clone(),
            num_seconds_per_year: NUM_SECONDS_IN_A_YEAR,
            genesis_protocol_version: config.protocol_version,
        }
    }

    /// Calculate validator reward for an epoch based on their block and chunk production stats.
    /// Returns map of validators with their rewards and amount of newly minted tokens including to protocol's treasury.
    /// See spec <https://nomicon.io/Economics/Economic#validator-rewards-calculation>.
    pub fn calculate_reward(
        &self,
        validator_block_chunk_stats: HashMap<AccountId, BlockChunkValidatorStats>,
        validator_stake: &HashMap<AccountId, Balance>,
        total_supply: Balance,
        protocol_version: ProtocolVersion,
        epoch_duration: u64,
        online_thresholds: ValidatorOnlineThresholds,
    ) -> (HashMap<AccountId, Balance>, Balance) {
        let mut res = HashMap::new();
        let num_validators = validator_block_chunk_stats.len();
        let use_hardcoded_value = protocol_version > self.genesis_protocol_version;
        let max_inflation_rate =
            if use_hardcoded_value { Rational32::new_raw(1, 20) } else { self.max_inflation_rate };
        let protocol_reward_rate = if use_hardcoded_value {
            Rational32::new_raw(1, 10)
        } else {
            self.protocol_reward_rate
        };
        let epoch_total_reward: u128 = (U256::from(*max_inflation_rate.numer() as u64)
            * U256::from(total_supply)
            * U256::from(epoch_duration)
            / (U256::from(self.num_seconds_per_year)
                * U256::from(*max_inflation_rate.denom() as u64)
                * U256::from(NUM_NS_IN_SECOND)))
        .as_u128();
        let epoch_protocol_treasury = (U256::from(epoch_total_reward)
            * U256::from(*protocol_reward_rate.numer() as u64)
            / U256::from(*protocol_reward_rate.denom() as u64))
        .as_u128();
        res.insert(self.protocol_treasury_account.clone(), epoch_protocol_treasury);
        if num_validators == 0 {
            return (res, 0);
        }
        let epoch_validator_reward = epoch_total_reward - epoch_protocol_treasury;
        let mut epoch_actual_reward = epoch_protocol_treasury;
        let total_stake: Balance = validator_stake.values().sum();
        for (account_id, stats) in validator_block_chunk_stats {
            let production_ratio =
                get_validator_online_ratio(&stats, online_thresholds.endorsement_cutoff_threshold);
            let average_produced_numer = production_ratio.numer();
            let average_produced_denom = production_ratio.denom();

            let expected_blocks = stats.block_stats.expected;
            let expected_chunks = stats.chunk_stats.expected();
            let expected_endorsements = stats.chunk_stats.endorsement_stats().expected;

            let online_min_numer =
                U256::from(*online_thresholds.online_min_threshold.numer() as u64);
            let online_min_denom =
                U256::from(*online_thresholds.online_min_threshold.denom() as u64);
            // If average of produced blocks below online min threshold, validator gets 0 reward.
            let reward = if average_produced_numer * online_min_denom
                < online_min_numer * average_produced_denom
                || (expected_chunks == 0 && expected_blocks == 0 && expected_endorsements == 0)
            {
                0
            } else {
                // cspell:ignore denum
                let stake = *validator_stake
                    .get(&account_id)
                    .unwrap_or_else(|| panic!("{} is not a validator", account_id));
                // Online reward multiplier is min(1., (uptime - online_threshold_min) / (online_threshold_max - online_threshold_min).
                let online_max_numer =
                    U256::from(*online_thresholds.online_max_threshold.numer() as u64);
                let online_max_denom =
                    U256::from(*online_thresholds.online_max_threshold.denom() as u64);
                let online_numer =
                    online_max_numer * online_min_denom - online_min_numer * online_max_denom;
                let mut uptime_numer = (average_produced_numer * online_min_denom
                    - online_min_numer * average_produced_denom)
                    * online_max_denom;
                let uptime_denum = online_numer * average_produced_denom;
                // Apply min between 1. and computed uptime.
                uptime_numer =
                    if uptime_numer > uptime_denum { uptime_denum } else { uptime_numer };
                (U512::from(epoch_validator_reward) * U512::from(uptime_numer) * U512::from(stake)
                    / U512::from(uptime_denum)
                    / U512::from(total_stake))
                .as_u128()
            };
            res.insert(account_id, reward);
            epoch_actual_reward += reward;
        }
        (res, epoch_actual_reward)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use near_primitives::types::{BlockChunkValidatorStats, ChunkStats, ValidatorStats};
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;
    use std::collections::HashMap;

    #[test]
    fn test_zero_produced_and_expected() {
        let epoch_length = 1;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Ratio::new(0, 1),
            num_blocks_per_year: 1000000,
            epoch_length,
            protocol_reward_rate: Ratio::new(0, 1),
            protocol_treasury_account: "near".parse().unwrap(),
            num_seconds_per_year: 1000000,
            genesis_protocol_version: PROTOCOL_VERSION,
        };
        let validator_block_chunk_stats = HashMap::from([
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::default(),
                },
            ),
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 1 },
                    chunk_stats: ChunkStats::new_with_production(0, 1),
                },
            ),
        ]);
        let validator_stake =
            HashMap::from([("test1".parse().unwrap(), 100), ("test2".parse().unwrap(), 100)]);
        let total_supply = 1_000_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
            ValidatorOnlineThresholds {
                online_min_threshold: Ratio::new(9, 10),
                online_max_threshold: Ratio::new(1, 1),
                endorsement_cutoff_threshold: None,
            },
        );
        assert_eq!(
            result.0,
            HashMap::from([
                ("near".parse().unwrap(), 0u128),
                ("test1".parse().unwrap(), 0u128),
                ("test2".parse().unwrap(), 0u128)
            ])
        );
    }

    /// Test reward calculation when validators are not fully online.
    #[test]
    fn test_reward_validator_different_online() {
        let epoch_length = 1000;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Ratio::new(1, 100),
            num_blocks_per_year: 1000,
            epoch_length,
            protocol_reward_rate: Ratio::new(0, 10),
            protocol_treasury_account: "near".parse().unwrap(),
            num_seconds_per_year: 1000,
            genesis_protocol_version: PROTOCOL_VERSION,
        };
        let validator_block_chunk_stats = HashMap::from([
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(945, 1000),
                },
            ),
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 999, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(999, 1000),
                },
            ),
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 850, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(850, 1000),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), 500_000),
            ("test2".parse().unwrap(), 500_000),
            ("test3".parse().unwrap(), 500_000),
        ]);
        let total_supply = 1_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
            ValidatorOnlineThresholds {
                online_min_threshold: Ratio::new(9, 10),
                online_max_threshold: Ratio::new(99, 100),
                endorsement_cutoff_threshold: None,
            },
        );
        // Total reward is 10_000_000. Divided by 3 equal stake validators - each gets 3_333_333.
        // test1 with 94.5% online gets 50% because of linear between (0.99-0.9) online.
        assert_eq!(
            result.0,
            HashMap::from([
                ("near".parse().unwrap(), 0),
                ("test1".parse().unwrap(), 1_666_666u128),
                ("test2".parse().unwrap(), 3_333_333u128),
                ("test3".parse().unwrap(), 0u128)
            ])
        );
        assert_eq!(result.1, 4_999_999u128);
    }

    /// Test reward calculation for chunk only or block only producers
    #[test]
    fn test_reward_chunk_only_producer() {
        let epoch_length = 1000;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Ratio::new(1, 100),
            num_blocks_per_year: 1000,
            epoch_length,
            protocol_reward_rate: Ratio::new(0, 10),
            protocol_treasury_account: "near".parse().unwrap(),
            num_seconds_per_year: 1000,
            genesis_protocol_version: PROTOCOL_VERSION,
        };
        let validator_block_chunk_stats = HashMap::from([
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_production(945, 1000),
                },
            ),
            // chunk only producer
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_production(999, 1000),
                },
            ),
            // block only producer (not implemented right now, just for testing)
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats::default(),
                },
            ),
            // a validator that expected blocks and chunks are both 0 (this could occur with very
            // small probability for validators with little stakes)
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::default(),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), 500_000),
            ("test2".parse().unwrap(), 500_000),
            ("test3".parse().unwrap(), 500_000),
            ("test4".parse().unwrap(), 500_000),
        ]);
        let total_supply = 1_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
            ValidatorOnlineThresholds {
                online_min_threshold: Ratio::new(9, 10),
                online_max_threshold: Ratio::new(99, 100),
                endorsement_cutoff_threshold: None,
            },
        );
        // Total reward is 10_000_000. Divided by 4 equal stake validators - each gets 2_500_000.
        // test1 with 94.5% online gets 50% because of linear between (0.99-0.9) online.
        {
            assert_eq!(
                result.0,
                HashMap::from([
                    ("near".parse().unwrap(), 0),
                    ("test1".parse().unwrap(), 1_250_000u128),
                    ("test2".parse().unwrap(), 2_500_000u128),
                    ("test3".parse().unwrap(), 1_250_000u128),
                    ("test4".parse().unwrap(), 0u128)
                ])
            );
            assert_eq!(result.1, 5_000_000u128);
        }
    }

    #[test]
    fn test_reward_stateless_validation() {
        let epoch_length = 1000;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Ratio::new(1, 100),
            num_blocks_per_year: 1000,
            epoch_length,
            protocol_reward_rate: Ratio::new(0, 10),
            protocol_treasury_account: "near".parse().unwrap(),
            num_seconds_per_year: 1000,
            genesis_protocol_version: PROTOCOL_VERSION,
        };
        let validator_block_chunk_stats = HashMap::from([
            // Blocks, chunks, endorsements
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 944, expected: 1000 },
                        endorsement: ValidatorStats { produced: 946, expected: 1000 },
                    },
                },
            ),
            // Chunks and endorsements
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 998, expected: 1000 },
                        endorsement: ValidatorStats { produced: 1000, expected: 1000 },
                    },
                },
            ),
            // Blocks and endorsements
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 940, expected: 1000 },
                    chunk_stats: ChunkStats::new_with_endorsement(950, 1000),
                },
            ),
            // Endorsements only
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(1000, 1000),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), 500_000),
            ("test2".parse().unwrap(), 500_000),
            ("test3".parse().unwrap(), 500_000),
            ("test4".parse().unwrap(), 500_000),
        ]);
        let total_supply = 1_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
            ValidatorOnlineThresholds {
                online_min_threshold: Ratio::new(9, 10),
                online_max_threshold: Ratio::new(99, 100),
                endorsement_cutoff_threshold: None,
            },
        );
        // Total reward is 10_000_000. Divided by 4 equal stake validators - each gets 2_500_000.
        // test1 with 94.5% online gets 50% because of linear between (0.99-0.9) online.
        {
            assert_eq!(
                result.0,
                HashMap::from([
                    ("near".parse().unwrap(), 0),
                    ("test1".parse().unwrap(), 1_250_000u128),
                    ("test2".parse().unwrap(), 2_500_000u128),
                    ("test3".parse().unwrap(), 1_250_000u128),
                    ("test4".parse().unwrap(), 2_500_000u128)
                ])
            );
            assert_eq!(result.1, 7_500_000u128);
        }
    }

    #[test]
    fn test_reward_stateless_validation_with_endorsement_cutoff() {
        let epoch_length = 1000;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Ratio::new(1, 100),
            num_blocks_per_year: 1000,
            epoch_length,
            protocol_reward_rate: Ratio::new(0, 10),
            protocol_treasury_account: "near".parse().unwrap(),
            num_seconds_per_year: 1000,
            genesis_protocol_version: PROTOCOL_VERSION,
        };
        let validator_block_chunk_stats = HashMap::from([
            // Blocks, chunks, endorsements - endorsement ratio cutoff is exceeded
            (
                "test1".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 944, expected: 1000 },
                        endorsement: ValidatorStats { produced: 946, expected: 1000 },
                    },
                },
            ),
            // Blocks, chunks, endorsements - endorsement ratio cutoff is not exceeded
            (
                "test2".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 945, expected: 1000 },
                    chunk_stats: ChunkStats {
                        production: ValidatorStats { produced: 944, expected: 1000 },
                        endorsement: ValidatorStats { produced: 446, expected: 1000 },
                    },
                },
            ),
            // Endorsements only - endorsement ratio cutoff is exceeded
            (
                "test3".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(946, 1000),
                },
            ),
            // Endorsements only - endorsement ratio cutoff is not exceeded
            (
                "test4".parse().unwrap(),
                BlockChunkValidatorStats {
                    block_stats: ValidatorStats { produced: 0, expected: 0 },
                    chunk_stats: ChunkStats::new_with_endorsement(446, 1000),
                },
            ),
        ]);
        let validator_stake = HashMap::from([
            ("test1".parse().unwrap(), 500_000),
            ("test2".parse().unwrap(), 500_000),
            ("test3".parse().unwrap(), 500_000),
            ("test4".parse().unwrap(), 500_000),
        ]);
        let total_supply = 1_000_000_000;
        let result = reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
            ValidatorOnlineThresholds {
                online_min_threshold: Ratio::new(9, 10),
                online_max_threshold: Ratio::new(99, 100),
                endorsement_cutoff_threshold: Some(50),
            },
        );
        // "test2" does not get reward since its uptime ratio goes below online_min_threshold,
        // because its endorsement ratio is below the cutoff threshold.
        // "test4" does not get reward since its endorsement ratio is below the cutoff threshold.
        {
            assert_eq!(
                result.0,
                HashMap::from([
                    ("near".parse().unwrap(), 0),
                    ("test1".parse().unwrap(), 1_750_000u128),
                    ("test2".parse().unwrap(), 0),
                    ("test3".parse().unwrap(), 2_500_000u128),
                    ("test4".parse().unwrap(), 0)
                ])
            );
            assert_eq!(result.1, 4_250_000u128);
        }
    }

    /// Test that under an extreme setting (total supply 100b, epoch length half a day),
    /// reward calculation will not overflow.
    #[test]
    fn test_reward_no_overflow() {
        let epoch_length = 60 * 60 * 12;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Ratio::new(5, 100),
            num_blocks_per_year: 60 * 60 * 24 * 365,
            // half a day
            epoch_length,
            protocol_reward_rate: Ratio::new(1, 10),
            protocol_treasury_account: "near".parse().unwrap(),
            num_seconds_per_year: 60 * 60 * 24 * 365,
            genesis_protocol_version: PROTOCOL_VERSION,
        };
        let validator_block_chunk_stats = HashMap::from([(
            "test".parse().unwrap(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 43200, expected: 43200 },
                chunk_stats: ChunkStats {
                    production: ValidatorStats { produced: 345600, expected: 345600 },
                    endorsement: ValidatorStats { produced: 345600, expected: 345600 },
                },
            },
        )]);
        let validator_stake = HashMap::from([("test".parse().unwrap(), 500_000 * 10_u128.pow(24))]);
        // some hypothetical large total supply (100b)
        let total_supply = 100_000_000_000 * 10_u128.pow(24);
        reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_supply,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
            ValidatorOnlineThresholds {
                online_min_threshold: Ratio::new(9, 10),
                online_max_threshold: Ratio::new(1, 1),
                endorsement_cutoff_threshold: None,
            },
        );
    }
}
