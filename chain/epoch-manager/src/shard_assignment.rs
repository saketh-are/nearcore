use crate::{EpochInfo, EpochManagerAdapter, RngSeed};
use near_primitives::errors::EpochError;
use near_primitives::shard_layout::ShardInfo;
use near_primitives::types::{
    AccountId, Balance, EpochId, NumShards, ShardId, ShardIndex, validator_stake::ValidatorStake,
};
use near_primitives::utils::min_heap::{MinHeap, PeekMut};
use near_store::trie::ShardUId;
use rand::Rng;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Marker struct to communicate the error where you try to assign validators to shards
/// and there are not enough to even meet the minimum per shard.
#[derive(Debug)]
pub struct NotEnoughValidators;

/// Abstraction to avoid using full validator info in tests.
pub trait HasStake {
    fn get_stake(&self) -> Balance;
}

impl HasStake for ValidatorStake {
    fn get_stake(&self) -> Balance {
        self.stake()
    }
}

/// A helper struct to maintain the shard assignment sorted by the number of
/// validators assigned to each shard.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ValidatorsFirstShardAssignmentItem {
    validators: usize,
    stake: Balance,
    shard_index: ShardIndex,
}

type ValidatorsFirstShardAssignment = MinHeap<ValidatorsFirstShardAssignmentItem>;

/// A helper struct to maintain the shard assignment sorted by the stake
/// assigned to each shard.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct StakeFirstShardAssignmentItem {
    stake: Balance,
    validators: usize,
    shard_index: ShardIndex,
}

impl From<ValidatorsFirstShardAssignmentItem> for StakeFirstShardAssignmentItem {
    fn from(v: ValidatorsFirstShardAssignmentItem) -> Self {
        Self { validators: v.validators, stake: v.stake, shard_index: v.shard_index }
    }
}

fn assign_to_satisfy_shards_inner<T: HasStake + Eq, I: Iterator<Item = (usize, T)>>(
    shard_assignment: &mut ValidatorsFirstShardAssignment,
    result: &mut Vec<Vec<T>>,
    cp_iter: &mut I,
    min_validators_per_shard: usize,
) {
    let mut buffer = Vec::with_capacity(shard_assignment.len());
    // Stores (shard_index, cp_index) meaning that cp at cp_index has already been
    // added to shard shard_index.  Used to make sure we don’t add a cp to the same
    // shard multiple times.
    let seen_capacity = result.len() * min_validators_per_shard;
    let mut seen = HashSet::<(ShardIndex, usize)>::with_capacity(seen_capacity);

    while shard_assignment.peek().unwrap().validators < min_validators_per_shard {
        // cp_iter is an infinite cycle iterator so getting next value can never
        // fail.  cp_index is index of each element in the iterator but the
        // indexing is done before cycling thus the same cp always gets the same
        // cp_index.
        let (cp_index, cp) = cp_iter.next().unwrap();
        // Decide which shard to assign this chunk producer to.  We mustn’t
        // assign producers to a single shard multiple times.
        loop {
            match shard_assignment.peek_mut() {
                None => {
                    // No shards left which don’t already contain this chunk
                    // producer.  Skip it and move to another producer.
                    break;
                }
                Some(top) if top.validators >= min_validators_per_shard => {
                    // `shard_assignment` is sorted by number of chunk producers,
                    // thus all remaining shards have min_validators_per_shard
                    // producers already assigned to them.  Don’t assign current
                    // one to any shard and move to next cp.
                    break;
                }
                Some(mut top) if seen.insert((top.shard_index, cp_index)) => {
                    // Chunk producer is not yet assigned to the shard and the
                    // shard still needs more producers.  Assign `cp` to it and
                    // move to next one.
                    top.validators += 1;
                    top.stake += cp.get_stake();
                    result[top.shard_index].push(cp);
                    break;
                }
                Some(top) => {
                    // This chunk producer is already assigned to this shard.
                    // Pop the shard from the heap for now and try assigning the
                    // producer to the next shard.  (We’ll look back at the
                    // shard once we figure out what to do with current `cp`).
                    buffer.push(PeekMut::pop(top));
                }
            }
        }
        // Any shards we skipped over (because `cp` was already assigned to
        // them) need to be put back into the heap.
        shard_assignment.extend(buffer.drain(..));
    }
}

/// Assigns validators to shards to satisfy `min_validators_per_shard`
/// condition.
/// This means that validators can be repeated.
fn assign_to_satisfy_shards<T: HasStake + Eq + Clone>(
    chunk_producers: Vec<T>,
    num_shards: NumShards,
    min_validators_per_shard: usize,
) -> Vec<Vec<T>> {
    let mut result: Vec<Vec<T>> = (0..num_shards).map(|_| Vec::new()).collect();

    // Initially, sort by number of validators first so we fill shards up.
    let mut shard_assignment: ValidatorsFirstShardAssignment = (0..num_shards)
        .map(|shard_index| shard_index as usize)
        .map(|shard_index| ValidatorsFirstShardAssignmentItem {
            validators: 0,
            stake: 0,
            shard_index,
        })
        .collect();

    // Distribute chunk producers until all shards have at least the
    // minimum requested number.  If there are not enough validators to satisfy
    // that requirement, assign some of the validators to multiple shards.
    let mut chunk_producers = chunk_producers.into_iter().enumerate().cycle();
    assign_to_satisfy_shards_inner(
        &mut shard_assignment,
        &mut result,
        &mut chunk_producers,
        min_validators_per_shard,
    );
    result
}

/// Get initial chunk producer assignment for the current epoch, given the
/// assignment for the previous epoch.
fn get_initial_chunk_producer_assignment(
    chunk_producers: &[ValidatorStake],
    num_shards: NumShards,
    prev_chunk_producers_assignment: Option<Vec<Vec<ValidatorStake>>>,
) -> Vec<Vec<usize>> {
    let Some(prev_assignment) = prev_chunk_producers_assignment else {
        return vec![vec![]; num_shards as usize];
    };

    assert_eq!(prev_assignment.len(), num_shards as usize);
    let chunk_producer_indices = chunk_producers
        .iter()
        .enumerate()
        .map(|(i, vs)| (vs.account_id().clone(), i))
        .collect::<HashMap<_, _>>();

    let mut assignment = vec![];
    for validator_stakes in prev_assignment {
        let mut chunk_producers = vec![];
        for validator_stake in validator_stakes {
            let chunk_producer_index = chunk_producer_indices.get(validator_stake.account_id());
            if let Some(&index) = chunk_producer_index {
                chunk_producers.push(index);
            }
        }
        assignment.push(chunk_producers);
    }
    assignment
}

#[derive(Eq, PartialEq, Ord, PartialOrd)]
/// Helper struct to maintain set of shards sorted by number of chunk producers.
struct ShardSetItem {
    shard_chunk_producer_num: usize,
    shard_index: usize,
}

/// Convert chunk producer assignment from the previous epoch to the assignment
/// for the current epoch, given the chunk producer list.
///
/// Caller must guarantee that `min_validators_per_shard` is achievable and
/// `prev_chunk_producers_assignment` corresponds to the same number of shards.
///
/// TODO(resharding) - implement shard assignment
/// The current shard assignment works fully based on the ShardIndex. During
/// resharding those indices will change and the assignment will move many
/// validators to different shards. This should be avoided.
fn assign_to_balance_shards(
    chunk_producers: Vec<ValidatorStake>,
    num_shards: NumShards,
    min_validators_per_shard: usize,
    shard_assignment_changes_limit: usize,
    rng_seed: RngSeed,
    prev_chunk_producers_assignment: Option<Vec<Vec<ValidatorStake>>>,
) -> Vec<Vec<ValidatorStake>> {
    let num_chunk_producers = chunk_producers.len();
    let mut chunk_producer_assignment = get_initial_chunk_producer_assignment(
        &chunk_producers,
        num_shards,
        prev_chunk_producers_assignment,
    );

    // Find and assign new validators first.
    let old_validators = chunk_producer_assignment.iter().flatten().collect::<HashSet<_>>();
    let new_validators =
        (0..num_chunk_producers).filter(|i| !old_validators.contains(i)).collect::<Vec<_>>();
    let mut shard_set: BTreeSet<ShardSetItem> = (0..num_shards)
        .map(|s| ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[s as usize].len(),
            shard_index: s as usize,
        })
        .collect();
    let mut new_assignments = new_validators.len();
    for validator_index in new_validators {
        let ShardSetItem { shard_index, .. } = shard_set.pop_first().unwrap();
        chunk_producer_assignment[shard_index].push(validator_index);
        shard_set.insert(ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[shard_index].len(),
            shard_index,
        });
    }

    // Reassign old validators to balance shards until the limit is reached.
    let rng = &mut EpochInfo::shard_assignment_rng(&rng_seed);
    let new_assignments_hard_limit = chunk_producers.len().max(shard_assignment_changes_limit);
    loop {
        let ShardSetItem {
            shard_chunk_producer_num: minimal_shard_validators_num,
            shard_index: minimal_shard,
        } = *shard_set.first().unwrap();
        let ShardSetItem {
            shard_chunk_producer_num: maximal_shard_validators_num,
            shard_index: maximal_shard,
        } = *shard_set.last().unwrap();
        let is_minimal_num_satisfied = minimal_shard_validators_num >= min_validators_per_shard;
        let is_balanced = maximal_shard_validators_num - minimal_shard_validators_num <= 1;

        if is_minimal_num_satisfied
            && (is_balanced || new_assignments >= shard_assignment_changes_limit)
        {
            break;
        }

        assert!(
            new_assignments <= new_assignments_hard_limit,
            "Couldn't balance {num_shards} shards in {new_assignments_hard_limit}\
             iterations. It means that some chunk producer was selected for \
             new shard twice which shouldn't happen."
        );
        assert_ne!(
            minimal_shard,
            maximal_shard,
            "Minimal shard and maximal shard are the same: {minimal_shard}. \
            Either {} chunk producers are not enough to satisfy minimal number \
            {min_validators_per_shard} for {num_shards} shards, or we try to \
            balance the shard with itself.",
            chunk_producers.len(),
        );

        let minimal_shard = shard_set.pop_first().unwrap().shard_index;
        let maximal_shard = shard_set.pop_last().unwrap().shard_index;
        let validator_pos = rng.gen_range(0..chunk_producer_assignment[maximal_shard].len());
        let validator_index = chunk_producer_assignment[maximal_shard].swap_remove(validator_pos);
        chunk_producer_assignment[minimal_shard].push(validator_index);
        shard_set.insert(ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[minimal_shard].len(),
            shard_index: minimal_shard,
        });
        shard_set.insert(ShardSetItem {
            shard_chunk_producer_num: chunk_producer_assignment[maximal_shard].len(),
            shard_index: maximal_shard,
        });
        new_assignments += 1;
    }
    chunk_producer_assignment
        .into_iter()
        .map(|mut assignment| {
            assignment.sort();
            assignment.into_iter().map(|i| chunk_producers[i].clone()).collect()
        })
        .collect()
}

/// Assign chunk producers to shards. The i-th element of the output is the
/// list of chunk producers assigned to the i-th shard, sorted by stake.
///
/// This function guarantees that, in order of priority:
/// * every shard has at least `min_validators_per_shard` assigned to it;
/// * chunk producer repeats are completely avoided if possible;
/// * if `prev_chunk_producers_assignment` is provided, it minimizes the need
/// for chunk producers there to change shards;
/// * finally, attempts to balance number of chunk producers at shards, while
/// `shard_assignment_changes_limit` allows.
/// See discussion on #11213 for more details.
///
/// Caller must guarantee that `chunk_producers` is sorted in non-increasing
/// order by stake and `prev_chunk_producers_assignment` corresponds to the
/// same number of shards.
///
/// Returns error if `chunk_producers.len() < min_validators_per_shard`.
pub(crate) fn assign_chunk_producers_to_shards(
    chunk_producers: Vec<ValidatorStake>,
    num_shards: NumShards,
    min_validators_per_shard: usize,
    shard_assignment_changes_limit: usize,
    rng_seed: RngSeed,
    prev_chunk_producers_assignment: Option<Vec<Vec<ValidatorStake>>>,
) -> Result<Vec<Vec<ValidatorStake>>, NotEnoughValidators> {
    // If there's not enough chunk producers to fill up a single shard there’s
    // nothing we can do. Return with an error.
    let num_chunk_producers = chunk_producers.len();
    if num_chunk_producers < min_validators_per_shard {
        return Err(NotEnoughValidators);
    }

    let result = if chunk_producers.len() < min_validators_per_shard * (num_shards as usize) {
        // We don't have enough chunk producers to allow assignment without
        // repeats.
        // Assign validators to satisfy only `min_validators_per_shard` condition.
        assign_to_satisfy_shards(chunk_producers, num_shards, min_validators_per_shard)
    } else {
        // We can avoid validator repeats, so we use other algorithm to balance
        // number of validators in shards.
        assign_to_balance_shards(
            chunk_producers,
            num_shards,
            min_validators_per_shard,
            shard_assignment_changes_limit,
            rng_seed,
            prev_chunk_producers_assignment,
        )
    };
    Ok(result)
}

/// Which shard the account belongs to in the given epoch.
pub fn account_id_to_shard_id(
    epoch_manager: &dyn EpochManagerAdapter,
    account_id: &AccountId,
    epoch_id: &EpochId,
) -> Result<ShardId, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    Ok(shard_layout.account_id_to_shard_id(account_id))
}

/// Which shard the account belongs to in the given epoch.
pub fn account_id_to_shard_info(
    epoch_manager: &dyn EpochManagerAdapter,
    account_id: &AccountId,
    epoch_id: &EpochId,
) -> Result<ShardInfo, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    let shard_id = shard_layout.account_id_to_shard_id(account_id);
    let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    let shard_index = shard_layout.get_shard_index(shard_id)?;
    Ok(ShardInfo { shard_index, shard_uid })
}

/// Converts `ShardId` (index of shard in the *current* layout) to
/// `ShardUId` (`ShardId` + the version of shard layout itself.)
pub fn shard_id_to_uid(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_id: ShardId,
    epoch_id: &EpochId,
) -> Result<ShardUId, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    Ok(ShardUId::from_shard_id_and_layout(shard_id, &shard_layout))
}

pub fn shard_id_to_index(
    epoch_manager: &dyn EpochManagerAdapter,
    shard_id: ShardId,
    epoch_id: &EpochId,
) -> Result<ShardIndex, EpochError> {
    let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
    Ok(shard_layout.get_shard_index(shard_id)?)
}

#[cfg(test)]
mod tests {
    use crate::RngSeed;
    use crate::shard_assignment::assign_chunk_producers_to_shards;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountId, Balance, ShardIndex};
    use std::collections::{HashMap, HashSet};

    fn validator_stake_for_test(n: usize) -> ValidatorStake {
        ValidatorStake::test(format!("test{:02}", n).parse().unwrap())
    }

    fn assignment_for_test(assignment: Vec<Vec<usize>>) -> Vec<Vec<ValidatorStake>> {
        assignment
            .into_iter()
            .map(|ids| ids.into_iter().map(validator_stake_for_test).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    }

    #[test]
    /// Tests shard assignment logic for minimal amount of validators and
    /// shards.
    fn test_shard_assignment_minimal() {
        let num_chunk_producers = 1;
        let target_assignment = assignment_for_test(vec![vec![0]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            1,
            1,
            1,
            RngSeed::default(),
            None,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests shard assignment logic when previous chunk producer is leaving the
    /// set.
    fn test_shard_assignment_change() {
        let num_chunk_producers = 1;
        let prev_assignment = assignment_for_test(vec![vec![1]]);
        let target_assignment = assignment_for_test(vec![vec![0]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            1,
            1,
            // We must assign new validator even if limit for balancing is zero.
            0,
            RngSeed::default(),
            Some(prev_assignment),
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that chunk producer repeats are supported if needed.
    fn test_shard_assignment_repeats() {
        let num_chunk_producers = 3;
        let prev_assignment =
            assignment_for_test(vec![vec![0, 1, 2], vec![0, 1, 2], vec![3, 4, 5]]);
        let target_assignment = assignment_for_test(vec![vec![0, 1], vec![1, 0], vec![2, 0]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            2,
            0,
            RngSeed::default(),
            Some(prev_assignment),
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that if there are enough validators to avoid repeats, new
    /// assignment is made in stable way, by reassigning some of the old chunk
    /// producers. Repeats must not happen, like in some incorrect ideas of
    /// the assignment algorithm we had.
    fn test_shard_reassignment() {
        let num_chunk_producers = 4;
        let prev_assignment = assignment_for_test(vec![vec![0, 1, 2], vec![3], vec![], vec![]]);
        let target_assignment = assignment_for_test(vec![vec![0], vec![3], vec![1], vec![2]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            4,
            1,
            // Set limit to zero, to check that it is ignored.
            0,
            RngSeed::default(),
            Some(prev_assignment),
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that if chunk producers are well-balanced already, no changes are
    /// made.
    fn test_shard_assignment_is_stable() {
        let num_chunk_producers = 4;
        let prev_assignment = assignment_for_test(vec![vec![2, 3], vec![0, 1]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            2,
            1,
            // As we don't change assignment at all, zero limit for balancing is enough.
            0,
            RngSeed::default(),
            Some(prev_assignment.clone()),
        )
        .unwrap();

        assert_eq!(assignment, prev_assignment);
    }

    #[test]
    /// Tests that limit of assignment changes is taken into account during
    /// rebalancing.
    fn test_shard_assignment_changes_limit() {
        let num_chunk_producers = 6;
        let prev_assignment = assignment_for_test(vec![vec![0, 1, 2, 3], vec![4], vec![5]]);
        let target_assignment = assignment_for_test(vec![vec![0, 1, 3], vec![2, 4], vec![5]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            1,
            1,
            RngSeed::default(),
            Some(prev_assignment),
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Tests that if there was no previous assignment and every chunk producer
    /// is new, the assignment is balanced because limit on shard changes can't
    /// be applied.
    fn test_shard_assignment_empty_start() {
        let num_chunk_producers = 10;
        let target_assignment =
            assignment_for_test(vec![vec![0, 3, 6, 9], vec![1, 4, 7], vec![2, 5, 8]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            1,
            1,
            RngSeed::default(),
            None,
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    #[test]
    /// Test case when perfect balance on number of validators is not
    /// achievable.
    fn test_shard_assignment_imperfect_balance() {
        let num_chunk_producers = 7;
        let prev_assignment = assignment_for_test(vec![vec![0, 1, 2, 3, 4], vec![5], vec![6]]);
        let target_assignment = assignment_for_test(vec![vec![0, 1, 4], vec![3, 5], vec![2, 6]]);

        let assignment = assign_chunk_producers_to_shards(
            (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
            3,
            1,
            5,
            RngSeed::default(),
            Some(prev_assignment),
        )
        .unwrap();

        assert_eq!(assignment, target_assignment);
    }

    fn validator_to_shard(assignment: &[Vec<ValidatorStake>]) -> HashMap<AccountId, ShardIndex> {
        assignment
            .iter()
            .enumerate()
            .flat_map(|(shard_index, cps)| {
                cps.iter().map(move |cp| (cp.account_id().clone(), shard_index))
            })
            .collect()
    }

    #[test]
    /// Tests that shard assignment algorithm converges to a balanced
    /// assignment, respecting the limit on shard changes.
    fn test_shard_assignment_convergence() {
        let num_chunk_producers = 15;
        let num_shards = 3;
        let mut assignment = assignment_for_test(vec![
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![11, 12, 13, 14],
            vec![],
        ]);

        let limit_per_iter = 2;
        let mut iters_left = 5;
        let mut is_balanced = false;
        while !is_balanced && iters_left > 0 {
            let new_assignment = assign_chunk_producers_to_shards(
                (0..num_chunk_producers).into_iter().map(validator_stake_for_test).collect(),
                num_shards,
                1,
                limit_per_iter,
                RngSeed::default(),
                Some(assignment.clone()),
            )
            .unwrap();

            let old_validator_to_shard = validator_to_shard(&assignment);
            let new_validator_to_shard = validator_to_shard(&new_assignment);
            let shard_changes = old_validator_to_shard
                .into_iter()
                .filter(|(v, s)| new_validator_to_shard.get(v) != Some(s))
                .count();
            assert!(
                shard_changes <= limit_per_iter,
                "Too many shard changes when {iters_left} iterations left"
            );

            assignment = new_assignment;
            is_balanced = assignment
                .iter()
                .all(|shard| shard.len() * (num_shards as usize) == num_chunk_producers);
            iters_left -= 1;
        }

        assert!(
            is_balanced,
            "Shard assignment didn't converge in 5 iterations, last assignment = {assignment:?}"
        );
        let original_chunk_producer_ids = (0..num_chunk_producers)
            .into_iter()
            .map(validator_stake_for_test)
            .map(|vs| vs.account_id().clone())
            .collect::<HashSet<_>>();
        let chunk_producer_ids = assignment
            .into_iter()
            .flat_map(|shard| shard.into_iter().map(|cp| cp.account_id().clone()))
            .collect::<HashSet<_>>();
        assert_eq!(original_chunk_producer_ids, chunk_producer_ids);
    }

    impl super::HasStake for (usize, Balance) {
        fn get_stake(&self) -> Balance {
            self.1
        }
    }
}
