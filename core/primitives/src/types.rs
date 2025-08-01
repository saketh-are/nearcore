use crate::account::{AccessKey, Account};
use crate::errors::EpochError;
use crate::hash::CryptoHash;
use crate::serialize::dec_format;
use crate::shard_layout::ShardLayout;
use crate::sharding::ChunkHash;
use crate::trie_key::TrieKey;
use borsh::{BorshDeserialize, BorshSerialize};
pub use chunk_validator_stats::ChunkStats;
use near_crypto::PublicKey;
use near_primitives_core::account::GasKey;
use near_primitives_core::hash::hash;
/// Reexport primitive types
pub use near_primitives_core::types::*;
use near_schema_checker_lib::ProtocolSchema;
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;

use self::chunk_extra::ChunkExtra;

mod chunk_validator_stats;

/// Hash used by to store state root.
pub type StateRoot = CryptoHash;

/// An arbitrary static string to make sure that this struct cannot be
/// serialized to look identical to another serialized struct. For chunk
/// production we are signing a chunk hash, so we need to make sure that
/// this signature means something different.
///
/// This is a messy workaround until we know what to do with NEP 483.
pub(crate) type SignatureDifferentiator = String;

/// Different types of finality.
#[derive(
    serde::Serialize, serde::Deserialize, Default, Clone, Debug, PartialEq, Eq, arbitrary::Arbitrary,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum Finality {
    #[serde(rename = "optimistic")]
    None,
    #[serde(rename = "near-final")]
    DoomSlug,
    #[serde(rename = "final")]
    #[default]
    Final,
}

/// Account ID with its public key.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct AccountWithPublicKey {
    pub account_id: AccountId,
    pub public_key: PublicKey,
}

/// Account info for validators
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct AccountInfo {
    pub account_id: AccountId,
    pub public_key: PublicKey,
    #[serde(with = "dec_format")]
    #[cfg_attr(feature = "schemars", schemars(with = "String"))]
    pub amount: Balance,
}

/// This type is used to mark keys (arrays of bytes) that are queried from store.
///
/// NOTE: Currently, this type is only used in the view_client and RPC to be able to transparently
/// pretty-serialize the bytes arrays as base64-encoded strings (see `serialize.rs`).
#[serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct StoreKey(
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(schema_with = "crate::serialize::base64_schema"))]
    Vec<u8>,
);

/// This type is used to mark values returned from store (arrays of bytes).
///
/// NOTE: Currently, this type is only used in the view_client and RPC to be able to transparently
/// pretty-serialize the bytes arrays as base64-encoded strings (see `serialize.rs`).
#[serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct StoreValue(
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(schema_with = "crate::serialize::base64_schema"))]
    Vec<u8>,
);

/// This type is used to mark function arguments.
///
/// NOTE: The main reason for this to exist (except the type-safety) is that the value is
/// transparently serialized and deserialized as a base64-encoded string when serde is used
/// (serde_json).
#[serde_as]
#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    derive_more::Deref,
    derive_more::From,
    derive_more::Into,
    BorshSerialize,
    BorshDeserialize,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct FunctionArgs(
    #[serde_as(as = "Base64")]
    #[cfg_attr(feature = "schemars", schemars(schema_with = "crate::serialize::base64_schema"))]
    Vec<u8>,
);

/// A structure used to indicate the kind of state changes due to transaction/receipt processing, etc.
#[derive(Debug, Clone)]
pub enum StateChangeKind {
    AccountTouched { account_id: AccountId },
    AccessKeyTouched { account_id: AccountId },
    DataTouched { account_id: AccountId },
    ContractCodeTouched { account_id: AccountId },
}

pub type StateChangesKinds = Vec<StateChangeKind>;

#[easy_ext::ext(StateChangesKindsExt)]
impl StateChangesKinds {
    pub fn from_changes(
        raw_changes: &mut dyn Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChangesKinds, std::io::Error> {
        raw_changes
            .filter_map(|raw_change| {
                let RawStateChangesWithTrieKey { trie_key, .. } = match raw_change {
                    Ok(p) => p,
                    Err(e) => return Some(Err(e)),
                };
                match trie_key {
                    TrieKey::Account { account_id } => {
                        Some(Ok(StateChangeKind::AccountTouched { account_id }))
                    }
                    TrieKey::ContractCode { account_id } => {
                        Some(Ok(StateChangeKind::ContractCodeTouched { account_id }))
                    }
                    TrieKey::AccessKey { account_id, .. } => {
                        Some(Ok(StateChangeKind::AccessKeyTouched { account_id }))
                    }
                    TrieKey::ContractData { account_id, .. } => {
                        Some(Ok(StateChangeKind::DataTouched { account_id }))
                    }
                    _ => None,
                }
            })
            .collect()
    }
}

/// A structure used to index state changes due to transaction/receipt processing and other things.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, PartialEq, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum StateChangeCause {
    /// A type of update that does not get finalized. Used for verification and execution of
    /// immutable smart contract methods. Attempt fo finalize a `TrieUpdate` containing such
    /// change will lead to panic.
    NotWritableToDisk = 0,
    /// A type of update that is used to mark the initial storage update, e.g. during genesis
    /// or in tests setup.
    InitialState = 1,
    /// Processing of a transaction.
    TransactionProcessing { tx_hash: CryptoHash } = 2,
    /// Before the receipt is going to be processed, inputs get drained from the state, which
    /// causes state modification.
    ActionReceiptProcessingStarted { receipt_hash: CryptoHash } = 3,
    /// Computation of gas reward.
    ActionReceiptGasReward { receipt_hash: CryptoHash } = 4,
    /// Processing of a receipt.
    ReceiptProcessing { receipt_hash: CryptoHash } = 5,
    /// The given receipt was postponed. This is either a data receipt or an action receipt.
    /// A `DataReceipt` can be postponed if the corresponding `ActionReceipt` is not received yet,
    /// or other data dependencies are not satisfied.
    /// An `ActionReceipt` can be postponed if not all data dependencies are received.
    PostponedReceipt { receipt_hash: CryptoHash } = 6,
    /// Updated delayed receipts queue in the state.
    /// We either processed previously delayed receipts or added more receipts to the delayed queue.
    UpdatedDelayedReceipts = 7,
    /// State change that happens when we update validator accounts. Not associated with any
    /// specific transaction or receipt.
    ValidatorAccountsUpdate = 8,
    /// State change that is happens due to migration that happens in first block of an epoch
    /// after protocol upgrade
    Migration = 9,
    /// Deprecated in #13155, we need to keep it to preserve enum variant tags for borsh serialization.
    _UnusedReshardingV2 = 10,
    /// Update persistent state kept by Bandwidth Scheduler after running the scheduling algorithm.
    BandwidthSchedulerStateUpdate = 11,
}

/// This represents the committed changes in the Trie with a change cause.
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct RawStateChange {
    pub cause: StateChangeCause,
    pub data: Option<Vec<u8>>,
}

/// List of committed changes with a cause for a given TrieKey
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct RawStateChangesWithTrieKey {
    pub trie_key: TrieKey,
    pub changes: Vec<RawStateChange>,
}

/// Consolidate state change of trie_key and the final value the trie key will be changed to
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, ProtocolSchema)]
pub struct ConsolidatedStateChange {
    pub trie_key: TrieKey,
    pub value: Option<Vec<u8>>,
}

/// key that was updated -> list of updates with the corresponding indexing event.
pub type RawStateChanges = std::collections::BTreeMap<Vec<u8>, RawStateChangesWithTrieKey>;

#[derive(Debug)]
pub enum StateChangesRequest {
    AccountChanges { account_ids: Vec<AccountId> },
    SingleAccessKeyChanges { keys: Vec<AccountWithPublicKey> },
    SingleGasKeyChanges { keys: Vec<AccountWithPublicKey> },
    AllAccessKeyChanges { account_ids: Vec<AccountId> },
    AllGasKeyChanges { account_ids: Vec<AccountId> },
    ContractCodeChanges { account_ids: Vec<AccountId> },
    DataChanges { account_ids: Vec<AccountId>, key_prefix: StoreKey },
}

#[derive(Debug)]
pub enum StateChangeValue {
    AccountUpdate { account_id: AccountId, account: Account },
    AccountDeletion { account_id: AccountId },
    AccessKeyUpdate { account_id: AccountId, public_key: PublicKey, access_key: AccessKey },
    AccessKeyDeletion { account_id: AccountId, public_key: PublicKey },
    GasKeyUpdate { account_id: AccountId, public_key: PublicKey, gas_key: GasKey },
    GasKeyNonceUpdate { account_id: AccountId, public_key: PublicKey, index: u32, nonce: u64 },
    GasKeyDeletion { account_id: AccountId, public_key: PublicKey },
    DataUpdate { account_id: AccountId, key: StoreKey, value: StoreValue },
    DataDeletion { account_id: AccountId, key: StoreKey },
    ContractCodeUpdate { account_id: AccountId, code: Vec<u8> },
    ContractCodeDeletion { account_id: AccountId },
}

impl StateChangeValue {
    pub fn affected_account_id(&self) -> &AccountId {
        match &self {
            StateChangeValue::AccountUpdate { account_id, .. }
            | StateChangeValue::AccountDeletion { account_id }
            | StateChangeValue::AccessKeyUpdate { account_id, .. }
            | StateChangeValue::AccessKeyDeletion { account_id, .. }
            | StateChangeValue::GasKeyUpdate { account_id, .. }
            | StateChangeValue::GasKeyNonceUpdate { account_id, .. }
            | StateChangeValue::GasKeyDeletion { account_id, .. }
            | StateChangeValue::DataUpdate { account_id, .. }
            | StateChangeValue::DataDeletion { account_id, .. }
            | StateChangeValue::ContractCodeUpdate { account_id, .. }
            | StateChangeValue::ContractCodeDeletion { account_id } => account_id,
        }
    }
}

#[derive(Debug)]
pub struct StateChangeWithCause {
    pub cause: StateChangeCause,
    pub value: StateChangeValue,
}

pub type StateChanges = Vec<StateChangeWithCause>;

#[easy_ext::ext(StateChangesExt)]
impl StateChanges {
    pub fn from_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let mut state_changes = Self::new();

        for raw_change in raw_changes {
            let RawStateChangesWithTrieKey { trie_key, changes } = raw_change?;

            match trie_key {
                TrieKey::Account { account_id } => state_changes.extend(changes.into_iter().map(
                    |RawStateChange { cause, data }| StateChangeWithCause {
                        cause,
                        value: if let Some(change_data) = data {
                            StateChangeValue::AccountUpdate {
                                account_id: account_id.clone(),
                                account: <_>::try_from_slice(&change_data).expect(
                                    "Failed to parse internally stored account information",
                                ),
                            }
                        } else {
                            StateChangeValue::AccountDeletion { account_id: account_id.clone() }
                        },
                    },
                )),
                TrieKey::AccessKey { account_id, public_key } => {
                    state_changes.extend(changes.into_iter().map(
                        |RawStateChange { cause, data }| StateChangeWithCause {
                            cause,
                            value: if let Some(change_data) = data {
                                StateChangeValue::AccessKeyUpdate {
                                    account_id: account_id.clone(),
                                    public_key: public_key.clone(),
                                    access_key: <_>::try_from_slice(&change_data)
                                        .expect("Failed to parse internally stored access key"),
                                }
                            } else {
                                StateChangeValue::AccessKeyDeletion {
                                    account_id: account_id.clone(),
                                    public_key: public_key.clone(),
                                }
                            },
                        },
                    ))
                }
                TrieKey::GasKey { account_id, public_key, index } => {
                    if let Some(index) = index {
                        state_changes.extend(changes.into_iter().filter_map(
                            |RawStateChange { cause, data }| {
                                if let Some(change_data) = data {
                                    Some(StateChangeWithCause {
                                        cause,
                                        value: StateChangeValue::GasKeyNonceUpdate {
                                            account_id: account_id.clone(),
                                            public_key: public_key.clone(),
                                            index,
                                            nonce: <_>::try_from_slice(&change_data).expect(
                                                "Failed to parse internally stored gas key nonce",
                                            ),
                                        },
                                    })
                                } else {
                                    // Deletion of a nonce can only be done with a corresponding
                                    // deletion of the gas key, so we don't need to report these.
                                    None
                                }
                            },
                        ));
                    } else {
                        state_changes.extend(changes.into_iter().map(
                            |RawStateChange { cause, data }| StateChangeWithCause {
                                cause,
                                value: if let Some(change_data) = data {
                                    StateChangeValue::GasKeyUpdate {
                                        account_id: account_id.clone(),
                                        public_key: public_key.clone(),
                                        gas_key: <_>::try_from_slice(&change_data)
                                            .expect("Failed to parse internally stored gas key"),
                                    }
                                } else {
                                    StateChangeValue::GasKeyDeletion {
                                        account_id: account_id.clone(),
                                        public_key: public_key.clone(),
                                    }
                                },
                            },
                        ));
                    }
                }
                TrieKey::ContractCode { account_id } => {
                    state_changes.extend(changes.into_iter().map(
                        |RawStateChange { cause, data }| StateChangeWithCause {
                            cause,
                            value: match data {
                                Some(change_data) => StateChangeValue::ContractCodeUpdate {
                                    account_id: account_id.clone(),
                                    code: change_data,
                                },
                                None => StateChangeValue::ContractCodeDeletion {
                                    account_id: account_id.clone(),
                                },
                            },
                        },
                    ));
                }
                TrieKey::ContractData { account_id, key } => {
                    state_changes.extend(changes.into_iter().map(
                        |RawStateChange { cause, data }| StateChangeWithCause {
                            cause,
                            value: if let Some(change_data) = data {
                                StateChangeValue::DataUpdate {
                                    account_id: account_id.clone(),
                                    key: key.to_vec().into(),
                                    value: change_data.into(),
                                }
                            } else {
                                StateChangeValue::DataDeletion {
                                    account_id: account_id.clone(),
                                    key: key.to_vec().into(),
                                }
                            },
                        },
                    ));
                }
                // The next variants considered as unnecessary as too low level
                TrieKey::ReceivedData { .. } => {}
                TrieKey::PostponedReceiptId { .. } => {}
                TrieKey::PendingDataCount { .. } => {}
                TrieKey::PostponedReceipt { .. } => {}
                TrieKey::DelayedReceiptIndices => {}
                TrieKey::DelayedReceipt { .. } => {}
                TrieKey::PromiseYieldIndices => {}
                TrieKey::PromiseYieldTimeout { .. } => {}
                TrieKey::PromiseYieldReceipt { .. } => {}
                TrieKey::BufferedReceiptIndices => {}
                TrieKey::BufferedReceipt { .. } => {}
                TrieKey::BandwidthSchedulerState => {}
                TrieKey::BufferedReceiptGroupsQueueData { .. } => {}
                TrieKey::BufferedReceiptGroupsQueueItem { .. } => {}
                // Global contract code is not a part of account, so ignoring it as well.
                TrieKey::GlobalContractCode { .. } => {}
            }
        }

        Ok(state_changes)
    }
    pub fn from_account_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::AccountUpdate { .. }
                        | StateChangeValue::AccountDeletion { .. }
                )
            })
            .collect())
    }

    pub fn from_access_key_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::AccessKeyUpdate { .. }
                        | StateChangeValue::AccessKeyDeletion { .. }
                )
            })
            .collect())
    }

    pub fn from_gas_key_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::GasKeyUpdate { .. }
                        | StateChangeValue::GasKeyNonceUpdate { .. }
                        | StateChangeValue::GasKeyDeletion { .. }
                )
            })
            .collect())
    }

    pub fn from_contract_code_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::ContractCodeUpdate { .. }
                        | StateChangeValue::ContractCodeDeletion { .. }
                )
            })
            .collect())
    }

    pub fn from_data_changes(
        raw_changes: impl Iterator<Item = Result<RawStateChangesWithTrieKey, std::io::Error>>,
    ) -> Result<StateChanges, std::io::Error> {
        let state_changes = Self::from_changes(raw_changes)?;

        Ok(state_changes
            .into_iter()
            .filter(|state_change| {
                matches!(
                    state_change.value,
                    StateChangeValue::DataUpdate { .. } | StateChangeValue::DataDeletion { .. }
                )
            })
            .collect())
    }
}

#[derive(
    PartialEq, Eq, Clone, Debug, BorshSerialize, BorshDeserialize, serde::Serialize, ProtocolSchema,
)]
pub struct StateRootNode {
    /// In Nightshade, data is the serialized TrieNodeWithSize.
    ///
    /// Beware that hash of an empty state root (i.e. once who’s data is an
    /// empty byte string) **does not** equal hash of an empty byte string.
    /// Instead, an all-zero hash indicates an empty node.
    pub data: Arc<[u8]>,

    /// In Nightshade, memory_usage is a field of TrieNodeWithSize.
    pub memory_usage: u64,
}

impl StateRootNode {
    pub fn empty() -> Self {
        static EMPTY: LazyLock<Arc<[u8]>> = LazyLock::new(|| Arc::new([]));
        StateRootNode { data: EMPTY.clone(), memory_usage: 0 }
    }
}

/// Epoch identifier -- wrapped hash, to make it easier to distinguish.
/// EpochId of epoch T is the hash of last block in T-2
/// EpochId of first two epochs is 0
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    Hash,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    derive_more::AsRef,
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    arbitrary::Arbitrary,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[as_ref(forward)]
pub struct EpochId(pub CryptoHash);

impl std::str::FromStr for EpochId {
    type Err = Box<dyn std::error::Error + Send + Sync>;

    /// Decodes base58-encoded string into a 32-byte crypto hash.
    fn from_str(epoch_id_str: &str) -> Result<Self, Self::Err> {
        Ok(EpochId(CryptoHash::from_str(epoch_id_str)?))
    }
}

/// Stores validator and its stake for two consecutive epochs.
/// It is necessary because the blocks on the epoch boundary need to contain approvals from both
/// epochs.
#[derive(serde::Serialize, Debug, Clone, PartialEq, Eq)]
pub struct ApprovalStake {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub stake_this_epoch: Balance,
    pub stake_next_epoch: Balance,
}

pub mod validator_stake {
    use crate::types::ApprovalStake;
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_crypto::{KeyType, PublicKey};
    use near_primitives_core::types::{AccountId, Balance};
    use serde::Serialize;

    pub use super::ValidatorStakeV1;

    /// Stores validator and its stake.
    #[derive(BorshSerialize, BorshDeserialize, Serialize, Debug, Clone, PartialEq, Eq)]
    #[serde(tag = "validator_stake_struct_version")]
    #[borsh(use_discriminant = true)]
    #[repr(u8)]
    pub enum ValidatorStake {
        V1(ValidatorStakeV1) = 0,
        // Warning: if you're adding a new version, make sure that the borsh encoding of
        // any `ValidatorStake` cannot be equal to the borsh encoding of any `ValidatorStakeV1`.
        // See `EpochSyncProofEpochData::use_versioned_bp_hash_format` for an explanation.
        // The simplest way to ensure that is to make sure that any new `ValidatorStakeVx`
        // begins with a field of type `AccountId`.
    }

    pub struct ValidatorStakeIter<'a> {
        collection: ValidatorStakeIterSource<'a>,
        curr_index: usize,
        len: usize,
    }

    impl<'a> ValidatorStakeIter<'a> {
        pub fn empty() -> Self {
            Self { collection: ValidatorStakeIterSource::V2(&[]), curr_index: 0, len: 0 }
        }

        pub fn v1(collection: &'a [ValidatorStakeV1]) -> Self {
            Self {
                collection: ValidatorStakeIterSource::V1(collection),
                curr_index: 0,
                len: collection.len(),
            }
        }

        pub fn new(collection: &'a [ValidatorStake]) -> Self {
            Self {
                collection: ValidatorStakeIterSource::V2(collection),
                curr_index: 0,
                len: collection.len(),
            }
        }

        pub fn len(&self) -> usize {
            self.len
        }
    }

    impl<'a> Iterator for ValidatorStakeIter<'a> {
        type Item = ValidatorStake;

        fn next(&mut self) -> Option<Self::Item> {
            if self.curr_index < self.len {
                let item = match self.collection {
                    ValidatorStakeIterSource::V1(collection) => {
                        ValidatorStake::V1(collection[self.curr_index].clone())
                    }
                    ValidatorStakeIterSource::V2(collection) => collection[self.curr_index].clone(),
                };
                self.curr_index += 1;
                Some(item)
            } else {
                None
            }
        }
    }

    enum ValidatorStakeIterSource<'a> {
        V1(&'a [ValidatorStakeV1]),
        V2(&'a [ValidatorStake]),
    }

    impl ValidatorStake {
        pub fn new_v1(account_id: AccountId, public_key: PublicKey, stake: Balance) -> Self {
            Self::V1(ValidatorStakeV1 { account_id, public_key, stake })
        }

        pub fn new(account_id: AccountId, public_key: PublicKey, stake: Balance) -> Self {
            Self::new_v1(account_id, public_key, stake)
        }

        pub fn test(account_id: AccountId) -> Self {
            Self::new_v1(account_id, PublicKey::empty(KeyType::ED25519), 0)
        }

        pub fn into_v1(self) -> ValidatorStakeV1 {
            match self {
                Self::V1(v1) => v1,
            }
        }

        #[inline]
        pub fn account_and_stake(self) -> (AccountId, Balance) {
            match self {
                Self::V1(v1) => (v1.account_id, v1.stake),
            }
        }

        #[inline]
        pub fn destructure(self) -> (AccountId, PublicKey, Balance) {
            match self {
                Self::V1(v1) => (v1.account_id, v1.public_key, v1.stake),
            }
        }

        #[inline]
        pub fn take_account_id(self) -> AccountId {
            match self {
                Self::V1(v1) => v1.account_id,
            }
        }

        #[inline]
        pub fn account_id(&self) -> &AccountId {
            match self {
                Self::V1(v1) => &v1.account_id,
            }
        }

        #[inline]
        pub fn take_public_key(self) -> PublicKey {
            match self {
                Self::V1(v1) => v1.public_key,
            }
        }

        #[inline]
        pub fn public_key(&self) -> &PublicKey {
            match self {
                Self::V1(v1) => &v1.public_key,
            }
        }

        #[inline]
        pub fn stake(&self) -> Balance {
            match self {
                Self::V1(v1) => v1.stake,
            }
        }

        #[inline]
        pub fn stake_mut(&mut self) -> &mut Balance {
            match self {
                Self::V1(v1) => &mut v1.stake,
            }
        }

        pub fn get_approval_stake(&self, is_next_epoch: bool) -> ApprovalStake {
            ApprovalStake {
                account_id: self.account_id().clone(),
                public_key: self.public_key().clone(),
                stake_this_epoch: if is_next_epoch { 0 } else { self.stake() },
                stake_next_epoch: if is_next_epoch { self.stake() } else { 0 },
            }
        }

        /// Returns the validator's number of mandates (rounded down) at `stake_per_seat`.
        ///
        /// It returns `u16` since it allows infallible conversion to `usize` and with [`u16::MAX`]
        /// equalling 65_535 it should be sufficient to hold the number of mandates per validator.
        ///
        /// # Why `u16` should be sufficient
        ///
        /// As of October 2023, a [recommended lower bound] for the stake required per mandate is
        /// 25k $NEAR. At this price, the validator with highest stake would have 1_888 mandates,
        /// which is well below `u16::MAX`.
        ///
        /// From another point of view, with more than `u16::MAX` mandates for validators, sampling
        /// mandates might become computationally too expensive. This might trigger an increase in
        /// the required stake per mandate, bringing down the number of mandates per validator.
        ///
        /// [recommended lower bound]: https://near.zulipchat.com/#narrow/stream/407237-pagoda.2Fcore.2Fstateless-validation/topic/validator.20seat.20assignment/near/393792901
        ///
        /// # Panics
        ///
        /// Panics if the number of mandates overflows `u16`.
        pub fn num_mandates(&self, stake_per_mandate: Balance) -> u16 {
            // Integer division in Rust returns the floor as described here
            // https://doc.rust-lang.org/std/primitive.u64.html#method.div_euclid
            u16::try_from(self.stake() / stake_per_mandate)
                .expect("number of mandates should fit u16")
        }

        /// Returns the weight attributed to the validator's partial mandate.
        ///
        /// A validator has a partial mandate if its stake cannot be divided evenly by
        /// `stake_per_mandate`. The remainder of that division is the weight of the partial
        /// mandate.
        ///
        /// Due to this definition a validator has exactly one partial mandate with `0 <= weight <
        /// stake_per_mandate`.
        ///
        /// # Example
        ///
        /// Let `V` be a validator with stake of 12. If `stake_per_mandate` equals 5 then the weight
        /// of `V`'s partial mandate is `12 % 5 = 2`.
        pub fn partial_mandate_weight(&self, stake_per_mandate: Balance) -> Balance {
            self.stake() % stake_per_mandate
        }
    }
}

/// Stores validator and its stake.
#[derive(
    BorshSerialize, BorshDeserialize, serde::Serialize, Debug, Clone, PartialEq, Eq, ProtocolSchema,
)]
pub struct ValidatorStakeV1 {
    /// Account that stakes money.
    pub account_id: AccountId,
    /// Public key of the proposed validator.
    pub public_key: PublicKey,
    /// Stake / weight of the validator.
    pub stake: Balance,
}

pub mod chunk_extra {
    use crate::bandwidth_scheduler::BandwidthRequests;
    use crate::congestion_info::CongestionInfo;
    use crate::types::StateRoot;
    use crate::types::validator_stake::{ValidatorStake, ValidatorStakeIter};
    use borsh::{BorshDeserialize, BorshSerialize};
    use near_primitives_core::hash::CryptoHash;
    use near_primitives_core::types::{Balance, Gas};

    pub use super::ChunkExtraV1;

    /// Information after chunk was processed, used to produce or check next chunk.
    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq, serde::Serialize)]
    #[borsh(use_discriminant = true)]
    #[repr(u8)]
    pub enum ChunkExtra {
        V1(ChunkExtraV1) = 0,
        V2(ChunkExtraV2) = 1,
        V3(ChunkExtraV3) = 2,
        V4(ChunkExtraV4) = 3,
    }

    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq, serde::Serialize)]
    pub struct ChunkExtraV2 {
        /// Post state root after applying give chunk.
        pub state_root: StateRoot,
        /// Root of merklizing results of receipts (transactions) execution.
        pub outcome_root: CryptoHash,
        /// Validator proposals produced by given chunk.
        pub validator_proposals: Vec<ValidatorStake>,
        /// Actually how much gas were used.
        pub gas_used: Gas,
        /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
        pub gas_limit: Gas,
        /// Total balance burnt after processing the current chunk.
        pub balance_burnt: Balance,
    }

    /// V2 -> V3: add congestion info fields.
    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq, serde::Serialize)]
    pub struct ChunkExtraV3 {
        /// Post state root after applying give chunk.
        pub state_root: StateRoot,
        /// Root of merklizing results of receipts (transactions) execution.
        pub outcome_root: CryptoHash,
        /// Validator proposals produced by given chunk.
        pub validator_proposals: Vec<ValidatorStake>,
        /// Actually how much gas were used.
        pub gas_used: Gas,
        /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
        pub gas_limit: Gas,
        /// Total balance burnt after processing the current chunk.
        pub balance_burnt: Balance,
        /// Congestion info about this shard after the chunk was applied.
        congestion_info: CongestionInfo,
    }

    /// V3 -> V4: add bandwidth requests field.
    #[derive(Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq, serde::Serialize)]
    pub struct ChunkExtraV4 {
        /// Post state root after applying give chunk.
        pub state_root: StateRoot,
        /// Root of merklizing results of receipts (transactions) execution.
        pub outcome_root: CryptoHash,
        /// Validator proposals produced by given chunk.
        pub validator_proposals: Vec<ValidatorStake>,
        /// Actually how much gas were used.
        pub gas_used: Gas,
        /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
        pub gas_limit: Gas,
        /// Total balance burnt after processing the current chunk.
        pub balance_burnt: Balance,
        /// Congestion info about this shard after the chunk was applied.
        congestion_info: CongestionInfo,
        /// Requests for bandwidth to send receipts to other shards.
        pub bandwidth_requests: BandwidthRequests,
    }

    impl ChunkExtra {
        /// This method creates a slimmed down and invalid ChunkExtra. It's used
        /// for resharding where we only need the state root. This should not be
        /// used as part of regular processing.
        pub fn new_with_only_state_root(state_root: &StateRoot) -> Self {
            // TODO(congestion_control) - integration with resharding
            let congestion_control = Some(CongestionInfo::default());
            Self::new(
                state_root,
                CryptoHash::default(),
                vec![],
                0,
                0,
                0,
                congestion_control,
                BandwidthRequests::empty(),
            )
        }

        pub fn new(
            state_root: &StateRoot,
            outcome_root: CryptoHash,
            validator_proposals: Vec<ValidatorStake>,
            gas_used: Gas,
            gas_limit: Gas,
            balance_burnt: Balance,
            congestion_info: Option<CongestionInfo>,
            bandwidth_requests: BandwidthRequests,
        ) -> Self {
            Self::V4(ChunkExtraV4 {
                state_root: *state_root,
                outcome_root,
                validator_proposals,
                gas_used,
                gas_limit,
                balance_burnt,
                congestion_info: congestion_info.unwrap(),
                bandwidth_requests,
            })
        }

        #[inline]
        pub fn outcome_root(&self) -> &StateRoot {
            match self {
                Self::V1(v1) => &v1.outcome_root,
                Self::V2(v2) => &v2.outcome_root,
                Self::V3(v3) => &v3.outcome_root,
                Self::V4(v4) => &v4.outcome_root,
            }
        }

        #[inline]
        pub fn state_root(&self) -> &StateRoot {
            match self {
                Self::V1(v1) => &v1.state_root,
                Self::V2(v2) => &v2.state_root,
                Self::V3(v3) => &v3.state_root,
                Self::V4(v4) => &v4.state_root,
            }
        }

        #[inline]
        pub fn state_root_mut(&mut self) -> &mut StateRoot {
            match self {
                Self::V1(v1) => &mut v1.state_root,
                Self::V2(v2) => &mut v2.state_root,
                Self::V3(v3) => &mut v3.state_root,
                Self::V4(v4) => &mut v4.state_root,
            }
        }

        #[inline]
        pub fn validator_proposals(&self) -> ValidatorStakeIter {
            match self {
                Self::V1(v1) => ValidatorStakeIter::v1(&v1.validator_proposals),
                Self::V2(v2) => ValidatorStakeIter::new(&v2.validator_proposals),
                Self::V3(v3) => ValidatorStakeIter::new(&v3.validator_proposals),
                Self::V4(v4) => ValidatorStakeIter::new(&v4.validator_proposals),
            }
        }

        #[inline]
        pub fn gas_limit(&self) -> Gas {
            match self {
                Self::V1(v1) => v1.gas_limit,
                Self::V2(v2) => v2.gas_limit,
                Self::V3(v3) => v3.gas_limit,
                Self::V4(v4) => v4.gas_limit,
            }
        }

        #[inline]
        pub fn gas_used(&self) -> Gas {
            match self {
                Self::V1(v1) => v1.gas_used,
                Self::V2(v2) => v2.gas_used,
                Self::V3(v3) => v3.gas_used,
                Self::V4(v4) => v4.gas_used,
            }
        }

        #[inline]
        pub fn balance_burnt(&self) -> Balance {
            match self {
                Self::V1(v1) => v1.balance_burnt,
                Self::V2(v2) => v2.balance_burnt,
                Self::V3(v3) => v3.balance_burnt,
                Self::V4(v4) => v4.balance_burnt,
            }
        }

        #[inline]
        pub fn congestion_info(&self) -> CongestionInfo {
            match self {
                Self::V1(_) | Self::V2(_) => {
                    debug_assert!(false, "Calling congestion_info on V1 or V2 header version");
                    Default::default()
                }
                Self::V3(v3) => v3.congestion_info,
                Self::V4(v4) => v4.congestion_info,
            }
        }

        #[inline]
        pub fn congestion_info_mut(&mut self) -> &mut CongestionInfo {
            match self {
                Self::V1(_) | Self::V2(_) => panic!("Calling congestion_info_mut on V1 or V2"),
                Self::V3(v3) => &mut v3.congestion_info,
                Self::V4(v4) => &mut v4.congestion_info,
            }
        }

        #[inline]
        pub fn bandwidth_requests(&self) -> Option<&BandwidthRequests> {
            match self {
                Self::V1(_) | Self::V2(_) | Self::V3(_) => None,
                Self::V4(extra) => Some(&extra.bandwidth_requests),
            }
        }
    }
}

/// Information after chunk was processed, used to produce or check next chunk.
#[derive(
    Debug, PartialEq, BorshSerialize, BorshDeserialize, Clone, Eq, ProtocolSchema, serde::Serialize,
)]
pub struct ChunkExtraV1 {
    /// Post state root after applying give chunk.
    pub state_root: StateRoot,
    /// Root of merklizing results of receipts (transactions) execution.
    pub outcome_root: CryptoHash,
    /// Validator proposals produced by given chunk.
    pub validator_proposals: Vec<ValidatorStakeV1>,
    /// Actually how much gas were used.
    pub gas_used: Gas,
    /// Gas limit, allows to increase or decrease limit based on expected time vs real time for computing the chunk.
    pub gas_limit: Gas,
    /// Total balance burnt after processing the current chunk.
    pub balance_burnt: Balance,
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum BlockId {
    #[cfg_attr(feature = "schemars", schemars(title = "block_height"))]
    Height(BlockHeight),
    Hash(CryptoHash),
}

pub type MaybeBlockId = Option<BlockId>;

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum SyncCheckpoint {
    Genesis,
    EarliestAvailable,
}

#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, arbitrary::Arbitrary,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum BlockReference {
    BlockId(BlockId),
    Finality(Finality),
    SyncCheckpoint(SyncCheckpoint),
}

impl BlockReference {
    pub fn latest() -> Self {
        Self::Finality(Finality::None)
    }
}

impl From<BlockId> for BlockReference {
    fn from(block_id: BlockId) -> Self {
        Self::BlockId(block_id)
    }
}

impl From<Finality> for BlockReference {
    fn from(finality: Finality) -> Self {
        Self::Finality(finality)
    }
}

#[derive(
    Default,
    BorshSerialize,
    BorshDeserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    ProtocolSchema,
    serde::Serialize,
)]
pub struct ValidatorStats {
    pub produced: NumBlocks,
    pub expected: NumBlocks,
}

impl ValidatorStats {
    /// Compare stats with threshold which is an expected percentage from 0 to
    /// 100.
    pub fn less_than(&self, threshold: u8) -> bool {
        self.produced * 100 < u64::from(threshold) * self.expected
    }
}

#[derive(Debug, BorshSerialize, BorshDeserialize, PartialEq, Eq, ProtocolSchema)]
pub struct BlockChunkValidatorStats {
    pub block_stats: ValidatorStats,
    pub chunk_stats: ChunkStats,
}

#[derive(serde::Deserialize, Debug, arbitrary::Arbitrary, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum EpochReference {
    EpochId(EpochId),
    BlockId(BlockId),
    Latest,
}

impl serde::Serialize for EpochReference {
    fn serialize<S>(&self, s: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // cspell:words newtype
        match self {
            EpochReference::EpochId(epoch_id) => {
                s.serialize_newtype_variant("EpochReference", 0, "epoch_id", epoch_id)
            }
            EpochReference::BlockId(block_id) => {
                s.serialize_newtype_variant("EpochReference", 1, "block_id", block_id)
            }
            EpochReference::Latest => {
                s.serialize_newtype_variant("EpochReference", 2, "latest", &())
            }
        }
    }
}

/// Either an epoch id or latest block hash.  When `EpochId` variant is used it
/// must be an identifier of a past epoch.  When `BlockHeight` is used it must
/// be hash of the latest block in the current epoch.  Using current epoch id
/// with `EpochId` or arbitrary block hash in past or present epochs will result
/// in errors.
#[derive(Clone, Debug)]
pub enum ValidatorInfoIdentifier {
    EpochId(EpochId),
    BlockHash(CryptoHash),
}

/// Reasons for removing a validator from the validator set.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Debug,
    PartialEq,
    Eq,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ValidatorKickoutReason {
    /// Deprecated
    _UnusedSlashed = 0,
    /// Validator didn't produce enough blocks.
    NotEnoughBlocks { produced: NumBlocks, expected: NumBlocks } = 1,
    /// Validator didn't produce enough chunks.
    NotEnoughChunks { produced: NumBlocks, expected: NumBlocks } = 2,
    /// Validator unstaked themselves.
    Unstaked = 3,
    /// Validator stake is now below threshold
    NotEnoughStake {
        #[serde(with = "dec_format", rename = "stake_u128")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        stake: Balance,
        #[serde(with = "dec_format", rename = "threshold_u128")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        threshold: Balance,
    } = 4,
    /// Enough stake but is not chosen because of seat limits.
    DidNotGetASeat = 5,
    /// Validator didn't produce enough chunk endorsements.
    NotEnoughChunkEndorsements { produced: NumBlocks, expected: NumBlocks } = 6,
    /// Validator's last block proposal was for a protocol version older than
    /// the network's voted protocol version.
    ProtocolVersionTooOld { version: ProtocolVersion, network_version: ProtocolVersion } = 7,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransactionOrReceiptId {
    Transaction { transaction_hash: CryptoHash, sender_id: AccountId },
    Receipt { receipt_id: CryptoHash, receiver_id: AccountId },
}

/// Provides information about current epoch validators.
/// Used to break dependency between epoch manager and runtime.
pub trait EpochInfoProvider: Send + Sync {
    /// Get current stake of a validator in the given epoch.
    /// If the account is not a validator, returns `None`.
    fn validator_stake(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<Balance>, EpochError>;

    /// Get the total stake of the given epoch.
    fn validator_total_stake(&self, epoch_id: &EpochId) -> Result<Balance, EpochError>;

    fn minimum_stake(&self, prev_block_hash: &CryptoHash) -> Result<Balance, EpochError>;

    /// Get the chain_id of the chain this epoch belongs to
    fn chain_id(&self) -> String;

    fn shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError>;
}

/// State changes for a range of blocks.
/// Expects that a block is present at most once in the list.
#[derive(borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct StateChangesForBlockRange {
    pub blocks: Vec<StateChangesForBlock>,
}

/// State changes for a single block.
/// Expects that a shard is present at most once in the list of state changes.
#[derive(borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct StateChangesForBlock {
    pub block_hash: CryptoHash,
    pub state_changes: Vec<StateChangesForShard>,
}

/// Key and value of a StateChanges column.
#[derive(borsh::BorshDeserialize, borsh::BorshSerialize)]
pub struct StateChangesForShard {
    pub shard_id: ShardId,
    pub state_changes: Vec<RawStateChangesWithTrieKey>,
}

/// Chunk application result.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkExecutionResult {
    pub chunk_extra: ChunkExtra,
    pub outgoing_receipts_root: CryptoHash,
}

/// Execution results for all chunks in the block.
/// For genesis inner hashmap is always empty.
pub struct BlockExecutionResults(pub HashMap<ChunkHash, ChunkExecutionResult>);

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChunkExecutionResultHash(pub CryptoHash);

impl ChunkExecutionResult {
    pub fn compute_hash(&self) -> ChunkExecutionResultHash {
        let bytes = borsh::to_vec(self).expect("Failed to serialize");
        ChunkExecutionResultHash(hash(&bytes))
    }
}

#[cfg(test)]
mod tests {
    use near_crypto::{KeyType, PublicKey};
    use near_primitives_core::types::Balance;

    use super::validator_stake::ValidatorStake;

    fn new_validator_stake(stake: Balance) -> ValidatorStake {
        ValidatorStake::new(
            "test_account".parse().unwrap(),
            PublicKey::empty(KeyType::ED25519),
            stake,
        )
    }

    #[test]
    fn test_validator_stake_num_mandates() {
        assert_eq!(new_validator_stake(0).num_mandates(5), 0);
        assert_eq!(new_validator_stake(10).num_mandates(5), 2);
        assert_eq!(new_validator_stake(12).num_mandates(5), 2);
    }

    #[test]
    fn test_validator_partial_mandate_weight() {
        assert_eq!(new_validator_stake(0).partial_mandate_weight(5), 0);
        assert_eq!(new_validator_stake(10).partial_mandate_weight(5), 0);
        assert_eq!(new_validator_stake(12).partial_mandate_weight(5), 2);
    }
}
