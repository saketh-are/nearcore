use crate::action::GlobalContractIdentifier;
use crate::hash::CryptoHash;
use crate::serialize::dec_format;
use crate::shard_layout::ShardLayoutError;
use crate::sharding::ChunkHash;
use crate::types::{AccountId, Balance, EpochId, Gas, Nonce};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::PublicKey;
use near_primitives_core::types::{BlockHeight, ProtocolVersion, ShardId};
use near_schema_checker_lib::ProtocolSchema;
use std::fmt::{Debug, Display};

/// Error returned in the ExecutionOutcome in case of failure
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum TxExecutionError {
    /// An error happened during Action execution
    ActionError(ActionError) = 0,
    /// An error happened during Transaction execution
    InvalidTxError(InvalidTxError) = 1,
}

impl std::error::Error for TxExecutionError {}

impl Display for TxExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            TxExecutionError::ActionError(e) => write!(f, "{}", e),
            TxExecutionError::InvalidTxError(e) => write!(f, "{}", e),
        }
    }
}

impl From<ActionError> for TxExecutionError {
    fn from(error: ActionError) -> Self {
        TxExecutionError::ActionError(error)
    }
}

impl From<InvalidTxError> for TxExecutionError {
    fn from(error: InvalidTxError) -> Self {
        TxExecutionError::InvalidTxError(error)
    }
}

/// Error returned from `Runtime::apply`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    /// An unexpected integer overflow occurred. The likely issue is an invalid state or the transition.
    UnexpectedIntegerOverflow(String),
    /// An error happened during TX verification and account charging.
    InvalidTxError(InvalidTxError),
    /// Unexpected error which is typically related to the node storage corruption.
    /// It's possible the input state is invalid or malicious.
    StorageError(StorageError),
    /// The incoming receipt didn't pass the validation, it's likely a malicious behavior.
    ReceiptValidationError(ReceiptValidationError),
    /// Error when accessing validator information. Happens inside epoch manager.
    ValidatorError(EpochError),
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for RuntimeError {}

/// Contexts in which `StorageError::MissingTrieValue` error might occur.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    BorshSerialize,
    BorshDeserialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum MissingTrieValueContext {
    /// Missing trie value when reading from TrieIterator.
    TrieIterator = 0,
    /// Missing trie value when reading from TriePrefetchingStorage.
    TriePrefetchingStorage = 1,
    /// Missing trie value when reading from TrieMemoryPartialStorage.
    TrieMemoryPartialStorage = 2,
    /// Missing trie value when reading from TrieStorage.
    TrieStorage = 3,
}

impl MissingTrieValueContext {
    pub fn metrics_label(&self) -> &str {
        match self {
            Self::TrieIterator => "trie_iterator",
            Self::TriePrefetchingStorage => "trie_prefetching_storage",
            Self::TrieMemoryPartialStorage => "trie_memory_partial_storage",
            Self::TrieStorage => "trie_storage",
        }
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    BorshSerialize,
    BorshDeserialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct MissingTrieValue {
    pub context: MissingTrieValueContext,
    pub hash: CryptoHash,
}

/// Errors which may occur during working with trie storages, storing
/// trie values (trie nodes and state values) by their hashes.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    BorshSerialize,
    BorshDeserialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum StorageError {
    /// Key-value db internal failure
    StorageInternalError = 0,
    /// Requested trie value by its hash which is missing in storage.
    MissingTrieValue(MissingTrieValue) = 1,
    /// Found trie node which shouldn't be part of state. Raised during
    /// validation of state sync parts where incorrect node was passed.
    /// TODO (#8997): consider including hash of trie node.
    UnexpectedTrieValue = 2,
    /// Either invalid state or key-value db is corrupted.
    /// For PartialStorage it cannot be corrupted.
    /// Error message is unreliable and for debugging purposes only. It's also probably ok to
    /// panic in every place that produces this error.
    /// We can check if db is corrupted by verifying everything in the state trie.
    StorageInconsistentState(String) = 3,
    /// Flat storage error, meaning that it doesn't support some block anymore.
    /// We guarantee that such block cannot become final, thus block processing
    /// must resume normally.
    FlatStorageBlockNotSupported(String) = 4,
    /// In-memory trie could not be loaded for some reason.
    MemTrieLoadingError(String) = 5,
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for StorageError {}

/// An error happened during TX execution
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum InvalidTxError {
    /// Happens if a wrong AccessKey used or AccessKey has not enough permissions
    InvalidAccessKeyError(InvalidAccessKeyError) = 0,
    /// TX signer_id is not a valid [`AccountId`]
    InvalidSignerId {
        signer_id: String,
    } = 1,
    /// TX signer_id is not found in a storage
    SignerDoesNotExist {
        signer_id: AccountId,
    } = 2,
    /// Transaction nonce must be strictly greater than `account[access_key].nonce`.
    InvalidNonce {
        tx_nonce: Nonce,
        ak_nonce: Nonce,
    } = 3,
    /// Transaction nonce is larger than the upper bound given by the block height
    NonceTooLarge {
        tx_nonce: Nonce,
        upper_bound: Nonce,
    } = 4,
    /// TX receiver_id is not a valid AccountId
    InvalidReceiverId {
        receiver_id: String,
    } = 5,
    /// TX signature is not valid
    InvalidSignature = 6,
    /// Account does not have enough balance to cover TX cost
    NotEnoughBalance {
        signer_id: AccountId,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        balance: Balance,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        cost: Balance,
    } = 7,
    /// Signer account doesn't have enough balance after transaction.
    LackBalanceForState {
        /// An account which doesn't have enough balance to cover storage.
        signer_id: AccountId,
        /// Required balance to cover the state.
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        amount: Balance,
    } = 8,
    /// An integer overflow occurred during transaction cost estimation.
    CostOverflow = 9,
    /// Transaction parent block hash doesn't belong to the current chain
    InvalidChain = 10,
    /// Transaction has expired
    Expired = 11,
    /// An error occurred while validating actions of a Transaction.
    ActionsValidation(ActionsValidationError) = 12,
    /// The size of serialized transaction exceeded the limit.
    TransactionSizeExceeded {
        size: u64,
        limit: u64,
    } = 13,
    /// Transaction version is invalid.
    InvalidTransactionVersion = 14,
    // Error occurred during storage access
    StorageError(StorageError) = 15,
    /// The receiver shard of the transaction is too congested to accept new
    /// transactions at the moment.
    ShardCongested {
        /// The congested shard.
        shard_id: u32,
        /// A value between 0 (no congestion) and 1 (max congestion).
        #[cfg_attr(feature = "schemars", schemars(with = "f64"))]
        congestion_level: ordered_float::NotNan<f64>,
    } = 16,
    /// The receiver shard of the transaction missed several chunks and rejects
    /// new transaction until it can make progress again.
    ShardStuck {
        /// The shard that fails making progress.
        shard_id: u32,
        /// The number of blocks since the last included chunk of the shard.
        missed_chunks: u64,
    } = 17,
}

impl From<StorageError> for InvalidTxError {
    fn from(error: StorageError) -> Self {
        InvalidTxError::StorageError(error)
    }
}

impl std::error::Error for InvalidTxError {}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum InvalidAccessKeyError {
    /// The access key identified by the `public_key` doesn't exist for the account
    AccessKeyNotFound { account_id: AccountId, public_key: Box<PublicKey> } = 0,
    /// Transaction `receiver_id` doesn't match the access key receiver_id
    ReceiverMismatch { tx_receiver: AccountId, ak_receiver: String } = 1,
    /// Transaction method name isn't allowed by the access key
    MethodNameMismatch { method_name: String } = 2,
    /// Transaction requires a full permission access key.
    RequiresFullAccess = 3,
    /// Access Key does not have enough allowance to cover transaction cost
    NotEnoughAllowance {
        account_id: AccountId,
        public_key: Box<PublicKey>,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        allowance: Balance,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        cost: Balance,
    } = 4,
    /// Having a deposit with a function call action is not allowed with a function call access key.
    DepositWithFunctionCall = 5,
}

/// Describes the error for validating a list of actions.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ActionsValidationError {
    /// The delete action must be a final action in transaction
    DeleteActionMustBeFinal = 0,
    /// The total prepaid gas (for all given actions) exceeded the limit.
    TotalPrepaidGasExceeded { total_prepaid_gas: Gas, limit: Gas } = 1,
    /// The number of actions exceeded the given limit.
    TotalNumberOfActionsExceeded { total_number_of_actions: u64, limit: u64 } = 2,
    /// The total number of bytes of the method names exceeded the limit in a Add Key action.
    AddKeyMethodNamesNumberOfBytesExceeded { total_number_of_bytes: u64, limit: u64 } = 3,
    /// The length of some method name exceeded the limit in a Add Key action.
    AddKeyMethodNameLengthExceeded { length: u64, limit: u64 } = 4,
    /// Integer overflow during a compute.
    IntegerOverflow = 5,
    /// Invalid account ID.
    InvalidAccountId { account_id: String } = 6,
    /// The size of the contract code exceeded the limit in a DeployContract action.
    ContractSizeExceeded { size: u64, limit: u64 } = 7,
    /// The length of the method name exceeded the limit in a Function Call action.
    FunctionCallMethodNameLengthExceeded { length: u64, limit: u64 } = 8,
    /// The length of the arguments exceeded the limit in a Function Call action.
    FunctionCallArgumentsLengthExceeded { length: u64, limit: u64 } = 9,
    /// An attempt to stake with a public key that is not convertible to ristretto.
    UnsuitableStakingKey { public_key: Box<PublicKey> } = 10,
    /// The attached amount of gas in a FunctionCall action has to be a positive number.
    FunctionCallZeroAttachedGas = 11,
    /// There should be the only one DelegateAction
    DelegateActionMustBeOnlyOne = 12,
    /// The transaction includes a feature that the current protocol version
    /// does not support.
    ///
    /// Note: we stringify the protocol feature name instead of using
    /// `ProtocolFeature` here because we don't want to leak the internals of
    /// that type into observable borsh serialization.
    UnsupportedProtocolFeature { protocol_feature: String, version: ProtocolVersion } = 13,
}

/// Describes the error for validating a receipt.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ReceiptValidationError {
    /// The `predecessor_id` of a Receipt is not valid.
    InvalidPredecessorId { account_id: String } = 0,
    /// The `receiver_id` of a Receipt is not valid.
    InvalidReceiverId { account_id: String } = 1,
    /// The `signer_id` of an ActionReceipt is not valid.
    InvalidSignerId { account_id: String } = 2,
    /// The `receiver_id` of a DataReceiver within an ActionReceipt is not valid.
    InvalidDataReceiverId { account_id: String } = 3,
    /// The length of the returned data exceeded the limit in a DataReceipt.
    ReturnedValueLengthExceeded { length: u64, limit: u64 } = 4,
    /// The number of input data dependencies exceeds the limit in an ActionReceipt.
    NumberInputDataDependenciesExceeded { number_of_input_data_dependencies: u64, limit: u64 } = 5,
    /// An error occurred while validating actions of an ActionReceipt.
    ActionsValidation(ActionsValidationError) = 6,
    /// Receipt is bigger than the limit.
    ReceiptSizeExceeded { size: u64, limit: u64 } = 7,
}

impl Display for ReceiptValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            ReceiptValidationError::InvalidPredecessorId { account_id } => {
                write!(f, "The predecessor_id `{}` of a Receipt is not valid.", account_id)
            }
            ReceiptValidationError::InvalidReceiverId { account_id } => {
                write!(f, "The receiver_id `{}` of a Receipt is not valid.", account_id)
            }
            ReceiptValidationError::InvalidSignerId { account_id } => {
                write!(f, "The signer_id `{}` of an ActionReceipt is not valid.", account_id)
            }
            ReceiptValidationError::InvalidDataReceiverId { account_id } => write!(
                f,
                "The receiver_id `{}` of a DataReceiver within an ActionReceipt is not valid.",
                account_id
            ),
            ReceiptValidationError::ReturnedValueLengthExceeded { length, limit } => write!(
                f,
                "The length of the returned data {} exceeded the limit {} in a DataReceipt",
                length, limit
            ),
            ReceiptValidationError::NumberInputDataDependenciesExceeded {
                number_of_input_data_dependencies,
                limit,
            } => write!(
                f,
                "The number of input data dependencies {} exceeded the limit {} in an ActionReceipt",
                number_of_input_data_dependencies, limit
            ),
            ReceiptValidationError::ActionsValidation(e) => write!(f, "{}", e),
            ReceiptValidationError::ReceiptSizeExceeded { size, limit } => {
                write!(f, "The size of the receipt exceeded the limit: {} > {}", size, limit)
            }
        }
    }
}

impl std::error::Error for ReceiptValidationError {}

impl Display for ActionsValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            ActionsValidationError::DeleteActionMustBeFinal => {
                write!(f, "The delete action must be the last action in transaction")
            }
            ActionsValidationError::TotalPrepaidGasExceeded { total_prepaid_gas, limit } => {
                write!(f, "The total prepaid gas {} exceeds the limit {}", total_prepaid_gas, limit)
            }
            ActionsValidationError::TotalNumberOfActionsExceeded {
                total_number_of_actions,
                limit,
            } => {
                write!(
                    f,
                    "The total number of actions {} exceeds the limit {}",
                    total_number_of_actions, limit
                )
            }
            ActionsValidationError::AddKeyMethodNamesNumberOfBytesExceeded {
                total_number_of_bytes,
                limit,
            } => write!(
                f,
                "The total number of bytes in allowed method names {} exceeds the maximum allowed number {} in a AddKey action",
                total_number_of_bytes, limit
            ),
            ActionsValidationError::AddKeyMethodNameLengthExceeded { length, limit } => write!(
                f,
                "The length of some method name {} exceeds the maximum allowed length {} in a AddKey action",
                length, limit
            ),
            ActionsValidationError::IntegerOverflow => {
                write!(f, "Integer overflow during a compute",)
            }
            ActionsValidationError::InvalidAccountId { account_id } => {
                write!(f, "Invalid account ID `{}`", account_id)
            }
            ActionsValidationError::ContractSizeExceeded { size, limit } => write!(
                f,
                "The length of the contract size {} exceeds the maximum allowed size {} in a DeployContract action",
                size, limit
            ),
            ActionsValidationError::FunctionCallMethodNameLengthExceeded { length, limit } => {
                write!(
                    f,
                    "The length of the method name {} exceeds the maximum allowed length {} in a FunctionCall action",
                    length, limit
                )
            }
            ActionsValidationError::FunctionCallArgumentsLengthExceeded { length, limit } => {
                write!(
                    f,
                    "The length of the arguments {} exceeds the maximum allowed length {} in a FunctionCall action",
                    length, limit
                )
            }
            ActionsValidationError::UnsuitableStakingKey { public_key } => write!(
                f,
                "The staking key must be ristretto compatible ED25519 key. {} is provided instead.",
                public_key,
            ),
            ActionsValidationError::FunctionCallZeroAttachedGas => write!(
                f,
                "The attached amount of gas in a FunctionCall action has to be a positive number",
            ),
            ActionsValidationError::DelegateActionMustBeOnlyOne => {
                write!(f, "The actions can contain the ony one DelegateAction")
            }
            ActionsValidationError::UnsupportedProtocolFeature { protocol_feature, version } => {
                write!(
                    f,
                    "Transaction requires protocol feature {} / version {} which is not supported by the current protocol version",
                    protocol_feature, version,
                )
            }
        }
    }
}

impl std::error::Error for ActionsValidationError {}

/// An error happened during Action execution
#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct ActionError {
    /// Index of the failed action in the transaction.
    /// Action index is not defined if ActionError.kind is `ActionErrorKind::LackBalanceForState`
    pub index: Option<u64>,
    /// The kind of ActionError happened
    pub kind: ActionErrorKind,
}

impl std::error::Error for ActionError {}

#[derive(
    BorshSerialize,
    BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum ActionErrorKind {
    /// Happens when CreateAccount action tries to create an account with account_id which is already exists in the storage
    AccountAlreadyExists {
        account_id: AccountId,
    } = 0,
    /// Happens when TX receiver_id doesn't exist (but action is not Action::CreateAccount)
    AccountDoesNotExist {
        account_id: AccountId,
    } = 1,
    /// A top-level account ID can only be created by registrar.
    CreateAccountOnlyByRegistrar {
        account_id: AccountId,
        registrar_account_id: AccountId,
        predecessor_id: AccountId,
    } = 2,

    /// A newly created account must be under a namespace of the creator account
    CreateAccountNotAllowed {
        account_id: AccountId,
        predecessor_id: AccountId,
    } = 3,
    /// Administrative actions like `DeployContract`, `Stake`, `AddKey`, `DeleteKey`. can be proceed only if sender=receiver
    /// or the first TX action is a `CreateAccount` action
    ActorNoPermission {
        account_id: AccountId,
        actor_id: AccountId,
    } = 4,
    /// Account tries to remove an access key that doesn't exist
    DeleteKeyDoesNotExist {
        account_id: AccountId,
        public_key: Box<PublicKey>,
    } = 5,
    /// The public key is already used for an existing access key
    AddKeyAlreadyExists {
        account_id: AccountId,
        public_key: Box<PublicKey>,
    } = 6,
    /// Account is staking and can not be deleted
    DeleteAccountStaking {
        account_id: AccountId,
    } = 7,
    /// ActionReceipt can't be completed, because the remaining balance will not be enough to cover storage.
    LackBalanceForState {
        /// An account which needs balance
        account_id: AccountId,
        /// Balance required to complete an action.
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        amount: Balance,
    } = 8,
    /// Account is not yet staked, but tries to unstake
    TriesToUnstake {
        account_id: AccountId,
    } = 9,
    /// The account doesn't have enough balance to increase the stake.
    TriesToStake {
        account_id: AccountId,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        stake: Balance,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        locked: Balance,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        balance: Balance,
    } = 10,
    InsufficientStake {
        account_id: AccountId,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        stake: Balance,
        #[serde(with = "dec_format")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        minimum_stake: Balance,
    } = 11,
    /// An error occurred during a `FunctionCall` Action, parameter is debug message.
    FunctionCallError(FunctionCallError) = 12,
    /// Error occurs when a new `ActionReceipt` created by the `FunctionCall` action fails
    /// receipt validation.
    NewReceiptValidationError(ReceiptValidationError) = 13,
    /// Error occurs when a `CreateAccount` action is called on a NEAR-implicit or ETH-implicit account.
    /// See NEAR-implicit account creation NEP: <https://github.com/nearprotocol/NEPs/pull/71>.
    /// Also, see ETH-implicit account creation NEP: <https://github.com/near/NEPs/issues/518>.
    ///
    /// TODO(#8598): This error is named very poorly. A better name would be
    /// `OnlyNamedAccountCreationAllowed`.
    OnlyImplicitAccountCreationAllowed {
        account_id: AccountId,
    } = 14,
    /// Delete account whose state is large is temporarily banned.
    DeleteAccountWithLargeState {
        account_id: AccountId,
    } = 15,
    /// Signature does not match the provided actions and given signer public key.
    DelegateActionInvalidSignature = 16,
    /// Receiver of the transaction doesn't match Sender of the delegate action
    DelegateActionSenderDoesNotMatchTxReceiver {
        sender_id: AccountId,
        receiver_id: AccountId,
    } = 17,
    /// Delegate action has expired. `max_block_height` is less than actual block height.
    DelegateActionExpired = 18,
    /// The given public key doesn't exist for Sender account
    DelegateActionAccessKeyError(InvalidAccessKeyError) = 19,
    /// DelegateAction nonce must be greater sender[public_key].nonce
    DelegateActionInvalidNonce {
        delegate_nonce: Nonce,
        ak_nonce: Nonce,
    } = 20,
    /// DelegateAction nonce is larger than the upper bound given by the block height
    DelegateActionNonceTooLarge {
        delegate_nonce: Nonce,
        upper_bound: Nonce,
    } = 21,
    GlobalContractDoesNotExist {
        identifier: GlobalContractIdentifier,
    } = 22,
}

impl From<ActionErrorKind> for ActionError {
    fn from(e: ActionErrorKind) -> ActionError {
        ActionError { index: None, kind: e }
    }
}

impl Display for InvalidTxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            InvalidTxError::InvalidSignerId { signer_id } => {
                write!(f, "Invalid signer account ID {:?} according to requirements", signer_id)
            }
            InvalidTxError::SignerDoesNotExist { signer_id } => {
                write!(f, "Signer {:?} does not exist", signer_id)
            }
            InvalidTxError::InvalidAccessKeyError(access_key_error) => {
                Display::fmt(&access_key_error, f)
            }
            InvalidTxError::InvalidNonce { tx_nonce, ak_nonce } => write!(
                f,
                "Transaction nonce {} must be larger than nonce of the used access key {}",
                tx_nonce, ak_nonce
            ),
            InvalidTxError::InvalidReceiverId { receiver_id } => {
                write!(f, "Invalid receiver account ID {:?} according to requirements", receiver_id)
            }
            InvalidTxError::InvalidSignature => {
                write!(f, "Transaction is not signed with the given public key")
            }
            InvalidTxError::NotEnoughBalance { signer_id, balance, cost } => write!(
                f,
                "Sender {:?} does not have enough balance {} for operation costing {}",
                signer_id, balance, cost
            ),
            InvalidTxError::LackBalanceForState { signer_id, amount } => {
                write!(
                    f,
                    "Failed to execute, because the account {:?} wouldn't have enough balance to cover storage, required to have {} yoctoNEAR more",
                    signer_id, amount
                )
            }
            InvalidTxError::CostOverflow => {
                write!(f, "Transaction gas or balance cost is too high")
            }
            InvalidTxError::InvalidChain => {
                write!(f, "Transaction parent block hash doesn't belong to the current chain")
            }
            InvalidTxError::Expired => {
                write!(f, "Transaction has expired")
            }
            InvalidTxError::ActionsValidation(error) => {
                write!(f, "Transaction actions validation error: {}", error)
            }
            InvalidTxError::NonceTooLarge { tx_nonce, upper_bound } => {
                write!(
                    f,
                    "Transaction nonce {} must be smaller than the access key nonce upper bound {}",
                    tx_nonce, upper_bound
                )
            }
            InvalidTxError::TransactionSizeExceeded { size, limit } => {
                write!(f, "Size of serialized transaction {} exceeded the limit {}", size, limit)
            }
            InvalidTxError::InvalidTransactionVersion => {
                write!(f, "Transaction version is invalid")
            }
            InvalidTxError::StorageError(error) => {
                write!(f, "Storage error: {}", error)
            }
            InvalidTxError::ShardCongested { shard_id, congestion_level } => {
                write!(
                    f,
                    "Shard {shard_id} is currently at congestion level {congestion_level:.3} and rejects new transactions."
                )
            }
            InvalidTxError::ShardStuck { shard_id, missed_chunks } => {
                write!(
                    f,
                    "Shard {shard_id} missed {missed_chunks} chunks and rejects new transactions."
                )
            }
        }
    }
}

impl From<InvalidAccessKeyError> for InvalidTxError {
    fn from(error: InvalidAccessKeyError) -> Self {
        InvalidTxError::InvalidAccessKeyError(error)
    }
}

impl Display for InvalidAccessKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            InvalidAccessKeyError::AccessKeyNotFound { account_id, public_key } => write!(
                f,
                "Signer {:?} doesn't have access key with the given public_key {}",
                account_id, public_key
            ),
            InvalidAccessKeyError::ReceiverMismatch { tx_receiver, ak_receiver } => write!(
                f,
                "Transaction receiver_id {:?} doesn't match the access key receiver_id {:?}",
                tx_receiver, ak_receiver
            ),
            InvalidAccessKeyError::MethodNameMismatch { method_name } => write!(
                f,
                "Transaction method name {:?} isn't allowed by the access key",
                method_name
            ),
            InvalidAccessKeyError::RequiresFullAccess => {
                write!(
                    f,
                    "Invalid access key type. Full-access keys are required for transactions that have multiple or non-function-call actions"
                )
            }
            InvalidAccessKeyError::NotEnoughAllowance {
                account_id,
                public_key,
                allowance,
                cost,
            } => write!(
                f,
                "Access Key {:?}:{} does not have enough balance {} for transaction costing {}",
                account_id, public_key, allowance, cost
            ),
            InvalidAccessKeyError::DepositWithFunctionCall => {
                write!(
                    f,
                    "Having a deposit with a function call action is not allowed with a function call access key."
                )
            }
        }
    }
}

impl std::error::Error for InvalidAccessKeyError {}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, Eq, ProtocolSchema)]
pub struct IntegerOverflowError;

impl std::fmt::Display for IntegerOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for IntegerOverflowError {}

impl From<IntegerOverflowError> for InvalidTxError {
    fn from(_: IntegerOverflowError) -> Self {
        InvalidTxError::CostOverflow
    }
}

impl From<IntegerOverflowError> for RuntimeError {
    fn from(err: IntegerOverflowError) -> Self {
        RuntimeError::UnexpectedIntegerOverflow(err.to_string())
    }
}

impl From<StorageError> for RuntimeError {
    fn from(e: StorageError) -> Self {
        RuntimeError::StorageError(e)
    }
}

impl From<InvalidTxError> for RuntimeError {
    fn from(e: InvalidTxError) -> Self {
        RuntimeError::InvalidTxError(e)
    }
}

impl From<EpochError> for RuntimeError {
    fn from(e: EpochError) -> Self {
        RuntimeError::ValidatorError(e)
    }
}

impl Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "Action #{}: {}", self.index.unwrap_or_default(), self.kind)
    }
}

impl Display for ActionErrorKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            ActionErrorKind::AccountAlreadyExists { account_id } => {
                write!(f, "Can't create a new account {:?}, because it already exists", account_id)
            }
            ActionErrorKind::AccountDoesNotExist { account_id } => write!(
                f,
                "Can't complete the action because account {:?} doesn't exist",
                account_id
            ),
            ActionErrorKind::ActorNoPermission { actor_id, account_id } => write!(
                f,
                "Actor {:?} doesn't have permission to account {:?} to complete the action",
                actor_id, account_id
            ),
            ActionErrorKind::LackBalanceForState { account_id, amount } => write!(
                f,
                "The account {} wouldn't have enough balance to cover storage, required to have {} yoctoNEAR more",
                account_id, amount
            ),
            ActionErrorKind::TriesToUnstake { account_id } => {
                write!(f, "Account {:?} is not yet staked, but tries to unstake", account_id)
            }
            ActionErrorKind::TriesToStake { account_id, stake, locked, balance } => write!(
                f,
                "Account {:?} tries to stake {}, but has staked {} and only has {}",
                account_id, stake, locked, balance
            ),
            ActionErrorKind::CreateAccountOnlyByRegistrar {
                account_id,
                registrar_account_id,
                predecessor_id,
            } => write!(
                f,
                "A top-level account ID {:?} can't be created by {:?}, short top-level account IDs can only be created by {:?}",
                account_id, predecessor_id, registrar_account_id,
            ),
            ActionErrorKind::CreateAccountNotAllowed { account_id, predecessor_id } => write!(
                f,
                "A sub-account ID {:?} can't be created by account {:?}",
                account_id, predecessor_id,
            ),
            ActionErrorKind::DeleteKeyDoesNotExist { account_id, .. } => write!(
                f,
                "Account {:?} tries to remove an access key that doesn't exist",
                account_id
            ),
            ActionErrorKind::AddKeyAlreadyExists { public_key, .. } => write!(
                f,
                "The public key {:?} is already used for an existing access key",
                public_key
            ),
            ActionErrorKind::DeleteAccountStaking { account_id } => {
                write!(f, "Account {:?} is staking and can not be deleted", account_id)
            }
            ActionErrorKind::FunctionCallError(s) => write!(f, "{:?}", s),
            ActionErrorKind::NewReceiptValidationError(e) => {
                write!(f, "An new action receipt created during a FunctionCall is not valid: {}", e)
            }
            ActionErrorKind::InsufficientStake { account_id, stake, minimum_stake } => write!(
                f,
                "Account {} tries to stake {} but minimum required stake is {}",
                account_id, stake, minimum_stake
            ),
            ActionErrorKind::OnlyImplicitAccountCreationAllowed { account_id } => write!(
                f,
                "CreateAccount action is called on hex-characters account of length 64 {}",
                account_id
            ),
            ActionErrorKind::DeleteAccountWithLargeState { account_id } => write!(
                f,
                "The state of account {} is too large and therefore cannot be deleted",
                account_id
            ),
            ActionErrorKind::DelegateActionInvalidSignature => {
                write!(f, "DelegateAction is not signed with the given public key")
            }
            ActionErrorKind::DelegateActionSenderDoesNotMatchTxReceiver {
                sender_id,
                receiver_id,
            } => write!(
                f,
                "Transaction receiver {} doesn't match DelegateAction sender {}",
                receiver_id, sender_id
            ),
            ActionErrorKind::DelegateActionExpired => write!(f, "DelegateAction has expired"),
            ActionErrorKind::DelegateActionAccessKeyError(access_key_error) => {
                Display::fmt(&access_key_error, f)
            }
            ActionErrorKind::DelegateActionInvalidNonce { delegate_nonce, ak_nonce } => write!(
                f,
                "DelegateAction nonce {} must be larger than nonce of the used access key {}",
                delegate_nonce, ak_nonce
            ),
            ActionErrorKind::DelegateActionNonceTooLarge { delegate_nonce, upper_bound } => write!(
                f,
                "DelegateAction nonce {} must be smaller than the access key nonce upper bound {}",
                delegate_nonce, upper_bound
            ),
            ActionErrorKind::GlobalContractDoesNotExist { identifier } => {
                write!(f, "Global contract identifier {:?} not found", identifier)
            }
        }
    }
}

#[derive(Eq, PartialEq, Clone)]
pub enum EpochError {
    /// Error calculating threshold from given stakes for given number of seats.
    /// Only should happened if calling code doesn't check for integer value of stake > number of seats.
    ThresholdError {
        stake_sum: Balance,
        num_seats: u64,
    },
    /// Requesting validators for an epoch that wasn't computed yet.
    EpochOutOfBounds(EpochId),
    /// Missing block hash in the storage (means there is some structural issue).
    MissingBlock(CryptoHash),
    /// Error due to IO (DB read/write, serialization, etc.).
    IOErr(String),
    /// Given account ID is not a validator in the given epoch ID.
    NotAValidator(AccountId, EpochId),
    /// Error getting information for a shard
    ShardingError(String),
    NotEnoughValidators {
        num_validators: u64,
        num_shards: u64,
    },
    /// Error selecting validators for a chunk.
    ChunkValidatorSelectionError(String),
    /// Error selecting chunk producer for a shard.
    ChunkProducerSelectionError(String),
}

impl std::error::Error for EpochError {}

impl Display for EpochError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EpochError::ThresholdError { stake_sum, num_seats } => write!(
                f,
                "Total stake {} must be higher than the number of seats {}",
                stake_sum, num_seats
            ),
            EpochError::EpochOutOfBounds(epoch_id) => {
                write!(f, "Epoch {:?} is out of bounds", epoch_id)
            }
            EpochError::MissingBlock(hash) => write!(f, "Missing block {}", hash),
            EpochError::IOErr(err) => write!(f, "IO: {}", err),
            EpochError::NotAValidator(account_id, epoch_id) => {
                write!(f, "{} is not a validator in epoch {:?}", account_id, epoch_id)
            }
            EpochError::ShardingError(err) => write!(f, "Sharding Error: {}", err),
            EpochError::NotEnoughValidators { num_shards, num_validators } => {
                write!(
                    f,
                    "There were not enough validator proposals to fill all shards. num_proposals: {}, num_shards: {}",
                    num_validators, num_shards
                )
            }
            EpochError::ChunkValidatorSelectionError(err) => {
                write!(f, "Error selecting validators for a chunk: {}", err)
            }
            EpochError::ChunkProducerSelectionError(err) => {
                write!(f, "Error selecting chunk producer: {}", err)
            }
        }
    }
}

impl Debug for EpochError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EpochError::ThresholdError { stake_sum, num_seats } => {
                write!(f, "ThresholdError({}, {})", stake_sum, num_seats)
            }
            EpochError::EpochOutOfBounds(epoch_id) => write!(f, "EpochOutOfBounds({:?})", epoch_id),
            EpochError::MissingBlock(hash) => write!(f, "MissingBlock({})", hash),
            EpochError::IOErr(err) => write!(f, "IOErr({})", err),
            EpochError::NotAValidator(account_id, epoch_id) => {
                write!(f, "NotAValidator({}, {:?})", account_id, epoch_id)
            }
            EpochError::ShardingError(err) => write!(f, "ShardingError({})", err),
            EpochError::NotEnoughValidators { num_shards, num_validators } => {
                write!(f, "NotEnoughValidators({}, {})", num_validators, num_shards)
            }
            EpochError::ChunkValidatorSelectionError(err) => {
                write!(f, "ChunkValidatorSelectionError({})", err)
            }
            EpochError::ChunkProducerSelectionError(err) => {
                write!(f, "ChunkProducerSelectionError({})", err)
            }
        }
    }
}

impl From<std::io::Error> for EpochError {
    fn from(error: std::io::Error) -> Self {
        EpochError::IOErr(error.to_string())
    }
}

impl From<ShardLayoutError> for EpochError {
    fn from(error: ShardLayoutError) -> Self {
        EpochError::ShardingError(error.to_string())
    }
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    serde::Deserialize,
    serde::Serialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
/// Error that can occur while preparing or executing Wasm smart-contract.
pub enum PrepareError {
    /// Error happened while serializing the module.
    Serialization = 0,
    /// Error happened while deserializing the module.
    Deserialization = 1,
    /// Internal memory declaration has been found in the module.
    InternalMemoryDeclared = 2,
    /// Gas instrumentation failed.
    ///
    /// This most likely indicates the module isn't valid.
    GasInstrumentation = 3,
    /// Stack instrumentation failed.
    ///
    /// This  most likely indicates the module isn't valid.
    StackHeightInstrumentation = 4,
    /// Error happened during instantiation.
    ///
    /// This might indicate that `start` function trapped, or module isn't
    /// instantiable and/or un-linkable.
    Instantiate = 5,
    /// Error creating memory.
    Memory = 6,
    /// Contract contains too many functions.
    TooManyFunctions = 7,
    /// Contract contains too many locals.
    TooManyLocals = 8,
}

/// A kind of a trap happened during execution of a binary
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    serde::Deserialize,
    serde::Serialize,
    strum::IntoStaticStr,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum WasmTrap {
    /// An `unreachable` opcode was executed.
    Unreachable = 0,
    /// Call indirect incorrect signature trap.
    IncorrectCallIndirectSignature = 1,
    /// Memory out of bounds trap.
    MemoryOutOfBounds = 2,
    /// Call indirect out of bounds trap.
    CallIndirectOOB = 3,
    /// An arithmetic exception, e.g. divided by zero.
    IllegalArithmetic = 4,
    /// Misaligned atomic access trap.
    MisalignedAtomicAccess = 5,
    /// Indirect call to null.
    IndirectCallToNull = 6,
    /// Stack overflow.
    StackOverflow = 7,
    /// Generic trap.
    GenericTrap = 8,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    serde::Deserialize,
    serde::Serialize,
    strum::IntoStaticStr,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum HostError {
    /// String encoding is bad UTF-16 sequence
    BadUTF16 = 0,
    /// String encoding is bad UTF-8 sequence
    BadUTF8 = 1,
    /// Exceeded the prepaid gas
    GasExceeded = 2,
    /// Exceeded the maximum amount of gas allowed to burn per contract
    GasLimitExceeded = 3,
    /// Exceeded the account balance
    BalanceExceeded = 4,
    /// Tried to call an empty method name
    EmptyMethodName = 5,
    /// Smart contract panicked
    GuestPanic { panic_msg: String } = 6,
    /// IntegerOverflow happened during a contract execution
    IntegerOverflow = 7,
    /// `promise_idx` does not correspond to existing promises
    InvalidPromiseIndex { promise_idx: u64 } = 8,
    /// Actions can only be appended to non-joint promise.
    CannotAppendActionToJointPromise = 9,
    /// Returning joint promise is currently prohibited
    CannotReturnJointPromise = 10,
    /// Accessed invalid promise result index
    InvalidPromiseResultIndex { result_idx: u64 } = 11,
    /// Accessed invalid register id
    InvalidRegisterId { register_id: u64 } = 12,
    /// Iterator `iterator_index` was invalidated after its creation by performing a mutable operation on trie
    IteratorWasInvalidated { iterator_index: u64 } = 13,
    /// Accessed memory outside the bounds
    MemoryAccessViolation = 14,
    /// VM Logic returned an invalid receipt index
    InvalidReceiptIndex { receipt_index: u64 } = 15,
    /// Iterator index `iterator_index` does not exist
    InvalidIteratorIndex { iterator_index: u64 } = 16,
    /// VM Logic returned an invalid account id
    InvalidAccountId = 17,
    /// VM Logic returned an invalid method name
    InvalidMethodName = 18,
    /// VM Logic provided an invalid public key
    InvalidPublicKey = 19,
    /// `method_name` is not allowed in view calls
    ProhibitedInView { method_name: String } = 20,
    /// The total number of logs will exceed the limit.
    NumberOfLogsExceeded { limit: u64 } = 21,
    /// The storage key length exceeded the limit.
    KeyLengthExceeded { length: u64, limit: u64 } = 22,
    /// The storage value length exceeded the limit.
    ValueLengthExceeded { length: u64, limit: u64 } = 23,
    /// The total log length exceeded the limit.
    TotalLogLengthExceeded { length: u64, limit: u64 } = 24,
    /// The maximum number of promises within a FunctionCall exceeded the limit.
    NumberPromisesExceeded { number_of_promises: u64, limit: u64 } = 25,
    /// The maximum number of input data dependencies exceeded the limit.
    NumberInputDataDependenciesExceeded { number_of_input_data_dependencies: u64, limit: u64 } = 26,
    /// The returned value length exceeded the limit.
    ReturnedValueLengthExceeded { length: u64, limit: u64 } = 27,
    /// The contract size for DeployContract action exceeded the limit.
    ContractSizeExceeded { size: u64, limit: u64 } = 28,
    /// The host function was deprecated.
    Deprecated { method_name: String } = 29,
    /// General errors for ECDSA recover.
    ECRecoverError { msg: String } = 30,
    /// Invalid input to alt_bn128 family of functions (e.g., point which isn't
    /// on the curve).
    AltBn128InvalidInput { msg: String } = 31,
    /// Invalid input to ed25519 signature verification function (e.g. signature cannot be
    /// derived from bytes).
    Ed25519VerifyInvalidInput { msg: String } = 32,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    serde::Deserialize,
    serde::Serialize,
    strum::IntoStaticStr,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum MethodResolveError {
    MethodEmptyName = 0,
    MethodNotFound = 1,
    MethodInvalidSignature = 2,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    serde::Deserialize,
    serde::Serialize,
    strum::IntoStaticStr,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum CompilationError {
    CodeDoesNotExist {
        account_id: AccountId,
    } = 0,
    PrepareError(PrepareError) = 1,
    /// This is for defense in depth.
    /// We expect our runtime-independent preparation code to fully catch all invalid wasms,
    /// but, if it ever misses something we’ll emit this error
    WasmerCompileError {
        msg: String,
    } = 2,
}

/// Serializable version of `near-vm-runner::FunctionCallError`.
///
/// Must never reorder/remove elements, can only add new variants at the end (but do that very
/// carefully). It describes stable serialization format, and only used by serialization logic.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshDeserialize,
    BorshSerialize,
    serde::Serialize,
    serde::Deserialize,
    ProtocolSchema,
)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum FunctionCallError {
    /// Wasm compilation error
    CompilationError(CompilationError) = 0,
    /// Wasm binary env link error
    ///
    /// Note: this is only to deserialize old data, use execution error for new data
    LinkError {
        msg: String,
    } = 1,
    /// Import/export resolve error
    MethodResolveError(MethodResolveError) = 2,
    /// A trap happened during execution of a binary
    ///
    /// Note: this is only to deserialize old data, use execution error for new data
    WasmTrap(WasmTrap) = 3,
    WasmUnknownError = 4,
    /// Note: this is only to deserialize old data, use execution error for new data
    HostError(HostError) = 5,
    // Unused, can be reused by a future error but must be exactly one error to keep ExecutionError
    // error borsh serialized at correct index
    _EVMError = 6,
    ExecutionError(String) = 7,
}

#[derive(Debug)]
pub enum ChunkAccessError {
    ChunkMissing(ChunkHash),
}

impl std::fmt::Display for ChunkAccessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!("{:?}", self))
    }
}

impl std::error::Error for ChunkAccessError {}

#[derive(Debug)]
pub enum InvalidSpiceCoreStatementsError {
    /// Information about uncertified chunks for previous block is missing.
    NoPrevUncertifiedChunks,
    /// Could not find validator for account_id from endorsement.
    NoValidatorForAccountId { index: usize, error: EpochError },
    /// Could not find shard_ids for endorsement epoch.
    NoShardIdsForEpochId { index: usize, error: EpochError },
    /// Spice core statement is invalid.
    InvalidCoreStatement { index: usize, reason: &'static str },
    /// Spice core statements skipped over execution result for chunk.
    SkippedExecutionResult { shard_id: ShardId, epoch_id: EpochId, height_created: BlockHeight },
    /// Could not find validator assignment for chunk.
    NoValidatorAssignments {
        shard_id: ShardId,
        epoch_id: EpochId,
        height_created: BlockHeight,
        error: EpochError,
    },
    /// Execution results for endorsed chunk are missing from block.
    NoExecutionResultForEndorsedChunk {
        shard_id: ShardId,
        epoch_id: EpochId,
        height_created: BlockHeight,
    },
}

impl std::fmt::Display for InvalidSpiceCoreStatementsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        f.write_str(&format!(" {:?}", self))
    }
}

impl std::error::Error for InvalidSpiceCoreStatementsError {}
