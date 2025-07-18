use crate::account::{AccessKey, Account};
use crate::hash::{CryptoHash, hash};
use crate::receipt::{Receipt, ReceiptOrStateStoredReceipt, ReceivedData};
use crate::shard_layout::ShardLayout;
use crate::trie_key::trie_key_parsers::{
    parse_account_id_from_access_key_key, parse_account_id_from_account_key,
    parse_account_id_from_contract_code_key, parse_account_id_from_contract_data_key,
    parse_account_id_from_gas_key_key, parse_account_id_from_received_data_key,
    parse_data_id_from_received_data_key, parse_data_key_from_contract_data_key,
    parse_index_from_delayed_receipt_key, parse_nonce_index_from_gas_key_key,
    parse_public_key_from_access_key_key, parse_public_key_from_gas_key_key,
};
use crate::trie_key::{TrieKey, col};
use crate::types::{AccountId, StoreKey, StoreValue};
use borsh::BorshDeserialize;
use near_crypto::PublicKey;
use near_primitives_core::account::GasKey;
use near_primitives_core::types::{Nonce, NonceIndex, ShardId};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::fmt::{Display, Formatter};

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub struct DelayedReceipt {
    #[serde(skip)]
    pub index: Option<u64>,

    #[serde(flatten)]
    pub receipt: Box<Receipt>,
}

/// Record in the state storage.
#[serde_as]
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
pub enum StateRecord {
    /// Account information.
    Account { account_id: AccountId, account: Account },
    /// Data records inside the contract, encoded in base64.
    Data { account_id: AccountId, data_key: StoreKey, value: StoreValue },
    /// Contract code encoded in base64.
    Contract {
        account_id: AccountId,
        #[serde_as(as = "Base64")]
        #[cfg_attr(feature = "schemars", schemars(with = "String"))]
        code: Vec<u8>,
    },
    /// Access key associated with some account.
    AccessKey { account_id: AccountId, public_key: PublicKey, access_key: AccessKey },
    /// Postponed Action Receipt.
    PostponedReceipt(Box<Receipt>),
    /// Received data from DataReceipt encoded in base64 for the given account_id and data_id.
    ReceivedData {
        account_id: AccountId,
        data_id: CryptoHash,
        #[serde_as(as = "Option<Base64>")]
        #[cfg_attr(feature = "schemars", schemars(with = "Option<String>"))]
        data: Option<Vec<u8>>,
    },
    /// Delayed Receipt.
    /// The receipt was delayed because the shard was overwhelmed.
    DelayedReceipt(DelayedReceipt),
    /// Gas key for an account.
    GasKey { account_id: AccountId, public_key: PublicKey, gas_key: GasKey },
    /// Nonce for a gas key index.
    GasKeyNonce { account_id: AccountId, public_key: PublicKey, index: NonceIndex, nonce: Nonce },
}

impl StateRecord {
    /// NOTE: This function is not safe to be running during block production. It contains a lot
    /// of `unwrap` and should only be used during `state_dump`.
    /// Most `unwrap()` here are because the implementation of columns and data are internal and
    /// can't be influenced by external calls.
    pub fn from_raw_key_value(key: &[u8], value: Vec<u8>) -> Option<StateRecord> {
        Self::from_raw_key_value_impl(key, value).unwrap_or(None)
    }

    pub fn from_raw_key_value_impl(
        key: &[u8],
        value: Vec<u8>,
    ) -> Result<Option<StateRecord>, std::io::Error> {
        Ok(match key[0] {
            col::ACCOUNT => Some(StateRecord::Account {
                account_id: parse_account_id_from_account_key(key)?,
                account: Account::try_from_slice(&value)?,
            }),
            col::CONTRACT_DATA => {
                let account_id = parse_account_id_from_contract_data_key(key)?;
                let data_key = parse_data_key_from_contract_data_key(key, &account_id)?;
                Some(StateRecord::Data {
                    account_id,
                    data_key: data_key.to_vec().into(),
                    value: value.into(),
                })
            }
            col::CONTRACT_CODE => Some(StateRecord::Contract {
                account_id: parse_account_id_from_contract_code_key(key)?,
                code: value,
            }),
            col::ACCESS_KEY => {
                let access_key = AccessKey::try_from_slice(&value)?;
                let account_id = parse_account_id_from_access_key_key(key)?;
                let public_key = parse_public_key_from_access_key_key(key, &account_id)?;
                Some(StateRecord::AccessKey { account_id, public_key, access_key })
            }
            col::GAS_KEY => {
                let account_id = parse_account_id_from_gas_key_key(key)?;
                let public_key = parse_public_key_from_gas_key_key(key, &account_id)?;
                let index = parse_nonce_index_from_gas_key_key(key, &account_id, &public_key)?;
                if let Some(index) = index {
                    let nonce = u64::try_from_slice(&value)?;
                    Some(StateRecord::GasKeyNonce { account_id, public_key, index, nonce })
                } else {
                    let gas_key = GasKey::try_from_slice(&value)?;
                    Some(StateRecord::GasKey { account_id, public_key, gas_key })
                }
            }
            col::RECEIVED_DATA => {
                let data = ReceivedData::try_from_slice(&value)?.data;
                let account_id = parse_account_id_from_received_data_key(key)?;
                let data_id = parse_data_id_from_received_data_key(key, &account_id)?;
                Some(StateRecord::ReceivedData { account_id, data_id, data })
            }
            col::POSTPONED_RECEIPT_ID => None,
            col::PENDING_DATA_COUNT => None,
            col::POSTPONED_RECEIPT => {
                let receipt = Receipt::try_from_slice(&value)?;
                Some(StateRecord::PostponedReceipt(Box::new(receipt)))
            }
            col::DELAYED_RECEIPT_OR_INDICES
                if key.len() == TrieKey::DelayedReceiptIndices.len() =>
            {
                None
            }
            col::DELAYED_RECEIPT_OR_INDICES => {
                let receipt = ReceiptOrStateStoredReceipt::try_from_slice(&value)?.into_receipt();
                let index = Some(parse_index_from_delayed_receipt_key(key)?);
                Some(StateRecord::DelayedReceipt(DelayedReceipt {
                    index,
                    receipt: Box::new(receipt),
                }))
            }
            _ => {
                println!("key[0]: {} is unreachable", key[0]);
                None
            }
        })
    }

    pub fn get_type_string(&self) -> String {
        match self {
            StateRecord::Account { .. } => "Account",
            StateRecord::Data { .. } => "Data",
            StateRecord::Contract { .. } => "Contract",
            StateRecord::AccessKey { .. } => "AccessKey",
            StateRecord::GasKey { .. } => "GasKey",
            StateRecord::GasKeyNonce { .. } => "GasKeyNonce",
            StateRecord::PostponedReceipt { .. } => "PostponedReceipt",
            StateRecord::ReceivedData { .. } => "ReceivedData",
            StateRecord::DelayedReceipt { .. } => "DelayedReceipt",
        }
        .to_string()
    }
}

impl Display for StateRecord {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            StateRecord::Account { account_id, account } => {
                write!(f, "Account {:?}: {:?}", account_id, account)
            }
            StateRecord::Data { account_id, data_key, value } => write!(
                f,
                "Storage {:?},{:?}: {:?}",
                account_id,
                to_printable(data_key.as_ref()),
                to_printable(value.as_ref())
            ),
            StateRecord::Contract { account_id, code: _ } => {
                write!(f, "Code for {:?}: ...", account_id)
            }
            StateRecord::AccessKey { account_id, public_key, access_key } => {
                write!(f, "Access key {:?},{:?}: {:?}", account_id, public_key, access_key)
            }
            StateRecord::ReceivedData { account_id, data_id, data } => write!(
                f,
                "Received data {:?},{:?}: {:?}",
                account_id,
                data_id,
                data.as_ref().map(|v| to_printable(v))
            ),
            StateRecord::PostponedReceipt(receipt) => write!(f, "Postponed receipt {:?}", receipt),
            StateRecord::DelayedReceipt(receipt) => write!(f, "Delayed receipt {:?}", receipt),
            StateRecord::GasKey { account_id, public_key, gas_key } => {
                write!(f, "Gas key {:?},{:?}: {:?}", account_id, public_key, gas_key)
            }
            StateRecord::GasKeyNonce { account_id, public_key, index, nonce } => {
                write!(f, "Gas key nonce {:?},{:?}[{}]: {}", account_id, public_key, index, nonce)
            }
        }
    }
}

fn to_printable(blob: &[u8]) -> String {
    if blob.len() > 60 {
        format!("{} bytes, hash: {}", blob.len(), hash(blob))
    } else {
        let ugly = blob.iter().any(|&x| x < b' ');
        if ugly {
            return format!("0x{}", hex::encode(blob));
        }
        match String::from_utf8(blob.to_vec()) {
            Ok(v) => v,
            Err(_e) => format!("0x{}", hex::encode(blob)),
        }
    }
}

pub fn state_record_to_shard_id(state_record: &StateRecord, shard_layout: &ShardLayout) -> ShardId {
    match state_record {
        StateRecord::Account { account_id, .. }
        | StateRecord::AccessKey { account_id, .. }
        | StateRecord::GasKey { account_id, .. }
        | StateRecord::GasKeyNonce { account_id, .. }
        | StateRecord::Contract { account_id, .. }
        | StateRecord::ReceivedData { account_id, .. }
        | StateRecord::Data { account_id, .. } => shard_layout.account_id_to_shard_id(account_id),
        StateRecord::PostponedReceipt(receipt) => receipt.receiver_shard_id(shard_layout).unwrap(),
        StateRecord::DelayedReceipt(receipt) => {
            receipt.receipt.receiver_shard_id(shard_layout).unwrap()
        }
    }
}

pub fn state_record_to_account_id(state_record: &StateRecord) -> &AccountId {
    match state_record {
        StateRecord::Account { account_id, .. }
        | StateRecord::AccessKey { account_id, .. }
        | StateRecord::GasKey { account_id, .. }
        | StateRecord::GasKeyNonce { account_id, .. }
        | StateRecord::Contract { account_id, .. }
        | StateRecord::ReceivedData { account_id, .. }
        | StateRecord::Data { account_id, .. } => account_id,
        StateRecord::PostponedReceipt(receipt) => receipt.receiver_id(),
        StateRecord::DelayedReceipt(receipt) => receipt.receipt.receiver_id(),
    }
}

pub fn is_contract_code_key(key: &[u8]) -> bool {
    debug_assert!(!key.is_empty());
    key[0] == col::CONTRACT_CODE
}
