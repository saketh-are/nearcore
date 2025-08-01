use crate::ApplyState;
use crate::actions::execute_function_call;
use crate::ext::RuntimeExt;
use crate::pipelining::ReceiptPreparationPipeline;
use crate::receipt_manager::ReceiptManager;
use near_crypto::{KeyType, PublicKey};
use near_parameters::RuntimeConfigStore;
use near_primitives::account::{AccessKey, Account};
use near_primitives::action::GlobalContractIdentifier;
use near_primitives::apply::ApplyChunkReason;
use near_primitives::bandwidth_scheduler::BlockBandwidthRequests;
use near_primitives::borsh::BorshDeserialize;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum, ReceiptV1};
use near_primitives::transaction::FunctionCallAction;
use near_primitives::trie_key::trie_key_parsers;
use near_primitives::types::{
    AccountId, BlockHeight, EpochHeight, EpochId, EpochInfoProvider, Gas, ShardId,
};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::{StateItem, ViewStateResult};
use near_primitives_core::config::ViewConfig;
use near_store::{TrieUpdate, get_access_key, get_account};
use near_vm_runner::logic::{ProtocolVersion, ReturnData};
use near_vm_runner::{ContractCode, ContractRuntimeCache};
use std::{str, sync::Arc, time::Instant};
use tracing::debug;

pub mod errors;

/// State for the view call.
#[derive(Debug)]
pub struct ViewApplyState {
    /// Currently building block height.
    pub block_height: BlockHeight,
    /// Prev block hash
    pub prev_block_hash: CryptoHash,
    /// To which shard the applied chunk belongs.
    pub shard_id: ShardId,
    /// Current epoch id
    pub epoch_id: EpochId,
    /// Current epoch height
    pub epoch_height: EpochHeight,
    /// The current block timestamp (number of non-leap-nanoseconds since January 1, 1970 0:00:00 UTC).
    pub block_timestamp: u64,
    /// Current Protocol version when we apply the state transition
    pub current_protocol_version: ProtocolVersion,
    /// Cache for compiled contracts.
    pub cache: Option<Box<dyn ContractRuntimeCache>>,
}

pub struct TrieViewer {
    /// Upper bound of the byte size of contract state that is still viewable. None is no limit
    state_size_limit: Option<u64>,
    /// Gas limit used when handling call_function queries.
    max_gas_burnt_view: Gas,
}

impl Default for TrieViewer {
    fn default() -> Self {
        let config_store = RuntimeConfigStore::new(None);
        let latest_runtime_config = config_store.get_config(PROTOCOL_VERSION);
        let max_gas_burnt = latest_runtime_config.wasm_config.limit_config.max_gas_burnt;
        Self { state_size_limit: None, max_gas_burnt_view: max_gas_burnt }
    }
}

impl TrieViewer {
    pub fn new(state_size_limit: Option<u64>, max_gas_burnt_view: Option<Gas>) -> Self {
        let max_gas_burnt_view =
            max_gas_burnt_view.unwrap_or_else(|| TrieViewer::default().max_gas_burnt_view);
        Self { state_size_limit, max_gas_burnt_view }
    }

    pub fn view_account(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Account, errors::ViewAccountError> {
        get_account(state_update, account_id)?.ok_or_else(|| {
            errors::ViewAccountError::AccountDoesNotExist {
                requested_account_id: account_id.clone(),
            }
        })
    }

    pub fn view_account_contract_code(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<ContractCode, errors::ViewContractCodeError> {
        let account = self.view_account(state_update, account_id)?;
        state_update.get_account_contract_code(account_id, account.contract().as_ref())?.ok_or_else(
            || errors::ViewContractCodeError::NoContractCode {
                contract_account_id: account_id.clone(),
            },
        )
    }

    pub fn view_global_contract_code(
        &self,
        state_update: &TrieUpdate,
        identifier: GlobalContractIdentifier,
    ) -> Result<ContractCode, errors::ViewContractCodeError> {
        state_update
            .get_global_contract_code(identifier.clone().into())?
            .ok_or(errors::ViewContractCodeError::NoGlobalContractCode { identifier })
    }

    pub fn view_access_key(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKey, errors::ViewAccessKeyError> {
        get_access_key(state_update, account_id, public_key)?.ok_or_else(|| {
            errors::ViewAccessKeyError::AccessKeyDoesNotExist { public_key: public_key.clone() }
        })
    }

    pub fn view_access_keys(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
    ) -> Result<Vec<(PublicKey, AccessKey)>, errors::ViewAccessKeyError> {
        let prefix = trie_key_parsers::get_raw_prefix_for_access_keys(account_id);
        let raw_prefix: &[u8] = prefix.as_ref();
        let access_keys =
            state_update
                .iter(&prefix)?
                .map(|key| {
                    let key = key?;
                    let public_key = &key[raw_prefix.len()..];
                    let access_key = near_store::get_access_key_raw(state_update, &key)?
                        .ok_or_else(|| errors::ViewAccessKeyError::InternalError {
                            error_message: "Unexpected missing key from iterator".to_string(),
                        })?;
                    PublicKey::try_from_slice(public_key)
                        .map_err(|_| errors::ViewAccessKeyError::InternalError {
                            error_message: format!(
                                "Unexpected invalid public key {:?} received from store",
                                public_key
                            ),
                        })
                        .map(|key| (key, access_key))
                })
                .collect::<Result<Vec<_>, errors::ViewAccessKeyError>>();
        access_keys
    }

    pub fn view_state(
        &self,
        state_update: &TrieUpdate,
        account_id: &AccountId,
        prefix: &[u8],
        include_proof: bool,
    ) -> Result<ViewStateResult, errors::ViewStateError> {
        match get_account(state_update, account_id)? {
            Some(account) => {
                let code_len = state_update
                    .get_code_len(
                        account_id.clone(),
                        account.local_contract_hash().unwrap_or_default(),
                    )?
                    .unwrap_or_default() as u64;
                if let Some(limit) = self.state_size_limit {
                    if account.storage_usage().saturating_sub(code_len) > limit {
                        return Err(errors::ViewStateError::AccountStateTooLarge {
                            requested_account_id: account_id.clone(),
                        });
                    }
                }
            }
            None => {
                return Err(errors::ViewStateError::AccountDoesNotExist {
                    requested_account_id: account_id.clone(),
                });
            }
        };

        let mut values = vec![];
        let query = trie_key_parsers::get_raw_prefix_for_contract_data(account_id, prefix);
        let acc_sep_len = query.len() - prefix.len();
        let mut iter = state_update.trie().disk_iter()?;
        iter.remember_visited_nodes(include_proof);
        iter.seek_prefix(&query)?;
        for item in &mut iter {
            let (key, value) = item?;
            values.push(StateItem { key: key[acc_sep_len..].to_vec().into(), value: value.into() });
        }
        let proof = iter.into_visited_nodes();
        Ok(ViewStateResult { values, proof })
    }

    pub fn call_function(
        &self,
        mut state_update: TrieUpdate,
        view_state: ViewApplyState,
        contract_id: &AccountId,
        method_name: &str,
        args: &[u8],
        logs: &mut Vec<String>,
        epoch_info_provider: &dyn EpochInfoProvider,
    ) -> Result<Vec<u8>, errors::CallFunctionError> {
        let now = Instant::now();
        let root = *state_update.get_root();
        let account = get_account(&state_update, contract_id)?.ok_or_else(|| {
            errors::CallFunctionError::AccountDoesNotExist {
                requested_account_id: contract_id.clone(),
            }
        })?;
        // TODO(#1015): Add ability to pass public key and originator_id
        let originator_id = contract_id;
        let public_key = PublicKey::empty(KeyType::ED25519);
        let empty_hash = CryptoHash::default();
        let mut receipt_manager = ReceiptManager::default();
        let config_store = RuntimeConfigStore::new(None);
        let config = config_store.get_config(PROTOCOL_VERSION);
        let apply_state = ApplyState {
            apply_reason: ApplyChunkReason::ViewTrackedShard,
            block_height: view_state.block_height,
            // Used for legacy reasons
            prev_block_hash: view_state.prev_block_hash,
            shard_id: view_state.shard_id,
            epoch_id: view_state.epoch_id,
            epoch_height: view_state.epoch_height,
            gas_price: 0,
            block_timestamp: view_state.block_timestamp,
            gas_limit: None,
            random_seed: root,
            current_protocol_version: view_state.current_protocol_version,
            config: Arc::clone(config),
            cache: view_state.cache,
            is_new_chunk: false,
            congestion_info: Default::default(),
            bandwidth_requests: BlockBandwidthRequests::empty(),
            trie_access_tracker_state: Default::default(),
        };
        let function_call = FunctionCallAction {
            method_name: method_name.to_string(),
            args: args.to_vec(),
            gas: self.max_gas_burnt_view,
            deposit: 0,
        };
        let action_receipt = ActionReceipt {
            signer_id: originator_id.clone(),
            signer_public_key: public_key,
            gas_price: 0,
            output_data_receivers: vec![],
            input_data_ids: vec![],
            actions: vec![function_call.clone().into()],
        };
        let receipt = Receipt::V1(ReceiptV1 {
            predecessor_id: contract_id.clone(),
            receiver_id: contract_id.clone(),
            receipt_id: empty_hash,
            receipt: ReceiptEnum::Action(action_receipt.clone()),
            priority: 0,
        });
        let pipeline = ReceiptPreparationPipeline::new(
            Arc::clone(config),
            apply_state.cache.as_ref().map(|v| v.handle()),
            state_update.contract_storage(),
        );
        let view_config = Some(ViewConfig { max_gas_burnt: self.max_gas_burnt_view });
        let code_hash = state_update.get_account_contract_hash(account.contract().as_ref())?;
        let contract = pipeline.get_contract(&receipt, code_hash, 0, view_config.clone());

        let mut runtime_ext = RuntimeExt::new(
            &mut state_update,
            &mut receipt_manager,
            contract_id.clone(),
            account,
            empty_hash,
            view_state.epoch_id,
            view_state.block_height,
            epoch_info_provider,
            view_state.current_protocol_version,
            config.wasm_config.storage_get_mode,
            Arc::clone(&apply_state.trie_access_tracker_state),
        );
        let outcome = execute_function_call(
            contract,
            &apply_state,
            &mut runtime_ext,
            originator_id,
            &action_receipt,
            [].into(),
            &function_call,
            &empty_hash,
            config,
            true,
            view_config,
        )
        .map_err(|e| errors::CallFunctionError::InternalError { error_message: e.to_string() })?;
        let elapsed = now.elapsed();
        let time_ms =
            (elapsed.as_secs() as f64 / 1_000.0) + f64::from(elapsed.subsec_nanos()) / 1_000_000.0;
        let time_str = format!("{:.*}ms", 2, time_ms);

        if let Some(err) = outcome.aborted {
            logs.extend(outcome.logs);
            let message = format!("wasm execution failed with error: {:?}", err);
            debug!(target: "runtime", "(exec time {}) {}", time_str, message);
            Err(errors::CallFunctionError::VMError { error_message: message })
        } else {
            debug!(target: "runtime", "(exec time {}) result of execution: {:?}", time_str, outcome);
            logs.extend(outcome.logs);
            let result = match outcome.return_data {
                ReturnData::Value(buf) => buf,
                ReturnData::ReceiptIndex(_) | ReturnData::None => vec![],
            };
            Ok(result)
        }
    }
}
