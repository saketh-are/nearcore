use std::sync::Arc;

use futures::{Future, TryFutureExt};
use near_client::StatusResponse;
use near_crypto::{PublicKey, Signer};
use near_jsonrpc::client::{JsonRpcClient, new_client};
use near_jsonrpc_client_internal::ChunkId;
use near_jsonrpc_primitives::errors::ServerError;
use near_jsonrpc_primitives::types::query::{QueryResponseKind, RpcQueryRequest, RpcQueryResponse};
use near_jsonrpc_primitives::types::transactions::{RpcTransactionStatusRequest, TransactionInfo};
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::Receipt;
use near_primitives::serialize::to_base64;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{
    AccountId, BlockHeight, BlockId, BlockReference, EpochReference, ShardId,
};
use near_primitives::views::{
    AccessKeyView, AccountView, BlockView, CallResult, ChunkView, ContractCodeView,
    EpochValidatorInfo, ExecutionOutcomeView, FinalExecutionOutcomeView, QueryRequest,
    TxExecutionStatus, ViewStateResult,
};

use crate::user::User;

use super::CommitError;

pub struct RpcUser {
    account_id: AccountId,
    signer: Arc<Signer>,
    addr: String,
}

impl RpcUser {
    fn actix<F, Fut, R>(&self, f: F) -> R
    where
        Fut: Future<Output = R> + 'static,
        F: FnOnce(JsonRpcClient) -> Fut + 'static,
    {
        let addr = self.addr.clone();
        actix::System::new()
            .block_on(async move { f(new_client(&format!("http://{}", addr))).await })
    }

    pub fn new(addr: &str, account_id: AccountId, signer: Arc<Signer>) -> RpcUser {
        RpcUser { account_id, addr: addr.to_owned(), signer }
    }

    pub fn get_status(&self) -> Option<StatusResponse> {
        self.actix(|client| client.status()).ok()
    }

    pub fn query(&self, request: QueryRequest) -> Result<RpcQueryResponse, String> {
        let request = RpcQueryRequest { request, block_reference: BlockReference::latest() };
        self.actix(move |client| client.query(request).map_err(|err| err.to_string()))
    }

    pub fn validators(
        &self,
        epoch_id_or_block_id: Option<EpochReference>,
    ) -> Result<EpochValidatorInfo, String> {
        self.actix(move |client| {
            client.validators(epoch_id_or_block_id).map_err(|err| err.to_string())
        })
    }
}

impl User for RpcUser {
    fn view_account(&self, account_id: &AccountId) -> Result<AccountView, String> {
        let query = QueryRequest::ViewAccount { account_id: account_id.clone() };
        match self.query(query)?.kind {
            QueryResponseKind::ViewAccount(account_view) => Ok(account_view),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn view_state(&self, account_id: &AccountId, prefix: &[u8]) -> Result<ViewStateResult, String> {
        let query = QueryRequest::ViewState {
            account_id: account_id.clone(),
            prefix: prefix.to_vec().into(),
            include_proof: false,
        };
        match self.query(query)?.kind {
            QueryResponseKind::ViewState(view_state_result) => Ok(view_state_result),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn is_locked(&self, account_id: &AccountId) -> Result<bool, String> {
        let query = QueryRequest::ViewAccessKeyList { account_id: account_id.clone() };
        match self.query(query)?.kind {
            QueryResponseKind::AccessKeyList(access_keys) => Ok(access_keys.keys.is_empty()),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn view_contract_code(&self, account_id: &AccountId) -> Result<ContractCodeView, String> {
        let query = QueryRequest::ViewCode { account_id: account_id.clone() };
        match self.query(query)?.kind {
            QueryResponseKind::ViewCode(contract_code_view) => Ok(contract_code_view),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn view_call(
        &self,
        account_id: &AccountId,
        method_name: &str,
        args: &[u8],
    ) -> Result<CallResult, String> {
        let query = QueryRequest::CallFunction {
            account_id: account_id.clone(),
            method_name: method_name.to_string(),
            args: args.to_vec().into(),
        };
        match self.query(query)?.kind {
            QueryResponseKind::CallResult(call_result) => Ok(call_result),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn commit_all_transactions(
        &self,
        signed_transactions: Vec<SignedTransaction>,
    ) -> Result<Vec<Result<FinalExecutionOutcomeView, CommitError>>, ServerError> {
        if let Some((last, signed_transactions)) = signed_transactions.split_last() {
            for tx in signed_transactions {
                self.add_transaction(tx.clone())?;
            }
            // This makes tests finish faster in case the the last transaction is invalid.
            let last_result = self.commit_transaction(last.clone());
            let mut result = signed_transactions
                .into_iter()
                .map(|t| {
                    let e = super::CommitError::OutcomeNotFound;
                    self.get_transaction_final_result(&t.get_hash()).ok_or(e)
                })
                .collect::<Vec<_>>();
            result.push(last_result);
            Ok(result)
        } else {
            Ok(vec![])
        }
    }

    fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), ServerError> {
        let bytes = borsh::to_vec(&transaction).unwrap();
        let _ = self.actix(move |client| client.broadcast_tx_async(to_base64(&bytes))).map_err(
            |err| {
                serde_json::from_value::<ServerError>(
                    *err.data.expect("server error must carry data"),
                )
                .expect("deserialize server error must be ok")
            },
        )?;
        Ok(())
    }

    fn commit_transaction(
        &self,
        transaction: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, CommitError> {
        let result =
            self.actix(move |client| client.send_tx(transaction, TxExecutionStatus::Final));
        match result {
            Ok(outcome) => Ok(outcome.final_execution_outcome.unwrap().into_outcome()),
            Err(err) => Err(CommitError::Server(
                serde_json::from_value::<ServerError>(*err.data.unwrap()).unwrap(),
            )),
        }
    }

    fn add_receipts(
        &self,
        _receipts: Vec<Receipt>,
        _use_flat_storage: bool,
    ) -> Result<(), ServerError> {
        // TODO: figure out if rpc will support this
        unimplemented!()
    }

    fn get_best_height(&self) -> Option<BlockHeight> {
        self.get_status().map(|status| status.sync_info.latest_block_height)
    }

    fn get_best_block_hash(&self) -> Option<CryptoHash> {
        self.get_status().map(|status| status.sync_info.latest_block_hash)
    }

    fn get_block_by_height(&self, height: BlockHeight) -> Option<BlockView> {
        self.actix(move |client| client.block(BlockReference::BlockId(BlockId::Height(height))))
            .ok()
    }

    fn get_block(&self, block_hash: CryptoHash) -> Option<BlockView> {
        self.actix(move |client| client.block(BlockReference::BlockId(BlockId::Hash(block_hash))))
            .ok()
    }

    fn get_chunk_by_height(&self, height: BlockHeight, shard_id: ShardId) -> Option<ChunkView> {
        self.actix(move |client| {
            client.chunk(ChunkId::BlockShardId(BlockId::Height(height), shard_id))
        })
        .ok()
    }

    fn get_transaction_result(&self, _hash: &CryptoHash) -> Option<ExecutionOutcomeView> {
        unimplemented!()
    }

    fn get_transaction_final_result(&self, hash: &CryptoHash) -> Option<FinalExecutionOutcomeView> {
        let request = RpcTransactionStatusRequest {
            transaction_info: TransactionInfo::TransactionId {
                tx_hash: *hash,
                sender_account_id: self.account_id.clone(),
            },
            wait_until: TxExecutionStatus::Final,
        };
        self.actix(move |client| client.tx(request))
            .unwrap()
            .final_execution_outcome
            .map(|o| o.into_outcome())
    }

    fn get_state_root(&self) -> CryptoHash {
        self.get_status().map(|status| status.sync_info.latest_state_root).unwrap()
    }

    fn get_access_key(
        &self,
        account_id: &AccountId,
        public_key: &PublicKey,
    ) -> Result<AccessKeyView, String> {
        let query = QueryRequest::ViewAccessKey {
            account_id: account_id.clone(),
            public_key: public_key.clone(),
        };
        match self.query(query)?.kind {
            QueryResponseKind::AccessKey(access_key) => Ok(access_key),
            _ => Err("Invalid type of response".into()),
        }
    }

    fn signer(&self) -> Arc<Signer> {
        self.signer.clone()
    }

    fn set_signer(&mut self, signer: Arc<Signer>) {
        self.signer = signer;
    }
}
