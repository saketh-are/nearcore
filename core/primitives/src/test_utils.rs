use crate::account::{AccessKey, AccessKeyPermission, Account};
use crate::action::{
    DeployGlobalContractAction, GlobalContractDeployMode, GlobalContractIdentifier,
    UseGlobalContractAction,
};
use crate::block::Block;
use crate::block_body::{BlockBody, ChunkEndorsementSignatures};
use crate::block_header::BlockHeader;
use crate::errors::EpochError;
use crate::hash::CryptoHash;
use crate::shard_layout::ShardLayout;
use crate::sharding::{ShardChunkHeader, ShardChunkHeaderV3};
use crate::stateless_validation::chunk_endorsements_bitmap::ChunkEndorsementsBitmap;
use crate::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, SignedTransaction, StakeAction, Transaction,
    TransactionV0, TransactionV1, TransferAction,
};
use crate::types::validator_stake::ValidatorStake;
use crate::types::{AccountId, Balance, EpochId, EpochInfoProvider, Gas, Nonce};
use crate::validator_signer::ValidatorSigner;
use crate::views::{ExecutionStatusView, FinalExecutionOutcomeView, FinalExecutionStatus};
use near_crypto::vrf::Value;
use near_crypto::{EmptySigner, PublicKey, SecretKey, Signature, Signer};
use near_primitives_core::account::AccountContract;
use near_primitives_core::types::{BlockHeight, MerkleHash, ProtocolVersion};
use std::collections::HashMap;
#[cfg(feature = "clock")]
use std::sync::Arc;

pub fn account_new(amount: Balance, code_hash: CryptoHash) -> Account {
    Account::new(
        amount,
        0,
        AccountContract::from_local_code_hash(code_hash),
        std::mem::size_of::<Account>() as u64,
    )
}

impl Transaction {
    pub fn new_v0(
        signer_id: AccountId,
        public_key: PublicKey,
        receiver_id: AccountId,
        nonce: Nonce,
        block_hash: CryptoHash,
    ) -> Self {
        Transaction::V0(TransactionV0 {
            signer_id,
            public_key,
            nonce,
            receiver_id,
            block_hash,
            actions: vec![],
        })
    }

    pub fn new_v1(
        signer_id: AccountId,
        public_key: PublicKey,
        receiver_id: AccountId,
        nonce: Nonce,
        block_hash: CryptoHash,
        priority_fee: u64,
    ) -> Self {
        Transaction::V1(TransactionV1 {
            signer_id,
            public_key,
            nonce,
            receiver_id,
            block_hash,
            actions: vec![],
            priority_fee,
        })
    }

    pub fn actions_mut(&mut self) -> &mut Vec<Action> {
        match self {
            Transaction::V0(tx) => &mut tx.actions,
            Transaction::V1(tx) => &mut tx.actions,
        }
    }

    pub fn nonce_mut(&mut self) -> &mut Nonce {
        match self {
            Transaction::V0(tx) => &mut tx.nonce,
            Transaction::V1(tx) => &mut tx.nonce,
        }
    }

    pub fn sign(self, signer: &Signer) -> SignedTransaction {
        let signature = signer.sign(self.get_hash_and_size().0.as_ref());
        SignedTransaction::new(signature, self)
    }

    pub fn create_account(mut self) -> Self {
        self.actions_mut().push(Action::CreateAccount(CreateAccountAction {}));
        self
    }

    pub fn deploy_contract(mut self, code: Vec<u8>) -> Self {
        self.actions_mut().push(Action::DeployContract(DeployContractAction { code }));
        self
    }

    pub fn function_call(
        mut self,
        method_name: String,
        args: Vec<u8>,
        gas: Gas,
        deposit: Balance,
    ) -> Self {
        self.actions_mut().push(Action::FunctionCall(Box::new(FunctionCallAction {
            method_name,
            args,
            gas,
            deposit,
        })));
        self
    }

    pub fn transfer(mut self, deposit: Balance) -> Self {
        self.actions_mut().push(Action::Transfer(TransferAction { deposit }));
        self
    }

    pub fn stake(mut self, stake: Balance, public_key: PublicKey) -> Self {
        self.actions_mut().push(Action::Stake(Box::new(StakeAction { stake, public_key })));
        self
    }
    pub fn add_key(mut self, public_key: PublicKey, access_key: AccessKey) -> Self {
        self.actions_mut().push(Action::AddKey(Box::new(AddKeyAction { public_key, access_key })));
        self
    }

    pub fn delete_key(mut self, public_key: PublicKey) -> Self {
        self.actions_mut().push(Action::DeleteKey(Box::new(DeleteKeyAction { public_key })));
        self
    }

    pub fn delete_account(mut self, beneficiary_id: AccountId) -> Self {
        self.actions_mut().push(Action::DeleteAccount(DeleteAccountAction { beneficiary_id }));
        self
    }
}

/// This block implements a set of helper functions to create transactions for testing purposes.
impl SignedTransaction {
    /// Creates v0 for now because v1 is prohibited in the protocol.
    /// Once v1 is allowed, this function should be updated to create v1 transactions.
    pub fn from_actions(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &Signer,
        actions: Vec<Action>,
        block_hash: CryptoHash,
        _priority_fee: u64,
    ) -> Self {
        Transaction::V0(TransactionV0 {
            nonce,
            signer_id,
            public_key: signer.public_key(),
            receiver_id,
            block_hash,
            actions,
        })
        .sign(signer)
    }

    /// Explicitly create v1 transaction to test in cases where errors are expected.
    pub fn from_actions_v1(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &Signer,
        actions: Vec<Action>,
        block_hash: CryptoHash,
        priority_fee: u64,
    ) -> Self {
        Transaction::V1(TransactionV1 {
            nonce,
            signer_id,
            public_key: signer.public_key(),
            receiver_id,
            block_hash,
            actions,
            priority_fee,
        })
        .sign(signer)
    }

    pub fn send_money(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &Signer,
        deposit: Balance,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::Transfer(TransferAction { deposit })],
            block_hash,
            0,
        )
    }

    pub fn stake(
        nonce: Nonce,
        signer_id: AccountId,
        signer: &Signer,
        stake: Balance,
        public_key: PublicKey,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id.clone(),
            signer_id,
            signer,
            vec![Action::Stake(Box::new(StakeAction { stake, public_key }))],
            block_hash,
            0,
        )
    }

    pub fn create_account(
        nonce: Nonce,
        originator: AccountId,
        new_account_id: AccountId,
        amount: Balance,
        public_key: PublicKey,
        signer: &Signer,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            originator,
            new_account_id,
            signer,
            vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::AddKey(Box::new(AddKeyAction {
                    public_key,
                    access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
                })),
                Action::Transfer(TransferAction { deposit: amount }),
            ],
            block_hash,
            0,
        )
    }

    pub fn deploy_contract(
        nonce: Nonce,
        contract_id: &AccountId,
        code: Vec<u8>,
        signer: &Signer,
        block_hash: CryptoHash,
    ) -> SignedTransaction {
        let signer_id = contract_id.clone();
        let receiver_id = contract_id.clone();
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::DeployContract(DeployContractAction { code })],
            block_hash,
            0,
        )
    }

    pub fn deploy_global_contract(
        nonce: Nonce,
        account_id: AccountId,
        code: Vec<u8>,
        signer: &Signer,
        block_hash: CryptoHash,
        deploy_mode: GlobalContractDeployMode,
    ) -> SignedTransaction {
        let signer_id = account_id.clone();
        let receiver_id = account_id;
        SignedTransaction::from_actions(
            nonce,
            signer_id,
            receiver_id,
            &signer,
            vec![Action::DeployGlobalContract(DeployGlobalContractAction {
                code: code.into(),
                deploy_mode,
            })],
            block_hash,
            0,
        )
    }

    pub fn use_global_contract(
        nonce: Nonce,
        account_id: &AccountId,
        signer: &Signer,
        block_hash: CryptoHash,
        contract_identifier: GlobalContractIdentifier,
    ) -> SignedTransaction {
        let signer_id = account_id.clone();
        let receiver_id = account_id.clone();
        SignedTransaction::from_actions(
            nonce,
            signer_id,
            receiver_id,
            &signer,
            vec![Action::UseGlobalContract(Box::new(UseGlobalContractAction {
                contract_identifier,
            }))],
            block_hash,
            0,
        )
    }

    pub fn create_contract(
        nonce: Nonce,
        originator: AccountId,
        new_account_id: AccountId,
        code: Vec<u8>,
        amount: Balance,
        public_key: PublicKey,
        signer: &Signer,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            originator,
            new_account_id,
            signer,
            vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::AddKey(Box::new(AddKeyAction {
                    public_key,
                    access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
                })),
                Action::Transfer(TransferAction { deposit: amount }),
                Action::DeployContract(DeployContractAction { code }),
            ],
            block_hash,
            0,
        )
    }

    pub fn call(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        signer: &Signer,
        deposit: Balance,
        method_name: String,
        args: Vec<u8>,
        gas: Gas,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::FunctionCall(Box::new(FunctionCallAction {
                args,
                method_name,
                gas,
                deposit,
            }))],
            block_hash,
            0,
        )
    }

    pub fn delete_account(
        nonce: Nonce,
        signer_id: AccountId,
        receiver_id: AccountId,
        beneficiary_id: AccountId,
        signer: &Signer,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id,
            receiver_id,
            signer,
            vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })],
            block_hash,
            0,
        )
    }

    pub fn empty(block_hash: CryptoHash) -> Self {
        Self::from_actions(
            0,
            "test".parse().unwrap(),
            "test".parse().unwrap(),
            &EmptySigner::new().into(),
            vec![],
            block_hash,
            0,
        )
    }

    pub fn add_key(
        nonce: Nonce,
        signer_id: AccountId,
        signer: &Signer,
        public_key: PublicKey,
        access_key: AccessKey,
        block_hash: CryptoHash,
    ) -> Self {
        Self::from_actions(
            nonce,
            signer_id.clone(),
            signer_id,
            signer,
            vec![Action::AddKey(Box::new(AddKeyAction { public_key, access_key }))],
            block_hash,
            0,
        )
    }
}

impl BlockHeader {
    pub fn set_latest_protocol_version(&mut self, latest_protocol_version: ProtocolVersion) {
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                header.inner_rest.latest_protocol_version = latest_protocol_version;
            }
            BlockHeader::BlockHeaderV2(header) => {
                header.inner_rest.latest_protocol_version = latest_protocol_version;
            }
            BlockHeader::BlockHeaderV3(header) => {
                header.inner_rest.latest_protocol_version = latest_protocol_version;
            }
            BlockHeader::BlockHeaderV4(header) => {
                header.inner_rest.latest_protocol_version = latest_protocol_version;
            }
            BlockHeader::BlockHeaderV5(header) => {
                header.inner_rest.latest_protocol_version = latest_protocol_version;
            }
        }
    }

    pub fn resign(&mut self, signer: &ValidatorSigner) {
        let hash = BlockHeader::compute_hash(
            *self.prev_hash(),
            &self.inner_lite_bytes(),
            &self.inner_rest_bytes(),
        );
        let signature = signer.sign_bytes(hash.as_ref());
        match self {
            BlockHeader::BlockHeaderV1(header) => {
                header.hash = hash;
                header.signature = signature;
            }
            BlockHeader::BlockHeaderV2(header) => {
                header.hash = hash;
                header.signature = signature;
            }
            BlockHeader::BlockHeaderV3(header) => {
                header.hash = hash;
                header.signature = signature;
            }
            BlockHeader::BlockHeaderV4(header) => {
                header.hash = hash;
                header.signature = signature;
            }
            BlockHeader::BlockHeaderV5(header) => {
                header.hash = hash;
                header.signature = signature;
            }
        }
    }

    pub fn init(&mut self) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.init(),
            BlockHeader::BlockHeaderV5(header) => header.init(),
        }
    }

    pub fn set_prev_hash(&mut self, value: CryptoHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.prev_hash = value,
            BlockHeader::BlockHeaderV5(header) => header.prev_hash = value,
        }
    }

    pub fn set_height(&mut self, value: BlockHeight) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.height = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_lite.height = value,
        }
    }

    pub fn set_epoch_id(&mut self, value: EpochId) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.epoch_id = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_lite.epoch_id = value,
        }
    }

    pub fn set_prev_state_root(&mut self, value: MerkleHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.prev_state_root = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_lite.prev_state_root = value,
        }
    }

    pub fn set_prev_chunk_outgoing_receipts_root(&mut self, value: MerkleHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => {
                header.inner_rest.prev_chunk_outgoing_receipts_root = value
            }
            BlockHeader::BlockHeaderV5(header) => {
                header.inner_rest.prev_chunk_outgoing_receipts_root = value
            }
        }
    }

    pub fn set_chunk_headers_root(&mut self, value: MerkleHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.chunk_headers_root = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_rest.chunk_headers_root = value,
        }
    }

    pub fn set_chunk_tx_root(&mut self, value: MerkleHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.chunk_tx_root = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_rest.chunk_tx_root = value,
        }
    }

    pub fn set_chunk_mask(&mut self, value: Vec<bool>) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.chunk_mask = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_rest.chunk_mask = value,
        }
    }

    pub fn set_chunk_endorsements(&mut self, value: ChunkEndorsementsBitmap) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(_) => {
                // BlockHeaderV4 can appear in tests but setting chunk endorsements will be no-op.
            }
            BlockHeader::BlockHeaderV5(header) => header.inner_rest.chunk_endorsements = value,
        }
    }

    pub fn set_prev_outcome_root(&mut self, value: MerkleHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.prev_outcome_root = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_lite.prev_outcome_root = value,
        }
    }

    pub fn set_timestamp(&mut self, value: u64) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.timestamp = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_lite.timestamp = value,
        }
    }

    pub fn set_prev_validator_proposals(&mut self, value: Vec<ValidatorStake>) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => {
                header.inner_rest.prev_validator_proposals = value
            }
            BlockHeader::BlockHeaderV5(header) => {
                header.inner_rest.prev_validator_proposals = value
            }
        }
    }

    pub fn set_next_gas_price(&mut self, value: Balance) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.next_gas_price = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_rest.next_gas_price = value,
        }
    }

    pub fn set_block_merkle_root(&mut self, value: CryptoHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_lite.block_merkle_root = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_lite.block_merkle_root = value,
        }
    }

    pub fn set_approvals(&mut self, value: Vec<Option<Box<Signature>>>) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.approvals = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_rest.approvals = value,
        }
    }

    pub fn set_block_body_hash(&mut self, value: CryptoHash) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.inner_rest.block_body_hash = value,
            BlockHeader::BlockHeaderV5(header) => header.inner_rest.block_body_hash = value,
        }
    }

    pub fn set_signature(&mut self, value: Signature) {
        match self {
            BlockHeader::BlockHeaderV1(_)
            | BlockHeader::BlockHeaderV2(_)
            | BlockHeader::BlockHeaderV3(_) => {
                unreachable!("old header should not appear in tests")
            }
            BlockHeader::BlockHeaderV4(header) => header.signature = value,
            BlockHeader::BlockHeaderV5(header) => header.signature = value,
        }
    }
}

impl ShardChunkHeader {
    pub fn get_mut(&mut self) -> &mut ShardChunkHeaderV3 {
        match self {
            ShardChunkHeader::V1(_) | ShardChunkHeader::V2(_) => {
                unreachable!("old header should not appear in tests")
            }
            ShardChunkHeader::V3(chunk) => chunk,
        }
    }
}

impl BlockBody {
    fn mut_chunks(&mut self) -> &mut Vec<ShardChunkHeader> {
        match self {
            BlockBody::V1(body) => &mut body.chunks,
            BlockBody::V2(body) => &mut body.chunks,
            BlockBody::V3(body) => &mut body.chunks,
        }
    }

    fn set_chunks(&mut self, chunks: Vec<ShardChunkHeader>) {
        match self {
            BlockBody::V1(body) => body.chunks = chunks,
            BlockBody::V2(body) => body.chunks = chunks,
            BlockBody::V3(body) => body.chunks = chunks,
        }
    }

    fn set_vrf_value(&mut self, vrf_value: Value) {
        match self {
            BlockBody::V1(body) => body.vrf_value = vrf_value,
            BlockBody::V2(body) => body.vrf_value = vrf_value,
            BlockBody::V3(body) => body.vrf_value = vrf_value,
        }
    }

    fn set_chunk_endorsements(&mut self, chunk_endorsements: Vec<ChunkEndorsementSignatures>) {
        match self {
            BlockBody::V1(_) => unreachable!("old body should not appear in tests"),
            BlockBody::V2(body) => body.chunk_endorsements = chunk_endorsements,
            BlockBody::V3(_) => unreachable!("block body for spice should not appear in tests"),
        }
    }
}

/// Builder class for blocks to make testing easier.
/// # Examples
///
/// // TODO(mm-near): change it to doc-tested code once we have easy way to create a genesis block.
/// let signer = EmptyValidatorSigner::default().into();
/// let test_block = test_utils::TestBlockBuilder::new(prev, signer).height(33).build();
#[cfg(feature = "clock")]
pub struct TestBlockBuilder {
    clock: near_time::Clock,
    prev: Block,
    signer: Arc<ValidatorSigner>,
    height: u64,
    epoch_id: EpochId,
    next_epoch_id: EpochId,
    next_bp_hash: CryptoHash,
    approvals: Vec<Option<Box<near_crypto::Signature>>>,
    block_merkle_root: CryptoHash,
}

#[cfg(feature = "clock")]
impl TestBlockBuilder {
    pub fn new(clock: near_time::Clock, prev: &Block, signer: Arc<ValidatorSigner>) -> Self {
        let mut tree = crate::merkle::PartialMerkleTree::default();
        tree.insert(*prev.hash());
        let next_epoch_id = if prev.header().is_genesis() {
            EpochId(*prev.hash())
        } else {
            *prev.header().next_epoch_id()
        };
        Self {
            clock,
            prev: Block::clone(prev),
            signer,
            height: prev.header().height() + 1,
            epoch_id: *prev.header().epoch_id(),
            next_epoch_id,
            next_bp_hash: *prev.header().next_bp_hash(),
            approvals: vec![],
            block_merkle_root: tree.root(),
        }
    }
    pub fn height(mut self, height: u64) -> Self {
        self.height = height;
        self
    }
    pub fn epoch_id(mut self, epoch_id: EpochId) -> Self {
        self.epoch_id = epoch_id;
        self
    }
    pub fn next_epoch_id(mut self, next_epoch_id: EpochId) -> Self {
        self.next_epoch_id = next_epoch_id;
        self
    }
    pub fn next_bp_hash(mut self, next_bp_hash: CryptoHash) -> Self {
        self.next_bp_hash = next_bp_hash;
        self
    }
    pub fn approvals(mut self, approvals: Vec<Option<Box<near_crypto::Signature>>>) -> Self {
        self.approvals = approvals;
        self
    }

    /// Updates the merkle tree by adding the previous hash, and updates the new block's merkle_root.
    pub fn block_merkle_tree(
        mut self,
        block_merkle_tree: &mut crate::merkle::PartialMerkleTree,
    ) -> Self {
        block_merkle_tree.insert(*self.prev.hash());
        self.block_merkle_root = block_merkle_tree.root();
        self
    }

    pub fn build(self) -> Arc<Block> {
        use crate::version::PROTOCOL_VERSION;

        tracing::debug!(target: "test", height=self.height, ?self.epoch_id, "produce block");
        Arc::new(Block::produce(
            PROTOCOL_VERSION,
            self.prev.header(),
            self.height,
            self.prev.header().block_ordinal() + 1,
            self.prev.chunks().iter_raw().cloned().collect(),
            vec![vec![]; self.prev.chunks().len()],
            self.epoch_id,
            self.next_epoch_id,
            None,
            self.approvals,
            num_rational::Ratio::new(0, 1),
            0,
            0,
            Some(0),
            self.signer.as_ref(),
            self.next_bp_hash,
            self.block_merkle_root,
            self.clock,
            None,
            None,
            vec![],
        ))
    }
}

impl Block {
    pub fn mut_header(&mut self) -> &mut BlockHeader {
        match self {
            Block::BlockV1(block) => &mut block.header,
            Block::BlockV2(block) => &mut block.header,
            Block::BlockV3(block) => &mut block.header,
            Block::BlockV4(block) => &mut block.header,
        }
    }

    pub fn mut_chunks(&mut self) -> &mut Vec<ShardChunkHeader> {
        match self {
            Block::BlockV1(_) => unreachable!(),
            Block::BlockV2(block) => &mut block.chunks,
            Block::BlockV3(block) => &mut block.body.chunks,
            Block::BlockV4(block) => block.body.mut_chunks(),
        }
    }

    pub fn set_chunks(&mut self, chunks: Vec<ShardChunkHeader>) {
        match self {
            Block::BlockV1(block) => {
                let legacy_chunks = chunks
                    .into_iter()
                    .map(|chunk| match chunk {
                        ShardChunkHeader::V1(header) => header,
                        ShardChunkHeader::V2(_) => {
                            panic!("Attempted to set V1 block chunks with V2")
                        }
                        ShardChunkHeader::V3(_) => {
                            panic!("Attempted to set V1 block chunks with V3")
                        }
                    })
                    .collect();
                block.chunks = legacy_chunks;
            }
            Block::BlockV2(block) => {
                block.chunks = chunks;
            }
            Block::BlockV3(block) => {
                block.body.chunks = chunks;
            }
            Block::BlockV4(block) => {
                block.body.set_chunks(chunks);
            }
        }
    }

    pub fn set_vrf_value(&mut self, vrf_value: Value) {
        match self {
            Block::BlockV1(_) => unreachable!(),
            Block::BlockV2(body) => {
                body.vrf_value = vrf_value;
            }
            Block::BlockV3(body) => {
                body.body.vrf_value = vrf_value;
            }
            Block::BlockV4(body) => {
                body.body.set_vrf_value(vrf_value);
            }
        };
    }

    pub fn set_chunk_endorsements(&mut self, chunk_endorsements: Vec<ChunkEndorsementSignatures>) {
        match self {
            Block::BlockV1(_) | Block::BlockV2(_) | Block::BlockV3(_) => (),
            Block::BlockV4(body) => {
                body.body.set_chunk_endorsements(chunk_endorsements);
            }
        };
    }
}

pub struct MockEpochInfoProvider {
    pub shard_layout: ShardLayout,
    pub validators: HashMap<AccountId, Balance>,
}

impl Default for MockEpochInfoProvider {
    fn default() -> Self {
        MockEpochInfoProvider {
            shard_layout: ShardLayout::single_shard(),
            validators: HashMap::new(),
        }
    }
}

impl MockEpochInfoProvider {
    pub fn new(shard_layout: ShardLayout) -> Self {
        MockEpochInfoProvider { shard_layout, validators: HashMap::new() }
    }
}

impl EpochInfoProvider for MockEpochInfoProvider {
    fn validator_stake(
        &self,
        _epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<Balance>, EpochError> {
        Ok(self.validators.get(account_id).cloned())
    }

    fn validator_total_stake(&self, _epoch_id: &EpochId) -> Result<Balance, EpochError> {
        Ok(self.validators.values().sum())
    }

    fn minimum_stake(&self, _prev_block_hash: &CryptoHash) -> Result<Balance, EpochError> {
        Ok(0)
    }

    fn chain_id(&self) -> String {
        "localnet".into()
    }

    fn shard_layout(&self, _epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        Ok(self.shard_layout.clone())
    }
}

/// Encode array of `u64` to be passed as a smart contract argument.
pub fn encode(xs: &[u64]) -> Vec<u8> {
    xs.iter().flat_map(|it| it.to_le_bytes()).collect()
}

// Helper function that creates a new signer for a given account, that uses the account name as seed.
// Should be used only in tests.
#[cfg(feature = "rand")]
pub fn create_test_signer(account_name: &str) -> ValidatorSigner {
    crate::validator_signer::InMemoryValidatorSigner::from_seed(
        account_name.parse().unwrap(),
        near_crypto::KeyType::ED25519,
        account_name,
    )
}

/// Helper function that creates a new signer for a given account, that uses the account name as seed.
///
/// This also works for predefined implicit accounts, where the signer will use the implicit key.
///
/// Should be used only in tests.
#[cfg(feature = "rand")]
pub fn create_user_test_signer(
    account_name: &near_primitives_core::account::id::AccountIdRef,
) -> near_crypto::Signer {
    let account_id = account_name.to_owned();
    if account_id == near_implicit_test_account() {
        near_crypto::InMemorySigner::from_secret_key(
            account_id,
            near_implicit_test_account_secret(),
        )
        .into()
    } else {
        near_crypto::InMemorySigner::from_seed(
            account_id,
            near_crypto::KeyType::ED25519,
            account_name.as_str(),
        )
    }
}

/// A fixed NEAR-implicit account for which tests can know the private key.
pub fn near_implicit_test_account() -> AccountId {
    "061b1dd17603213b00e1a1e53ba060ad427cef4887bd34a5e0ef09010af23b0a".parse().unwrap()
}

/// Private key for the fixed NEAR-implicit test account.
pub fn near_implicit_test_account_secret() -> SecretKey {
    "ed25519:5roj6k68kvZu3UEJFyXSfjdKGrodgZUfFLZFpzYXWtESNsLWhYrq3JGi4YpqeVKuw1m9R2TEHjfgWT1fjUqB1DNy".parse().unwrap()
}

/// A fixed ETH-implicit account.
pub fn eth_implicit_test_account() -> AccountId {
    "0x96791e923f8cf697ad9c3290f2c9059f0231b24c".parse().unwrap()
}

impl FinalExecutionOutcomeView {
    #[track_caller]
    /// Check transaction and all transitive receipts for success status.
    pub fn assert_success(&self) {
        assert!(
            matches!(self.status, FinalExecutionStatus::SuccessValue(_)),
            "error: {:?}",
            self.status
        );
        for (i, receipt) in self.receipts_outcome.iter().enumerate() {
            assert!(
                matches!(
                    receipt.outcome.status,
                    ExecutionStatusView::SuccessReceiptId(_) | ExecutionStatusView::SuccessValue(_),
                ),
                "receipt #{i} failed: {receipt:?}",
            );
        }
    }

    /// Calculates how much NEAR was burnt for gas, after refunds.
    pub fn tokens_burnt(&self) -> Balance {
        self.transaction_outcome.outcome.tokens_burnt
            + self.receipts_outcome.iter().map(|r| r.outcome.tokens_burnt).sum::<u128>()
    }
}
