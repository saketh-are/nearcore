use crate::client::{StateRequestHeader, StateRequestPart, StateResponse};
/// Type that belong to the network protocol.
pub use crate::network_protocol::{
    Disconnect, Encoding, Handshake, HandshakeFailureReason, PeerMessage, RoutingTableUpdate,
    SignedAccountData,
};
/// Exported types, which are part of network protocol.
pub use crate::network_protocol::{
    Edge, PartialEdgeInfo, PartialEncodedChunkForwardMsg, PartialEncodedChunkRequestMsg,
    PartialEncodedChunkResponseMsg, PeerChainInfoV2, PeerInfo, SnapshotHostInfo, StateResponseInfo,
    StateResponseInfoV1, StateResponseInfoV2,
};
use crate::routing::routing_table_view::RoutingTableInfo;
pub use crate::state_sync::StateSyncResponse;
use near_async::messaging::{AsyncSender, Sender};
use near_async::{MultiSend, MultiSendMessage, MultiSenderFrom, time};
use near_crypto::PublicKey;
use near_primitives::block::{ApprovalMessage, Block};
use near_primitives::epoch_sync::CompressedEpochSyncProof;
use near_primitives::genesis::GenesisId;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::optimistic_block::OptimisticBlock;
use near_primitives::sharding::{PartialEncodedChunkWithArcReceipts, ReceiptProof};
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest, ContractCodeResponse, PartialEncodedContractDeploys,
};
use near_primitives::stateless_validation::partial_witness::PartialEncodedStateWitness;
use near_primitives::stateless_validation::state_witness::{
    ChunkStateWitness, ChunkStateWitnessAck,
};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, BlockHeight, EpochHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::Arc;

/// Number of hops a message is allowed to travel before being dropped.
/// This is used to avoid infinite loop because of inconsistent view of the network
/// by different nodes.
pub const ROUTED_MESSAGE_TTL: u8 = 100;

/// Peer type.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, strum::IntoStaticStr)]
pub enum PeerType {
    /// Inbound session
    Inbound,
    /// Outbound session
    Outbound,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnownProducer {
    pub account_id: AccountId,
    pub addr: Option<SocketAddr>,
    pub peer_id: PeerId,
    pub next_hops: Option<Vec<PeerId>>,
}

/// Ban reason.
#[derive(
    borsh::BorshSerialize,
    borsh::BorshDeserialize,
    Debug,
    Clone,
    PartialEq,
    Eq,
    Copy,
    ProtocolSchema,
)]
#[borsh(use_discriminant = false)]
pub enum ReasonForBan {
    None = 0,
    BadBlock = 1,
    BadBlockHeader = 2,
    HeightFraud = 3,
    BadHandshake = 4,
    BadBlockApproval = 5,
    Abusive = 6,
    InvalidSignature = 7,
    InvalidPeerId = 8,
    InvalidHash = 9,
    InvalidEdge = 10,
    InvalidDistanceVector = 11,
    Blacklisted = 14,
    ProvidedNotEnoughHeaders = 15,
    BadChunkStateWitness = 16,
}

/// Banning signal sent from Peer instance to PeerManager
/// just before Peer instance is stopped.
#[derive(actix::Message, Debug)]
#[rtype(result = "()")]
pub struct Ban {
    pub peer_id: PeerId,
    pub ban_reason: ReasonForBan,
}

/// Status of the known peers.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum KnownPeerStatus {
    /// We got information about this peer from someone, but we didn't
    /// verify them yet. This peer might not exist, invalid IP etc.
    /// Also the peers that we failed to connect to, will be marked as 'Unknown'.
    Unknown,
    /// We know that this peer exists - we were connected to it, or it was provided as boot node.
    NotConnected,
    /// We're currently connected to this peer.
    Connected,
    /// We banned this peer for some reason. Once the ban time is over, it will move to 'NotConnected' state.
    Banned(ReasonForBan, time::Utc),
}

/// Information node stores about known peers.
#[derive(Debug, Clone)]
pub struct KnownPeerState {
    pub peer_info: PeerInfo,
    pub status: KnownPeerStatus,
    pub first_seen: time::Utc,
    pub last_seen: time::Utc,
    // Last time we tried to connect to this peer.
    // This data is not persisted in storage.
    pub last_outbound_attempt: Option<(time::Utc, Result<(), String>)>,
}

impl KnownPeerState {
    pub fn new(peer_info: PeerInfo, now: time::Utc) -> Self {
        KnownPeerState {
            peer_info,
            status: KnownPeerStatus::Unknown,
            first_seen: now,
            last_seen: now,
            last_outbound_attempt: None,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ConnectionInfo {
    pub peer_info: PeerInfo,
    pub time_established: time::Utc,
    pub time_connected_until: time::Utc,
}

impl KnownPeerStatus {
    pub fn is_banned(&self) -> bool {
        matches!(self, KnownPeerStatus::Banned(_, _))
    }
}

/// Set of account keys.
/// This is information which chain pushes to network to implement tier1.
/// See ChainInfo.
pub type AccountKeys = HashMap<AccountId, HashSet<PublicKey>>;

/// Network-relevant data about the chain.
// TODO(gprusak): it is more like node info, or sth.
#[derive(Debug, Clone)]
pub struct ChainInfo {
    pub tracked_shards: Vec<ShardId>,
    // The latest block on chain.
    pub block: Arc<Block>,
    // Public keys of accounts participating in the BFT consensus
    // It currently includes "block producers", "chunk producers" and "approvers".
    // They are collectively known as "validators".
    // Peers acting on behalf of these accounts have a higher
    // priority on the NEAR network than other peers.
    pub tier1_accounts: Arc<AccountKeys>,
}

#[derive(Debug, actix::Message)]
#[rtype(result = "()")]
pub struct SetChainInfo(pub ChainInfo);

/// Public actix interface of `PeerManagerActor`.
#[derive(actix::Message, Debug, strum::IntoStaticStr)]
#[rtype(result = "PeerManagerMessageResponse")]
#[allow(clippy::large_enum_variant)]
pub enum PeerManagerMessageRequest {
    NetworkRequests(NetworkRequests),
    /// Request PeerManager to call `tier1_advertise_proxies()`. Used internally.
    /// The effect would be accounts data known by this node broadcasted to other tier1 nodes.
    /// That includes info about validator signer of this node.
    AdvertiseTier1Proxies,
    /// Request PeerManager to connect to the given peer.
    /// Used in tests and internally by PeerManager.
    /// TODO: replace it with AsyncContext::spawn/run_later for internal use.
    OutboundTcpConnect(crate::tcp::Stream),
    /// The following types of requests are used to trigger actions in the Peer Manager for testing.
    /// TEST-ONLY: Fetch current routing table.
    FetchRoutingTable,
}

impl PeerManagerMessageRequest {
    pub fn as_network_requests(self) -> NetworkRequests {
        if let PeerManagerMessageRequest::NetworkRequests(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkRequests(");
        }
    }

    pub fn as_network_requests_ref(&self) -> &NetworkRequests {
        if let PeerManagerMessageRequest::NetworkRequests(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkRequests");
        }
    }
}

/// List of all replies to messages to `PeerManager`. See `PeerManagerMessageRequest` for more details.
#[derive(actix::MessageResponse, Debug)]
pub enum PeerManagerMessageResponse {
    NetworkResponses(NetworkResponses),
    AdvertiseTier1Proxies,
    /// TEST-ONLY
    OutboundTcpConnect,
    FetchRoutingTable(RoutingTableInfo),
}

impl PeerManagerMessageResponse {
    pub fn as_network_response(self) -> NetworkResponses {
        if let PeerManagerMessageResponse::NetworkResponses(item) = self {
            item
        } else {
            panic!("expected PeerMessageRequest::NetworkResponses");
        }
    }
}

impl From<NetworkResponses> for PeerManagerMessageResponse {
    fn from(msg: NetworkResponses) -> Self {
        PeerManagerMessageResponse::NetworkResponses(msg)
    }
}

// TODO(#1313): Use Box
#[derive(Clone, strum::AsRefStr, Debug, Eq, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum NetworkRequests {
    /// Sends block, either when block was just produced or when requested.
    Block { block: Arc<Block> },
    /// Sends optimistic block as soon as the production window for the height starts.
    OptimisticBlock { chunk_producers: Arc<Vec<AccountId>>, optimistic_block: OptimisticBlock },
    /// Sends approval.
    Approval { approval_message: ApprovalMessage },
    /// Request block with given hash from given peer.
    BlockRequest { hash: CryptoHash, peer_id: PeerId },
    /// Request given block headers.
    BlockHeadersRequest { hashes: Vec<CryptoHash>, peer_id: PeerId },
    /// Request state header for given shard and given sync hash.
    StateRequestHeader { shard_id: ShardId, sync_hash: CryptoHash, sync_prev_prev_hash: CryptoHash },
    /// Request state part for given shard and given sync hash.
    StateRequestPart {
        shard_id: ShardId,
        sync_hash: CryptoHash,
        sync_prev_prev_hash: CryptoHash,
        part_id: u64,
    },
    /// Ban given peer.
    BanPeer { peer_id: PeerId, ban_reason: ReasonForBan },
    /// Announce account
    AnnounceAccount(AnnounceAccount),
    /// Broadcast information about a hosted snapshot.
    SnapshotHostInfo { sync_hash: CryptoHash, epoch_height: EpochHeight, shards: Vec<ShardId> },

    /// Request chunk parts and/or receipts
    PartialEncodedChunkRequest {
        target: AccountIdOrPeerTrackingShard,
        request: PartialEncodedChunkRequestMsg,
        create_time: time::Instant,
    },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkResponse { route_back: CryptoHash, response: PartialEncodedChunkResponseMsg },
    /// Information about chunk such as its header, some subset of parts and/or incoming receipts
    PartialEncodedChunkMessage {
        account_id: AccountId,
        partial_encoded_chunk: PartialEncodedChunkWithArcReceipts,
    },
    /// Forwarding a chunk part to a validator tracking the shard
    PartialEncodedChunkForward { account_id: AccountId, forward: PartialEncodedChunkForwardMsg },
    /// Valid transaction but since we are not validators we send this transaction to current validators.
    ForwardTx(AccountId, SignedTransaction),
    /// Query transaction status
    TxStatus(AccountId, AccountId, CryptoHash),
    /// Acknowledgement to a chunk's state witness, sent back to the originating chunk producer.
    ChunkStateWitnessAck(AccountId, ChunkStateWitnessAck),
    /// Message for a chunk endorsement, sent by a chunk validator to the block producer.
    ChunkEndorsement(AccountId, ChunkEndorsement),
    /// Message from chunk producer to set of chunk validators to send state witness part.
    PartialEncodedStateWitness(Vec<(AccountId, PartialEncodedStateWitness)>),
    /// Message from chunk validator to all other chunk validators to forward state witness part.
    PartialEncodedStateWitnessForward(Vec<AccountId>, PartialEncodedStateWitness),
    /// Requests an epoch sync
    EpochSyncRequest { peer_id: PeerId },
    /// Response to an epoch sync request
    EpochSyncResponse { peer_id: PeerId, proof: CompressedEpochSyncProof },
    /// Message from chunk producer to chunk validators containing the code-hashes of contracts
    /// accessed for the main state transition in the witness.
    ChunkContractAccesses(Vec<AccountId>, ChunkContractAccesses),
    /// Message from chunk validator to chunk producer to request missing contract code.
    /// This message is currently sent as a result of receiving the ChunkContractAccesses message
    /// and failing to find the corresponding code for the hashes received.
    ContractCodeRequest(AccountId, ContractCodeRequest),
    /// Message from chunk producer to chunk validators to send the contract code as response to ContractCodeRequest.
    ContractCodeResponse(AccountId, ContractCodeResponse),
    /// Message originates from the chunk producer and distributed among other validators,
    /// containing the code of the newly-deployed contracts during the main state transition of the witness.
    PartialEncodedContractDeploys(Vec<AccountId>, PartialEncodedContractDeploys),
    // TODO(spice): remove and depend on separate data distribution.
    /// Mocked message to the chunk executor with block hash and relevant incoming receipts.
    TestonlySpiceIncomingReceipts { block_hash: CryptoHash, receipt_proofs: Vec<ReceiptProof> },
    /// Mocked message with state witness that will eventually be distributed by the spice
    /// distribution layer.
    TestonlySpiceStateWitness { state_witness: ChunkStateWitness },
}

#[derive(Debug, actix::Message, strum::IntoStaticStr)]
#[rtype(result = "()")]
pub enum StateSyncEvent {
    StatePartReceived(ShardId, u64),
}

/// Combines peer address info, chain.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FullPeerInfo {
    pub peer_info: PeerInfo,
    pub chain_info: PeerChainInfo,
}

/// These are the information needed for highest height peers. For these peers, we guarantee that
/// the height and hash of the latest block are set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HighestHeightPeerInfo {
    pub peer_info: PeerInfo,
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Height and hash of the highest block we've ever received from the peer
    pub highest_block_height: BlockHeight,
    /// Hash of the latest block
    pub highest_block_hash: CryptoHash,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
}

impl From<FullPeerInfo> for Option<HighestHeightPeerInfo> {
    fn from(p: FullPeerInfo) -> Self {
        if p.chain_info.last_block.is_some() {
            Some(HighestHeightPeerInfo {
                peer_info: p.peer_info,
                genesis_id: p.chain_info.genesis_id,
                highest_block_height: p.chain_info.last_block.unwrap().height,
                highest_block_hash: p.chain_info.last_block.unwrap().hash,
                tracked_shards: p.chain_info.tracked_shards,
                archival: p.chain_info.archival,
            })
        } else {
            None
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BlockInfo {
    pub height: BlockHeight,
    pub hash: CryptoHash,
}

/// This is the internal representation of PeerChainInfoV2.
/// We separate these two structs because PeerChainInfoV2 is part of network protocol, and can't be
/// modified easily.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PeerChainInfo {
    /// Chain Id and hash of genesis block.
    pub genesis_id: GenesisId,
    /// Height and hash of the highest block we've ever received from the peer
    pub last_block: Option<BlockInfo>,
    /// Shards that the peer is tracking.
    pub tracked_shards: Vec<ShardId>,
    /// Denote if a node is running in archival mode or not.
    pub archival: bool,
}

// Information about the connected peer that is shared with the rest of the system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectedPeerInfo {
    pub full_peer_info: FullPeerInfo,
    /// Number of bytes we've received from the peer.
    pub received_bytes_per_sec: u64,
    /// Number of bytes we've sent to the peer.
    pub sent_bytes_per_sec: u64,
    /// Last time requested peers.
    pub last_time_peer_requested: time::Instant,
    /// Last time we received a message from this peer.
    pub last_time_received_message: time::Instant,
    /// Time where the connection was established.
    pub connection_established_time: time::Instant,
    /// Who started connection. Inbound (other) or Outbound (us).
    pub peer_type: PeerType,
    /// Nonce used for the connection with the peer.
    pub nonce: u64,
}

#[derive(Debug, Default, Clone, actix::MessageResponse, PartialEq, Eq)]
pub struct NetworkInfo {
    /// TIER2 connections.
    pub connected_peers: Vec<ConnectedPeerInfo>,
    pub num_connected_peers: usize,
    pub peer_max_count: u32,
    pub highest_height_peers: Vec<HighestHeightPeerInfo>,
    pub sent_bytes_per_sec: u64,
    pub received_bytes_per_sec: u64,
    /// Accounts of known block and chunk producers from routing table.
    pub known_producers: Vec<KnownProducer>,
    /// Collected data about the current TIER1 accounts.
    pub tier1_accounts_keys: Vec<PublicKey>,
    pub tier1_accounts_data: Vec<Arc<SignedAccountData>>,
    /// TIER1 connections.
    pub tier1_connections: Vec<ConnectedPeerInfo>,
}

#[derive(Debug, actix::MessageResponse, PartialEq, Eq)]
pub enum NetworkResponses {
    NoResponse,
    RouteNotFound,
    /// For some requests, it is necessary that the node has successfully
    /// performed IP self-discovery
    MyPublicAddrNotKnown,
    NoDestinationsAvailable,
    /// Occurs in response to NetworkRequests which do not specify a target peer;
    /// the network layer selects and returns the destination for the message.
    SelectedDestination(PeerId),
}

#[derive(Clone, MultiSend, MultiSenderFrom)]
pub struct PeerManagerAdapter {
    pub async_request_sender: AsyncSender<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    pub request_sender: Sender<PeerManagerMessageRequest>,
    pub set_chain_info_sender: Sender<SetChainInfo>,
    pub state_sync_event_sender: Sender<StateSyncEvent>,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerManagerSenderForNetwork {
    pub tier3_request_sender: Sender<Tier3Request>,
}

#[derive(Clone, MultiSend, MultiSenderFrom, MultiSendMessage)]
#[multi_send_message_derive(Debug)]
#[multi_send_input_derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRequestSenderForNetwork {
    pub state_request_header: AsyncSender<StateRequestHeader, Option<StateResponse>>,
    pub state_request_part: AsyncSender<StateRequestPart, Option<StateResponse>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network_protocol::{
        RawRoutedMessage, RoutedMessage, T2MessageBody, TieredMessageBody,
    };

    const ALLOWED_SIZE: usize = 1 << 20;
    const NOTIFY_SIZE: usize = 1024;

    macro_rules! assert_size {
        ($type:ident) => {
            let struct_size = std::mem::size_of::<$type>();
            if struct_size >= NOTIFY_SIZE {
                println!("The size of {} is {}", stringify!($type), struct_size);
            }
            assert!(struct_size <= ALLOWED_SIZE);
        };
    }

    #[test]
    fn test_size() {
        assert_size!(HandshakeFailureReason);
        assert_size!(NetworkRequests);
        assert_size!(NetworkResponses);
        assert_size!(Handshake);
        assert_size!(RoutingTableUpdate);
        assert_size!(FullPeerInfo);
        assert_size!(NetworkInfo);
    }

    macro_rules! assert_size {
        ($type:ident) => {
            let struct_size = std::mem::size_of::<$type>();
            if struct_size >= NOTIFY_SIZE {
                println!("The size of {} is {}", stringify!($type), struct_size);
            }
            assert!(struct_size <= ALLOWED_SIZE);
        };
    }

    #[test]
    fn test_enum_size() {
        assert_size!(PeerType);
        assert_size!(TieredMessageBody);
        assert_size!(KnownPeerStatus);
        assert_size!(ReasonForBan);
    }

    #[test]
    fn test_struct_size() {
        assert_size!(PeerInfo);
        assert_size!(AnnounceAccount);
        assert_size!(RawRoutedMessage);
        assert_size!(RoutedMessage);
        assert_size!(KnownPeerState);
        assert_size!(Ban);
        assert_size!(StateResponseInfoV1);
        assert_size!(PartialEncodedChunkRequestMsg);
    }

    #[test]
    fn routed_message_body_compatibility_smoke_test() {
        #[track_caller]
        fn check(msg: TieredMessageBody, expected: &[u8]) {
            let actual = borsh::to_vec(&msg).unwrap();
            assert_eq!(actual.as_slice(), expected);
        }

        let msg: TieredMessageBody =
            T2MessageBody::TxStatusRequest("test_x".parse().unwrap(), CryptoHash([42; 32])).into();
        check(
            msg,
            &[
                1, 1, 6, 0, 0, 0, 116, 101, 115, 116, 95, 120, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                42, 42,
            ],
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
/// Defines the destination for a network request.
/// The request should be sent either to the `account_id` as a routed message, or directly to
/// any peer that tracks the shard.
/// If `prefer_peer` is `true`, should be sent to the peer, unless no peer tracks the shard, in which
/// case fall back to sending to the account.
/// Otherwise, send to the account, unless we do not know the route, in which case send to the peer.
pub struct AccountIdOrPeerTrackingShard {
    /// Target account to send the request to
    pub account_id: Option<AccountId>,
    /// Whether to check peers first or target account first
    pub prefer_peer: bool,
    /// Select peers that track shard `shard_id`
    pub shard_id: ShardId,
    /// Select peers that are archival nodes if it is true
    pub only_archival: bool,
    /// Only send messages to peers whose latest chain height is no less `min_height`
    pub min_height: BlockHeight,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, actix::Message)]
#[rtype(result = "()")]
/// An inbound request to which a response should be sent over Tier3
pub struct Tier3Request {
    /// Target peer to send the response to
    pub peer_info: PeerInfo,
    /// Contents of the request
    pub body: Tier3RequestBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, strum::IntoStaticStr)]
pub enum Tier3RequestBody {
    StateHeader(StateHeaderRequestBody),
    StatePart(StatePartRequestBody),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StatePartRequestBody {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
    pub part_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StateHeaderRequestBody {
    pub shard_id: ShardId,
    pub sync_hash: CryptoHash,
}
