use crate::network_protocol::borsh_ as net;
/// Contains borsh <-> network_protocol conversions.
use crate::network_protocol::{self as mem, RoutedMessageV3};
use crate::network_protocol::{PeersRequest, PeersResponse};

impl From<&net::Handshake> for mem::Handshake {
    fn from(x: &net::Handshake) -> Self {
        Self {
            protocol_version: x.protocol_version,
            oldest_supported_version: x.oldest_supported_version,
            sender_peer_id: x.sender_peer_id.clone(),
            target_peer_id: x.target_peer_id.clone(),
            sender_listen_port: x.sender_listen_port,
            sender_chain_info: x.sender_chain_info.clone(),
            partial_edge_info: x.partial_edge_info.clone(),
            owned_account: None,
        }
    }
}

impl From<&mem::Handshake> for net::Handshake {
    fn from(x: &mem::Handshake) -> Self {
        Self {
            protocol_version: x.protocol_version,
            oldest_supported_version: x.oldest_supported_version,
            sender_peer_id: x.sender_peer_id.clone(),
            target_peer_id: x.target_peer_id.clone(),
            sender_listen_port: x.sender_listen_port,
            sender_chain_info: x.sender_chain_info.clone(),
            partial_edge_info: x.partial_edge_info.clone(),
        }
    }
}

//////////////////////////////////////////

impl From<&net::HandshakeFailureReason> for mem::HandshakeFailureReason {
    fn from(x: &net::HandshakeFailureReason) -> Self {
        match x {
            net::HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => mem::HandshakeFailureReason::ProtocolVersionMismatch {
                version: *version,
                oldest_supported_version: *oldest_supported_version,
            },
            net::HandshakeFailureReason::GenesisMismatch(genesis_id) => {
                mem::HandshakeFailureReason::GenesisMismatch(genesis_id.clone())
            }
            net::HandshakeFailureReason::InvalidTarget => {
                mem::HandshakeFailureReason::InvalidTarget
            }
        }
    }
}

impl From<&mem::HandshakeFailureReason> for net::HandshakeFailureReason {
    fn from(x: &mem::HandshakeFailureReason) -> Self {
        match x {
            mem::HandshakeFailureReason::ProtocolVersionMismatch {
                version,
                oldest_supported_version,
            } => net::HandshakeFailureReason::ProtocolVersionMismatch {
                version: *version,
                oldest_supported_version: *oldest_supported_version,
            },
            mem::HandshakeFailureReason::GenesisMismatch(genesis_id) => {
                net::HandshakeFailureReason::GenesisMismatch(genesis_id.clone())
            }
            mem::HandshakeFailureReason::InvalidTarget => {
                net::HandshakeFailureReason::InvalidTarget
            }
        }
    }
}

//////////////////////////////////////////

impl From<net::RoutingTableUpdate> for mem::RoutingTableUpdate {
    fn from(x: net::RoutingTableUpdate) -> Self {
        Self { edges: x.edges, accounts: x.accounts }
    }
}

impl From<mem::RoutingTableUpdate> for net::RoutingTableUpdate {
    fn from(x: mem::RoutingTableUpdate) -> Self {
        Self { edges: x.edges, accounts: x.accounts }
    }
}

//////////////////////////////////////////

impl From<net::AdvertisedPeerDistance> for mem::AdvertisedPeerDistance {
    fn from(x: net::AdvertisedPeerDistance) -> Self {
        Self { destination: x.destination, distance: x.distance }
    }
}

impl From<mem::AdvertisedPeerDistance> for net::AdvertisedPeerDistance {
    fn from(x: mem::AdvertisedPeerDistance) -> Self {
        Self { destination: x.destination, distance: x.distance }
    }
}

//////////////////////////////////////////

impl From<net::DistanceVector> for mem::DistanceVector {
    fn from(x: net::DistanceVector) -> Self {
        Self {
            root: x.root,
            distances: x.distances.into_iter().map(|y| y.into()).collect(),
            edges: x.edges,
        }
    }
}

impl From<mem::DistanceVector> for net::DistanceVector {
    fn from(x: mem::DistanceVector) -> Self {
        Self {
            root: x.root,
            distances: x.distances.into_iter().map(|y| y.into()).collect(),
            edges: x.edges,
        }
    }
}

//////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ParsePeerMessageError {
    #[error("HandshakeV2 is deprecated")]
    DeprecatedHandshakeV2,
    #[error("RoutingTableSyncV2 is deprecated")]
    DeprecatedRoutingTableSyncV2,
    #[error("EpochSync is deprecated")]
    DeprecatedEpochSync,
    #[error("ResponseUpdateNonce is deprecated")]
    DeprecatedResponseUpdateNonce,
}

impl TryFrom<&net::PeerMessage> for mem::PeerMessage {
    type Error = ParsePeerMessageError;
    fn try_from(x: &net::PeerMessage) -> Result<Self, Self::Error> {
        Ok(match x.clone() {
            net::PeerMessage::Handshake(h) => mem::PeerMessage::Tier2Handshake((&h).into()),
            net::PeerMessage::HandshakeFailure(pi, hfr) => {
                mem::PeerMessage::HandshakeFailure(pi, (&hfr).into())
            }
            net::PeerMessage::LastEdge(e) => mem::PeerMessage::LastEdge(e),
            net::PeerMessage::SyncRoutingTable(rtu) => {
                mem::PeerMessage::SyncRoutingTable(rtu.into())
            }
            net::PeerMessage::RequestUpdateNonce(e) => mem::PeerMessage::RequestUpdateNonce(e),
            net::PeerMessage::_ResponseUpdateNonce => {
                return Err(Self::Error::DeprecatedResponseUpdateNonce);
            }
            net::PeerMessage::PeersRequest => mem::PeerMessage::PeersRequest(PeersRequest {
                max_peers: None,
                max_direct_peers: None,
            }),
            net::PeerMessage::PeersResponse(pis) => {
                mem::PeerMessage::PeersResponse(PeersResponse { peers: pis, direct_peers: vec![] })
            }
            net::PeerMessage::BlockHeadersRequest(bhs) => {
                mem::PeerMessage::BlockHeadersRequest(bhs)
            }
            net::PeerMessage::BlockHeaders(bhs) => mem::PeerMessage::BlockHeaders(bhs),
            net::PeerMessage::BlockRequest(bh) => mem::PeerMessage::BlockRequest(bh),
            net::PeerMessage::Block(b) => mem::PeerMessage::Block(b),
            net::PeerMessage::OptimisticBlock(ob) => mem::PeerMessage::OptimisticBlock(ob),
            net::PeerMessage::Transaction(t) => mem::PeerMessage::Transaction(t),
            net::PeerMessage::Routed(r) => mem::PeerMessage::Routed(Box::new(
                mem::RoutedMessage::V3(RoutedMessageV3::from(*r)),
            )),
            net::PeerMessage::Disconnect => mem::PeerMessage::Disconnect(mem::Disconnect {
                // This flag is used by the disconnecting peer to advise the other peer that there
                // is a reason to remove the connection from storage (for example, a peer ban).
                // In the absence of such information, it should default to false.
                remove_from_connection_store: false,
            }),
            net::PeerMessage::Challenge(c) => mem::PeerMessage::Challenge(c),
            net::PeerMessage::_HandshakeV2 => return Err(Self::Error::DeprecatedHandshakeV2),
            net::PeerMessage::_EpochSyncRequest => return Err(Self::Error::DeprecatedEpochSync),
            net::PeerMessage::_EpochSyncResponse => return Err(Self::Error::DeprecatedEpochSync),
            net::PeerMessage::_EpochSyncFinalizationRequest => {
                return Err(Self::Error::DeprecatedEpochSync);
            }
            net::PeerMessage::_EpochSyncFinalizationResponse => {
                return Err(Self::Error::DeprecatedEpochSync);
            }
            net::PeerMessage::_RoutingTableSyncV2 => {
                return Err(Self::Error::DeprecatedRoutingTableSyncV2);
            }
            net::PeerMessage::DistanceVector(dv) => mem::PeerMessage::DistanceVector(dv.into()),
            net::PeerMessage::StateRequestHeader(shard_id, sync_hash) => {
                mem::PeerMessage::StateRequestHeader(shard_id, sync_hash)
            }
            net::PeerMessage::StateRequestPart(shard_id, sync_hash, part_id) => {
                mem::PeerMessage::StateRequestPart(shard_id, sync_hash, part_id)
            }
            net::PeerMessage::VersionedStateResponse(sri) => {
                mem::PeerMessage::VersionedStateResponse(sri)
            }
            net::PeerMessage::SyncSnapshotHosts(ssh) => mem::PeerMessage::SyncSnapshotHosts(ssh),
            net::PeerMessage::EpochSyncRequest => mem::PeerMessage::EpochSyncRequest,
            net::PeerMessage::EpochSyncResponse(proof) => {
                mem::PeerMessage::EpochSyncResponse(proof)
            }
        })
    }
}

// We are working on deprecating Borsh support for network messages altogether,
// so any new message variants are simply unsupported.
impl From<&mem::PeerMessage> for net::PeerMessage {
    fn from(x: &mem::PeerMessage) -> Self {
        match x.clone() {
            mem::PeerMessage::Tier1Handshake(_) => {
                panic!("Tier1Handshake is not supported in Borsh encoding")
            }
            mem::PeerMessage::Tier2Handshake(h) => net::PeerMessage::Handshake((&h).into()),
            mem::PeerMessage::Tier3Handshake(_) => {
                panic!("Tier3Handshake is not supported in Borsh encoding")
            }
            mem::PeerMessage::HandshakeFailure(pi, hfr) => {
                net::PeerMessage::HandshakeFailure(pi, (&hfr).into())
            }
            mem::PeerMessage::LastEdge(e) => net::PeerMessage::LastEdge(e),
            mem::PeerMessage::SyncRoutingTable(rtu) => {
                net::PeerMessage::SyncRoutingTable(rtu.into())
            }
            mem::PeerMessage::RequestUpdateNonce(e) => net::PeerMessage::RequestUpdateNonce(e),
            mem::PeerMessage::DistanceVector(dv) => net::PeerMessage::DistanceVector(dv.into()),

            // This message is not supported, we translate it to an empty RoutingTableUpdate.
            mem::PeerMessage::SyncAccountsData(_) => {
                net::PeerMessage::SyncRoutingTable(net::RoutingTableUpdate::default())
            }

            mem::PeerMessage::PeersRequest(_) => net::PeerMessage::PeersRequest,
            mem::PeerMessage::PeersResponse(pr) => net::PeerMessage::PeersResponse(pr.peers),
            mem::PeerMessage::BlockHeadersRequest(bhs) => {
                net::PeerMessage::BlockHeadersRequest(bhs)
            }
            mem::PeerMessage::BlockHeaders(bhs) => net::PeerMessage::BlockHeaders(bhs),
            mem::PeerMessage::BlockRequest(bh) => net::PeerMessage::BlockRequest(bh),
            mem::PeerMessage::Block(b) => net::PeerMessage::Block(b),
            mem::PeerMessage::OptimisticBlock(ob) => net::PeerMessage::OptimisticBlock(ob),
            mem::PeerMessage::Transaction(t) => net::PeerMessage::Transaction(t),
            mem::PeerMessage::Routed(r) => net::PeerMessage::Routed(Box::new(r.msg_v1())),
            mem::PeerMessage::Disconnect(_) => net::PeerMessage::Disconnect,
            mem::PeerMessage::Challenge(c) => net::PeerMessage::Challenge(c),
            mem::PeerMessage::StateRequestHeader(shard_id, sync_hash) => {
                net::PeerMessage::StateRequestHeader(shard_id, sync_hash)
            }
            mem::PeerMessage::StateRequestPart(shard_id, sync_hash, part_id) => {
                net::PeerMessage::StateRequestPart(shard_id, sync_hash, part_id)
            }
            mem::PeerMessage::VersionedStateResponse(sri) => {
                net::PeerMessage::VersionedStateResponse(sri)
            }
            mem::PeerMessage::SyncSnapshotHosts(ssh) => net::PeerMessage::SyncSnapshotHosts(ssh),
            mem::PeerMessage::EpochSyncRequest => net::PeerMessage::EpochSyncRequest,
            mem::PeerMessage::EpochSyncResponse(proof) => {
                net::PeerMessage::EpochSyncResponse(proof)
            }
        }
    }
}
