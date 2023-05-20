use crate::network_protocol::PeerIdOrHash;
use crate::routing;
use crate::routing::route_back_cache::RouteBackCache;
use lru::LruCache;
use near_async::time;
use near_primitives::hash::CryptoHash;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

const LAST_ROUTED_CACHE_SIZE: usize = 10_000;

pub(crate) struct RoutingTableViewV2(Mutex<Inner>);

struct Inner {
    /// For each peer, the set of neighbors which are one hop closer to `my_peer_id`.
    /// Alternatively, if we look at the set of all shortest path from `my_peer_id` to peer,
    /// this will be the set of first nodes on all such paths.
    next_hops: Arc<routing::NextHopTable>,
    /// Hash of messages that requires routing back to respective previous hop.
    route_back: RouteBackCache,

    /// Counter of number of calls to find_route_by_peer_id.
    find_route_calls: u64,
    /// Last time the given peer was selected by find_route_by_peer_id.
    last_routed: LruCache<PeerId, u64>,
}

impl Inner {
    fn count_next_hops_for_peer_id(&self, peer_id: &PeerId) -> usize {
        match self.next_hops.get(peer_id) {
            Some(hops) => hops.len(),
            None => 0,
        }
    }

    /// Select a connected peer on some shortest path to `peer_id`.
    /// If there are several such peers, pick the least recently used one.
    fn find_route_from_peer_id(&mut self, peer_id: &PeerId) -> Result<PeerId, FindRouteError> {
        let peers = self.next_hops.get(peer_id).ok_or(FindRouteError::PeerUnreachable)?;
        let next_hop = peers
            .iter()
            .min_by_key(|p| self.last_routed.get(*p).copied().unwrap_or(0))
            .ok_or(FindRouteError::PeerUnreachable)?;
        self.last_routed.put(next_hop.clone(), self.find_route_calls);
        self.find_route_calls += 1;
        Ok(next_hop.clone())
    }

    // Find route back with given hash and removes it from cache.
    fn fetch_route_back(&mut self, clock: &time::Clock, hash: CryptoHash) -> Option<PeerId> {
        self.route_back.remove(clock, &hash)
    }
}

#[derive(Debug)]
pub(crate) enum FindRouteError {
    PeerUnreachable,
    RouteBackNotFound,
}

impl RoutingTableViewV2 {
    pub fn new() -> Self {
        Self(Mutex::new(Inner {
            next_hops: Default::default(),
            route_back: RouteBackCache::default(),
            find_route_calls: 0,
            last_routed: LruCache::new(LAST_ROUTED_CACHE_SIZE),
        }))
    }

    pub(crate) fn update(&self, next_hops: Arc<routing::NextHopTable>) {
        self.0.lock().next_hops = next_hops;
    }

    pub(crate) fn reachable_peers(&self) -> usize {
        // There is an implicit assumption here that all next_hops entries are non-empty.
        // To enforce this, we would need to make NextHopTable a newtype rather than an alias,
        // and add appropriate constructors, which would filter out empty entries.
        self.0.lock().next_hops.len()
    }

    pub(crate) fn count_next_hops_for_peer_id(&self, peer_id: &PeerId) -> usize {
        self.0.lock().count_next_hops_for_peer_id(peer_id)
    }

    pub(crate) fn find_route(
        &self,
        clock: &time::Clock,
        target: &PeerIdOrHash,
    ) -> Result<PeerId, FindRouteError> {
        let mut inner = self.0.lock();
        match target {
            PeerIdOrHash::PeerId(peer_id) => inner.find_route_from_peer_id(peer_id),
            PeerIdOrHash::Hash(hash) => {
                inner.fetch_route_back(clock, *hash).ok_or(FindRouteError::RouteBackNotFound)
            }
        }
    }

    pub(crate) fn view_route(&self, peer_id: &PeerId) -> Option<Vec<PeerId>> {
        self.0.lock().next_hops.get(peer_id).cloned()
    }

    pub(crate) fn add_route_back(&self, clock: &time::Clock, hash: CryptoHash, peer_id: PeerId) {
        self.0.lock().route_back.insert(clock, hash, peer_id);
    }

    pub(crate) fn compare_route_back(&self, hash: CryptoHash, peer_id: &PeerId) -> bool {
        self.0.lock().route_back.get(&hash).map_or(false, |value| value == peer_id)
    }

    pub(crate) fn info(&self) -> RoutingTableInfoV2 {
        let inner = self.0.lock();
        RoutingTableInfoV2 { next_hops: inner.next_hops.clone() }
    }
}

#[derive(Debug)]
pub struct RoutingTableInfoV2 {
    pub next_hops: Arc<routing::NextHopTable>,
}
