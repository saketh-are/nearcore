use crate::store;
use lru::LruCache;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::types::AccountId;
use parking_lot::Mutex;
use std::collections::HashMap;

#[cfg(test)]
mod tests;

const ANNOUNCE_ACCOUNT_CACHE_SIZE: usize = 10_000;

struct Inner {
    /// Maps an account_id to a peer owning it.
    account_peers: LruCache<AccountId, AnnounceAccount>,
    /// Subset of account_peers, which we have broadcasted to the peers.
    /// It is used to skip rebroadcasting the same data multiple times.
    /// It contains less entries than account_peers in case some AnnounceAccounts
    /// have been loaded from storage without broadcasting.
    account_peers_broadcasted: LruCache<AccountId, AnnounceAccount>,
    /// Access to store on disk
    store: store::Store,
}

impl Inner {
    /// Get AnnounceAccount for the given AccountId.
    fn get_announce(&mut self, account_id: &AccountId) -> Option<AnnounceAccount> {
        if let Some(aa) = self.account_peers.get(account_id) {
            return Some(aa.clone());
        }
        match self.store.get_account_announcement(&account_id) {
            Err(e) => {
                tracing::warn!(target: "network", "Error loading announce account from store: {:?}", e);
                None
            }
            Ok(None) => None,
            Ok(Some(a)) => {
                self.account_peers.put(account_id.clone(), a.clone());
                Some(a)
            }
        }
    }
}

pub(crate) struct Cache(Mutex<Inner>);

impl Cache {
    pub fn new(store: store::Store) -> Self {
        Self(Mutex::new(Inner {
            account_peers: LruCache::new(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            account_peers_broadcasted: LruCache::new(ANNOUNCE_ACCOUNT_CACHE_SIZE),
            store,
        }))
    }

    /// Adds accounts to the cache.
    /// Returns the diff: new values that should be broadcasted.
    /// Note: There is at most one peer id per account id.
    pub(crate) fn add_accounts(&self, aas: Vec<AnnounceAccount>) -> Vec<AnnounceAccount> {
        let mut inner = self.0.lock();
        let mut res = vec![];
        for aa in aas {
            // We skip broadcasting stuff that is already broadcasted.
            if inner.account_peers_broadcasted.get(&aa.account_id).map(|x| &x.epoch_id)
                == Some(&aa.epoch_id)
            {
                continue;
            }
            inner.account_peers.put(aa.account_id.clone(), aa.clone());
            inner.account_peers_broadcasted.put(aa.account_id.clone(), aa.clone());
            // Add account to store. Best effort
            if let Err(e) = inner.store.set_account_announcement(&aa.account_id, &aa) {
                tracing::warn!(target: "network", "Error saving announce account to store: {:?}", e);
            }
            res.push(aa);
        }
        res
    }

    /// Find peer that owns this AccountId.
    pub(crate) fn get_account_owner(&self, account_id: &AccountId) -> Option<PeerId> {
        self.0.lock().get_announce(account_id).map(|announce_account| announce_account.peer_id)
    }

    /// Public interface for `account_peers`.
    /// Get keys currently on cache.
    pub(crate) fn get_accounts_keys(&self) -> Vec<AccountId> {
        self.0.lock().account_peers.iter().map(|(k, _v)| k).cloned().collect()
    }

    /// Get announce accounts on cache.
    pub(crate) fn get_announcements(&self) -> Vec<AnnounceAccount> {
        self.0.lock().account_peers.iter().map(|(_, v)| v.clone()).collect()
    }

    /// Get AnnounceAccount for the given AccountIds, that we already broadcasted.
    pub(crate) fn get_broadcasted_announcements<'a>(
        &'a self,
        account_ids: impl Iterator<Item = &'a AccountId>,
    ) -> HashMap<AccountId, AnnounceAccount> {
        let mut inner = self.0.lock();
        account_ids
            .filter_map(|id| {
                inner.account_peers_broadcasted.get(id).map(|a| (id.clone(), a.clone()))
            })
            .collect()
    }
}