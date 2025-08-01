use near_async::messaging::CanSend;
use near_async::time::{Clock, Duration, Utc};
use near_chain::{Chain, ChainStoreAccess};
use near_client_primitives::types::SyncStatus;
use near_network::types::PeerManagerMessageRequest;
use near_network::types::{HighestHeightPeerInfo, NetworkRequests, PeerManagerAdapter};
use near_primitives::block::Tip;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use rand::seq::SliceRandom;
use rand::thread_rng;
use std::cmp::min;
use tracing::{debug, warn};

/// Maximum number of block headers send over the network.
pub const MAX_BLOCK_HEADERS: u64 = 512;

/// Maximum number of block header hashes to send as part of a locator.
pub const MAX_BLOCK_HEADER_HASHES: usize = 20;

pub const NS_PER_SECOND: u128 = 1_000_000_000;

/// Progress of downloading the currently requested batch of headers.
struct BatchProgress {
    /// An intermediate timeout by which a certain number of headers is expected.
    timeout: Utc,
    /// Height expected at the moment of `timeout`.
    expected_height: BlockHeight,
    /// Header head height at the moment this batch was requested.
    header_head_height: BlockHeight,
    highest_height_of_peers: BlockHeight,
}

/// Helper to keep track of sync headers.
/// Handles major re-orgs by finding closest header that matches and re-downloading headers from that point.
pub struct HeaderSync {
    clock: Clock,

    network_adapter: PeerManagerAdapter,

    /// Progress of downloading the currently requested batch of headers.
    // TODO: Change type to Option<BatchProgress>.
    batch_progress: BatchProgress,

    /// Peer from which the next batch of headers was requested.
    syncing_peer: Option<HighestHeightPeerInfo>,

    /// When the stalling was first detected.
    stalling_ts: Option<Utc>,

    /// How much time to wait after initial header sync.
    initial_timeout: Duration,

    /// How much time to wait after some progress is made in header sync.
    progress_timeout: Duration,

    /// How much time to wait before banning a peer in header sync if sync is too slow.
    stall_ban_timeout: Duration,

    /// Expected increase of header head height per second during header sync
    expected_height_per_second: u64,

    /// Not for production use.
    /// Expected height when node will be automatically shut down, so header
    /// sync can be stopped.
    shutdown_height: near_chain_configs::MutableConfigValue<Option<BlockHeight>>,
}

impl HeaderSync {
    pub fn new(
        clock: Clock,
        network_adapter: PeerManagerAdapter,
        initial_timeout: Duration,
        progress_timeout: Duration,
        stall_ban_timeout: Duration,
        expected_height_per_second: u64,
        shutdown_height: near_chain_configs::MutableConfigValue<Option<BlockHeight>>,
    ) -> Self {
        HeaderSync {
            clock: clock.clone(),
            network_adapter,
            batch_progress: BatchProgress {
                timeout: clock.now_utc(),
                expected_height: 0,
                header_head_height: 0,
                highest_height_of_peers: 0,
            },
            syncing_peer: None,
            stalling_ts: None,
            initial_timeout,
            progress_timeout,
            stall_ban_timeout,
            expected_height_per_second,
            shutdown_height,
        }
    }

    /// Can update `sync_status` to `HeaderSync`.
    /// Can request a new batch of headers from a peer.
    /// This function won't tell you that header sync is complete.
    pub fn run(
        &mut self,
        sync_status: &mut SyncStatus,
        chain: &Chain,
        highest_height: BlockHeight,
        highest_height_peers: &[HighestHeightPeerInfo],
    ) -> Result<(), near_chain::Error> {
        let _span =
            tracing::debug_span!(target: "sync", "run_sync", sync_type = "HeaderSync").entered();
        let head = chain.head()?;
        let header_head = chain.header_head()?;

        // Check if we need to start a new request for a batch of header.
        if !self.header_sync_due(sync_status, &header_head, highest_height) {
            // Either
            // * header sync is not needed, or
            // * a request is already in-flight and more progress is expected.
            return Ok(());
        }

        // TODO: Why call `header_sync_due()` if that decision can be overridden here?
        let enable_header_sync = match sync_status {
            SyncStatus::HeaderSync { .. }
            | SyncStatus::BlockSync { .. }
            | SyncStatus::EpochSyncDone
            | SyncStatus::StateSyncDone => {
                // TODO: Transitioning from BlockSync to HeaderSync is fine if the highest height of peers gets too far from our header_head_height. However it's currently unconditional.
                true
            }
            SyncStatus::NoSync | SyncStatus::AwaitingPeers => {
                debug!(target: "sync", "Sync: initial transition to Header sync. Header head {} at {}",
                    header_head.last_block_hash, header_head.height,
                );
                true
            }
            SyncStatus::EpochSync { .. } | SyncStatus::StateSync { .. } => false,
        };

        if !enable_header_sync {
            // Header sync is blocked for whatever reason.
            return Ok(());
        }

        // start_height is used to report the progress of header sync, e.g. to say that it's 50% complete.
        // This number has no other functional value.
        let start_height = sync_status.start_height().unwrap_or(head.height);

        sync_status.update(SyncStatus::HeaderSync {
            start_height,
            current_height: header_head.height,
            highest_height,
        });

        self.syncing_peer = None;
        // Pick a new random peer to request the next batch of headers.
        if let Some(peer) = highest_height_peers.choose(&mut thread_rng()).cloned() {
            let shutdown_height = self.shutdown_height.get().unwrap_or(u64::MAX);
            let highest_height = peer.highest_block_height.min(shutdown_height);
            if highest_height > header_head.height {
                self.request_headers(chain, &peer)?;
                self.syncing_peer = Some(peer);
            }
        }
        Ok(())
    }

    /// Returns the height that we expect to reach starting from `old_height` after `time_delta`.
    fn compute_expected_height(
        &self,
        old_height: BlockHeight,
        time_delta: Duration,
    ) -> BlockHeight {
        (old_height as u128
            + (time_delta.whole_nanoseconds() as u128 * self.expected_height_per_second as u128
                / NS_PER_SECOND)) as u64
    }

    /// Returns whether a new batch of headers needs to be requested.
    // Checks whether the batch of headers is completely downloaded, or if the peer failed to satisfy our expectations for long enough.
    // If yes, then returns true to request a new batch of headers. Maybe bans a peer.
    // Otherwise, returns false to indicate that we're expecting more headers from the same requested batch.
    // TODO: This function should check the difference between the current header_head height and the highest height of the peers.
    // TODO: Triggering header sync to get 1 header (or even 0 headers) makes little sense.
    pub(crate) fn header_sync_due(
        &mut self,
        sync_status: &SyncStatus,
        header_head: &Tip,
        highest_height: BlockHeight,
    ) -> bool {
        let now = self.clock.now_utc();
        let BatchProgress {
            timeout,
            expected_height: old_expected_height,
            header_head_height: prev_height,
            highest_height_of_peers: prev_highest_height,
        } = self.batch_progress;
        // Received all headers from a batch requested on the previous iteration.
        // Can proceed to the next iteration.
        let all_headers_received =
            header_head.height >= min(prev_height + MAX_BLOCK_HEADERS - 4, prev_highest_height);

        // Did we receive as many headers as we expected from the peer?
        // If not, consider the peer stalling.
        // This can be either the initial timeout, or any of the progress timeouts after the initial timeout.
        let stalling = header_head.height <= old_expected_height && now > timeout;

        // Always enable header sync if we're able to do header sync but are not doing it already.
        let force_sync = match sync_status {
            SyncStatus::NoSync | SyncStatus::AwaitingPeers | SyncStatus::EpochSyncDone => true,
            _ => false,
        };

        if force_sync || all_headers_received || stalling {
            // Request a new batch of headers.

            self.batch_progress = BatchProgress {
                timeout: now + self.initial_timeout,
                expected_height: self
                    .compute_expected_height(header_head.height, self.initial_timeout),
                header_head_height: header_head.height,
                highest_height_of_peers: highest_height,
            };

            // Record the timestamp when the stalling was first noticed.
            if stalling {
                if self.stalling_ts.is_none() {
                    self.stalling_ts = Some(now);
                }
            } else {
                self.stalling_ts = None;
            }

            if all_headers_received {
                // As the batch of headers is received completely, reset the stalling timestamp.
                self.stalling_ts = None;
            } else {
                if let Some(ref stalling_ts) = self.stalling_ts {
                    // syncing_peer is expected to be present.
                    if let Some(ref peer) = self.syncing_peer {
                        match sync_status {
                            SyncStatus::HeaderSync { highest_height, .. } => {
                                if now > *stalling_ts + self.stall_ban_timeout
                                    && *highest_height == peer.highest_block_height
                                {
                                    // This message is used in sync_ban.py test. Consider checking there as well if you change it.
                                    // The peer is one of the peers with the highest height, but we consider the peer stalling.
                                    warn!(target: "sync", "Sync: ban a peer: {}, for not providing enough headers. Peer's height:  {}", peer.peer_info, peer.highest_block_height);
                                    // Ban the peer, which blocks all interactions with the peer for some time.
                                    // TODO: Consider not banning straightaway, but give a node a few attempts before banning it.
                                    // TODO: Prefer not to request the next batch of headers from the same peer.
                                    self.network_adapter.send(
                                        PeerManagerMessageRequest::NetworkRequests(
                                            NetworkRequests::BanPeer {
                                                peer_id: peer.peer_info.id.clone(),
                                                ban_reason: near_network::types::ReasonForBan::ProvidedNotEnoughHeaders,
                                            },
                                        ),
                                    );
                                    // Will retry without this peer.
                                    self.syncing_peer = None;
                                    return false;
                                }
                            }
                            _ => {
                                // Unexpected
                            }
                        }
                    }
                }
            }
            self.syncing_peer = None;
            // Return true to request a new batch of headers.
            true
        } else {
            // Manage the currently requested batch of headers.
            // Note that it is guaranteed that `now < timeout`, because otherwise it will be `stalling` or `all_headers_received`.

            // Resetting the timeout as long as we make progress.
            if self.made_enough_progress(header_head.height, old_expected_height, now, timeout) {
                // Update our expectation.
                // `new_expected_height` can be beyond the requested batch of header, but that is fine.
                let new_expected_height =
                    self.compute_expected_height(header_head.height, self.progress_timeout);
                self.batch_progress = BatchProgress {
                    timeout: now + self.progress_timeout,
                    expected_height: new_expected_height,
                    header_head_height: prev_height,
                    highest_height_of_peers: prev_highest_height,
                };
            }
            // Keep getting headers from the same batch.
            // Don't request a new batch of headers.
            false
        }
    }

    /// Checks whether the node made enough progress.
    /// Returns true iff it needs less time than (timeout-now) to get (expected_height - current_height) headers at the rate of `expected_height_per_second` headers per second.
    fn made_enough_progress(
        &self,
        current_height: BlockHeight,
        expected_height: BlockHeight,
        now: Utc,
        timeout: Utc,
    ) -> bool {
        if now <= timeout {
            self.compute_expected_height(current_height, timeout - now) >= expected_height
        } else {
            current_height >= expected_height
        }
    }

    /// Request headers from a given peer to advance the chain.
    fn request_headers(
        &self,
        chain: &Chain,
        peer: &HighestHeightPeerInfo,
    ) -> Result<(), near_chain::Error> {
        let locator = self.get_locator(chain)?;
        debug!(target: "sync", "Sync: request headers: asking {} for headers, {:?}", peer.peer_info.id, locator);
        self.network_adapter.send(PeerManagerMessageRequest::NetworkRequests(
            NetworkRequests::BlockHeadersRequest {
                hashes: locator,
                peer_id: peer.peer_info.id.clone(),
            },
        ));
        Ok(())
    }

    // The remote side will return MAX_BLOCK_HEADERS headers, starting from the first hash in
    // the returned "locator" list that is on their canonical chain.
    //
    // The locator allows us to start syncing from a reasonably recent common ancestor. Since
    // we don't know which fork the remote side is on, we include a few hashes. The first one
    // we include is the tip of our chain, and the next one is 2 blocks back (on the same chain,
    // by number of blocks (or in other words, by ordinals), not by height), then 4 blocks
    // back, then 8 blocks back, etc, until we reach the most recent final block. The reason
    // why we stop at the final block is because the consensus guarantees us that the final
    // blocks observed by all nodes are on the same fork.
    fn get_locator(&self, chain: &Chain) -> Result<Vec<CryptoHash>, near_chain::Error> {
        let store = chain.chain_store();
        let tip = store.header_head()?;
        // We could just get the ordinal from the header, but it's off by one: #8177.
        // Note: older block headers don't have ordinals, so for them we can't get the ordinal from header,
        // have to use get_block_merkle_tree.
        let tip_ordinal = store.get_block_merkle_tree(&tip.last_block_hash)?.size();
        let final_head = store.final_head()?;
        let final_head_ordinal = store.get_block_merkle_tree(&final_head.last_block_hash)?.size();
        let ordinals = get_locator_ordinals(final_head_ordinal, tip_ordinal);
        let mut locator: Vec<CryptoHash> = vec![];
        for ordinal in &ordinals {
            match store.get_block_hash_from_ordinal(*ordinal) {
                Ok(block_hash) => {
                    locator.push(block_hash);
                }
                Err(e) => {
                    // In the case of epoch sync, it is normal and expected that we will not have
                    // many headers before the tip, so that case is fine.
                    if *ordinal == tip_ordinal {
                        return Err(e);
                    }
                    debug!(target: "sync", "Sync: failed to get block hash from ordinal {}; \
                        this is normal if we just finished epoch sync. Error: {:?}", ordinal, e);
                }
            }
        }
        debug!(target: "sync", "Sync: locator: {:?} ordinals: {:?}", locator, ordinals);
        Ok(locator)
    }
}

/// Step back from highest to lowest ordinal, in powers of 2 steps, limited by MAX_BLOCK_HEADERS
/// heights per step, and limited by MAX_BLOCK_HEADER_HASHES steps in total.
fn get_locator_ordinals(lowest_ordinal: u64, highest_ordinal: u64) -> Vec<u64> {
    let mut current = highest_ordinal;
    let mut ordinals = vec![];
    let mut step = 2;
    while current > lowest_ordinal && ordinals.len() < MAX_BLOCK_HEADER_HASHES as usize - 1 {
        ordinals.push(current);
        if current <= lowest_ordinal + step {
            break;
        }
        current -= step;
        // Do not step back more than MAX_BLOCK_HEADERS, as the gap in between would not
        // allow us to sync to a more recent block.
        step = min(step * 2, MAX_BLOCK_HEADERS);
    }
    ordinals.push(lowest_ordinal);
    ordinals
}

#[cfg(test)]
mod test {
    use near_async::messaging::IntoMultiSender;
    use near_async::time::{Clock, Duration, FakeClock, Utc};
    use near_chain::test_utils::{process_block_sync, setup, setup_with_tx_validity_period};
    use near_chain::types::Tip;
    use near_chain::{BlockProcessingArtifact, Provenance, retrieve_headers};
    use near_chain_configs::MutableConfigValue;
    use near_client_primitives::types::SyncStatus;
    use near_crypto::{KeyType, PublicKey};
    use near_network::test_utils::MockPeerManagerAdapter;
    use near_network::types::{
        BlockInfo, FullPeerInfo, HighestHeightPeerInfo, NetworkRequests, PeerInfo,
    };
    use near_primitives::block::{Approval, Block};
    use near_primitives::genesis::GenesisId;
    use near_primitives::merkle::PartialMerkleTree;
    use near_primitives::network::PeerId;
    use near_primitives::test_utils::TestBlockBuilder;
    use near_primitives::types::EpochId;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;
    use std::sync::Arc;
    use std::thread;

    use crate::sync::header::{HeaderSync, MAX_BLOCK_HEADERS, get_locator_ordinals};

    #[test]
    fn test_get_locator_ordinals() {
        assert_eq!(get_locator_ordinals(0, 0), vec![0]);
        assert_eq!(get_locator_ordinals(0, 1), vec![1, 0]);
        assert_eq!(get_locator_ordinals(0, 2), vec![2, 0]);
        assert_eq!(get_locator_ordinals(0, 3), vec![3, 1, 0]);
        assert_eq!(get_locator_ordinals(0, 10), vec![10, 8, 4, 0]);
        assert_eq!(get_locator_ordinals(0, 100), vec![100, 98, 94, 86, 70, 38, 0]);
        assert_eq!(
            get_locator_ordinals(0, 1000),
            vec![1000, 998, 994, 986, 970, 938, 874, 746, 490, 0]
        );
        // Locator is still reasonable size even given large height.
        assert_eq!(
            get_locator_ordinals(0, 10000),
            vec![
                10000, 9998, 9994, 9986, 9970, 9938, 9874, 9746, 9490, 8978, 8466, 7954, 7442,
                6930, 6418, 5906, 5394, 4882, 4370, 0
            ]
        );
        assert_eq!(get_locator_ordinals(100, 100), vec![100]);
        assert_eq!(get_locator_ordinals(100, 101), vec![101, 100]);
        assert_eq!(get_locator_ordinals(100, 102), vec![102, 100]);
        assert_eq!(get_locator_ordinals(100, 103), vec![103, 101, 100]);
        assert_eq!(get_locator_ordinals(100, 110), vec![110, 108, 104, 100]);
        assert_eq!(get_locator_ordinals(100, 200), vec![200, 198, 194, 186, 170, 138, 100]);
        assert_eq!(
            get_locator_ordinals(20000, 21000),
            vec![21000, 20998, 20994, 20986, 20970, 20938, 20874, 20746, 20490, 20000]
        );
        assert_eq!(
            get_locator_ordinals(20000, 30000),
            vec![
                30000, 29998, 29994, 29986, 29970, 29938, 29874, 29746, 29490, 28978, 28466, 27954,
                27442, 26930, 26418, 25906, 25394, 24882, 24370, 20000
            ]
        );
    }

    /// Starts two chains that fork of genesis and checks that they can sync headers to the longest.
    #[test]
    fn test_sync_headers_fork() {
        let mock_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut header_sync = HeaderSync::new(
            Clock::real(),
            mock_adapter.as_multi_sender(),
            Duration::seconds(10),
            Duration::seconds(2),
            Duration::seconds(120),
            1_000_000_000,
            MutableConfigValue::new(None, "expected_shutdown"),
        );
        let (mut chain, _, _, signer) = setup(Clock::real());
        for _ in 0..3 {
            let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
            // Have gaps in the chain, so we don't have final blocks (i.e. last final block is
            // genesis). Otherwise we violate consensus invariants.
            let block = TestBlockBuilder::new(Clock::real(), &prev, signer.clone())
                .height(prev.header().height() + 2)
                .build();
            process_block_sync(
                &mut chain,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        let (mut chain2, _, _, signer2) = setup(Clock::real());
        for _ in 0..5 {
            let prev = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            // Have gaps in the chain, so we don't have final blocks (i.e. last final block is
            // genesis). Otherwise we violate consensus invariants.
            let block = TestBlockBuilder::new(Clock::real(), &prev, signer2.clone())
                .height(prev.header().height() + 2)
                .build();
            process_block_sync(
                &mut chain2,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        let mut sync_status = SyncStatus::NoSync;
        let peer1 = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: near_network::types::PeerChainInfo {
                genesis_id: GenesisId {
                    chain_id: "unittest".to_string(),
                    hash: *chain.genesis().hash(),
                },
                tracked_shards: vec![],
                archival: false,
                last_block: Some(BlockInfo {
                    height: chain2.head().unwrap().height,
                    hash: chain2.head().unwrap().last_block_hash,
                }),
            },
        };
        let head = chain.head().unwrap();
        assert!(
            header_sync
                .run(
                    &mut sync_status,
                    &mut chain,
                    head.height,
                    &[<FullPeerInfo as Into<Option<_>>>::into(peer1.clone()).unwrap()]
                )
                .is_ok()
        );
        assert!(sync_status.is_syncing());
        // Check that it queried last block, and then stepped down to genesis block to find common block with the peer.

        let item = mock_adapter.pop().unwrap().as_network_requests();
        assert_eq!(
            item,
            NetworkRequests::BlockHeadersRequest {
                // chain is 6 -> 4 -> 2 -> 0.
                hashes: [6, 2, 0]
                    .iter()
                    .map(|i| *chain.get_block_by_height(*i).unwrap().hash())
                    .collect(),
                peer_id: peer1.peer_info.id
            }
        );
    }

    #[test]
    fn test_sync_headers_fork_from_final_block() {
        let mock_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut header_sync = HeaderSync::new(
            Clock::real(),
            mock_adapter.as_multi_sender(),
            Duration::seconds(10),
            Duration::seconds(2),
            Duration::seconds(120),
            1_000_000_000,
            MutableConfigValue::new(None, "expected_shutdown"),
        );
        let (mut chain, _, _, signer) = setup(Clock::real());
        let (mut chain2, _, _, signer2) = setup(Clock::real());
        for chain in [&mut chain, &mut chain2] {
            // Both chains share a common final block at height 3.
            for _ in 0..5 {
                let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
                let block = TestBlockBuilder::new(Clock::real(), &prev, signer.clone()).build();
                process_block_sync(
                    chain,
                    block.into(),
                    Provenance::PRODUCED,
                    &mut BlockProcessingArtifact::default(),
                )
                .unwrap();
            }
        }
        for _ in 0..7 {
            let prev = chain.get_block(&chain.head().unwrap().last_block_hash).unwrap();
            // Test with huge gaps to make sure we are still able to find locators.
            let block = TestBlockBuilder::new(Clock::real(), &prev, signer.clone())
                .height(prev.header().height() + 1000)
                .build();
            process_block_sync(
                &mut chain,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        for _ in 0..3 {
            let prev = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            // Test with huge gaps, but 3 blocks here produce a higher height than the 7 blocks
            // above.
            let block = TestBlockBuilder::new(Clock::real(), &prev, signer2.clone())
                .height(prev.header().height() + 3100)
                .build();
            process_block_sync(
                &mut chain2,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        let mut sync_status = SyncStatus::NoSync;
        let peer1 = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: near_network::types::PeerChainInfo {
                genesis_id: GenesisId {
                    chain_id: "unittest".to_string(),
                    hash: *chain.genesis().hash(),
                },
                tracked_shards: vec![],
                archival: false,
                last_block: Some(BlockInfo {
                    height: chain2.head().unwrap().height,
                    hash: chain2.head().unwrap().last_block_hash,
                }),
            },
        };
        let head = chain.head().unwrap();
        assert!(
            header_sync
                .run(
                    &mut sync_status,
                    &mut chain,
                    head.height,
                    &[<FullPeerInfo as Into<Option<_>>>::into(peer1.clone()).unwrap()]
                )
                .is_ok()
        );
        assert!(sync_status.is_syncing());
        // Check that it queried last block, and then stepped down to genesis block to find common block with the peer.

        let item = mock_adapter.pop().unwrap().as_network_requests();
        assert_eq!(
            item,
            NetworkRequests::BlockHeadersRequest {
                // chain is 7005 -> 6005 -> 5005 -> 4005 -> 3005 -> 2005 -> 1005 -> 5 -> 4 -> 3 -> 2 -> 1 -> 0
                // where 3 is final.
                hashes: [7005, 5005, 1005, 3]
                    .iter()
                    .map(|i| *chain.get_block_by_height(*i).unwrap().hash())
                    .collect(),
                peer_id: peer1.peer_info.id
            }
        );
    }

    /// Sets up `HeaderSync` with particular tolerance for slowness, and makes sure that a peer that
    /// sends headers below the threshold gets banned, and the peer that sends them faster doesn't get
    /// banned.
    /// Also makes sure that if `header_sync_due` is checked more frequently than the `progress_timeout`
    /// the peer doesn't get banned. (specifically, that the expected height downloaded gets properly
    /// adjusted for time passed)
    #[test]
    fn slow_test_slow_header_sync() {
        let network_adapter = Arc::new(MockPeerManagerAdapter::default());
        let highest_height = 1000;

        // Setup header_sync with expectation of 25 headers/second
        let mut header_sync = HeaderSync::new(
            Clock::real(),
            network_adapter.as_multi_sender(),
            Duration::seconds(1),
            Duration::seconds(1),
            Duration::seconds(3),
            25,
            MutableConfigValue::new(None, "expected_shutdown"),
        );

        let set_syncing_peer = |header_sync: &mut HeaderSync| {
            header_sync.syncing_peer = Some(HighestHeightPeerInfo {
                peer_info: PeerInfo {
                    id: PeerId::new(PublicKey::empty(KeyType::ED25519)),
                    addr: None,
                    account_id: None,
                },
                genesis_id: Default::default(),
                highest_block_height: 0,
                highest_block_hash: Default::default(),
                tracked_shards: vec![],
                archival: false,
            });
            header_sync.syncing_peer.as_mut().unwrap().highest_block_height = highest_height;
        };
        set_syncing_peer(&mut header_sync);

        let (chain, _, _, signer) = setup(Clock::real());
        let genesis = chain.get_block(&chain.genesis().hash().clone()).unwrap();

        let mut last_block = &genesis;
        let mut all_blocks = vec![];
        for i in 0..61 {
            let current_height = 3 + i * 5;
            let block = TestBlockBuilder::new(Clock::real(), last_block, signer.clone())
                .height(current_height)
                .build();
            all_blocks.push(block);
            last_block = &all_blocks[all_blocks.len() - 1];
        }

        let mut last_added_block_ord = 0;
        // First send 30 heights every second for a while and make sure it doesn't get
        // banned
        for _iter in 0..12 {
            let block = &all_blocks[last_added_block_ord];
            let current_height = block.header().height();
            set_syncing_peer(&mut header_sync);
            header_sync.header_sync_due(
                &SyncStatus::HeaderSync {
                    start_height: current_height,
                    current_height,
                    highest_height,
                },
                &Tip::from_header(block.header()),
                highest_height,
            );

            last_added_block_ord += 3;

            thread::sleep(std::time::Duration::from_millis(500));
        }
        // 6 blocks / second is fast enough, we should not have banned the peer
        assert!(network_adapter.requests.read().is_empty());

        // Now the same, but only 20 heights / sec
        for _iter in 0..12 {
            let block = &all_blocks[last_added_block_ord];
            let current_height = block.header().height();
            set_syncing_peer(&mut header_sync);
            header_sync.header_sync_due(
                &SyncStatus::HeaderSync {
                    start_height: current_height,
                    current_height,
                    highest_height,
                },
                &Tip::from_header(block.header()),
                highest_height,
            );

            last_added_block_ord += 2;

            thread::sleep(std::time::Duration::from_millis(500));
        }
        // This time the peer should be banned, because 4 blocks/s is not fast enough
        let ban_peer = network_adapter.requests.write().pop_back().unwrap();

        if let NetworkRequests::BanPeer { .. } = ban_peer.as_network_requests() {
            /* expected */
        } else {
            assert!(false);
        }
    }

    #[test]
    fn slow_test_sync_from_very_behind() {
        let mock_adapter = Arc::new(MockPeerManagerAdapter::default());
        let mut header_sync = HeaderSync::new(
            Clock::real(),
            mock_adapter.as_multi_sender(),
            Duration::seconds(10),
            Duration::seconds(2),
            Duration::seconds(120),
            1_000_000_000,
            MutableConfigValue::new(None, "expected_shutdown"),
        );

        let clock = FakeClock::new(Utc::UNIX_EPOCH);
        // Don't bother with epoch switches. It's not relevant.
        let (mut chain, _, _, _) = setup_with_tx_validity_period(clock.clock(), 100, 10000);
        let (mut chain2, _, _, signer2) = setup_with_tx_validity_period(clock.clock(), 100, 10000);
        // Set up the second chain with 2000+ blocks.
        let mut block_merkle_tree = PartialMerkleTree::default();
        block_merkle_tree.insert(*chain.genesis().hash()); // for genesis block
        for _ in 0..(4 * MAX_BLOCK_HEADERS + 10) {
            let last_block = chain2.get_block(&chain2.head().unwrap().last_block_hash).unwrap();
            let this_height = last_block.header().height() + 1;
            let (epoch_id, next_epoch_id) = if last_block.header().is_genesis() {
                (*last_block.header().next_epoch_id(), EpochId(*last_block.hash()))
            } else {
                (*last_block.header().epoch_id(), *last_block.header().next_epoch_id())
            };
            let block = Arc::new(Block::produce(
                PROTOCOL_VERSION,
                last_block.header(),
                this_height,
                last_block.header().block_ordinal() + 1,
                last_block.chunks().iter_raw().cloned().collect(),
                vec![vec![]; last_block.chunks().len()],
                epoch_id,
                next_epoch_id,
                None,
                [&signer2]
                    .iter()
                    .map(|signer| {
                        Some(Box::new(
                            Approval::new(
                                *last_block.hash(),
                                last_block.header().height(),
                                this_height,
                                signer.as_ref(),
                            )
                            .signature,
                        ))
                    })
                    .collect(),
                Ratio::new(0, 1),
                0,
                100,
                Some(0),
                signer2.as_ref(),
                *last_block.header().next_bp_hash(),
                block_merkle_tree.root(),
                clock.clock(),
                None,
                None,
                vec![],
            ));
            block_merkle_tree.insert(*block.hash());
            chain2.process_block_header(block.header()).unwrap(); // just to validate
            process_block_sync(
                &mut chain2,
                block.into(),
                Provenance::PRODUCED,
                &mut BlockProcessingArtifact::default(),
            )
            .unwrap();
        }
        let mut sync_status = SyncStatus::NoSync;
        let peer1 = FullPeerInfo {
            peer_info: PeerInfo::random(),
            chain_info: near_network::types::PeerChainInfo {
                genesis_id: GenesisId {
                    chain_id: "unittest".to_string(),
                    hash: *chain.genesis().hash(),
                },
                tracked_shards: vec![],
                archival: false,
                last_block: Some(BlockInfo {
                    height: chain2.head().unwrap().height,
                    hash: chain2.head().unwrap().last_block_hash,
                }),
            },
        };
        // It should be done in 5 iterations, but give it 10 iterations just in case it would
        // get into an infinite loop because of some bug and cause the test to hang.
        for _ in 0..10 {
            let header_head = chain.header_head().unwrap();
            if header_head.last_block_hash == chain2.header_head().unwrap().last_block_hash {
                // sync is done.
                break;
            }
            assert!(
                header_sync
                    .run(
                        &mut sync_status,
                        &mut chain,
                        header_head.height,
                        &[<FullPeerInfo as Into<Option<_>>>::into(peer1.clone()).unwrap()]
                    )
                    .is_ok()
            );
            match sync_status {
                SyncStatus::HeaderSync { .. } => {}
                _ => panic!("Unexpected sync status: {:?}", sync_status),
            }
            let message = match mock_adapter.pop() {
                Some(message) => message.as_network_requests(),
                None => {
                    panic!("No message was sent; current height: {}", header_head.height);
                }
            };
            match message {
                NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                    assert_eq!(peer_id, peer1.peer_info.id);
                    let headers =
                        retrieve_headers(chain2.chain_store(), hashes, MAX_BLOCK_HEADERS).unwrap();
                    assert!(!headers.is_empty(), "No headers were returned");
                    match chain.sync_block_headers(headers) {
                        Ok(_) => {}
                        Err(e) => {
                            panic!("Error inserting headers: {:?}", e);
                        }
                    }
                }
                _ => panic!("Unexpected network message: {:?}", message),
            }
            if chain.header_head().unwrap().height <= header_head.height {
                panic!(
                    "Syncing is not making progress. Head was not updated from {}",
                    header_head.height
                );
            }
        }
        let new_tip = chain.header_head().unwrap();
        assert_eq!(new_tip.last_block_hash, chain2.head().unwrap().last_block_hash);
    }
}
