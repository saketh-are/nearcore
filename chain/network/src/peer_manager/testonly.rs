use crate::PeerManagerActor;
use crate::accounts_data::AccountDataCacheSnapshot;
use crate::broadcast;
use crate::client::ClientSenderForNetworkInput;
use crate::client::ClientSenderForNetworkMessage;
use crate::client::StateRequestPart;
use crate::client::StateResponse;
use crate::config;
use crate::network_protocol::SnapshotHostInfo;
use crate::network_protocol::StateResponseInfo;
use crate::network_protocol::StateResponseInfoV2;
use crate::network_protocol::SyncSnapshotHosts;
use crate::network_protocol::testonly as data;
use crate::network_protocol::{
    EdgeState, Encoding, PeerInfo, PeerMessage, SignedAccountData, SyncAccountsData,
};
use crate::peer;
use crate::peer::peer_actor::ClosingReason;
use crate::peer_manager::network_state::NetworkState;
use crate::peer_manager::peer_manager_actor::Event as PME;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::snapshot_hosts::SnapshotHostsCache;
use crate::state_witness::PartialWitnessSenderForNetworkInput;
use crate::state_witness::PartialWitnessSenderForNetworkMessage;
use crate::tcp;
use crate::test_utils;
use crate::testonly::actix::ActixSystem;
use crate::types::StateRequestSenderForNetworkInput;
use crate::types::StateRequestSenderForNetworkMessage;
use crate::types::{
    AccountKeys, ChainInfo, KnownPeerStatus, NetworkRequests, PeerManagerMessageRequest,
    PeerManagerSenderForNetworkInput, PeerManagerSenderForNetworkMessage, ReasonForBan,
};
use futures::FutureExt;
use near_async::messaging::IntoMultiSender;
use near_async::messaging::Sender;
use near_async::time;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::state_sync::ShardStateSyncResponse;
use near_primitives::state_sync::ShardStateSyncResponseV2;
use near_primitives::types::AccountId;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// cspell:ignore eventfd epoll fcntl socketpair
/// Each actix arbiter (in fact, the underlying tokio runtime) creates 4 file descriptors:
/// 1. eventfd2()
/// 2. epoll_create1()
/// 3. fcntl() duplicating one end of some globally shared socketpair()
/// 4. fcntl() duplicating epoll socket created in (2)
/// This gives 5 file descriptors per PeerActor (4 + 1 TCP socket).
pub(crate) const FDS_PER_PEER: usize = 5;

#[derive(actix::Message)]
#[rtype("()")]
struct WithNetworkState(
    Box<dyn Send + FnOnce(Arc<NetworkState>) -> Pin<Box<dyn Send + 'static + Future<Output = ()>>>>,
);

impl actix::Handler<WithNetworkState> for PeerManagerActor {
    type Result = ();
    fn handle(
        &mut self,
        WithNetworkState(f): WithNetworkState,
        _: &mut Self::Context,
    ) -> Self::Result {
        assert!(actix::Arbiter::current().spawn(f(self.state.clone())));
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    ShardsManager(ShardsManagerRequestFromNetwork),
    Client(ClientSenderForNetworkInput),
    StateRequestSender(StateRequestSenderForNetworkInput),
    PeerManager(PME),
    PeerManagerSender(PeerManagerSenderForNetworkInput),
    PartialWitness(PartialWitnessSenderForNetworkInput),
}

pub(crate) struct ActorHandler {
    pub cfg: config::NetworkConfig,
    pub events: broadcast::Receiver<Event>,
    pub actix: ActixSystem<PeerManagerActor>,
}

pub(crate) fn unwrap_sync_accounts_data_processed(ev: Event) -> Option<SyncAccountsData> {
    match ev {
        Event::PeerManager(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncAccountsData(msg),
        )) => Some(msg),
        _ => None,
    }
}

pub(crate) fn unwrap_sync_snapshot_hosts_data_processed(ev: Event) -> Option<SyncSnapshotHosts> {
    match ev {
        Event::PeerManager(PME::MessageProcessed(
            tcp::Tier::T2,
            PeerMessage::SyncSnapshotHosts(msg),
        )) => Some(msg),
        _ => None,
    }
}

pub(crate) fn make_chain_info(
    chain: &data::Chain,
    validators: &[&config::NetworkConfig],
) -> ChainInfo {
    // Construct ChainInfo with tier1_accounts set to `validators`.
    let mut chain_info = chain.get_chain_info();
    let mut account_keys = AccountKeys::new();
    for cfg in validators {
        let s = &cfg.validator.signer.get().unwrap();
        account_keys.entry(s.validator_id().clone()).or_default().insert(s.public_key());
    }
    chain_info.tier1_accounts = Arc::new(account_keys);
    chain_info
}

pub(crate) struct RawConnection {
    pub events: broadcast::Receiver<Event>,
    pub stream: tcp::Stream,
    pub cfg: peer::testonly::PeerConfig,
}

impl RawConnection {
    pub async fn handshake(mut self, clock: &time::Clock) -> peer::testonly::PeerHandle {
        let stream_id = self.stream.id();
        let mut peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;

        // Wait for the new peer to complete the handshake.
        peer.complete_handshake().await;

        // Wait for the peer manager to complete the handshake.
        self.events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    panic!("handshake aborted: {}", ev.reason)
                }
                _ => None,
            })
            .await;
        peer
    }

    // Try to perform a handshake. PeerManager is expected to reject the handshake.
    pub async fn manager_fail_handshake(mut self, clock: &time::Clock) -> ClosingReason {
        let stream_id = self.stream.id();
        let peer =
            peer::testonly::PeerHandle::start_endpoint(clock.clone(), self.cfg, self.stream).await;
        let reason = self
            .events
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(ev.reason)
                }
                Event::PeerManager(PME::HandshakeCompleted(ev)) if ev.stream_id == stream_id => {
                    panic!("PeerManager accepted the handshake")
                }
                _ => None,
            })
            .await;
        drop(peer);
        reason
    }
}

impl ActorHandler {
    pub fn peer_info(&self) -> PeerInfo {
        PeerInfo {
            id: PeerId::new(self.cfg.node_key.public_key()),
            addr: self.cfg.node_addr.as_ref().map(|a| **a),
            account_id: None,
        }
    }

    pub async fn send_outbound_connect(&self, peer_info: &PeerInfo, tier: tcp::Tier) {
        let addr = self.actix.addr.clone();
        let peer_info = peer_info.clone();
        let stream = tcp::Stream::connect(&peer_info, tier, &config::SocketOptions::default())
            .await
            .unwrap();
        addr.do_send(PeerManagerMessageRequest::OutboundTcpConnect(stream));
    }

    pub fn connect_to(
        &self,
        peer_info: &PeerInfo,
        tier: tcp::Tier,
    ) -> impl 'static + Send + Future<Output = tcp::StreamId> {
        let addr = self.actix.addr.clone();
        let events = self.events.clone();
        let peer_info = peer_info.clone();
        async move {
            let stream = tcp::Stream::connect(&peer_info, tier, &config::SocketOptions::default())
                .await
                .unwrap();
            let mut events = events.from_now();
            let stream_id = stream.id();
            addr.do_send(PeerManagerMessageRequest::OutboundTcpConnect(stream));
            events
                .recv_until(|ev| match &ev {
                    Event::PeerManager(PME::HandshakeCompleted(ev))
                        if ev.stream_id == stream_id =>
                    {
                        Some(stream_id)
                    }
                    Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                        panic!("PeerManager rejected the handshake")
                    }
                    _ => None,
                })
                .await
        }
    }

    pub async fn with_state<R: 'static + Send, Fut: 'static + Send + Future<Output = R>>(
        &self,
        f: impl 'static + Send + FnOnce(Arc<NetworkState>) -> Fut,
    ) -> R {
        let (send, recv) = tokio::sync::oneshot::channel();
        self.actix
            .addr
            .send(WithNetworkState(Box::new(|s| {
                Box::pin(async { send.send(f(s).await).ok().unwrap() })
            })))
            .await
            .unwrap();
        recv.await.unwrap()
    }

    pub async fn start_inbound(
        &self,
        chain: Arc<data::Chain>,
        network_cfg: config::NetworkConfig,
    ) -> RawConnection {
        // To avoid race condition:
        // 1. reserve a TCP port
        // 2. snapshot event stream
        // 3. establish connection.
        let socket = tcp::Socket::bind();
        let events = self.events.from_now();
        let stream = socket.connect(&self.peer_info(), tcp::Tier::T2).await;
        let stream_id = stream.id();
        let conn = RawConnection {
            events,
            stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                force_encoding: Some(Encoding::Proto),
            },
        };
        // Wait until the TCP connection is accepted or rejected.
        // The Handshake is not performed yet.
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeStarted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                _ => None,
            })
            .await;
        conn
    }

    pub async fn start_outbound(
        &self,
        chain: Arc<data::Chain>,
        network_cfg: config::NetworkConfig,
        tier: tcp::Tier,
    ) -> RawConnection {
        let (outbound_stream, inbound_stream) =
            tcp::Stream::loopback(network_cfg.node_id(), tier).await;
        let stream_id = outbound_stream.id();
        let events = self.events.from_now();
        self.actix.addr.do_send(PeerManagerMessageRequest::OutboundTcpConnect(outbound_stream));
        let conn = RawConnection {
            events,
            stream: inbound_stream,
            cfg: peer::testonly::PeerConfig {
                network: network_cfg,
                chain,
                force_encoding: Some(Encoding::Proto),
            },
        };
        // Wait until the handshake started or connection is closed.
        // The Handshake is not performed yet.
        conn.events
            .clone()
            .recv_until(|ev| match ev {
                Event::PeerManager(PME::HandshakeStarted(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                Event::PeerManager(PME::ConnectionClosed(ev)) if ev.stream_id == stream_id => {
                    Some(())
                }
                _ => None,
            })
            .await;
        conn
    }

    /// Checks internal consistency of the PeerManagerActor.
    /// This is a partial implementation, add more invariant checks
    /// if needed.
    pub async fn check_consistency(&self) {
        self.with_state(|s| async move {
            // Check that the set of ready connections matches the PeerStore state.
            let tier2: HashSet<_> = s.tier2.load().ready.keys().cloned().collect();
            let store: HashSet<_> = s
                .peer_store
                .dump()
                .into_iter()
                .filter_map(|state| {
                    if state.status == KnownPeerStatus::Connected {
                        Some(state.peer_info.id)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(tier2, store);

            // Check that the local_edges of the graph match the TIER2 connection pool.
            let node_id = s.config.node_id();
            let local_edges: HashSet<_> = s
                .graph
                .load()
                .local_edges
                .values()
                .filter_map(|e| match e.edge_type() {
                    EdgeState::Active => Some(e.other(&node_id).unwrap().clone()),
                    EdgeState::Removed => None,
                })
                .collect();
            assert_eq!(tier2, local_edges);
        })
        .await
    }

    pub async fn fix_local_edges(&self, clock: &time::Clock, timeout: time::Duration) {
        let clock = clock.clone();
        self.with_state(move |s| async move { s.fix_local_edges(&clock, timeout).await }).await
    }

    pub async fn set_chain_info(&self, chain_info: ChainInfo) -> bool {
        self.with_state(move |s| async move { s.set_chain_info(chain_info) }).await
    }

    pub async fn tier1_advertise_proxies(
        &self,
        clock: &time::Clock,
    ) -> Option<Arc<SignedAccountData>> {
        let clock = clock.clone();
        self.with_state(move |s| async move { s.tier1_advertise_proxies(&clock).await }).await
    }

    pub async fn disconnect(&self, peer_id: &PeerId) {
        let peer_id = peer_id.clone();
        self.with_state(move |s| async move {
            let num_stopped = s
                .tier2
                .load()
                .ready
                .values()
                .filter(|c| c.peer_info.id == peer_id)
                .map(|c| {
                    c.stop(None);
                    ()
                })
                .count();
            assert_eq!(num_stopped, 1);
        })
        .await
    }

    pub async fn disconnect_and_ban(
        &self,
        clock: &time::Clock,
        peer_id: &PeerId,
        reason: ReasonForBan,
    ) {
        // TODO(gprusak): make it wait asynchronously for the connection to get closed.
        // TODO(gprusak): figure out how to await for both ends to disconnect.
        let clock = clock.clone();
        let peer_id = peer_id.clone();
        self.with_state(move |s| async move { s.disconnect_and_ban(&clock, &peer_id, reason) })
            .await
    }

    pub async fn peer_store_update(&self, clock: &time::Clock) {
        let clock = clock.clone();
        self.with_state(move |s| async move { s.peer_store.update(&clock) }).await;
    }

    pub async fn send_ping(&self, clock: &time::Clock, nonce: u64, target: PeerId) {
        let clock = clock.clone();
        self.with_state(move |s| async move {
            s.send_ping(&clock, tcp::Tier::T2, nonce, target);
        })
        .await;
    }

    pub async fn announce_account(&self, aa: AnnounceAccount) {
        self.actix
            .addr
            .send(PeerManagerMessageRequest::NetworkRequests(NetworkRequests::AnnounceAccount(aa)))
            .await
            .unwrap();
    }

    // Awaits until the accounts_data state satisfies predicate `pred`.
    pub async fn wait_for_accounts_data_pred(
        &self,
        pred: impl Fn(Arc<AccountDataCacheSnapshot>) -> bool,
    ) {
        let mut events = self.events.from_now();
        loop {
            let got = self.with_state(move |s| async move { s.accounts_data.load() }).await;
            if pred(got) {
                break;
            }
            // It is important that we wait for the next PeerMessage::SyncAccountsData to get
            // PROCESSED, not just RECEIVED. Otherwise we would get a race condition.
            events.recv_until(unwrap_sync_accounts_data_processed).await;
        }
    }

    // Awaits until the accounts_data state matches `want`.
    pub async fn wait_for_accounts_data(&self, want: &HashSet<Arc<SignedAccountData>>) {
        self.wait_for_accounts_data_pred(|cache| {
            &cache.data.values().cloned().collect::<HashSet<_>>() == want
        })
        .await
    }

    // Awaits until the snapshot_hosts state satisfies predicate `pred`.
    pub async fn wait_for_snapshot_hosts_pred(
        &self,
        pred: impl Fn(Arc<SnapshotHostsCache>) -> bool,
    ) {
        let mut events = self.events.from_now();
        loop {
            let got = self.with_state(move |s| async move { s.snapshot_hosts.clone() }).await;
            if pred(got) {
                break;
            }

            // If the state doesn't match, wait until the next snapshot_hosts event is processed and check again.
            events.recv_until(unwrap_sync_snapshot_hosts_data_processed).await;
        }
    }

    // Awaits until the snapshot_hosts state matches `want`.
    pub async fn wait_for_snapshot_hosts(&self, want: &HashSet<Arc<SnapshotHostInfo>>) {
        self.wait_for_snapshot_hosts_pred(|cache| {
            &cache.get_hosts().into_iter().collect::<HashSet<_>>() == want
        })
        .await
    }

    pub async fn wait_for_direct_connection(&self, target_peer_id: PeerId) {
        let mut events = self.events.from_now();
        loop {
            let connections =
                self.with_state(|s| async move { s.tier2.load().ready.clone() }).await;

            if connections.contains_key(&target_peer_id) {
                return;
            }

            events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::HandshakeCompleted { .. }) => Some(()),
                    _ => None,
                })
                .await;
        }
    }

    // Awaits until the routing_table matches `want`.
    pub async fn wait_for_routing_table(&self, want: &[(PeerId, Vec<PeerId>)]) {
        let mut events = self.events.from_now();
        loop {
            let got =
                self.with_state(|s| async move { s.graph.routing_table.info().next_hops }).await;
            if test_utils::expected_routing_tables(&got, want) {
                return;
            }
            events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::EdgesAdded { .. }) => Some(()),
                    _ => None,
                })
                .await;
        }
    }

    pub async fn wait_for_account_owner(&self, account: &AccountId) -> PeerId {
        let mut events = self.events.from_now();
        loop {
            let account = account.clone();
            let got = self
                .with_state(|s| async move { s.account_announcements.get_account_owner(&account) })
                .await;
            if let Some(got) = got {
                return got;
            }
            events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::AccountsAdded(_)) => Some(()),
                    _ => None,
                })
                .await;
        }
    }

    pub async fn wait_for_num_connected_peers(&self, wanted: usize) {
        let mut events = self.events.from_now();
        loop {
            let got = self.with_state(|s| async move { s.tier2.load().ready.len() }).await;
            if got == wanted {
                return;
            }
            events
                .recv_until(|ev| match ev {
                    Event::PeerManager(PME::EdgesAdded { .. }) => Some(()),
                    _ => None,
                })
                .await;
        }
    }

    /// Executes `NetworkState::tier1_connect` method.
    pub async fn tier1_connect(&self, clock: &time::Clock) {
        let clock = clock.clone();
        self.with_state(move |s| async move {
            s.tier1_connect(&clock).await;
        })
        .await;
    }

    /// Executes `NetworkState::update_connection_store` method.
    pub async fn update_connection_store(&self, clock: &time::Clock) {
        let clock = clock.clone();
        self.with_state(move |s| async move {
            s.update_connection_store(&clock);
        })
        .await;
    }
}

pub(crate) async fn start(
    clock: time::Clock,
    store: Arc<dyn near_store::db::Database>,
    cfg: config::NetworkConfig,
    chain: Arc<data::Chain>,
) -> ActorHandler {
    let (send, mut recv) = broadcast::unbounded_channel();
    let actix = ActixSystem::spawn({
        let mut cfg = cfg.clone();
        let chain = chain.clone();
        move || {
            let genesis_id = chain.genesis_id.clone();
            cfg.event_sink = Sender::from_fn({
                let send = send.clone();
                move |event| {
                    send.send(Event::PeerManager(event));
                }
            });
            let client_sender = Sender::from_fn({
                let send = send.clone();
                move |event: ClientSenderForNetworkMessage| {
                    // NOTE(robin-near): This is a pretty bad hack to preserve previous behavior
                    // of the test code.
                    // For some specific events we craft a response and send it back, while for
                    // most other events we send it to the sink (for what? I have no idea).
                    match event {
                        ClientSenderForNetworkMessage::_announce_account(msg) => {
                            (msg.callback)(
                                std::future::ready(Ok(Ok(msg
                                    .message
                                    .0
                                    .iter()
                                    .map(|(account, _)| account.clone())
                                    .collect())))
                                .boxed(),
                            );
                            send.send(Event::Client(
                                ClientSenderForNetworkInput::_announce_account(msg.message),
                            ));
                        }
                        _ => {
                            send.send(Event::Client(event.into_input()));
                        }
                    }
                }
            });
            let state_request_sender = Sender::from_fn({
                let send = send.clone();
                move |event: StateRequestSenderForNetworkMessage| {
                    // NOTE: See above comment for explanation about this code.
                    match event {
                        StateRequestSenderForNetworkMessage::_state_request_part(msg) => {
                            let StateRequestPart { part_id, shard_id, sync_hash } = msg.message;
                            let part = Some((part_id, vec![]));
                            let state_response =
                                ShardStateSyncResponse::V2(ShardStateSyncResponseV2 {
                                    header: None,
                                    part,
                                });
                            let result =
                                Some(StateResponse(Box::new(StateResponseInfo::V2(Box::new(
                                    StateResponseInfoV2 { shard_id, sync_hash, state_response },
                                )))));
                            (msg.callback)(std::future::ready(Ok(result)).boxed());
                            send.send(Event::StateRequestSender(
                                StateRequestSenderForNetworkInput::_state_request_part(msg.message),
                            ));
                        }
                        _ => {
                            send.send(Event::StateRequestSender(event.into_input()));
                        }
                    }
                }
            });
            let peer_manager_sender = Sender::from_fn({
                let send = send.clone();
                move |event: PeerManagerSenderForNetworkMessage| {
                    send.send(Event::PeerManagerSender(event.into_input()));
                }
            });
            let shards_manager_sender = Sender::from_fn({
                let send = send.clone();
                move |event| {
                    send.send(Event::ShardsManager(event));
                }
            });
            let state_witness_sender = Sender::from_fn({
                let send = send.clone();
                move |event: PartialWitnessSenderForNetworkMessage| {
                    send.send(Event::PartialWitness(event.into_input()));
                }
            });
            PeerManagerActor::spawn(
                clock,
                store,
                cfg,
                client_sender.break_apart().into_multi_sender(),
                state_request_sender.break_apart().into_multi_sender(),
                peer_manager_sender.break_apart().into_multi_sender(),
                shards_manager_sender,
                state_witness_sender.break_apart().into_multi_sender(),
                genesis_id,
            )
            .unwrap()
        }
    })
    .await;
    let h = ActorHandler { cfg, actix, events: recv.clone() };
    // Wait for the server to start.
    recv.recv_until(|ev| match ev {
        Event::PeerManager(PME::ServerStarted) => Some(()),
        _ => None,
    })
    .await;
    h.set_chain_info(chain.get_chain_info()).await;
    h
}
