use crate::client::{ClientSenderForNetwork, SetNetworkInfo, StateRequestHeader, StateRequestPart};
use crate::config;
use crate::debug::{DebugStatus, GetDebugStatus};
use crate::network_protocol::{self, T2MessageBody};
use crate::network_protocol::{
    Disconnect, Edge, PeerIdOrHash, PeerMessage, Ping, Pong, RawRoutedMessage, StateHeaderRequest,
    StatePartRequest,
};
use crate::network_protocol::{SyncSnapshotHosts, T1MessageBody};
use crate::peer::peer_actor::PeerActor;
use crate::peer_manager::connection;
use crate::peer_manager::network_state::{NetworkState, WhitelistNode};
use crate::peer_manager::peer_store;
use crate::shards_manager::ShardsManagerRequestFromNetwork;
use crate::state_witness::PartialWitnessSenderForNetwork;
use crate::stats::metrics;
use crate::store;
use crate::tcp;
use crate::types::{
    ConnectedPeerInfo, HighestHeightPeerInfo, KnownProducer, NetworkInfo, NetworkRequests,
    NetworkResponses, PeerInfo, PeerManagerMessageRequest, PeerManagerMessageResponse,
    PeerManagerSenderForNetwork, PeerType, SetChainInfo, SnapshotHostInfo, StateHeaderRequestBody,
    StatePartRequestBody, StateRequestSenderForNetwork, StateSyncEvent, Tier3Request,
    Tier3RequestBody,
};
use ::time::ext::InstantExt as _;
use actix::fut::future::wrap_future;
use actix::{Actor as _, AsyncContext as _};
use anyhow::Context as _;
use near_async::messaging::{SendAsync, Sender};
use near_async::time;
use near_o11y::span_wrapped_msg::SpanWrappedMessageExt;
use near_performance_metrics_macros::perf;
use near_primitives::genesis::GenesisId;
use near_primitives::network::{AnnounceAccount, PeerId};
use near_primitives::views::{
    ConnectionInfoView, EdgeView, KnownPeerStateView, NetworkGraphView, NetworkRoutesView,
    PeerStoreView, RecentOutboundConnectionsView, SnapshotHostInfoView, SnapshotHostsView,
};
use network_protocol::MAX_SHARDS_PER_SNAPSHOT_HOST_INFO;
use rand::Rng;
use rand::seq::{IteratorRandom, SliceRandom};
use rand::thread_rng;
use std::cmp::min;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tracing::Instrument as _;

/// Ratio between consecutive attempts to establish connection with another peer.
/// In the kth step node should wait `10 * EXPONENTIAL_BACKOFF_RATIO**k` milliseconds
const EXPONENTIAL_BACKOFF_RATIO: f64 = 1.1;
/// The initial waiting time between consecutive attempts to establish connection
const MONITOR_PEERS_INITIAL_DURATION: time::Duration = time::Duration::milliseconds(10);
/// How often should we check whether local edges match the connection pool.
const FIX_LOCAL_EDGES_INTERVAL: time::Duration = time::Duration::seconds(60);
/// How much time we give fix_local_edges() to resolve the discrepancies, before forcing disconnect.
const FIX_LOCAL_EDGES_TIMEOUT: time::Duration = time::Duration::seconds(6);

/// Number of times to attempt reconnection when trying to re-establish a connection.
const MAX_RECONNECT_ATTEMPTS: usize = 6;

/// How often to report bandwidth stats.
const REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL: time::Duration =
    time::Duration::milliseconds(60_000);

/// If we received more than `REPORT_BANDWIDTH_THRESHOLD_BYTES` of data from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_BYTES: usize = 10_000_000;
/// If we received more than REPORT_BANDWIDTH_THRESHOLD_COUNT` of messages from given peer it's bandwidth stats will be reported.
const REPORT_BANDWIDTH_THRESHOLD_COUNT: usize = 10_000;

/// If a peer is more than these blocks behind (comparing to our current head) - don't route any messages through it.
/// We are updating the list of unreliable peers every MONITOR_PEER_MAX_DURATION (60 seconds) - so the current
/// horizon value is roughly matching this threshold (if the node is 60 blocks behind, it will take it a while to recover).
/// If we set this horizon too low (for example 2 blocks) - we're risking excluding a lot of peers in case of a short
/// network issue.
const UNRELIABLE_PEER_HORIZON: u64 = 60;

/// Due to implementation limits of `Graph` in `near-network`, we support up to 128 client.
pub const MAX_TIER2_PEERS: usize = 128;

/// When picking a peer to connect to, we'll pick from the 'safer peers'
/// (a.k.a. ones that we've been connected to in the past) with these odds.
/// Otherwise, we'd pick any peer that we've heard about.
const PREFER_PREVIOUSLY_CONNECTED_PEER: f64 = 0.6;

/// How often to update the connections in storage.
pub(crate) const UPDATE_CONNECTION_STORE_INTERVAL: time::Duration = time::Duration::minutes(1);
/// How often to poll the NetworkState for closed connections we'd like to re-establish.
pub(crate) const POLL_CONNECTION_STORE_INTERVAL: time::Duration = time::Duration::minutes(1);

/// The length of time that a Tier3 connection is allowed to idle before it is stopped
const TIER3_IDLE_TIMEOUT: time::Duration = time::Duration::seconds(15);

/// Actor that manages peers connections.
pub struct PeerManagerActor {
    pub(crate) clock: time::Clock,
    /// Peer information for this node.
    my_peer_id: PeerId,
    /// Flag that track whether we started attempts to establish outbound connections.
    started_connect_attempts: bool,

    /// State that is shared between multiple threads (including PeerActors).
    pub(crate) state: Arc<NetworkState>,
}

/// TEST-ONLY
/// A generic set of events (observable in tests) that the Network may generate.
/// Ideally the tests should observe only public API properties, but until
/// we are at that stage, feel free to add any events that you need to observe.
/// In particular prefer emitting a new event to polling for a state change.
#[derive(Debug, PartialEq, Eq, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum Event {
    PeerManagerStarted,
    ServerStarted,
    RoutedMessageDropped,
    AccountsAdded(Vec<AnnounceAccount>),
    EdgesAdded(Vec<Edge>),
    Ping(Ping),
    Pong(Pong),
    // Reported once a message has been processed.
    // In contrast to typical RPC protocols, many P2P messages do not trigger
    // sending a response at the end of processing.
    // However, for precise instrumentation in tests it is useful to know when
    // processing has been finished. We simulate the "RPC response" by reporting
    // an event MessageProcessed.
    //
    // Given that processing is asynchronous and unstructured as of now,
    // it is hard to pinpoint all the places when the processing of a message is
    // actually complete. Currently this event is reported only for some message types,
    // feel free to add support for more.
    MessageProcessed(tcp::Tier, PeerMessage),
    // Reported when a reconnect loop is spawned.
    ReconnectLoopSpawned(PeerInfo),
    // Reported when a handshake has been started.
    HandshakeStarted(crate::peer::peer_actor::HandshakeStartedEvent),
    // Reported when a handshake has been successfully completed.
    HandshakeCompleted(crate::peer::peer_actor::HandshakeCompletedEvent),
    // Reported when the TCP connection has been closed.
    ConnectionClosed(crate::peer::peer_actor::ConnectionClosedEvent),
}

impl actix::Actor for PeerManagerActor {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // Periodically push network information to client.
        self.push_network_info_trigger(ctx, self.state.config.push_info_period);

        // Attempt to reconnect to recent outbound connections from storage
        if self.state.config.connect_to_reliable_peers_on_startup {
            tracing::debug!(target: "network", "Reconnecting to reliable peers from storage");
            self.bootstrap_outbound_from_recent_connections(ctx);
        } else {
            tracing::debug!(target: "network", "Skipping reconnection to reliable peers");
        }

        // Periodically starts peer monitoring.
        tracing::debug!(target: "network",
               max_period=?self.state.config.monitor_peers_max_period,
               "monitor_peers_trigger");
        self.monitor_peers_trigger(
            ctx,
            MONITOR_PEERS_INITIAL_DURATION,
            (MONITOR_PEERS_INITIAL_DURATION, self.state.config.monitor_peers_max_period),
        );

        // Periodically fix local edges.
        let clock = self.clock.clone();
        let state = self.state.clone();
        ctx.spawn(wrap_future(async move {
            let mut interval = time::Interval::new(clock.now(), FIX_LOCAL_EDGES_INTERVAL);
            loop {
                interval.tick(&clock).await;
                state.fix_local_edges(&clock, FIX_LOCAL_EDGES_TIMEOUT).await;
            }
        }));

        // Periodically update the connection store.
        let clock = self.clock.clone();
        let state = self.state.clone();
        ctx.spawn(wrap_future(async move {
            let mut interval = time::Interval::new(clock.now(), UPDATE_CONNECTION_STORE_INTERVAL);
            loop {
                interval.tick(&clock).await;
                state.update_connection_store(&clock);
            }
        }));

        // Periodically prints bandwidth stats for each peer.
        self.report_bandwidth_stats_trigger(ctx, REPORT_BANDWIDTH_STATS_TRIGGER_INTERVAL);

        #[cfg(test)]
        self.state.config.event_sink.send(Event::PeerManagerStarted);
    }

    /// Try to gracefully disconnect from connected peers.
    fn stopping(&mut self, _ctx: &mut Self::Context) -> actix::Running {
        tracing::warn!("PeerManager: stopping");
        self.state.tier2.broadcast_message(Arc::new(PeerMessage::Disconnect(Disconnect {
            remove_from_connection_store: false,
        })));
        actix::Running::Stop
    }

    fn stopped(&mut self, _ctx: &mut Self::Context) {
        actix::Arbiter::current().stop();
    }
}

impl PeerManagerActor {
    pub fn spawn(
        clock: time::Clock,
        store: Arc<dyn near_store::db::Database>,
        config: config::NetworkConfig,
        client: ClientSenderForNetwork,
        state_request_adapter: StateRequestSenderForNetwork,
        peer_manager_adapter: PeerManagerSenderForNetwork,
        shards_manager_adapter: Sender<ShardsManagerRequestFromNetwork>,
        partial_witness_adapter: PartialWitnessSenderForNetwork,
        genesis_id: GenesisId,
    ) -> anyhow::Result<actix::Addr<Self>> {
        let config = config.verify().context("config")?;
        let store = store::Store::from(store);
        let peer_store = peer_store::PeerStore::new(&clock, config.peer_store.clone())
            .context("PeerStore::new")?;
        tracing::debug!(target: "network",
               len = peer_store.len(),
               boot_nodes = config.peer_store.boot_nodes.len(),
               banned = peer_store.count_banned(),
               "Found known peers");
        tracing::debug!(target: "network", blacklist = ?config.peer_store.blacklist, "Blacklist");
        let whitelist_nodes = {
            let mut v = vec![];
            for wn in &config.whitelist_nodes {
                v.push(WhitelistNode::from_peer_info(wn)?);
            }
            v
        };
        let my_peer_id = config.node_id();
        let arbiter = actix::Arbiter::new().handle();
        let clock = clock;
        let state = Arc::new(NetworkState::new(
            &clock,
            store,
            peer_store,
            config,
            genesis_id,
            client,
            state_request_adapter,
            peer_manager_adapter,
            shards_manager_adapter,
            partial_witness_adapter,
            whitelist_nodes,
        ));
        arbiter.spawn({
            let arbiter = arbiter.clone();
            let state = state.clone();
            let clock = clock.clone();
            async move {
                // Start server if address provided.
                if let Some(server_addr) = &state.config.node_addr {
                    tracing::debug!(target: "network", at = ?server_addr, "starting public server");
                    let listener = match server_addr.listener() {
                        Ok(it) => it,
                        Err(e) => {
                            panic!("failed to start listening on server_addr={server_addr:?} e={e:?}")
                        }
                    };
                    #[cfg(test)]
                    state.config.event_sink.send(Event::ServerStarted);
                    arbiter.spawn({
                        let clock = clock.clone();
                        let state = state.clone();
                        async move {
                            loop {
                                if let Ok(stream) = listener.accept().await {
                                    // Always let the new peer to send a handshake message.
                                    // Only then we can decide whether we should accept a connection.
                                    // It is expected to be reasonably cheap: eventually, for TIER2 network
                                    // we would like to exchange set of connected peers even without establishing
                                    // a proper connection.
                                    tracing::debug!(target: "network", from = ?stream.peer_addr, "got new connection");
                                    if let Err(err) =
                                        PeerActor::spawn(clock.clone(), stream, None, state.clone())
                                    {
                                        tracing::info!(target:"network", ?err, "PeerActor::spawn()");
                                    }
                                }
                            }
                        }
                    });
                }

                // Connect to TIER1 proxies and broadcast the list those connections periodically.
                let tier1 = state.config.tier1.clone();
                arbiter.spawn({
                    let clock = clock.clone();
                    let state = state.clone();
                    let mut interval = time::Interval::new(clock.now(), tier1.advertise_proxies_interval);
                    async move {
                        loop {
                            interval.tick(&clock).await;
                            state.tier1_request_full_sync();
                            state.tier1_advertise_proxies(&clock).await;
                        }
                    }
                });

                // Update TIER1 connections periodically.
                arbiter.spawn({
                    let clock = clock.clone();
                    let state = state.clone();
                    let mut interval = tokio::time::interval(tier1.connect_interval.try_into().unwrap());
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    async move {
                        loop {
                            interval.tick().await;
                            state.tier1_connect(&clock).await;
                        }
                    }
                });

                // Periodically poll the connection store for connections we'd like to re-establish
                arbiter.spawn({
                    let clock = clock.clone();
                    let state = state.clone();
                    let arbiter = arbiter.clone();
                    let mut interval = time::Interval::new(clock.now(), POLL_CONNECTION_STORE_INTERVAL);
                    async move {
                        loop {
                            interval.tick(&clock).await;
                            // Poll the NetworkState for all pending reconnect attempts
                            let pending_reconnect = state.poll_pending_reconnect();
                            // Spawn a separate reconnect loop for each pending reconnect attempt
                            for peer_info in pending_reconnect {
                                arbiter.spawn({
                                    let state = state.clone();
                                    let clock = clock.clone();
                                    let peer_info = peer_info.clone();
                                    async move {
                                        state.reconnect(clock, peer_info, MAX_RECONNECT_ATTEMPTS).await;
                                    }
                                });

                                #[cfg(test)]
                                state.config.event_sink.send(Event::ReconnectLoopSpawned(peer_info));
                            }
                        }
                    }
                });
            }
        });
        Ok(Self::start_in_arbiter(&arbiter, move |_ctx| Self {
            my_peer_id: my_peer_id.clone(),
            started_connect_attempts: false,
            state,
            clock,
        }))
    }

    /// Periodically prints bandwidth stats for each peer.
    fn report_bandwidth_stats_trigger(
        &self,
        ctx: &mut actix::Context<Self>,
        every: time::Duration,
    ) {
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["report_bandwidth_stats"])
            .start_timer();
        let mut total_bandwidth_used_by_all_peers: usize = 0;
        let mut total_msg_received_count: usize = 0;
        for (peer_id, connected_peer) in &self.state.tier2.load().ready {
            let bandwidth_used =
                connected_peer.stats.received_bytes.swap(0, Ordering::Relaxed) as usize;
            let msg_received_count =
                connected_peer.stats.received_messages.swap(0, Ordering::Relaxed) as usize;
            if bandwidth_used > REPORT_BANDWIDTH_THRESHOLD_BYTES
                || msg_received_count > REPORT_BANDWIDTH_THRESHOLD_COUNT
            {
                tracing::debug!(target: "bandwidth",
                    ?peer_id,
                    bandwidth_used, msg_received_count, "Peer bandwidth exceeded threshold",
                );
            }
            total_bandwidth_used_by_all_peers += bandwidth_used;
            total_msg_received_count += msg_received_count;
        }

        tracing::info!(
            target: "bandwidth",
            total_bandwidth_used_by_all_peers,
            total_msg_received_count, "Bandwidth stats"
        );

        near_performance_metrics::actix::run_later(
            ctx,
            every.try_into().unwrap(),
            move |act, ctx| {
                act.report_bandwidth_stats_trigger(ctx, every);
            },
        );
    }

    /// Check if it is needed to create a new outbound connection.
    /// If the number of active connections is less than `ideal_connections_lo` or
    /// (the number of outgoing connections is less than `minimum_outbound_peers`
    ///     and the total connections is less than `max_num_peers`)
    fn is_outbound_bootstrap_needed(&self) -> bool {
        let tier2 = self.state.tier2.load();
        let total_connections = tier2.ready.len() + tier2.outbound_handshakes.len();
        let potential_outbound_connections =
            tier2.ready.values().filter(|peer| peer.peer_type == PeerType::Outbound).count()
                + tier2.outbound_handshakes.len();

        (total_connections < self.state.config.ideal_connections_lo as usize
            || (total_connections < self.state.config.max_num_peers as usize
                && potential_outbound_connections
                    < self.state.config.minimum_outbound_peers as usize))
            && !self.state.config.outbound_disabled
    }

    /// Returns peers close to the highest height
    fn highest_height_peers(&self) -> Vec<HighestHeightPeerInfo> {
        let infos: Vec<HighestHeightPeerInfo> = self
            .state
            .tier2
            .load()
            .ready
            .values()
            .filter_map(|p| p.full_peer_info().into())
            .collect();

        // This finds max height among peers, and returns one peer close to such height.
        let max_height = match infos.iter().map(|i| i.highest_block_height).max() {
            Some(height) => height,
            None => return vec![],
        };
        // Find all peers whose height is within `highest_peer_horizon` from max height peer(s).
        infos
            .into_iter()
            .filter(|i| {
                i.highest_block_height.saturating_add(self.state.config.highest_peer_horizon)
                    >= max_height
            })
            .collect()
    }

    // Get peers that are potentially unreliable and we should avoid routing messages through them.
    // Currently we're picking the peers that are too much behind (in comparison to us).
    fn unreliable_peers(&self) -> HashSet<PeerId> {
        // If chain info is not set, that means we haven't received chain info message
        // from chain yet. Return empty set in that case. This should only last for a short period
        // of time.
        let binding = self.state.chain_info.load();
        let chain_info = if let Some(it) = binding.as_ref() {
            it
        } else {
            return HashSet::new();
        };
        let my_height = chain_info.block.header().height();
        // Find all peers whose height is below `highest_peer_horizon` from max height peer(s).
        // or the ones we don't have height information yet
        self.state
            .tier2
            .load()
            .ready
            .values()
            .filter(|p| {
                p.last_block
                    .load()
                    .as_ref()
                    .map(|x| x.height.saturating_add(UNRELIABLE_PEER_HORIZON) < my_height)
                    .unwrap_or(false)
            })
            .map(|p| p.peer_info.id.clone())
            .collect()
    }

    /// Check if the number of connections (excluding whitelisted ones) exceeds ideal_connections_hi.
    /// If so, constructs a safe set of peers and selects one random peer outside of that set
    /// and sends signal to stop connection to it gracefully.
    ///
    /// Safe set construction process:
    /// 1. Add all whitelisted peers to the safe set.
    /// 2. If the number of outbound connections is less or equal than minimum_outbound_connections,
    ///    add all outbound connections to the safe set.
    /// 3. Find all peers who sent us a message within the last peer_recent_time_window,
    ///    and add them one by one to the safe_set (starting from earliest connection time)
    ///    until safe set has safe_set_size elements.
    fn maybe_stop_active_connection(&self) {
        let tier2 = self.state.tier2.load();
        let filter_peers = |predicate: &dyn Fn(&connection::Connection) -> bool| -> Vec<_> {
            tier2
                .ready
                .values()
                .filter(|peer| predicate(&*peer))
                .map(|peer| peer.peer_info.id.clone())
                .collect()
        };

        // Build safe set
        let mut safe_set = HashSet::new();

        // Add whitelisted nodes to the safe set.
        let whitelisted_peers = filter_peers(&|p| self.state.is_peer_whitelisted(&p.peer_info));
        safe_set.extend(whitelisted_peers);

        // If there is not enough non-whitelisted peers, return without disconnecting anyone.
        if tier2.ready.len() - safe_set.len() <= self.state.config.ideal_connections_hi as usize {
            return;
        }

        // If there is not enough outbound peers, add them to the safe set.
        let outbound_peers = filter_peers(&|p| p.peer_type == PeerType::Outbound);
        if outbound_peers.len() + tier2.outbound_handshakes.len()
            <= self.state.config.minimum_outbound_peers as usize
        {
            safe_set.extend(outbound_peers);
        }

        // If there is not enough archival peers, add them to the safe set.
        if self.state.config.archive {
            let archival_peers = filter_peers(&|p| p.archival);
            if archival_peers.len()
                <= self.state.config.archival_peer_connections_lower_bound as usize
            {
                safe_set.extend(archival_peers);
            }
        }

        // Find all recently active peers.
        let now = self.clock.now();
        let mut active_peers: Vec<Arc<connection::Connection>> = tier2
            .ready
            .values()
            .filter(|p| {
                now - p.last_time_received_message.load()
                    < self.state.config.peer_recent_time_window
            })
            .cloned()
            .collect();

        // Sort by established time.
        active_peers.sort_by_key(|p| p.established_time);
        // Saturate safe set with recently active peers.
        let set_limit = self.state.config.safe_set_size as usize;
        for p in active_peers {
            if safe_set.len() >= set_limit {
                break;
            }
            safe_set.insert(p.peer_info.id.clone());
        }

        // Build valid candidate list to choose the peer to be removed. All peers outside the safe set.
        let candidates = tier2.ready.values().filter(|p| !safe_set.contains(&p.peer_info.id));
        if let Some(p) = candidates.choose(&mut rand::thread_rng()) {
            tracing::debug!(target: "network", id = ?p.peer_info.id,
                tier2_len = tier2.ready.len(),
                ideal_connections_hi = self.state.config.ideal_connections_hi,
                "Stop active connection"
            );
            p.stop(None);
        }
    }

    /// TIER3 connections are established ad-hoc to transmit individual large messages.
    /// Here we terminate these "single-purpose" connections after an idle timeout.
    ///
    /// When a TIER3 connection is established the intended message is already prepared in-memory,
    /// so there is no concern of the timeout falling in between the handshake and the payload.
    ///
    /// A finer detail is that as long as a TIER3 connection remains open it can be reused to
    /// transmit additional TIER3 payloads intended for the same peer. In such cases the message
    /// can be lost if the timeout is reached precisely while it is in flight. For simplicity we
    /// accept this risk; network requests are understood as unreliable and the requesting node has
    /// retry logic anyway. TODO(saketh): consider if we can improve this in a simple way.
    fn stop_tier3_idle_connections(&self) {
        let now = self.clock.now();
        self.state
            .tier3
            .load()
            .ready
            .values()
            .filter(|p| now - p.last_time_received_message.load() > TIER3_IDLE_TIMEOUT)
            .for_each(|p| p.stop(None));
    }

    /// Periodically monitor list of peers and:
    ///  - request new peers from connected peers,
    ///  - bootstrap outbound connections from known peers,
    ///  - un-ban peers that have been banned for awhile,
    ///  - remove expired peers,
    ///
    /// # Arguments:
    /// - `interval` - Time between consequent runs.
    /// - `default_interval` - we will set `interval` to this value once, after first successful connection
    /// - `max_interval` - maximum value of interval
    /// NOTE: in the current implementation `interval` increases by 1% every time, and it will
    ///       reach value of `max_internal` eventually.
    fn monitor_peers_trigger(
        &mut self,
        ctx: &mut actix::Context<Self>,
        mut interval: time::Duration,
        (default_interval, max_interval): (time::Duration, time::Duration),
    ) {
        let _span = tracing::trace_span!(target: "network", "monitor_peers_trigger").entered();
        let _timer =
            metrics::PEER_MANAGER_TRIGGER_TIME.with_label_values(&["monitor_peers"]).start_timer();

        self.state.peer_store.update(&self.clock);

        if self.is_outbound_bootstrap_needed() {
            let tier2 = self.state.tier2.load();
            // With some odds - try picking one of the 'NotConnected' peers -- these are the ones that we were able to connect to in the past.
            let prefer_previously_connected_peer =
                thread_rng().gen_bool(PREFER_PREVIOUSLY_CONNECTED_PEER);
            if let Some(peer_info) = self.state.peer_store.unconnected_peer(
                |peer_state| {
                    // Ignore connecting to ourself
                    self.my_peer_id == peer_state.peer_info.id
                    || self.state.config.node_addr.as_ref().map(|a|**a) == peer_state.peer_info.addr
                    // Or to peers we are currently trying to connect to
                    || tier2.outbound_handshakes.contains(&peer_state.peer_info.id)
                },
                prefer_previously_connected_peer,
            ) {
                // Start monitor_peers_attempts from start after we discover the first healthy peer
                if !self.started_connect_attempts {
                    self.started_connect_attempts = true;
                    interval = default_interval;
                }
                ctx.spawn(wrap_future({
                    let state = self.state.clone();
                    let clock = self.clock.clone();
                    async move {
                        let result = async {
                            let stream = tcp::Stream::connect(&peer_info, tcp::Tier::T2, &state.config.socket_options).await.context("tcp::Stream::connect()")?;
                            PeerActor::spawn_and_handshake(clock.clone(),stream,None,state.clone()).await.context("PeerActor::spawn()")?;
                            anyhow::Ok(())
                        }.await;

                        if let Err(ref err) = result {
                            tracing::info!(target: "network", err = format!("{:#}", err), "tier2 failed to connect to {peer_info}");
                        }
                        if state.peer_store.peer_connection_attempt(&clock, &peer_info.id, result).is_err() {
                            tracing::error!(target: "network", ?peer_info, "Failed to store connection attempt.");
                        }
                    }.instrument(tracing::trace_span!(target: "network", "monitor_peers_trigger_connect"))
                }));
            }
        }

        // If there are too many active connections try to remove some connections
        self.maybe_stop_active_connection();

        // Close Tier3 connections which have been idle for too long
        self.stop_tier3_idle_connections();

        // Find peers that are not reliable (too much behind) - and make sure that we're not routing messages through them.
        let unreliable_peers = self.unreliable_peers();
        metrics::PEER_UNRELIABLE.set(unreliable_peers.len() as i64);
        self.state.set_unreliable_peers(unreliable_peers);

        let new_interval = min(max_interval, interval * EXPONENTIAL_BACKOFF_RATIO);

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.monitor_peers_trigger(ctx, new_interval, (default_interval, max_interval));
            },
        );
    }

    /// Re-establish each outbound connection in the connection store (single attempt)
    fn bootstrap_outbound_from_recent_connections(&self, ctx: &mut actix::Context<Self>) {
        for conn_info in self.state.connection_store.get_recent_outbound_connections() {
            ctx.spawn(wrap_future({
                let state = self.state.clone();
                let clock = self.clock.clone();
                let peer_info = conn_info.peer_info.clone();
                async move {
                    state.reconnect(clock, peer_info, 1).await;
                }
            }));

            #[cfg(test)]
            self.state
                .config
                .event_sink
                .send(Event::ReconnectLoopSpawned(conn_info.peer_info.clone()));
        }
    }

    pub(crate) fn get_network_info(&self) -> NetworkInfo {
        let tier1 = self.state.tier1.load();
        let tier2 = self.state.tier2.load();
        let now = self.clock.now();
        let graph = self.state.graph.load();
        let connected_peer = |cp: &Arc<connection::Connection>| ConnectedPeerInfo {
            full_peer_info: cp.full_peer_info(),
            received_bytes_per_sec: cp.stats.received_bytes_per_sec.load(Ordering::Relaxed),
            sent_bytes_per_sec: cp.stats.sent_bytes_per_sec.load(Ordering::Relaxed),
            last_time_peer_requested: cp.last_time_peer_requested.load().unwrap_or(now),
            last_time_received_message: cp.last_time_received_message.load(),
            connection_established_time: cp.established_time,
            peer_type: cp.peer_type,
            nonce: match graph.local_edges.get(&cp.peer_info.id) {
                Some(e) => e.nonce(),
                None => 0,
            },
        };
        NetworkInfo {
            connected_peers: tier2.ready.values().map(connected_peer).collect(),
            tier1_connections: tier1.ready.values().map(connected_peer).collect(),
            num_connected_peers: tier2.ready.len(),
            peer_max_count: self.state.config.max_num_peers,
            highest_height_peers: self.highest_height_peers(),
            sent_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.sent_bytes_per_sec.load(Ordering::Relaxed))
                .sum(),
            received_bytes_per_sec: tier2
                .ready
                .values()
                .map(|x| x.stats.received_bytes_per_sec.load(Ordering::Relaxed))
                .sum(),
            known_producers: self
                .state
                .account_announcements
                .get_announcements()
                .into_iter()
                .map(|announce_account| KnownProducer {
                    account_id: announce_account.account_id,
                    peer_id: announce_account.peer_id.clone(),
                    // TODO: fill in the address.
                    addr: None,
                    next_hops: self.state.graph.routing_table.view_route(&announce_account.peer_id),
                })
                .collect(),
            tier1_accounts_keys: self.state.accounts_data.load().keys.iter().cloned().collect(),
            tier1_accounts_data: self.state.accounts_data.load().data.values().cloned().collect(),
        }
    }

    fn push_network_info_trigger(&self, ctx: &mut actix::Context<Self>, interval: time::Duration) {
        let _span = tracing::trace_span!(target: "network", "push_network_info_trigger").entered();
        let network_info = self.get_network_info();
        let _timer = metrics::PEER_MANAGER_TRIGGER_TIME
            .with_label_values(&["push_network_info"])
            .start_timer();
        // TODO(gprusak): just spawn a loop.
        let state = self.state.clone();
        ctx.spawn(wrap_future(
            async move {
                state.client.send_async(SetNetworkInfo(network_info).span_wrap()).await.ok();
            }
            .instrument(
                tracing::trace_span!(target: "network", "push_network_info_trigger_future"),
            ),
        ));

        near_performance_metrics::actix::run_later(
            ctx,
            interval.try_into().unwrap(),
            move |act, ctx| {
                act.push_network_info_trigger(ctx, interval);
            },
        );
    }

    #[perf]
    fn handle_msg_network_requests(
        &mut self,
        msg: NetworkRequests,
        ctx: &mut actix::Context<Self>,
    ) -> NetworkResponses {
        let msg_type: &str = msg.as_ref();
        let _span =
            tracing::trace_span!(target: "network", "handle_msg_network_requests", msg_type)
                .entered();
        metrics::REQUEST_COUNT_BY_TYPE_TOTAL.with_label_values(&[msg.as_ref()]).inc();
        match msg {
            NetworkRequests::Block { block } => {
                self.state.tier2.broadcast_message(Arc::new(PeerMessage::Block(block)));
                NetworkResponses::NoResponse
            }
            NetworkRequests::OptimisticBlock { chunk_producers, optimistic_block } => {
                // TODO(saketh): the chunk_producers are identified by their validator AccountId,
                // but OptimisticBlock is sent over a direct PeerMessage. Hence we have to perform
                // a conversion here from AccountId to peer connection. Consider reworking this.
                let msg = Arc::new(PeerMessage::OptimisticBlock(optimistic_block));
                for target_account in &*chunk_producers {
                    if let Some(conn) = self.state.get_tier1_proxy_for_account_id(&target_account) {
                        conn.send_message(msg.clone());
                    }
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::Approval { approval_message } => {
                self.state.send_message_to_account(
                    &self.clock,
                    &approval_message.target,
                    T1MessageBody::BlockApproval(approval_message.approval).into(),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::BlockRequest { hash, peer_id } => {
                if self.state.tier2.send_message(peer_id, Arc::new(PeerMessage::BlockRequest(hash)))
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::BlockHeadersRequest { hashes, peer_id } => {
                if self
                    .state
                    .tier2
                    .send_message(peer_id, Arc::new(PeerMessage::BlockHeadersRequest(hashes)))
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::StateRequestHeader { shard_id, sync_hash, sync_prev_prev_hash } => {
                // Select a peer which has advertised availability of the desired
                // state snapshot.
                let Some(peer_id) = self
                    .state
                    .snapshot_hosts
                    .select_host_for_header(&sync_prev_prev_hash, shard_id)
                else {
                    tracing::debug!(target: "network", %shard_id, ?sync_hash, "no snapshot hosts available");
                    return NetworkResponses::NoDestinationsAvailable;
                };

                // If we have a direct connection we can simply send a StateRequestHeader message
                // over it. This is a bit of a hack for upgradability and can be deleted in the
                // next release.
                {
                    if self.state.tier2.send_message(
                        peer_id.clone(),
                        Arc::new(PeerMessage::StateRequestHeader(shard_id, sync_hash)),
                    ) {
                        return NetworkResponses::SelectedDestination(peer_id);
                    }
                }

                // The node needs to include its own public address in the request
                // so that the response can be sent over a direct Tier3 connection.
                let Some(addr) = *self.state.my_public_addr.read() else {
                    return NetworkResponses::MyPublicAddrNotKnown;
                };

                let routed_message = self.state.sign_message(
                    &self.clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(peer_id.clone()),
                        body: T2MessageBody::StateHeaderRequest(StateHeaderRequest {
                            shard_id,
                            sync_hash,
                            addr,
                        })
                        .into(),
                    },
                );

                if !self.state.send_message_to_peer(&self.clock, tcp::Tier::T2, routed_message) {
                    return NetworkResponses::RouteNotFound;
                }

                tracing::debug!(target: "network", %shard_id, ?sync_hash, "requesting state header from host {peer_id}");
                NetworkResponses::SelectedDestination(peer_id)
            }
            NetworkRequests::StateRequestPart {
                shard_id,
                sync_hash,
                sync_prev_prev_hash,
                part_id,
            } => {
                // The node needs to include its own public address in the request
                // so that the response can be sent over a direct Tier3 connection.
                let Some(addr) = *self.state.my_public_addr.read() else {
                    return NetworkResponses::MyPublicAddrNotKnown;
                };

                // Select a peer which has advertised availability of the desired
                // state snapshot.
                let Some(peer_id) = self.state.snapshot_hosts.select_host_for_part(
                    &sync_prev_prev_hash,
                    shard_id,
                    part_id,
                ) else {
                    tracing::debug!(target: "network", %shard_id, ?sync_hash, ?part_id, "no snapshot hosts available");
                    return NetworkResponses::NoDestinationsAvailable;
                };

                let routed_message = self.state.sign_message(
                    &self.clock,
                    RawRoutedMessage {
                        target: PeerIdOrHash::PeerId(peer_id.clone()),
                        body: T2MessageBody::StatePartRequest(StatePartRequest {
                            shard_id,
                            sync_hash,
                            part_id,
                            addr,
                        })
                        .into(),
                    },
                );

                if !self.state.send_message_to_peer(&self.clock, tcp::Tier::T2, routed_message) {
                    return NetworkResponses::RouteNotFound;
                }

                tracing::debug!(target: "network", %shard_id, ?sync_hash, ?part_id, "requesting state part from host {peer_id}");
                NetworkResponses::SelectedDestination(peer_id)
            }
            NetworkRequests::SnapshotHostInfo { sync_hash, mut epoch_height, mut shards } => {
                if shards.len() > MAX_SHARDS_PER_SNAPSHOT_HOST_INFO {
                    tracing::warn!(
                        "PeerManager: Sending out a SnapshotHostInfo message with {} shards, \
                                    this is more than the allowed limit. The list of shards will be truncated. \
                                    Please adjust the MAX_SHARDS_PER_SNAPSHOT_HOST_INFO constant ({})",
                        shards.len(),
                        MAX_SHARDS_PER_SNAPSHOT_HOST_INFO
                    );

                    // We can's send out more than MAX_SHARDS_PER_SNAPSHOT_HOST_INFO shards because other nodes would
                    // ban us for abusive behavior. Let's truncate the shards vector by choosing a random subset of
                    // MAX_SHARDS_PER_SNAPSHOT_HOST_INFO shard ids. Choosing a random subset slightly increases the chances
                    // that other nodes will have snapshot sync information about all shards from some node.
                    shards = shards
                        .choose_multiple(&mut rand::thread_rng(), MAX_SHARDS_PER_SNAPSHOT_HOST_INFO)
                        .copied()
                        .collect();
                }
                // Sort the shards to keep things tidy
                shards.sort();

                let peer_id = self.state.config.node_id();

                // Hacky workaround for test environments only.
                // When starting a chain from scratch the first two snapshots both have epoch height 1.
                // The epoch height is used as a version number for SnapshotHostInfo and if duplicated,
                // prevents the second snapshot from being advertised as new information to the network.
                // To avoid this problem, we re-index the very first epoch with epoch_height=0.
                if epoch_height == 1 && self.state.snapshot_hosts.get_host_info(&peer_id).is_none()
                {
                    epoch_height = 0;
                }

                // Sign the information about the locally created snapshot using the keys in the
                // network config before broadcasting it
                let snapshot_host_info = Arc::new(SnapshotHostInfo::new(
                    self.state.config.node_id(),
                    sync_hash,
                    epoch_height,
                    shards,
                    &self.state.config.node_key,
                ));

                // Insert our info to our own cache.
                self.state.snapshot_hosts.insert_skip_verify(snapshot_host_info.clone());

                self.state.tier2.broadcast_message(Arc::new(PeerMessage::SyncSnapshotHosts(
                    SyncSnapshotHosts { hosts: vec![snapshot_host_info] },
                )));
                NetworkResponses::NoResponse
            }
            NetworkRequests::BanPeer { peer_id, ban_reason } => {
                self.state.disconnect_and_ban(&self.clock, &peer_id, ban_reason);
                NetworkResponses::NoResponse
            }
            NetworkRequests::AnnounceAccount(announce_account) => {
                let state = self.state.clone();
                ctx.spawn(wrap_future(async move {
                    state.add_accounts(vec![announce_account]).await;
                }));
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedChunkRequest { target, request, create_time } => {
                metrics::PARTIAL_ENCODED_CHUNK_REQUEST_DELAY.observe(
                    (self.clock.now().signed_duration_since(create_time)).as_seconds_f64(),
                );
                let mut success = false;

                // Make two attempts to send the message. First following the preference of `prefer_peer`,
                // and if it fails, against the preference.
                for prefer_peer in &[target.prefer_peer, !target.prefer_peer] {
                    if !prefer_peer {
                        if let Some(account_id) = target.account_id.as_ref() {
                            if self.state.send_message_to_account(
                                &self.clock,
                                account_id,
                                T2MessageBody::PartialEncodedChunkRequest(request.clone()).into(),
                            ) {
                                success = true;
                                break;
                            }
                        }
                    } else {
                        let mut matching_peers = vec![];
                        for (peer_id, peer) in &self.state.tier2.load().ready {
                            let last_block = peer.last_block.load();
                            if (peer.archival || !target.only_archival)
                                && last_block.is_some()
                                && last_block.as_ref().unwrap().height >= target.min_height
                                && peer.tracked_shards.contains(&target.shard_id)
                            {
                                matching_peers.push(peer_id.clone());
                            }
                        }

                        if let Some(matching_peer) = matching_peers.iter().choose(&mut thread_rng())
                        {
                            if self.state.send_message_to_peer(
                                &self.clock,
                                tcp::Tier::T2,
                                self.state.sign_message(
                                    &self.clock,
                                    RawRoutedMessage {
                                        target: PeerIdOrHash::PeerId(matching_peer.clone()),
                                        body: T2MessageBody::PartialEncodedChunkRequest(
                                            request.clone(),
                                        )
                                        .into(),
                                    },
                                ),
                            ) {
                                success = true;
                                break;
                            }
                        } else {
                            tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find any matching peer for chunk");
                        }
                    }
                }

                if success {
                    NetworkResponses::NoResponse
                } else {
                    tracing::debug!(target: "network", chunk_hash=?request.chunk_hash, "Failed to find a route for chunk");
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkResponse { route_back, response } => {
                if self.state.send_message_to_peer(
                    &self.clock,
                    tcp::Tier::T2,
                    self.state.sign_message(
                        &self.clock,
                        RawRoutedMessage {
                            target: PeerIdOrHash::Hash(route_back),
                            body: T2MessageBody::PartialEncodedChunkResponse(response).into(),
                        },
                    ),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkMessage { account_id, partial_encoded_chunk } => {
                if self.state.send_message_to_account(
                    &self.clock,
                    &account_id,
                    T1MessageBody::VersionedPartialEncodedChunk(Box::new(
                        partial_encoded_chunk.into(),
                    ))
                    .into(),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::PartialEncodedChunkForward { account_id, forward } => {
                if self.state.send_message_to_account(
                    &self.clock,
                    &account_id,
                    T1MessageBody::PartialEncodedChunkForward(forward).into(),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ForwardTx(account_id, tx) => {
                if self.state.send_message_to_account(
                    &self.clock,
                    &account_id,
                    T2MessageBody::ForwardTx(tx).into(),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::TxStatus(account_id, signer_account_id, tx_hash) => {
                if self.state.send_message_to_account(
                    &self.clock,
                    &account_id,
                    T2MessageBody::TxStatusRequest(signer_account_id, tx_hash).into(),
                ) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ChunkStateWitnessAck(target, ack) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T2MessageBody::ChunkStateWitnessAck(ack).into(),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::ChunkEndorsement(target, endorsement) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::VersionedChunkEndorsement(endorsement).into(),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedStateWitness(validator_witness_tuple) => {
                let Some(partial_witness) = validator_witness_tuple.first().map(|(_, w)| w) else {
                    return NetworkResponses::NoResponse;
                };
                let part_owners = validator_witness_tuple
                    .iter()
                    .map(|(validator, _)| validator.clone())
                    .collect::<Vec<_>>();
                let _span = tracing::debug_span!(target: "network",
                    "send partial_encoded_state_witnesses",
                    height = partial_witness.chunk_production_key().height_created,
                    shard_id = %partial_witness.chunk_production_key().shard_id,
                    part_owners_len = part_owners.len(),
                    tag_witness_distribution = true,
                )
                .entered();

                for (chunk_validator, partial_witness) in validator_witness_tuple {
                    self.state.send_message_to_account(
                        &self.clock,
                        &chunk_validator,
                        T1MessageBody::PartialEncodedStateWitness(partial_witness).into(),
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedStateWitnessForward(
                chunk_validators,
                partial_witness,
            ) => {
                let _span = tracing::debug_span!(target: "network",
                    "send partial_encoded_state_witness_forward",
                    height = partial_witness.chunk_production_key().height_created,
                    shard_id = %partial_witness.chunk_production_key().shard_id,
                    part_ord = partial_witness.part_ord(),
                    tag_witness_distribution = true,
                )
                .entered();
                for chunk_validator in chunk_validators {
                    self.state.send_message_to_account(
                        &self.clock,
                        &chunk_validator,
                        T1MessageBody::PartialEncodedStateWitnessForward(partial_witness.clone())
                            .into(),
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::EpochSyncRequest { peer_id } => {
                if self.state.tier2.send_message(peer_id, PeerMessage::EpochSyncRequest.into()) {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::EpochSyncResponse { peer_id, proof } => {
                if self
                    .state
                    .tier2
                    .send_message(peer_id, PeerMessage::EpochSyncResponse(proof).into())
                {
                    NetworkResponses::NoResponse
                } else {
                    NetworkResponses::RouteNotFound
                }
            }
            NetworkRequests::ChunkContractAccesses(validators, accesses) => {
                for validator in validators {
                    self.state.send_message_to_account(
                        &self.clock,
                        &validator,
                        T1MessageBody::ChunkContractAccesses(accesses.clone()).into(),
                    );
                }
                NetworkResponses::NoResponse
            }
            NetworkRequests::ContractCodeRequest(target, request) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::ContractCodeRequest(request).into(),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::ContractCodeResponse(target, response) => {
                self.state.send_message_to_account(
                    &self.clock,
                    &target,
                    T1MessageBody::ContractCodeResponse(response).into(),
                );
                NetworkResponses::NoResponse
            }
            NetworkRequests::PartialEncodedContractDeploys(accounts, deploys) => {
                // Send to last account separately to avoid clone when sending to a single target.
                let (last_account, other_accounts) = accounts.split_last().unwrap();
                for account in other_accounts {
                    self.state.send_message_to_account(
                        &self.clock,
                        &account,
                        T2MessageBody::PartialEncodedContractDeploys(deploys.clone()).into(),
                    );
                }
                self.state.send_message_to_account(
                    &self.clock,
                    &last_account,
                    T2MessageBody::PartialEncodedContractDeploys(deploys).into(),
                );
                NetworkResponses::NoResponse
            }
            // TODO(spice): remove
            NetworkRequests::TestonlySpiceIncomingReceipts { .. } => {
                debug_assert!(false);
                NetworkResponses::NoResponse
            }
            // TODO(spice): remove
            NetworkRequests::TestonlySpiceStateWitness { .. } => {
                debug_assert!(false);
                NetworkResponses::NoResponse
            }
        }
    }

    fn handle_peer_manager_message(
        &mut self,
        msg: PeerManagerMessageRequest,
        ctx: &mut actix::Context<Self>,
    ) -> PeerManagerMessageResponse {
        match msg {
            PeerManagerMessageRequest::NetworkRequests(msg) => {
                PeerManagerMessageResponse::NetworkResponses(
                    self.handle_msg_network_requests(msg, ctx),
                )
            }
            PeerManagerMessageRequest::AdvertiseTier1Proxies => {
                let state = self.state.clone();
                let clock = self.clock.clone();
                ctx.spawn(wrap_future(async move {
                    state.tier1_advertise_proxies(&clock).await;
                }));
                PeerManagerMessageResponse::AdvertiseTier1Proxies
            }
            PeerManagerMessageRequest::OutboundTcpConnect(stream) => {
                let peer_addr = stream.peer_addr;
                if let Err(err) =
                    PeerActor::spawn(self.clock.clone(), stream, None, self.state.clone())
                {
                    tracing::info!(target:"network", ?err, ?peer_addr, "spawn_outbound()");
                }
                PeerManagerMessageResponse::OutboundTcpConnect
            }
            // TEST-ONLY
            PeerManagerMessageRequest::FetchRoutingTable => {
                PeerManagerMessageResponse::FetchRoutingTable(self.state.graph.routing_table.info())
            }
        }
    }
}

impl actix::Handler<SetChainInfo> for PeerManagerActor {
    type Result = ();
    #[perf]
    fn handle(&mut self, SetChainInfo(info): SetChainInfo, ctx: &mut Self::Context) {
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&["SetChainInfo"]).start_timer();
        // We call self.state.set_chain_info()
        // synchronously, therefore, assuming actix in-order delivery,
        // there will be no race condition between subsequent SetChainInfo
        // calls.
        if !self.state.set_chain_info(info) {
            // We early exit in case the set of TIER1 account keys hasn't changed.
            return;
        }

        let state = self.state.clone();
        let clock = self.clock.clone();
        ctx.spawn(wrap_future(
            async move {
                // This node might have become a TIER1 node due to the change of the key set.
                // If so we should recompute and re-advertise the list of proxies.
                // This is mostly important in case a node is its own proxy. In all other cases
                // (when proxies are different nodes) the update of the key set happens asynchronously
                // and this node won't be able to connect to proxies until it happens (and only the
                // connected proxies are included in the advertisement). We run tier1_advertise_proxies
                // periodically in the background anyway to cover those cases.
                state.tier1_advertise_proxies(&clock).await;
            }
            .in_current_span(),
        ));
    }
}

impl actix::Handler<PeerManagerMessageRequest> for PeerManagerActor {
    type Result = PeerManagerMessageResponse;
    #[perf]
    fn handle(&mut self, msg: PeerManagerMessageRequest, ctx: &mut Self::Context) -> Self::Result {
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&[(&msg).into()]).start_timer();
        self.handle_peer_manager_message(msg, ctx)
    }
}

impl actix::Handler<StateSyncEvent> for PeerManagerActor {
    type Result = ();
    #[perf]
    fn handle(&mut self, msg: StateSyncEvent, _ctx: &mut Self::Context) -> Self::Result {
        let _timer =
            metrics::PEER_MANAGER_MESSAGES_TIME.with_label_values(&[(&msg).into()]).start_timer();
        match msg {
            StateSyncEvent::StatePartReceived(shard_id, part_id) => {
                self.state.snapshot_hosts.part_received(shard_id, part_id);
            }
        }
    }
}

impl actix::Handler<Tier3Request> for PeerManagerActor {
    type Result = ();
    #[perf]
    fn handle(&mut self, request: Tier3Request, ctx: &mut Self::Context) -> Self::Result {
        let _timer = metrics::PEER_MANAGER_TIER3_REQUEST_TIME
            .with_label_values(&[(&request.body).into()])
            .start_timer();

        let state = self.state.clone();
        let clock = self.clock.clone();
        ctx.spawn(wrap_future(
            async move {
                let tier3_response = match request.body {
                    Tier3RequestBody::StateHeader(StateHeaderRequestBody { shard_id, sync_hash }) => {
                        match state.state_request_adapter.send_async(StateRequestHeader { shard_id, sync_hash }).await {
                            Ok(Some(client_response)) => {
                                PeerMessage::VersionedStateResponse(*client_response.0)
                            }
                            Ok(None) => {
                                tracing::debug!(target: "network", ?request, "client declined to respond");
                                return;
                            }
                            Err(err) => {
                                tracing::error!(target: "network", ?request, ?err, "client failed to respond");
                                return;
                            }
                        }
                    }
                    Tier3RequestBody::StatePart(StatePartRequestBody { shard_id, sync_hash, part_id }) => {
                        match state.state_request_adapter.send_async(StateRequestPart { shard_id, sync_hash, part_id }).await {
                            Ok(Some(client_response)) => {
                                PeerMessage::VersionedStateResponse(*client_response.0)
                            }
                            Ok(None) => {
                                tracing::debug!(target: "network", "client declined to respond to {:?}", request);
                                return;
                            }
                            Err(err) => {
                                tracing::error!(target: "network", ?err, "client failed to respond to {:?}", request);
                                return;
                            }
                        }
                    }
                };

                // Establish a tier3 connection if we don't have one already
                if !state.tier3.load().ready.contains_key(&request.peer_info.id) {
                    let result = async {
                        let stream = tcp::Stream::connect(
                            &request.peer_info,
                            tcp::Tier::T3,
                            &state.config.socket_options
                        ).await.context("tcp::Stream::connect()")?;
                        PeerActor::spawn_and_handshake(clock.clone(),stream,None,state.clone()).await.context("PeerActor::spawn()")?;
                        anyhow::Ok(())
                    }.await;

                    if let Err(ref err) = result {
                        tracing::info!(target: "network", err = format!("{:#}", err), "tier3 failed to connect to {}", request.peer_info);
                    }
                }

                state.tier3.send_message(request.peer_info.id, Arc::new(tier3_response));
            }
        ));
    }
}

impl actix::Handler<GetDebugStatus> for PeerManagerActor {
    type Result = DebugStatus;
    #[perf]
    fn handle(&mut self, msg: GetDebugStatus, _ctx: &mut actix::Context<Self>) -> Self::Result {
        match msg {
            GetDebugStatus::PeerStore => {
                let mut peer_states_view = self
                    .state
                    .peer_store
                    .load()
                    .iter()
                    .map(|(peer_id, known_peer_state)| KnownPeerStateView {
                        peer_id: peer_id.clone(),
                        status: format!("{:?}", known_peer_state.status),
                        addr: format!("{:?}", known_peer_state.peer_info.addr),
                        first_seen: known_peer_state.first_seen.unix_timestamp(),
                        last_seen: known_peer_state.last_seen.unix_timestamp(),
                        last_attempt: known_peer_state.last_outbound_attempt.clone().map(
                            |(attempt_time, attempt_result)| {
                                let foo = match attempt_result {
                                    Ok(_) => String::from("Ok"),
                                    Err(err) => format!("Error: {:?}", err.as_str()),
                                };
                                (attempt_time.unix_timestamp(), foo)
                            },
                        ),
                    })
                    .collect::<Vec<_>>();

                peer_states_view.sort_by_key(|a| {
                    (
                        -a.last_attempt.clone().map(|(attempt_time, _)| attempt_time).unwrap_or(0),
                        -a.last_seen,
                    )
                });
                DebugStatus::PeerStore(PeerStoreView { peer_states: peer_states_view })
            }
            GetDebugStatus::Graph => DebugStatus::Graph(NetworkGraphView {
                edges: self
                    .state
                    .graph
                    .load()
                    .edges
                    .values()
                    .map(|edge| {
                        let key = edge.key();
                        EdgeView { peer0: key.0.clone(), peer1: key.1.clone(), nonce: edge.nonce() }
                    })
                    .collect(),
                next_hops: (*self.state.graph.routing_table.info().next_hops).clone(),
            }),
            GetDebugStatus::RecentOutboundConnections => {
                DebugStatus::RecentOutboundConnections(RecentOutboundConnectionsView {
                    recent_outbound_connections: self
                        .state
                        .connection_store
                        .get_recent_outbound_connections()
                        .iter()
                        .map(|c| ConnectionInfoView {
                            peer_id: c.peer_info.id.clone(),
                            addr: format!("{:?}", c.peer_info.addr),
                            time_established: c.time_established.unix_timestamp(),
                            time_connected_until: c.time_connected_until.unix_timestamp(),
                        })
                        .collect::<Vec<_>>(),
                })
            }
            GetDebugStatus::Routes => {
                #[cfg(feature = "distance_vector_routing")]
                return DebugStatus::Routes(self.state.graph_v2.get_debug_view());
                #[cfg(not(feature = "distance_vector_routing"))]
                return DebugStatus::Routes(NetworkRoutesView::default());
            }
            GetDebugStatus::SnapshotHosts => DebugStatus::SnapshotHosts(SnapshotHostsView {
                hosts: self
                    .state
                    .snapshot_hosts
                    .get_hosts()
                    .iter()
                    .map(|h| SnapshotHostInfoView {
                        peer_id: h.peer_id.clone(),
                        sync_hash: h.sync_hash,
                        epoch_height: h.epoch_height,
                        shards: h.shards.clone().into_iter().map(Into::into).collect(),
                    })
                    .collect::<Vec<_>>(),
            }),
        }
    }
}
