use crate::PeerManagerActor;
use crate::network_protocol::PeerInfo;
use crate::types::{
    NetworkInfo, NetworkResponses, PeerManagerMessageRequest, PeerManagerMessageResponse,
    SetChainInfo, StateSyncEvent, Tier3Request,
};
use actix::{Actor, ActorContext, Context, Handler};
use futures::{Future, FutureExt, future};
use near_async::messaging::{CanSend, MessageWithCallback};
use near_crypto::{KeyType, SecretKey};
use near_primitives::hash::hash;
use near_primitives::network::PeerId;
use near_primitives::types::EpochId;
use near_primitives::utils::index_to_bytes;
use parking_lot::RwLock;
use rand::{RngCore, thread_rng};
use std::collections::{HashMap, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;
use tokio::sync::Notify;
use tracing::debug;

// `peer_id_from_seed` generate `PeerId` from seed for unit tests
pub fn peer_id_from_seed(seed: &str) -> PeerId {
    PeerId::new(SecretKey::from_seed(KeyType::ED25519, seed).public_key())
}

// `convert_boot_nodes` generate list of `PeerInfos` for unit tests
pub fn convert_boot_nodes(boot_nodes: Vec<(&str, std::net::SocketAddr)>) -> Vec<PeerInfo> {
    let mut result = vec![];
    for (peer_seed, addr) in boot_nodes {
        let id = peer_id_from_seed(peer_seed);
        result.push(PeerInfo::new(id, addr));
    }
    result
}

/// Timeouts by stopping system without any condition and raises panic.
/// Useful in tests to prevent them from running forever.
#[allow(unreachable_code)]
pub fn wait_or_panic(max_wait_ms: u64) {
    actix::spawn(tokio::time::sleep(tokio::time::Duration::from_millis(max_wait_ms)).then(|_| {
        panic!("Timeout exceeded.");
        future::ready(())
    }));
}

/// Waits until condition or timeouts with panic.
/// Use in tests to check for a condition and stop or fail otherwise.
///
/// Prefer using [`wait_or_timeout`], which is not specific to actix.
///
/// # Example
///
/// ```rust,ignore
/// use actix::{System, Actor};
/// use near_network::test_utils::WaitOrTimeoutActor;
/// use std::time::{Instant, Duration};
///
/// near_actix_test_utils::run_actix(async {
///     let start = Instant::now();
///     WaitOrTimeoutActor::new(
///         Box::new(move |ctx| {
///             if start.elapsed() > Duration::from_millis(10) {
///                 System::current().stop()
///             }
///         }),
///         1000,
///         60000,
///     ).start();
/// });
/// ```
pub struct WaitOrTimeoutActor {
    f: Box<dyn FnMut(&mut Context<WaitOrTimeoutActor>)>,
    check_interval_ms: u64,
    max_wait_ms: u64,
    ms_slept: u64,
}

impl WaitOrTimeoutActor {
    pub fn new(
        f: Box<dyn FnMut(&mut Context<WaitOrTimeoutActor>)>,
        check_interval_ms: u64,
        max_wait_ms: u64,
    ) -> Self {
        WaitOrTimeoutActor { f, check_interval_ms, max_wait_ms, ms_slept: 0 }
    }

    fn wait_or_timeout(&mut self, ctx: &mut Context<Self>) {
        (self.f)(ctx);

        near_performance_metrics::actix::run_later(
            ctx,
            tokio::time::Duration::from_millis(self.check_interval_ms),
            move |act, ctx| {
                act.ms_slept += act.check_interval_ms;
                if act.ms_slept > act.max_wait_ms {
                    println!("BBBB Slept {}; max_wait_ms {}", act.ms_slept, act.max_wait_ms);
                    panic!("Timed out waiting for the condition");
                }
                act.wait_or_timeout(ctx);
            },
        );
    }
}

impl Actor for WaitOrTimeoutActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        self.wait_or_timeout(ctx);
    }
}

/// Blocks until `cond` returns `ControlFlow::Break`, checking it every
/// `check_interval_ms`.
///
/// If condition wasn't fulfilled within `max_wait_ms`, returns an error.
pub async fn wait_or_timeout<C, F, T>(
    check_interval_ms: u64,
    max_wait_ms: u64,
    mut cond: C,
) -> Result<T, tokio::time::error::Elapsed>
where
    C: FnMut() -> F,
    F: Future<Output = ControlFlow<T>>,
{
    assert!(
        check_interval_ms < max_wait_ms,
        "interval shorter than wait time, did you swap the argument order?"
    );
    let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(check_interval_ms));
    tokio::time::timeout(tokio::time::Duration::from_millis(max_wait_ms), async {
        loop {
            interval.tick().await;
            if let ControlFlow::Break(res) = cond().await {
                break res;
            }
        }
    })
    .await
}

// Gets random PeerId
pub fn random_peer_id() -> PeerId {
    let sk = SecretKey::from_random(KeyType::ED25519);
    PeerId::new(sk.public_key())
}

// Gets random EpochId
pub fn random_epoch_id() -> EpochId {
    EpochId(hash(index_to_bytes(thread_rng().next_u64()).as_ref()))
}

// Compare whenever routing table match.
pub fn expected_routing_tables(
    got: &HashMap<PeerId, Vec<PeerId>>,
    want: &[(PeerId, Vec<PeerId>)],
) -> bool {
    if got.len() != want.len() {
        return false;
    }

    for (target, want_peers) in want {
        let got_peers = match got.get(target) {
            Some(ps) => ps,
            None => {
                return false;
            }
        };
        if got_peers.len() != want_peers.len() {
            return false;
        }
        for peer in want_peers {
            if !got_peers.contains(peer) {
                return false;
            }
        }
    }
    true
}

/// `GetInfo` gets `NetworkInfo` from `PeerManager`.
#[derive(actix::Message, Debug)]
#[rtype(result = "NetworkInfo")]
pub struct GetInfo {}

impl Handler<GetInfo> for PeerManagerActor {
    type Result = crate::types::NetworkInfo;

    fn handle(&mut self, _msg: GetInfo, _ctx: &mut Context<Self>) -> Self::Result {
        self.get_network_info()
    }
}

// `StopSignal is used to stop PeerManagerActor for unit tests
#[derive(actix::Message, Default, Debug)]
#[rtype(result = "()")]
pub struct StopSignal {
    pub should_panic: bool,
}

impl StopSignal {
    pub fn should_panic() -> Self {
        Self { should_panic: true }
    }
}

impl Handler<StopSignal> for PeerManagerActor {
    type Result = ();

    fn handle(&mut self, msg: StopSignal, ctx: &mut Self::Context) -> Self::Result {
        debug!(target: "network", "Receive Stop Signal.");

        if msg.should_panic {
            panic!("Node crashed");
        } else {
            ctx.stop();
        }
    }
}

// Mocked `PeerManager` adapter, has a queue of `PeerManagerMessageRequest` messages.
#[derive(Default)]
pub struct MockPeerManagerAdapter {
    pub requests: Arc<RwLock<VecDeque<PeerManagerMessageRequest>>>,
    pub notify: Notify,
}

impl CanSend<MessageWithCallback<PeerManagerMessageRequest, PeerManagerMessageResponse>>
    for MockPeerManagerAdapter
{
    fn send(
        &self,
        message: MessageWithCallback<PeerManagerMessageRequest, PeerManagerMessageResponse>,
    ) {
        self.requests.write().push_back(message.message);
        self.notify.notify_one();
        (message.callback)(
            std::future::ready(Ok(PeerManagerMessageResponse::NetworkResponses(
                NetworkResponses::NoResponse,
            )))
            .boxed(),
        );
    }
}

impl CanSend<PeerManagerMessageRequest> for MockPeerManagerAdapter {
    fn send(&self, msg: PeerManagerMessageRequest) {
        self.requests.write().push_back(msg);
        self.notify.notify_one();
    }
}

impl CanSend<SetChainInfo> for MockPeerManagerAdapter {
    fn send(&self, _msg: SetChainInfo) {}
}

impl CanSend<StateSyncEvent> for MockPeerManagerAdapter {
    fn send(&self, _msg: StateSyncEvent) {}
}

impl CanSend<Tier3Request> for MockPeerManagerAdapter {
    fn send(&self, _msg: Tier3Request) {}
}

impl MockPeerManagerAdapter {
    pub fn pop(&self) -> Option<PeerManagerMessageRequest> {
        self.requests.write().pop_front()
    }
    pub fn pop_most_recent(&self) -> Option<PeerManagerMessageRequest> {
        self.requests.write().pop_back()
    }
    pub fn put_back_most_recent(&self, request: PeerManagerMessageRequest) {
        self.requests.write().push_back(request);
    }
    /// Calls the handler for each message, but removing only those for which the handler returns
    /// None (or else the returned message gets requeued; the returned message should be the same
    /// as the one given). Returns true if any message was removed.
    pub fn handle_filtered(
        &self,
        mut f: impl FnMut(PeerManagerMessageRequest) -> Option<PeerManagerMessageRequest>,
    ) -> bool {
        // We get the count first and then pop one by one, that way we avoid
        // grabbing the lock for the whole duration, which might lead to a
        // deadlock if the processing of the request results in more network
        // messages.
        let num_requests = self.requests.read().len();
        let mut handled = false;
        for _ in 0..num_requests {
            let request = self.requests.write().pop_front().unwrap();
            if let Some(request) = f(request) {
                self.requests.write().push_back(request);
            } else {
                handled = true;
            }
        }
        handled
    }
}

#[derive(actix::Message, Clone, Debug)]
#[rtype(result = "()")]
pub struct SetAdvOptions {
    pub set_max_peers: Option<u64>,
}
