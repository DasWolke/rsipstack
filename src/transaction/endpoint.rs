use super::{
    key::TransactionKey,
    make_via_branch,
    timer::Timer,
    transaction::{Transaction, TransactionEvent, TransactionEventSender},
    SipConnection, TransactionReceiver, TransactionSender, TransactionTimer,
};
use crate::{
    transport::{
        connection::{TransportReceiver, TransportSender},
        SipAddr, TransportEvent, TransportLayer,
    },
    Error, Result, USER_AGENT,
};
use rsip::SipMessage;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    select,
    sync::mpsc::{error, unbounded_channel},
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

/// SIP Endpoint Core Implementation
///
/// `EndpointInner` is the core implementation of a SIP endpoint that manages
/// transactions, timers, and transport layer communication. It serves as the
/// central coordination point for all SIP protocol operations.
///
/// # Key Responsibilities
///
/// * Managing active SIP transactions
/// * Handling SIP timers (Timer A, B, D, E, F, G, K)
/// * Coordinating with the transport layer
/// * Processing incoming and outgoing SIP messages
/// * Maintaining transaction state and cleanup
///
/// # Fields
///
/// * `allows` - List of supported SIP methods
/// * `user_agent` - User-Agent header value for outgoing messages
/// * `timers` - Timer management system for SIP timers
/// * `transport_layer` - Transport layer for network communication
/// * `finished_transactions` - Cache of completed transactions
/// * `transactions` - Active transaction senders
/// * `incoming_sender` - Channel for incoming transaction notifications
/// * `cancel_token` - Cancellation token for graceful shutdown
/// * `timer_interval` - Interval for timer processing
/// * `transport_tx` - Transport event sender
/// * `transport_rx` - Transport event receiver
/// * `t1`, `t4`, `t1x64` - SIP timer values as per RFC 3261
///
/// # Timer Values
///
/// * `t1` - RTT estimate (default 500ms)
/// * `t4` - Maximum duration a message will remain in the network (default 4s)
/// * `t1x64` - Maximum retransmission timeout (default 32s)
pub struct EndpointInner {
    pub allows: Vec<rsip::Method>,
    pub user_agent: String,
    pub timers: Timer<TransactionTimer>,
    pub transport_layer: TransportLayer,
    pub finished_transactions: Mutex<HashMap<TransactionKey, Option<SipMessage>>>,
    pub transactions: Mutex<HashMap<TransactionKey, TransactionEventSender>>,
    incoming_sender: Mutex<Option<TransactionSender>>,
    cancel_token: CancellationToken,
    timer_interval: Duration,

    pub(crate) transport_tx: TransportSender,
    pub(crate) transport_rx: Mutex<Option<TransportReceiver>>,

    pub t1: Duration,
    pub t4: Duration,
    pub t1x64: Duration,
}
pub type EndpointInnerRef = Arc<EndpointInner>;

/// SIP Endpoint Builder
///
/// `EndpointBuilder` provides a fluent interface for constructing SIP endpoints
/// with custom configuration. It follows the builder pattern to allow flexible
/// endpoint configuration.
///
/// # Examples
///
/// ```rust
/// use rsipstack::EndpointBuilder;
/// use std::time::Duration;
///
/// let endpoint = EndpointBuilder::new()
///     .with_user_agent("MyApp/1.0")
///     .with_timer_interval(Duration::from_millis(10))
///     .with_allows(vec![rsip::Method::Invite, rsip::Method::Bye])
///     .build();
/// ```
pub struct EndpointBuilder {
    allows: Vec<rsip::Method>,
    user_agent: String,
    transport_layer: Option<TransportLayer>,
    cancel_token: Option<CancellationToken>,
    timer_interval: Option<Duration>,
}

/// SIP Endpoint
///
/// `Endpoint` is the main entry point for SIP protocol operations. It provides
/// a high-level interface for creating and managing SIP transactions, handling
/// incoming requests, and coordinating with the transport layer.
///
/// # Key Features
///
/// * Transaction management and lifecycle
/// * Automatic timer handling per RFC 3261
/// * Transport layer abstraction
/// * Graceful shutdown support
/// * Incoming request processing
///
/// # Examples
///
/// ```rust,no_run
/// use rsipstack::EndpointBuilder;
/// use tokio_util::sync::CancellationToken;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let endpoint = EndpointBuilder::new()
///         .with_user_agent("MyApp/1.0")
///         .build();
///     
///     // Get incoming transactions
///     let mut incoming = endpoint.incoming_transactions();
///     
///     // Start the endpoint (in production, you'd run this in a separate task)
///     // let endpoint_inner = endpoint.inner.clone();
///     // tokio::spawn(async move {
///     //     endpoint_inner.serve().await.ok();
///     // });
///     
///     // Process incoming transactions
///     while let Some(transaction) = incoming.recv().await {
///         // Handle transaction
///         break; // Exit for example
///     }
///     
///     Ok(())
/// }
/// ```
///
/// # Lifecycle
///
/// 1. Create endpoint using `EndpointBuilder`
/// 2. Start serving with `serve()` method
/// 3. Process incoming transactions via `incoming_transactions()`
/// 4. Shutdown gracefully with `shutdown()`
pub struct Endpoint {
    pub inner: EndpointInnerRef,
}

impl EndpointInner {
    pub fn new(
        user_agent: String,
        transport_layer: TransportLayer,
        cancel_token: CancellationToken,
        timer_interval: Option<Duration>,
        allows: Vec<rsip::Method>,
    ) -> Arc<Self> {
        let (transport_tx, transport_rx) = unbounded_channel();
        Arc::new(EndpointInner {
            allows,
            user_agent,
            timers: Timer::new(),
            transport_layer,
            transactions: Mutex::new(HashMap::new()),
            finished_transactions: Mutex::new(HashMap::new()),
            timer_interval: timer_interval.unwrap_or(Duration::from_millis(20)),
            transport_tx,
            transport_rx: Mutex::new(Some(transport_rx)),
            cancel_token,
            incoming_sender: Mutex::new(None),
            t1: Duration::from_millis(500),
            t4: Duration::from_secs(4),
            t1x64: Duration::from_millis(64 * 500),
        })
    }

    pub async fn serve(self: &Arc<Self>) -> Result<()> {
        select! {
            _ = self.cancel_token.cancelled() => {
            },
            r = self.clone().process_timer() => {
                _ = r?
            },
            r = self.clone().process_transport_layer() => {
                _ = r?
            },
        }
        Ok(())
    }

    // process transport layer, receive message from transport layer
    async fn process_transport_layer(self: Arc<Self>) -> Result<()> {
        let transport_tx = self.transport_tx.clone();
        self.transport_layer.serve_listens(transport_tx).await.ok();

        let mut transport_rx = match self.transport_rx.lock().unwrap().take() {
            Some(rx) => rx,
            None => {
                return Err(Error::EndpointError("transport_rx not set".to_string()));
            }
        };

        while let Some(event) = transport_rx.recv().await {
            match event {
                TransportEvent::Incoming(msg, connection, from) => {
                    match self.on_received_message(msg, connection).await {
                        Ok(()) => {}
                        Err(e) => {
                            warn!("on_received_message error:{} {:?}", from, e);
                        }
                    }
                }
                TransportEvent::New(t) => {
                    trace!("new connection {} ", t);
                }
                TransportEvent::Closed(t) => {
                    trace!("connection closed {} ", t);
                }
            }
        }
        Ok(())
    }

    pub async fn process_timer(self: Arc<Self>) -> Result<()> {
        while !self.cancel_token.is_cancelled() {
            for t in self.timers.poll(Instant::now()) {
                match t {
                    TransactionTimer::TimerCleanup(key) => {
                        debug!("TimerCleanup {}", key);
                        self.transactions.lock().unwrap().remove(&key);
                        self.finished_transactions.lock().unwrap().remove(&key);
                        continue;
                    }
                    _ => {}
                }

                if let Some(tu) = { self.transactions.lock().unwrap().get(t.key()) } {
                    match tu.send(TransactionEvent::Timer(t)) {
                        Ok(_) => {}
                        Err(error::SendError(t)) => match t {
                            TransactionEvent::Timer(t) => {
                                self.detach_transaction(t.key(), None);
                            }
                            _ => {}
                        },
                    }
                }
            }
            sleep(self.timer_interval).await;
        }
        Ok(())
    }

    pub fn attach_incoming_sender(&self, sender: Option<TransactionSender>) {
        *self.incoming_sender.lock().unwrap() = sender;
    }

    // receive message from transport layer
    pub async fn on_received_message(
        self: &Arc<Self>,
        msg: SipMessage,
        connection: SipConnection,
    ) -> Result<()> {
        let mut key = match &msg {
            SipMessage::Request(req) => {
                TransactionKey::from_request(req, super::key::TransactionRole::Server)?
            }
            SipMessage::Response(resp) => {
                TransactionKey::from_response(resp, super::key::TransactionRole::Client)?
            }
        };

        // check is the termination of an existing transaction
        let last_message = self
            .finished_transactions
            .lock()
            .unwrap()
            .get(&key)
            .map(|m| m.clone())
            .flatten();

        if let Some(last_message) = last_message {
            connection.send(last_message, None).await?;
            return Ok(());
        }

        match self.transactions.lock().unwrap().get(&key) {
            Some(tu) => {
                tu.send(TransactionEvent::Received(msg, Some(connection)))
                    .map_err(|e| Error::TransactionError(e.to_string(), key))?;
                return Ok(());
            }
            None => {}
        }
        // if the transaction is not exist, create a new transaction
        let request = match msg {
            SipMessage::Request(req) => req,
            SipMessage::Response(resp) => {
                debug!("the transaction is not exist {} {}", key, resp);
                return Ok(());
            }
        };

        if self.incoming_sender.lock().unwrap().is_none() {
            // For OPTIONS requests, respond with 200 OK instead of 503
            let resp = if request.method == rsip::Method::Options {
                let mut resp = self.make_response(&request, rsip::StatusCode::OK, None);
                // Add Allow header with supported methods
                let allowed_methods = self.allows.iter()
                    .map(|m| m.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                resp.headers.unique_push(rsip::Header::Allow(allowed_methods.into()));
                resp.headers.unique_push(rsip::Header::ContentLength(0.into()));
                resp
            } else {
                self.make_response(&request, rsip::StatusCode::ServiceUnavailable, None)
            };
            connection.send(resp.into(), None).await?;
            
            // For OPTIONS, we handled it successfully, so don't return an error
            if request.method == rsip::Method::Options {
                return Ok(());
            }
            
            return Err(Error::TransactionError(
                "incoming_sender not set".to_string(),
                key,
            ));
        }

        if matches!(request.method, rsip::Method::Ack | rsip::Method::Cancel) {
            key =
                TransactionKey::from_ack_or_cancel(&request, super::key::TransactionRole::Server)?;
        }

        let tx =
            Transaction::new_server(key.clone(), request.clone(), self.clone(), Some(connection));

        self.incoming_sender
            .lock()
            .unwrap()
            .as_ref()
            .map(|s| s.send(tx));
        return Ok(());
    }

    pub fn attach_transaction(&self, key: &TransactionKey, tu_sender: TransactionEventSender) {
        trace!("attach_transaction {}", key);
        self.transactions
            .lock()
            .unwrap()
            .insert(key.clone(), tu_sender);
    }

    pub fn detach_transaction(&self, key: &TransactionKey, last_message: Option<SipMessage>) {
        trace!("detach_transaction {}", key);
        self.transactions.lock().unwrap().remove(key);

        if let Some(msg) = last_message {
            let timer_k_duration = if msg.is_request() {
                self.t4
            } else {
                self.t1x64
            };

            self.timers.timeout(
                timer_k_duration,
                TransactionTimer::TimerCleanup(key.clone()), // maybe use TimerK ???
            );

            self.finished_transactions
                .lock()
                .unwrap()
                .insert(key.clone(), Some(msg));
        }
    }

    pub fn get_addrs(&self) -> Vec<SipAddr> {
        self.transport_layer.get_addrs()
    }

    pub fn get_record_route(&self) -> Result<rsip::typed::RecordRoute> {
        let first_addr = self
            .transport_layer
            .get_addrs()
            .first()
            .ok_or(Error::EndpointError("not sipaddrs".to_string()))
            .cloned()?;
        let rr = rsip::UriWithParamsList(vec![rsip::UriWithParams {
            uri: first_addr.into(),
            params: vec![rsip::Param::Other("lr".into(), None)].into(),
        }]);
        Ok(rr.into())
    }

    pub fn get_via(
        &self,
        addr: Option<crate::transport::SipAddr>,
        branch: Option<rsip::Param>,
    ) -> Result<rsip::typed::Via> {
        let mut first_addr = match addr {
            Some(addr) => addr,
            None => self
                .transport_layer
                .get_addrs()
                .first()
                .ok_or(Error::EndpointError("not sipaddrs".to_string()))
                .cloned()?,
        };

        // If the address is 0.0.0.0, try to get the actual local IP
        if first_addr.addr.host == rsip::Host::IpAddr(std::net::IpAddr::V4(std::net::Ipv4Addr::new(0, 0, 0, 0))) {
            if let Ok(interfaces) = get_if_addrs::get_if_addrs() {
                if let Some(ip) = interfaces
                    .iter()
                    .find(|i| !i.is_loopback() && matches!(i.addr, get_if_addrs::IfAddr::V4(_)))
                    .and_then(|i| match &i.addr {
                        get_if_addrs::IfAddr::V4(addr) => Some(std::net::IpAddr::V4(addr.ip)),
                        _ => None,
                    })
                {
                    first_addr.addr.host = ip.into();
                    debug!("Replaced 0.0.0.0 with actual local IP {} in Via header", ip);
                }
            }
        }

        let via = rsip::typed::Via {
            version: rsip::Version::V2,
            transport: first_addr.r#type.unwrap_or_default(),
            uri: first_addr.addr.into(),
            params: vec![
                branch.unwrap_or_else(|| make_via_branch()),
                rsip::Param::Other("rport".into(), None),
            ]
            .into(),
        };
        Ok(via)
    }
}

impl EndpointBuilder {
    pub fn new() -> Self {
        EndpointBuilder {
            allows: Vec::new(),
            user_agent: USER_AGENT.to_string(),
            transport_layer: None,
            cancel_token: None,
            timer_interval: None,
        }
    }

    pub fn with_user_agent(&mut self, user_agent: &str) -> &mut Self {
        self.user_agent = user_agent.to_string();
        self
    }

    pub fn with_transport_layer(&mut self, transport_layer: TransportLayer) -> &mut Self {
        self.transport_layer.replace(transport_layer);
        self
    }

    pub fn with_cancel_token(&mut self, cancel_token: CancellationToken) -> &mut Self {
        self.cancel_token.replace(cancel_token);
        self
    }

    pub fn with_timer_interval(&mut self, timer_interval: Duration) -> &mut Self {
        self.timer_interval.replace(timer_interval);
        self
    }
    pub fn with_allows(&mut self, allows: Vec<rsip::Method>) -> &mut Self {
        self.allows = allows;
        self
    }
    pub fn build(&mut self) -> Endpoint {
        let cancel_token = self.cancel_token.take().unwrap_or_default();

        let transport_layer = self
            .transport_layer
            .take()
            .unwrap_or(TransportLayer::new(cancel_token.child_token()));

        let allows = self.allows.to_owned();
        let user_agent = self.user_agent.to_owned();
        let timer_interval = self.timer_interval.to_owned();

        let core = EndpointInner::new(
            user_agent,
            transport_layer,
            cancel_token,
            timer_interval,
            allows,
        );

        Endpoint { inner: core }
    }
}

impl Endpoint {
    pub async fn serve(&self) {
        let inner = self.inner.clone();
        match inner.serve().await {
            Ok(()) => {
                info!("endpoint shutdown");
            }
            Err(e) => {
                warn!("endpoint serve error: {:?}", e);
            }
        }
    }

    pub fn shutdown(&self) {
        info!("endpoint shutdown requested");
        self.inner.cancel_token.cancel();
    }

    //
    // get incoming requests from the endpoint
    //
    pub fn incoming_transactions(&self) -> TransactionReceiver {
        let (tx, rx) = unbounded_channel();
        self.inner.attach_incoming_sender(Some(tx));
        rx
    }

    pub fn get_addrs(&self) -> Vec<SipAddr> {
        self.inner.transport_layer.get_addrs()
    }
}
