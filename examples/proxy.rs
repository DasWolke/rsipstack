use clap::Parser;
use get_if_addrs::get_if_addrs;
use rsip::headers::UntypedHeader;
use rsip::prelude::{HeadersExt, ToTypedHeader};
use rsipstack::dialog::DialogId;
use rsipstack::rsip_ext::{extract_uri_from_contact, RsipHeadersExt};
use rsipstack::transaction::key::{TransactionKey, TransactionRole};
use rsipstack::transaction::transaction::Transaction;
use rsipstack::transaction::TransactionReceiver;
use rsipstack::transport::udp::UdpConnection;
use rsipstack::transport::SipAddr;
use rsipstack::{header_pop, Error, Result};
use rsipstack::{transport::TransportLayer, EndpointBuilder};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::{env, vec};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// A SIP client example that sends a REGISTER request to a SIP server.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// SIP address
    #[arg(long)]
    addr: Option<String>,
    /// SIP port
    #[arg(long, default_value = "25060")]
    port: u16,

    /// External IP address
    #[arg(long)]
    external_ip: Option<String>,
}
#[derive(Clone)]
struct User {
    username: String,
    destination: SipAddr,
}

struct AppState {
    users: Mutex<HashMap<String, User>>,
    sessions: Mutex<HashSet<DialogId>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .try_init()
        .ok();
    if let Err(e) = dotenv::dotenv() {
        info!("Failed to load .env file: {}", e);
    }
    let args = Args::parse();

    let token = CancellationToken::new();
    let transport_layer = TransportLayer::new(token.clone());

    let external_ip = args
        .external_ip
        .unwrap_or(env::var("EXTERNAL_IP").unwrap_or_default());

    let external = if external_ip.is_empty() {
        None
    } else {
        Some(format!("{}:{}", external_ip, args.port).parse()?)
    };
    let addr = match args.addr {
        Some(addr) => addr.parse::<std::net::IpAddr>()?,
        None => get_if_addrs()?
            .iter()
            .find(|i| !i.is_loopback())
            .map(|i| match i.addr {
                get_if_addrs::IfAddr::V4(ref addr) => Ok(std::net::IpAddr::V4(addr.ip)),
                _ => Err(Error::Error("No IPv4 address found".to_string())),
            })
            .unwrap_or(Err(Error::Error("No interface found".to_string())))?,
    };
    let connection = UdpConnection::create_connection(
        format!("{}:{}", addr, args.port).parse()?,
        external.clone(),
    )
    .await?;

    transport_layer.add_transport(connection.into());

    let endpoint = EndpointBuilder::new()
        .with_cancel_token(token.clone())
        .with_transport_layer(transport_layer)
        .build();

    let incoming = endpoint.incoming_transactions();

    select! {
        _ = endpoint.serve() => {
            info!("user agent finished");
        }
        r = process_incoming_request(incoming) => {
            info!("serve loop finished {:?}", r);
        }
    }
    Ok(())
}

async fn process_incoming_request(mut incoming: TransactionReceiver) -> Result<()> {
    let state = Arc::new(AppState {
        users: Mutex::new(HashMap::new()),
        sessions: Mutex::new(HashSet::new()),
    });
    while let Some(mut tx) = incoming.recv().await {
        info!("Received transaction: {:?}", tx.key);
        let state = state.clone();
        match tx.original.method {
            rsip::Method::Invite => {
                tokio::spawn(async move {
                    handle_invite(state, tx).await?;
                    Ok::<_, Error>(())
                });
            }
            rsip::Method::Bye => {
                tokio::spawn(async move {
                    handle_bye(state, tx).await?;
                    Ok::<_, Error>(())
                });
            }
            rsip::Method::Register => {
                handle_register(state, tx).await?;
            }
            rsip::Method::Ack => {
                info!("received out of transaction ack: {:?}", tx.original.method);
                let dialog_id = DialogId::try_from(&tx.original)?;
                if !state.sessions.lock().unwrap().contains(&dialog_id) {
                    tx.reply(rsip::StatusCode::NotAcceptable).await?;
                    continue;
                }
                // forward ACK
                let callee = tx
                    .original
                    .to_header()?
                    .uri()?
                    .auth
                    .map(|a| a.user)
                    .unwrap_or_default();
                let target = state.users.lock().unwrap().get(&callee).cloned();
                let target = match target {
                    Some(u) => u,
                    None => {
                        info!("ack user not found: {}", callee);
                        tx.reply(rsip::StatusCode::NotAcceptable).await?;
                        continue;
                    }
                };
                // send to target
                let mut ack_req = tx.original.clone();
                let via = tx.endpoint_inner.get_via(None, None)?;
                ack_req.headers.push_front(via.into());
                let key = TransactionKey::from_request(&ack_req, TransactionRole::Client)
                    .expect("client_transaction");
                let mut ack_tx =
                    Transaction::new_client(key, ack_req, tx.endpoint_inner.clone(), None);
                ack_tx.destination = Some(target.destination);
                ack_tx.send().await?;
            }
            _ => {
                tx.reply(rsip::StatusCode::NotAcceptable).await?;
            }
        }
    }
    Ok::<_, Error>(())
}

impl TryFrom<&rsip::Request> for User {
    type Error = Error;
    fn try_from(req: &rsip::Request) -> Result<Self> {
        let contact = extract_uri_from_contact(req.contact_header()?.value())?;
        let via = req.via_header()?.typed()?;

        let mut destination = SipAddr {
            r#type: via.uri.transport().cloned(),
            addr: contact.host_with_port,
        };

        via.params.iter().for_each(|param| match param {
            rsip::Param::Transport(t) => {
                destination.r#type = Some(t.clone());
            }
            rsip::Param::Received(r) => match r.value().try_into() {
                Ok(addr) => destination.addr.host = addr,
                Err(_) => {}
            },
            rsip::Param::Other(o, Some(v)) => {
                if o.value().eq_ignore_ascii_case("rport") {
                    match v.value().try_into() {
                        Ok(port) => destination.addr.port = Some(port),
                        Err(_) => {}
                    }
                }
            }
            _ => {}
        });

        let username = req
            .from_header()?
            .uri()?
            .user()
            .unwrap_or_default()
            .to_string();

        Ok(User {
            username,
            destination,
        })
    }
}

async fn handle_register(state: Arc<AppState>, mut tx: Transaction) -> Result<()> {
    let user = match User::try_from(&tx.original) {
        Ok(u) => u,
        Err(e) => {
            info!("failed to parse contact: {:?}", e);
            return tx.reply(rsip::StatusCode::BadRequest).await;
        }
    };

    let contact = rsip::typed::Contact {
        display_name: None,
        uri: rsip::Uri {
            scheme: Some(rsip::Scheme::Sip),
            auth: Some(rsip::Auth {
                user: user.username.clone(),
                password: None,
            }),
            host_with_port: user.destination.addr.clone(),
            ..Default::default()
        },
        params: vec![],
    };
    match tx.original.expires_header() {
        Some(expires) => match expires.value().parse::<u32>() {
            Ok(v) => {
                if v == 0 {
                    // remove user
                    info!("unregistered user: {} -> {}", user.username, contact);
                    state.users.lock().unwrap().remove(&user.username);
                    return tx.reply(rsip::StatusCode::OK).await;
                }
            }
            Err(_) => {}
        },
        None => {}
    }

    info!("Registered user: {} -> {}", user.username, contact);
    state
        .users
        .lock()
        .unwrap()
        .insert(user.username.clone(), user);

    let headers = vec![contact.into(), rsip::Header::Expires(60.into())];
    tx.reply_with(rsip::StatusCode::OK, headers, None).await
}

async fn handle_invite(state: Arc<AppState>, mut tx: Transaction) -> Result<()> {
    let caller = tx.original.from_header()?.uri()?.to_string();
    let callee = tx
        .original
        .to_header()?
        .uri()?
        .auth
        .map(|a| a.user)
        .unwrap_or_default();
    let target = state.users.lock().unwrap().get(&callee).cloned();

    let record_route = tx.endpoint_inner.get_record_route()?;

    let target = match target {
        Some(u) => u,
        None => {
            info!("user not found: {}", callee);
            tx.reply_with(rsip::StatusCode::NotFound, vec![record_route.into()], None)
                .await
                .ok();
            // wait for ACK
            while let Some(msg) = tx.receive().await {
                match msg {
                    rsip::message::SipMessage::Request(req) => match req.method {
                        rsip::Method::Ack => {
                            info!("received no-2xx ACK");
                            break;
                        }
                        _ => {}
                    },
                    _ => {}
                }
            }
            return Ok(());
        }
    };

    let mut inv_req = tx.original.clone();
    let via = tx.endpoint_inner.get_via(None, None)?;
    inv_req.headers.push_front(via.into());
    inv_req.headers.push_front(record_route.clone().into());
    let key = TransactionKey::from_request(&inv_req, TransactionRole::Client)
        .expect("client_transaction");

    info!("Forwarding INVITE to: {} -> {}", caller, target.destination);

    let mut inv_tx = Transaction::new_client(key, inv_req, tx.endpoint_inner.clone(), None);
    inv_tx.destination = Some(target.destination);

    // add Via
    inv_tx.send().await?;

    loop {
        if inv_tx.is_terminated() {
            break;
        }
        select! {
            msg = inv_tx.receive() => {
                info!("UAC Received message: {}", msg.as_ref().map(|m| m.to_string()).unwrap_or_default());
                if let Some(msg) = msg {
                    match msg {
                        rsip::message::SipMessage::Response(mut resp) => {
                            // pop first Via
                            header_pop!(resp.headers, rsip::Header::Via);
                            resp.headers.push_front(record_route.clone().into());
                            if resp.status_code.kind() == rsip::StatusCodeKind::Successful {
                                let dialog_id = DialogId::try_from(&resp)?;
                                info!("add session: {}", dialog_id);
                                state.sessions.lock().unwrap().insert(dialog_id);
                            }
                            tx.respond(resp).await?;
                        }
                        _ => {}
                    }
                }
            }
            msg = tx.receive() => {
                info!("UAS Received message: {}", msg.as_ref().map(|m| m.to_string()).unwrap_or_default());
                if let Some(msg) = msg {
                    match msg {
                        rsip::message::SipMessage::Request(req) => match req.method {
                            rsip::Method::Ack => {
                                inv_tx.send_ack(req).await?;
                            }
                            rsip::Method::Cancel => {
                                inv_tx.send_cancel(req).await?;
                            }
                            _ => {}
                        },
                        rsip::message::SipMessage::Response(_) => {}
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_bye(state: Arc<AppState>, mut tx: Transaction) -> Result<()> {
    let caller = tx.original.from_header()?.uri()?.to_string();
    let callee = tx
        .original
        .to_header()?
        .uri()?
        .auth
        .map(|a| a.user)
        .unwrap_or_default();
    let target = state.users.lock().unwrap().get(&callee).cloned();
    let peer = match target {
        Some(u) => u,
        None => {
            info!("bye user not found: {}", callee);
            return tx.reply(rsip::StatusCode::NotFound).await;
        }
    };

    let mut inv_req = tx.original.clone();
    let via = tx.endpoint_inner.get_via(None, None)?;
    inv_req.headers.push_front(via.into());

    let key = TransactionKey::from_request(&inv_req, TransactionRole::Client)
        .expect("client_transaction");

    info!("Forwarding BYE to: {} -> {}", caller, peer.destination);

    let mut bye_tx = Transaction::new_client(key, inv_req, tx.endpoint_inner.clone(), None);
    bye_tx.destination = Some(peer.destination);

    bye_tx.send().await?;

    let dialog_id = DialogId::try_from(&bye_tx.original)?;
    if state.sessions.lock().unwrap().remove(&dialog_id) {
        info!("removed session: {}", dialog_id);
    }

    while let Some(msg) = bye_tx.receive().await {
        match msg {
            rsip::message::SipMessage::Response(mut resp) => {
                // pop first Via
                header_pop!(resp.headers, rsip::Header::Via);
                info!("UAC/BYE Forwarding response: {}", resp.to_string());
                tx.respond(resp).await?;
            }
            _ => {
                error!("UAC/BYE Received request: {}", msg.to_string());
            }
        }
    }

    Ok(())
}
