use clap::Parser;
use play_file::{build_rtp_conn, play_example_file};
use rsip::prelude::HeadersExt;
use rsip::typed::MediaType;
use rsipstack::dialog::dialog::{Dialog, DialogState, DialogStateReceiver, DialogStateSender};
use rsipstack::dialog::dialog_layer::DialogLayer;
use rsipstack::dialog::invitation::InviteOption;
use rsipstack::dialog::server_dialog::ServerInviteDialog;
use rsipstack::transaction::endpoint::EndpointInnerRef;
use rsipstack::Result;
use rsipstack::{
    dialog::{authenticate::Credential, registration::Registration},
    transaction::TransactionReceiver,
    transport::{udp::UdpConnection, TransportLayer},
    EndpointBuilder, Error,
};
use std::{env, sync::Arc, time::Duration};
use tokio::sync::mpsc::unbounded_channel;
use tokio::{select, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
mod play_file;
mod stun;
#[derive(Debug, Clone)]
struct MediaSessionOption {
    pub stun: bool,
    pub stun_server: Option<String>,
    pub external_ip: Option<String>,
    pub rtp_start_port: u16,
}

/// A SIP client example that sends a REGISTER request to a SIP server.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// SIP port
    #[arg(long, default_value = "25060")]
    port: u16,

    /// SIP port
    #[arg(long, default_value = "12000")]
    rtp_start_port: u16,

    /// External IP address
    #[arg(long)]
    external_ip: Option<String>,

    /// SIP server address
    #[arg(long)]
    sip_server: Option<String>,

    /// SIP user
    #[arg(long)]
    user: Option<String>,

    /// SIP password
    #[arg(long)]
    password: Option<String>,

    #[arg(long, default_value = "restsend.com:3478")]
    stun_server: Option<String>,

    #[arg(long)]
    stun: bool,

    #[arg(long)]
    call: Option<String>,
}

// A sip client example, that sends a REGISTER request to a sip server.
#[tokio::main]
async fn main() -> rsipstack::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_file(true)
        .with_line_number(true)
        .with_timer(tracing_subscriber::fmt::time::LocalTime::rfc_3339())
        .try_init()
        .ok();

    if let Err(e) = dotenv::dotenv() {
        info!("Failed to load .env file: {}", e);
    }

    let args = Args::parse();

    info!("Starting SIP client");

    let sip_server = args
        .sip_server
        .unwrap_or(env::var("SIP_SERVER").unwrap_or_default());
    let sip_username = args
        .user
        .unwrap_or(env::var("SIP_USERNAME").unwrap_or_default());
    let sip_password = args
        .password
        .unwrap_or(env::var("SIP_PASSWORD").unwrap_or_default());

    let opt = MediaSessionOption {
        stun: args.stun,
        stun_server: args.stun_server.clone(),
        external_ip: args.external_ip.clone(),
        rtp_start_port: args.rtp_start_port,
    };

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

    let addr = stun::get_first_non_loopback_interface()?;
    let mut connection = UdpConnection::create_connection(
        format!("{}:{}", addr, args.port).parse()?,
        external.clone(),
    )
    .await?;

    if external.is_none() && args.stun {
        if let Some(server) = args.stun_server {
            match stun::external_by_stun(&mut connection, &server, Duration::from_secs(5)).await {
                Ok(socket) => info!("external IP: {:?}", socket),
                Err(e) => info!("Failed to get external IP, stunserver {} : {:?}", server, e),
            }
        }
    }

    transport_layer.add_transport(connection.into());

    let endpoint = EndpointBuilder::new()
        .with_cancel_token(token.clone())
        .with_transport_layer(transport_layer)
        .build();

    let credential = Credential {
        username: sip_username.clone(),
        password: sip_password,
        realm: None,
    };

    let incoming = endpoint.incoming_transactions();
    let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));

    let (state_sender, state_receiver) = unbounded_channel();

    let first_addr = endpoint
        .get_addrs()
        .first()
        .ok_or(crate::Error::Error("no address found".to_string()))?
        .clone();

    let contact = rsip::Uri {
        scheme: Some(rsip::Scheme::Sip),
        auth: Some(rsip::Auth {
            user: sip_username,
            password: None,
        }),
        host_with_port: first_addr.addr.into(),
        params: vec![],
        headers: vec![],
    };

    select! {
        _ = endpoint.serve() => {
            info!("user agent finished");
        }
        r = process_registration(endpoint.inner.clone(), sip_server, credential.clone(), token.clone()) => {
            info!("register loop finished {:?}", r);
        }
        r = process_incoming_request(dialog_layer.clone(), incoming, state_sender.clone(), contact.clone()) => {
            info!("serve loop finished {:?}", r);
        }
        r = process_dialog(dialog_layer.clone(), state_receiver, opt.clone()) => {
            info!("dialog loop finished {:?}", r);
        }
        r = async {
            if let Some(callee) = args.call.clone() {
                let invite_option = InviteOption {
                    callee: callee.try_into().expect("callee"),
                    caller: contact.clone(),
                    content_type: None,
                    offer: None,
                    contact: contact.clone(),
                    credential: Some(credential.clone()),
                    headers: None,
                };

                match make_call(dialog_layer, invite_option, opt, state_sender).await {
                    Ok(_) => info!("Call finished"),
                    Err(e) => info!("Failed to make call: {:?}", e),
                }
            }
            select! {
                _ = token.cancelled() => {
                    info!("token cancelled")
                }
            }
        } => {
            info!("dialog loop finished {:?}", r);
        }
    }
    Ok(())
}

async fn process_registration(
    endpoint: EndpointInnerRef,
    sip_server: String,
    credential: Credential,
    cancel_token: CancellationToken,
) -> Result<()> {
    if sip_server.is_empty() {
        select! {
            _ = cancel_token.cancelled() => {
                info!("recv cancel token canceled in registration process");
                return Err(Error::Error("sip_server is empty".to_owned()));
            }
        }
    }

    let mut registration = Registration::new(endpoint, Some(credential));
    loop {
        let resp = registration.register(&sip_server).await?;
        debug!("received response: {}", resp.to_string());
        if resp.status_code != rsip::StatusCode::OK {
            return Err(rsipstack::Error::Error("Failed to register".to_string()));
        }
        sleep(Duration::from_secs(registration.expires().max(50) as u64)).await;
    }
    #[allow(unreachable_code)]
    Ok::<_, Error>(())
}

async fn process_incoming_request(
    dialog_layer: Arc<DialogLayer>,
    mut incoming: TransactionReceiver,
    state_sender: DialogStateSender,
    contact: rsip::Uri,
) -> Result<()> {
    while let Some(mut tx) = incoming.recv().await {
        info!("Received transaction: {:?}", tx.key);

        match tx.original.to_header()?.tag()?.as_ref() {
            Some(_) => match dialog_layer.match_dialog(&tx.original) {
                Some(mut d) => {
                    tokio::spawn(async move {
                        d.handle(tx).await?;
                        Ok::<_, Error>(())
                    });
                    continue;
                }
                None => {
                    info!("dialog not found: {}", tx.original);
                    tx.reply(rsip::StatusCode::CallTransactionDoesNotExist)
                        .await?;
                    continue;
                }
            },
            None => {}
        }
        // out dialog, new server dialog
        match tx.original.method {
            rsip::Method::Invite | rsip::Method::Ack => {
                let mut dialog = match dialog_layer.get_or_create_server_invite(
                    &tx,
                    state_sender.clone(),
                    None,
                    Some(contact.clone()),
                ) {
                    Ok(d) => d,
                    Err(e) => {
                        // 481 Dialog/Transaction Does Not Exist
                        info!("Failed to obtain dialog: {:?}", e);
                        tx.reply(rsip::StatusCode::CallTransactionDoesNotExist)
                            .await?;
                        continue;
                    }
                };
                tokio::spawn(async move {
                    dialog.handle(tx).await?;
                    Ok::<_, Error>(())
                });
            }
            _ => {
                info!("Received request: {:?}", tx.original.method);
                tx.reply(rsip::StatusCode::OK).await?;
            }
        }
    }
    Ok::<_, Error>(())
}

async fn process_dialog(
    dialog_layer: Arc<DialogLayer>,
    state_receiver: DialogStateReceiver,
    opt: MediaSessionOption,
) -> Result<()> {
    let mut state_receiver = state_receiver;
    while let Some(state) = state_receiver.recv().await {
        match state {
            DialogState::Calling(id) => {
                info!("Calling dialog {}", id);
                let dialog = match dialog_layer.get_dialog(&id) {
                    Some(d) => d,
                    None => {
                        info!("Dialog not found {}", id);
                        continue;
                    }
                };
                match dialog {
                    Dialog::ServerInvite(d) => {
                        // play example pcmu of handling incoming call
                        //
                        // [A] Ai answer, [R] Reject, [E] Play example pcmu
                        play_example_pcmu(&opt, d).await?;
                    }
                    Dialog::ClientInvite(_) => {
                        info!("Client invite dialog {}", id);
                    }
                }
            }
            DialogState::Early(id, resp) => {
                info!("Early dialog {} {}", id, resp);
            }
            DialogState::Terminated(id, reason) => {
                info!("Dialog terminated {} {:?}", id, reason);
                dialog_layer.remove_dialog(&id);
            }
            _ => {
                info!("Received dialog state: {}", state);
            }
        }
    }
    Ok(())
}

async fn make_call(
    dialog_layer: Arc<DialogLayer>,
    invite_option: InviteOption,
    media_option: MediaSessionOption,
    state_sender: DialogStateSender,
) -> Result<()> {
    let ssrc = rand::random::<u32>();
    let (rtp_conn, offer) = build_rtp_conn(&media_option, ssrc).await?;
    let mut invite_option = invite_option;
    invite_option.offer = Some(offer.into());

    let (dialog, resp) = dialog_layer.do_invite(invite_option, state_sender).await?;
    let rtp_token = dialog.cancel_token().child_token();
    let resp = resp.ok_or(Error::Error("No response".to_string()))?;

    if resp.status_code != rsip::StatusCode::OK {
        info!("Failed to make call: {:?}", resp);
        return Err(Error::Error("Failed to make call".to_string()));
    }

    let body = String::from_utf8_lossy(resp.body()).to_string();
    info!("Received response: {}", resp);

    let answer = match sdp_rs::SessionDescription::try_from(body.as_str()) {
        Ok(s) => s,
        Err(e) => {
            info!("Failed to parse answer SDP: {:?} {}", e, body);
            return Err(Error::Error("Failed to parse SDP".to_string()));
        }
    };

    let peer_addr = match answer.connection {
        Some(c) => c.connection_address.base,
        None => {
            info!("No connection address in answer SDP");
            return Err(Error::Error(
                "No connection address in answer SDP".to_string(),
            ));
        }
    };

    let peer_port = answer
        .media_descriptions
        .first()
        .map(|m| m.media.port)
        .ok_or(Error::Error("No audio port in answer SDP".to_string()))?;

    let peer_addr = format!("{}:{}", peer_addr, peer_port);
    info!("Peer address: {}", peer_addr);
    play_example_file(rtp_conn, rtp_token, ssrc, peer_addr)
        .await
        .expect("play example file");
    dialog.bye().await.expect("send BYE");
    Ok(())
}

async fn play_example_pcmu(opt: &MediaSessionOption, dialog: ServerInviteDialog) -> Result<()> {
    let ssrc = rand::random::<u32>();
    let (conn, answer) = build_rtp_conn(opt, ssrc).await?;

    let headers = vec![rsip::typed::ContentType(MediaType::Sdp(vec![])).into()];
    dialog.accept(Some(headers), Some(answer.into()))?;

    let body = String::from_utf8_lossy(dialog.initial_request().body()).to_string();
    let offer = match sdp_rs::SessionDescription::try_from(body.as_str()) {
        Ok(s) => s,
        Err(e) => {
            info!("Failed to parse offer SDP: {:?} {}", e, body);
            return Err(Error::Error("Failed to parse SDP".to_string()));
        }
    };

    let peer_addr = match offer.connection {
        Some(c) => c.connection_address.base,
        None => {
            info!("No connection address in offer SDP");
            return Err(Error::Error(
                "No connection address in offer SDP".to_string(),
            ));
        }
    };

    let peer_port = offer
        .media_descriptions
        .first()
        .map(|m| m.media.port)
        .ok_or(Error::Error("No audio port in offer SDP".to_string()))?;

    let peer_addr = format!("{}:{}", peer_addr, peer_port);
    let rtp_token = dialog.cancel_token().child_token();
    tokio::spawn(async move {
        play_example_file(conn, rtp_token, ssrc, peer_addr)
            .await
            .expect("play example file");
        dialog.bye().await.expect("send BYE");
    });
    Ok(())
}
