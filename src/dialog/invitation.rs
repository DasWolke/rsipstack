use super::{
    authenticate::Credential,
    client_dialog::ClientInviteDialog,
    dialog::{DialogInner, DialogStateSender},
    dialog_layer::DialogLayer,
};
use crate::{
    dialog::{dialog::Dialog, DialogId},
    transaction::{
        key::{TransactionKey, TransactionRole},
        make_tag,
        transaction::Transaction,
    },
    Result,
};
use rsip::{Request, Response};
use std::sync::Arc;
use tracing::{debug, info};

/// INVITE Request Options
///
/// `InviteOption` contains all the parameters needed to create and send
/// an INVITE request to establish a SIP session. This structure provides
/// a convenient way to specify all the necessary information for initiating
/// a call or session.
///
/// # Fields
///
/// * `caller` - URI of the calling party (From header)
/// * `callee` - URI of the called party (To header and Request-URI)
/// * `content_type` - MIME type of the message body (default: "application/sdp")
/// * `offer` - Optional message body (typically SDP offer)
/// * `contact` - Contact URI for this user agent
/// * `credential` - Optional authentication credentials
/// * `headers` - Optional additional headers to include
///
/// # Examples
///
/// ## Basic Voice Call
///
/// ```rust,no_run
/// # use rsipstack::dialog::invitation::InviteOption;
/// # fn example() -> rsipstack::Result<()> {
/// # let sdp_offer_bytes = vec![];
/// let invite_option = InviteOption {
///     caller: "sip:alice@example.com".try_into()?,
///     callee: "sip:bob@example.com".try_into()?,
///     content_type: Some("application/sdp".to_string()),
///     offer: Some(sdp_offer_bytes),
///     contact: "sip:alice@192.168.1.100:5060".try_into()?,
///     credential: None,
///     headers: None,
/// };
/// # Ok(())
/// # }
/// ```
///
/// ```rust,no_run
/// # use rsipstack::dialog::dialog_layer::DialogLayer;
/// # use rsipstack::dialog::invitation::InviteOption;
/// # fn example() -> rsipstack::Result<()> {
/// # let dialog_layer: DialogLayer = todo!();
/// # let invite_option: InviteOption = todo!();
/// let request = dialog_layer.make_invite_request(&invite_option)?;
/// println!("Created INVITE to: {}", request.uri);
/// # Ok(())
/// # }
/// ```
///
/// ## Call with Custom Headers
///
/// ```rust,no_run
/// # use rsipstack::dialog::invitation::InviteOption;
/// # fn example() -> rsipstack::Result<()> {
/// # let sdp_bytes = vec![];
/// # let auth_credential = todo!();
/// let custom_headers = vec![
///     rsip::Header::UserAgent("MyApp/1.0".into()),
///     rsip::Header::Subject("Important Call".into()),
/// ];
///
/// let invite_option = InviteOption {
///     caller: "sip:alice@example.com".try_into()?,
///     callee: "sip:bob@example.com".try_into()?,
///     content_type: Some("application/sdp".to_string()),
///     offer: Some(sdp_bytes),
///     contact: "sip:alice@192.168.1.100:5060".try_into()?,
///     credential: Some(auth_credential),
///     headers: Some(custom_headers),
/// };
/// # Ok(())
/// # }
/// ```
///
/// ## Call with Authentication
///
/// ```rust,no_run
/// # use rsipstack::dialog::invitation::InviteOption;
/// # use rsipstack::dialog::authenticate::Credential;
/// # fn example() -> rsipstack::Result<()> {
/// # let sdp_bytes = vec![];
/// let credential = Credential {
///     username: "alice".to_string(),
///     password: "secret123".to_string(),
///     realm: Some("example.com".to_string()),
/// };
///
/// let invite_option = InviteOption {
///     caller: "sip:alice@example.com".try_into()?,
///     callee: "sip:bob@example.com".try_into()?,
///     content_type: None, // Will default to "application/sdp"
///     offer: Some(sdp_bytes),
///     contact: "sip:alice@192.168.1.100:5060".try_into()?,
///     credential: Some(credential),
///     headers: None,
/// };
/// # Ok(())
/// # }
/// ```
pub struct InviteOption {
    pub caller: rsip::Uri,
    pub callee: rsip::Uri,
    pub content_type: Option<String>,
    pub offer: Option<Vec<u8>>,
    pub contact: rsip::Uri,
    pub credential: Option<Credential>,
    pub headers: Option<Vec<rsip::Header>>,
}

impl DialogLayer {
    /// Create an INVITE request from options
    ///
    /// Constructs a properly formatted SIP INVITE request based on the
    /// provided options. This method handles all the required headers
    /// and parameters according to RFC 3261.
    ///
    /// # Parameters
    ///
    /// * `opt` - INVITE options containing all necessary parameters
    ///
    /// # Returns
    ///
    /// * `Ok(Request)` - Properly formatted INVITE request
    /// * `Err(Error)` - Failed to create request
    ///
    /// # Generated Headers
    ///
    /// The method automatically generates:
    /// * Via header with branch parameter
    /// * From header with tag parameter
    /// * To header (without tag for initial request)
    /// * Contact header
    /// * Content-Type header
    /// * CSeq header with incremented sequence number
    /// * Call-ID header
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use rsipstack::dialog::dialog_layer::DialogLayer;
    /// # use rsipstack::dialog::invitation::InviteOption;
    /// # fn example() -> rsipstack::Result<()> {
    /// # let dialog_layer: DialogLayer = todo!();
    /// # let invite_option: InviteOption = todo!();
    /// let request = dialog_layer.make_invite_request(&invite_option)?;
    /// println!("Created INVITE to: {}", request.uri);
    /// # Ok(())
    /// # }
    /// ```
    pub fn make_invite_request(&self, opt: &InviteOption) -> Result<Request> {
        self.make_invite_request_with_public_address(opt, None)
    }

    fn make_invite_request_with_public_address(
        &self, 
        opt: &InviteOption,
        public_address: Option<(std::net::IpAddr, u16)>,
    ) -> Result<Request> {
        let last_seq = self.increment_last_seq();
        let to = rsip::typed::To {
            display_name: None,
            uri: opt.callee.clone(),
            params: vec![],
        };
        let recipient = to.uri.clone();

        let form = rsip::typed::From {
            display_name: None,
            uri: opt.caller.clone(),
            params: vec![],
        }
        .with_tag(make_tag());

        // Create Via header with public address if provided
        let via_addr = public_address.map(|(ip, port)| crate::transport::SipAddr {
            r#type: Some(rsip::Transport::Udp),
            addr: rsip::HostWithPort {
                host: ip.into(),
                port: Some(port.into()),
            },
        });
        let via = self.endpoint.get_via(via_addr, None)?;
        let mut request =
            self.endpoint
                .make_request(rsip::Method::Invite, recipient, via, form, to, last_seq);

        let contact = rsip::typed::Contact {
            display_name: None,
            uri: opt.contact.clone(),
            params: vec![],
        };

        request
            .headers
            .unique_push(rsip::Header::Contact(contact.into()));

        request.headers.unique_push(rsip::Header::ContentType(
            opt.content_type
                .clone()
                .unwrap_or("application/sdp".to_string())
                .into(),
        ));
        // can override default headers
        if let Some(headers) = opt.headers.as_ref() {
            for header in headers {
                request.headers.unique_push(header.clone());
            }
        }
        Ok(request)
    }

    /// Send an INVITE request and create a client dialog
    ///
    /// This is the main method for initiating outbound calls. It creates
    /// an INVITE request, sends it, and manages the resulting dialog.
    /// The method handles the complete INVITE transaction including
    /// authentication challenges and response processing.
    ///
    /// # Parameters
    ///
    /// * `opt` - INVITE options containing all call parameters
    /// * `state_sender` - Channel for receiving dialog state updates
    ///
    /// # Returns
    ///
    /// * `Ok((ClientInviteDialog, Option<Response>))` - Created dialog and final response
    /// * `Err(Error)` - Failed to send INVITE or process responses
    ///
    /// # Call Flow
    ///
    /// 1. Creates INVITE request from options
    /// 2. Creates client dialog and transaction
    /// 3. Sends INVITE request
    /// 4. Processes responses (1xx, 2xx, 3xx-6xx)
    /// 5. Handles authentication challenges if needed
    /// 6. Returns established dialog and final response
    ///
    /// # Examples
    ///
    /// ## Basic Call Setup
    ///
    /// ```rust,no_run
    /// # use rsipstack::dialog::dialog_layer::DialogLayer;
    /// # use rsipstack::dialog::invitation::InviteOption;
    /// # async fn example() -> rsipstack::Result<()> {
    /// # let dialog_layer: DialogLayer = todo!();
    /// # let invite_option: InviteOption = todo!();
    /// # let state_sender = todo!();
    /// let (dialog, response) = dialog_layer.do_invite(invite_option, state_sender).await?;
    ///
    /// if let Some(resp) = response {
    ///     match resp.status_code {
    ///         rsip::StatusCode::OK => {
    ///             println!("Call answered!");
    ///             // Process SDP answer in resp.body
    ///         },
    ///         rsip::StatusCode::BusyHere => {
    ///             println!("Called party is busy");
    ///         },
    ///         _ => {
    ///             println!("Call failed: {}", resp.status_code);
    ///         }
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Monitoring Dialog State
    ///
    /// ```rust,no_run
    /// # use rsipstack::dialog::dialog_layer::DialogLayer;
    /// # use rsipstack::dialog::invitation::InviteOption;
    /// # use rsipstack::dialog::dialog::DialogState;
    /// # async fn example() -> rsipstack::Result<()> {
    /// # let dialog_layer: DialogLayer = todo!();
    /// # let invite_option: InviteOption = todo!();
    /// let (state_tx, mut state_rx) = tokio::sync::mpsc::unbounded_channel();
    /// let (dialog, response) = dialog_layer.do_invite(invite_option, state_tx).await?;
    ///
    /// // Monitor dialog state changes
    /// tokio::spawn(async move {
    ///     while let Some(state) = state_rx.recv().await {
    ///         match state {
    ///             DialogState::Early(_, resp) => {
    ///                 println!("Ringing: {}", resp.status_code);
    ///             },
    ///             DialogState::Confirmed(_) => {
    ///                 println!("Call established");
    ///             },
    ///             DialogState::Terminated(_, code) => {
    ///                 println!("Call ended: {:?}", code);
    ///                 break;
    ///             },
    ///             _ => {}
    ///         }
    ///     }
    /// });
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Error Handling
    ///
    /// The method can fail for various reasons:
    /// * Network connectivity issues
    /// * Authentication failures
    /// * Invalid SIP URIs or headers
    /// * Transaction timeouts
    /// * Protocol violations
    ///
    /// # Authentication
    ///
    /// If credentials are provided in the options, the method will
    /// automatically handle 401/407 authentication challenges by
    /// resending the request with proper authentication headers.
    pub async fn do_invite(
        &self,
        opt: InviteOption,
        state_sender: DialogStateSender,
    ) -> Result<(ClientInviteDialog, Option<Response>)> {
        self.do_invite_with_public_address(opt, state_sender, None).await
    }

    /// Send an INVITE request with public address and create a client dialog
    ///
    /// This is similar to `do_invite` but allows specifying a public address
    /// to be used for Via headers in the INVITE and all subsequent in-dialog
    /// requests. This is useful for NAT traversal when the public address has
    /// been discovered through REGISTER or other means.
    ///
    /// # Parameters
    ///
    /// * `opt` - INVITE options containing all call parameters
    /// * `state_sender` - Channel for receiving dialog state updates
    /// * `public_address` - Optional public IP and port to use in Via headers
    ///
    /// # Returns
    ///
    /// * `Ok((ClientInviteDialog, Option<Response>))` - Created dialog and final response
    /// * `Err(Error)` - Failed to send INVITE or process responses
    pub async fn do_invite_with_public_address(
        &self,
        opt: InviteOption,
        state_sender: DialogStateSender,
        public_address: Option<(std::net::IpAddr, u16)>,
    ) -> Result<(ClientInviteDialog, Option<Response>)> {
        let mut request = self.make_invite_request_with_public_address(&opt, public_address)?;
        request.body = opt.offer.unwrap_or_default();
        request.headers.unique_push(rsip::Header::ContentLength(
            (request.body.len() as u32).into(),
        ));

        let id = DialogId::try_from(&request)?;
        let dlg_inner = DialogInner::new(
            TransactionRole::Client,
            id.clone(),
            request.clone(),
            self.endpoint.clone(),
            state_sender,
            opt.credential,
            Some(opt.contact),
        )?;

        let dialog = ClientInviteDialog {
            inner: Arc::new(dlg_inner),
        };

        // Set the public address if provided
        if let Some((public_ip, public_port)) = public_address {
            let public_sip_addr = crate::transport::SipAddr {
                r#type: Some(rsip::Transport::Udp),
                addr: rsip::HostWithPort {
                    host: public_ip.into(),
                    port: Some(public_port.into()),
                },
            };
            dialog.set_public_address(public_sip_addr);
            info!("UAC dialog configured with public address: {}:{}", public_ip, public_port);
        }

        let key =
            TransactionKey::from_request(&dialog.inner.initial_request, TransactionRole::Client)?;
        let tx = Transaction::new_client(key, request.clone(), self.endpoint.clone(), None);

        self.inner
            .dialogs
            .write()
            .unwrap()
            .insert(id.clone(), Dialog::ClientInvite(dialog.clone()));

        info!("client invite dialog created: {:?}", id);

        match dialog.process_invite(tx).await {
            Ok((new_dialog_id, resp)) => {
                debug!(
                    "client invite dialog confirmed: {} => {}",
                    id, new_dialog_id
                );
                self.inner.dialogs.write().unwrap().remove(&id);
                // update with new dialog id
                self.inner
                    .dialogs
                    .write()
                    .unwrap()
                    .insert(new_dialog_id, Dialog::ClientInvite(dialog.clone()));
                return Ok((dialog, resp));
            }
            Err(e) => {
                self.inner.dialogs.write().unwrap().remove(&id);
                return Err(e);
            }
        }
    }
}
