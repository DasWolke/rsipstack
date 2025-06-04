# rsipstack - A SIP Stack written in Rust

**WIP** This is a work in progress and is not yet ready for production use.

A RFC 3261 compliant SIP stack written in Rust. The goal of this project is to provide a high-performance, reliable, and easy-to-use SIP stack that can be used in various scenarios.

## Features

- **RFC 3261 Compliant**: Full compliance with SIP specification
- **Multiple Transport Support**: UDP, TCP, TLS, WebSocket
- **Transaction Layer**: Complete SIP transaction state machine
- **Dialog Layer**: SIP dialog management
- **Digest Authentication**: Built-in authentication support
- **High Performance**: Built with Rust for maximum performance
- **Easy to Use**: Simple and intuitive API design

## TODO
- [x] Transport support
  - [x] UDP
  - [x] TCP
  - [x] TLS
  - [x] WebSocket
- [x] Digest Authentication
- [x] Transaction Layer
- [x] Dialog Layer
- [ ] WASM target

## Use Cases

This SIP stack can be used in various scenarios, including but not limited to:

- Integration with WebRTC for browser-based communication, such as WebRTC SBC.
- Building custom SIP proxies or registrars
- Building custom SIP user agents (SIP.js alternative)

## Why Rust?

We are a group of developers who are passionate about SIP and Rust. We believe that Rust is a great language for building high-performance network applications, and we want to bring the power of Rust to the SIP/WebRTC/SFU world.

## Quick Start Examples

### 1. Simple SIP Connection

The most basic way to use rsipstack is through direct SIP connections, supporting both UDP and TCP transports:

```bash
# Run as UDP server (default)
cargo run --example simple_connection

# Run as UDP client sending messages to a server
cargo run --example simple_connection -- --mode client --target 127.0.0.1:5060  --port 5061

# Run as TCP server
cargo run --example simple_connection -- --transport tcp --mode server --port 5060

```

This example demonstrates:
- Creating UDP/TCP connections and listeners
- Sending raw SIP messages (OPTIONS, MESSAGE, REGISTER)
- Handling incoming SIP requests and responses
- Basic SIP message parsing and creation

### 2. SIP Proxy Server

A stateful SIP proxy that routes calls between registered users:

```bash
# Run proxy server
cargo run --example proxy -- --port 25060 --addr 127.0.0.1

# Run with external IP
cargo run --example proxy -- --port 25060 --external-ip 1.2.3.4
```

This example demonstrates:
- SIP user registration and location service
- Call routing between registered users
- Transaction forwarding and response handling
- Session management for active calls
- Handling INVITE, BYE, REGISTER, and ACK methods

### 3. SIP User Agent Client

A complete SIP client with registration, calling, and media support:

```bash
# Local demo proxy
cargo run --example client -- --port 25061 --sip-server 127.0.0.1:25060

# Register with a SIP server
cargo run --example client -- --sip-server sip.example.com --user alice --password secret
```

This example demonstrates:
- SIP user registration with digest authentication
- Making and receiving SIP calls (INVITE/BYE)
- Dialog management for call sessions
- RTP media streaming with file playback
- STUN support for NAT traversal


## API Usage Guide

### 1. Simple SIP Connection

```rust
use rsipstack::transport::{udp::UdpConnection, SipAddr};

// Create UDP connection
let connection = UdpConnection::create_connection("127.0.0.1:5060".parse()?, None).await?;

// Send raw SIP message
let sip_message = "OPTIONS sip:test@example.com SIP/2.0\r\n...";
connection.send_raw(sip_message.as_bytes(), &target_addr).await?;
```

### 2. Using Endpoint and Transactions

```rust
use rsipstack::{EndpointBuilder, transport::TransportLayer};
use tokio_util::sync::CancellationToken;

// Build endpoint with transport layer
let cancel_token = CancellationToken::new();
let transport_layer = TransportLayer::new(cancel_token.clone());
let endpoint = EndpointBuilder::new()
    .with_transport_layer(transport_layer)
    .with_cancel_token(cancel_token)
    .build();

// Handle incoming transactions
let mut incoming = endpoint.incoming_transactions();
while let Some(transaction) = incoming.recv().await {
    // Process transaction based on method
    match transaction.original.method {
        rsip::Method::Register => {
            transaction.reply(rsip::StatusCode::OK).await?;
        }
        rsip::Method::Options => {
            transaction.reply(rsip::StatusCode::OK).await?;
        }
        // ... handle other methods
    }
}
```

### 3. Creating a User Agent Client

```rust
use rsipstack::dialog::{DialogLayer, registration::Registration};
use rsipstack::dialog::authenticate::Credential;

// Create dialog layer
let dialog_layer = Arc::new(DialogLayer::new(endpoint.inner.clone()));

// Register with server
let credential = Credential {
    username: "alice".to_string(),
    password: "secret".to_string(),
    realm: None,
};

let registration = Registration::new(
    endpoint.inner.clone(),
    "sip:alice@example.com".parse()?,
    "sip:registrar.example.com".parse()?,
    credential,
)?;

// Make outgoing call
let invite_option = InviteOption {
    callee: "sip:bob@example.com".parse()?,
    caller: "sip:alice@example.com".parse()?,
    content_type: None,
    offer: None,
    contact: "sip:alice@192.168.1.100:5060".parse()?,
    credential: Some(credential),
    headers: None,
};

let invite_dialog = dialog_layer.create_invite_dialog(invite_option).await?;
```

### 4. Implementing a Proxy

```rust
use rsipstack::transaction::Transaction;
use std::collections::HashMap;

// Handle incoming requests
while let Some(mut transaction) = incoming.recv().await {
    match transaction.original.method {
        rsip::Method::Register => {
            // Store user registration
            let user = User::try_from(&transaction.original)?;
            users.insert(user.username.clone(), user);
            transaction.reply(rsip::StatusCode::OK).await?;
        }
        rsip::Method::Invite => {
            // Route call to registered user
            let callee = extract_callee(&transaction.original)?;
            if let Some(target) = users.get(&callee) {
                // Forward request
                let mut forwarded_tx = transaction.create_client_transaction()?;
                forwarded_tx.destination = Some(target.destination.clone());
                forwarded_tx.send().await?;
            } else {
                transaction.reply(rsip::StatusCode::NotFound).await?;
            }
        }
        // ... handle other methods
    }
}
```

## Running Tests

### Unit Tests
```bash
cargo test
```


### Benchmark Tests
```bash
# Run server
cargo run -r --bin bench_ua  -- -m server -p 5060

# Run client with 1000 calls
cargo run -r  --bin bench_ua  -- -m client -p 5061 -s 127.0.0.1:5060 -c 1000
```

The test monitor:

```bash
=== SIP Benchmark UA Stats ===
Dialogs: 9992
Active Calls: 9983
Rejected Calls: 0
Failed Calls: 0
Total Calls: 250276
Calls/Second: 1501
============================
```

## Documentation

- [API Documentation](https://docs.rs/rsipstack)
- [Examples](./examples/)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.