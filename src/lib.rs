use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};

pub mod ack_manager;
pub mod congestion;
pub mod connection;
pub mod crypto;
pub mod encoding;
pub mod endpoint;
pub mod frame;
pub mod frame_assembler;
pub mod packet;
pub mod rtt_estimator;
pub mod socket;
pub mod space;
pub mod stream;
pub mod timers;
pub mod transport_parameters;

pub mod mock_socket;

#[derive(Debug)]
struct ConnectionToEndpoint {
    data: BytesMut,
    addr: SocketAddr,
}

#[derive(Debug)]
struct EndpointToConnection {
    data: Bytes,
    addr: SocketAddr,
}
