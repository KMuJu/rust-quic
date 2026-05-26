use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};

pub mod core;
pub mod io;
pub mod socket;

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
