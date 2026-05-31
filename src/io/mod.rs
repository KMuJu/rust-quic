use std::net::SocketAddr;

use bytes::{Bytes, BytesMut};

pub mod connection;
pub mod endpoint;

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
