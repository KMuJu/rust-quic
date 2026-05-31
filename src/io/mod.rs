use std::net::SocketAddr;

use bytes::Bytes;

use crate::core::packet::ProtectedPacket;

pub mod connection;
pub mod endpoint;

pub(crate) struct ConnectionToEndpoint {
    data: Bytes,
    addr: SocketAddr,
}

pub(crate) struct EndpointToConnection {
    protected: ProtectedPacket,
    addr: SocketAddr,
}
