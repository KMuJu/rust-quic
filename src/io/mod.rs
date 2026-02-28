use std::net::SocketAddr;

use bytes::BytesMut;

pub mod connection;
pub mod encoding;
pub mod endpoint;
pub mod frame;
pub mod frame_assembler;
pub mod packet;
pub mod stream;

#[derive(Debug)]
struct ConnectionToEndpoint {
    data: BytesMut,
    addr: SocketAddr,
}

#[derive(Debug)]
struct EndpointToConnection {
    data: Vec<u8>,
    addr: SocketAddr,
}
