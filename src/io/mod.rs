use std::net::SocketAddr;

use bytes::Bytes;

use crate::core::{
    connection_id::{ConnectionID, InternalCID},
    packet::ProtectedPacket,
};

pub mod connection;
pub mod endpoint;

pub(crate) enum ConnectionToEndpoint {
    Datagram { data: Bytes, addr: SocketAddr },
    NewConnectionIdRequest { internal_cid: InternalCID },
}

pub(crate) enum EndpointToConnection {
    Packet {
        protected: ProtectedPacket,
        addr: SocketAddr,
    },
    NewConnectionId {
        cid: ConnectionID,
    },
}
