use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::Bytes;
use rand::rngs::StdRng;

use crate::core::{
    connection::{ConnectionCore, ConnectionID},
    packet::ProtectedPacket,
    stream::Side,
    transport_parameters::TransportParam,
};

pub struct EndpointCore {
    rng: StdRng,
    cid_len: u8,
    connections: HashMap<ConnectionID, SocketAddr>,
}

pub enum EndpointOutput {
    RouteToConnection {
        dst_cid: ConnectionID,
        protected: ProtectedPacket,
        migration: Option<SocketAddr>,
    },
    NewConnection {
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        protected: ProtectedPacket,
        remote: SocketAddr,
    },
    Drop,
    SendDatagram,
}

impl EndpointCore {
    pub fn new() -> Self {
        Self {
            rng: rand::make_rng(),
            cid_len: 4,
            connections: HashMap::new(),
        }
    }

    pub fn handle_datagram(&mut self, data: Bytes, remote: SocketAddr) -> EndpointOutput {
        let protected = ProtectedPacket::new(data, self.cid_len).expect("Could not decode header");
        let dst_cid = protected.dst_cid();

        if let Some(old_remote) = self.connections.get(&dst_cid) {
            if old_remote == &remote {
                return EndpointOutput::RouteToConnection {
                    dst_cid,
                    protected,
                    migration: None,
                };
            }

            return EndpointOutput::RouteToConnection {
                dst_cid,
                protected,
                migration: Some(remote),
            };
        }

        if !protected.is_initial() {
            // TODO: Stateless reset
            return EndpointOutput::Drop;
        }

        // TODO:
        // Server config must be some?
        // Do retry?
        // Validate token?

        self.connections.insert(dst_cid, remote);
        let src_cid = protected.src_cid().unwrap();
        EndpointOutput::NewConnection {
            dst_cid,
            src_cid,
            protected,
            remote,
        }
    }

    pub fn add_cid(&mut self, local_cid: ConnectionID, remote: SocketAddr) {
        self.connections.insert(local_cid, remote);
    }

    pub fn initial_cids(&mut self) -> (ConnectionID, ConnectionID) {
        (
            ConnectionID::new(self.cid_len, &mut self.rng),
            ConnectionID::new(self.cid_len, &mut self.rng),
        )
    }

    pub fn new_client_conn(&mut self) -> (ConnectionCore, ConnectionID) {
        let transport_params = Arc::new(TransportParam::new());
        let (cid, peer_cid) = self.initial_cids();

        let core = ConnectionCore::new(cid, peer_cid, Side::Client, transport_params);

        (core, cid)
    }

    pub fn new_server_conn(
        &mut self,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
    ) -> ConnectionCore {
        let transport_params = Arc::new(TransportParam::new());

        ConnectionCore::new(dst_cid, src_cid, Side::Server, transport_params)
    }
}

impl Default for EndpointCore {
    fn default() -> Self {
        Self::new()
    }
}
