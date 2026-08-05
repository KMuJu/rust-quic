use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use bytes::BytesMut;
use rand::rngs::StdRng;

use crate::core::{
    config::{ClientConfig, ServerConfig},
    connection::{ConnectionCore, ConnectionID},
    crypto::CryptoConn,
    packet::ProtectedPacket,
    stream::Side,
    transport_parameters::TransportParam,
};

pub struct EndpointCore {
    rng: StdRng,
    cid_len: u8,
    connections: HashMap<ConnectionID, SocketAddr>,
    client_config: Option<ClientConfig>,
    server_config: Option<ServerConfig>,
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
    pub fn new(client_config: Option<ClientConfig>, server_config: Option<ServerConfig>) -> Self {
        Self {
            rng: rand::make_rng(),
            cid_len: 4,
            connections: HashMap::new(),
            client_config,
            server_config,
        }
    }

    pub fn handle_datagram(&mut self, data: BytesMut, remote: SocketAddr) -> EndpointOutput {
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

    fn initial_cids(&mut self) -> (ConnectionID, ConnectionID) {
        (
            ConnectionID::new(self.cid_len, &mut self.rng),
            ConnectionID::new(self.cid_len, &mut self.rng),
        )
    }

    pub fn new_client_conn(&mut self, name: &str) -> (ConnectionCore, ConnectionID) {
        let transport_params = Arc::new(TransportParam::new());
        let (local_cid, remote_cid) = self.initial_cids();

        let crypto = CryptoConn::new_client(
            self.client_config.as_ref().unwrap().crypto.clone(),
            name,
            vec![],
        )
        .unwrap();

        let core = ConnectionCore::new(
            local_cid,
            remote_cid,
            remote_cid,
            Side::Client,
            crypto,
            transport_params,
        );

        (core, local_cid)
    }

    pub fn new_server_conn(
        &mut self,
        first_cid: ConnectionID,
        remote_cid: ConnectionID,
        remote: SocketAddr,
    ) -> (ConnectionCore, ConnectionID) {
        let transport_params = Arc::new(TransportParam::new());

        let crypto =
            CryptoConn::new_server(self.server_config.as_ref().unwrap().crypto.clone(), vec![])
                .unwrap();

        let local_cid = ConnectionID::new(self.cid_len, &mut self.rng);

        self.connections.insert(local_cid, remote);

        (
            ConnectionCore::new(
                local_cid,
                remote_cid,
                first_cid,
                Side::Server,
                crypto,
                transport_params,
            ),
            local_cid,
        )
    }
}
