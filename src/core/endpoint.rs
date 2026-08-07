use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
};

use bytes::BytesMut;
use rand::rngs::StdRng;

use crate::core::{
    config::{ClientConfig, ServerConfig},
    connection::ConnectionCore,
    connection_id::{self, ConnectionID, InternalCID},
    crypto::CryptoConn,
    packet::ProtectedPacket,
    stream::Side,
    transport_parameters::TransportParam,
};

pub struct EndpointCore {
    rng: StdRng,
    cid_len: u8,
    connections_addresses: HashMap<InternalCID, SocketAddr>,
    connections: HashMap<ConnectionID, InternalCID>,
    current_internal_cid: InternalCID,
    client_config: Option<ClientConfig>,
    server_config: Option<ServerConfig>,
}

pub enum EndpointOutput {
    RouteToConnection {
        dst_cid: InternalCID,
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
            connections_addresses: HashMap::new(),
            connections: HashMap::new(),
            current_internal_cid: 0,
            client_config,
            server_config,
        }
    }

    pub fn handle_datagram(&mut self, data: BytesMut, remote: SocketAddr) -> EndpointOutput {
        let protected = ProtectedPacket::new(data, self.cid_len).expect("Could not decode header");
        let dst_cid = protected.dst_cid();

        println!("[INTERNAL] Current connections: {:?}", self.connections);

        if let Some(internal_cid) = self.connections.get(&dst_cid)
            && let Some(old_remote) = self.connections_addresses.get(internal_cid)
        {
            if old_remote == &remote {
                return EndpointOutput::RouteToConnection {
                    dst_cid: *internal_cid,
                    protected,
                    migration: None,
                };
            }

            return EndpointOutput::RouteToConnection {
                dst_cid: *internal_cid,
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

        let src_cid = protected.src_cid().unwrap();
        EndpointOutput::NewConnection {
            dst_cid,
            src_cid,
            protected,
            remote,
        }
    }

    pub fn add_cid(&mut self, internal_cid: InternalCID, remote: SocketAddr) {
        self.connections_addresses.insert(internal_cid, remote);
    }

    fn initial_cids(&mut self) -> (ConnectionID, ConnectionID) {
        (
            ConnectionID::new(self.cid_len, &mut self.rng),
            ConnectionID::new(self.cid_len, &mut self.rng),
        )
    }

    fn current_icid(&mut self) -> InternalCID {
        let t = self.current_internal_cid;
        self.current_internal_cid += 1;
        t
    }

    pub fn new_client_conn(&mut self, name: &str) -> (ConnectionCore, InternalCID) {
        let transport_params = Arc::new(TransportParam::new());
        let (local_cid, remote_cid) = self.initial_cids();

        let crypto = CryptoConn::new_client(
            self.client_config.as_ref().unwrap().crypto.clone(),
            name,
            vec![],
        )
        .unwrap();

        let internal_cid = self.current_icid();
        self.connections.insert(local_cid, internal_cid);

        let core = ConnectionCore::new(
            local_cid,
            remote_cid,
            remote_cid,
            Side::Client,
            crypto,
            transport_params,
        );

        (core, internal_cid)
    }

    pub fn new_server_conn(
        &mut self,
        first_cid: ConnectionID,
        remote_cid: ConnectionID,
        remote: SocketAddr,
    ) -> (ConnectionCore, InternalCID) {
        let transport_params = Arc::new(TransportParam::new());

        let crypto =
            CryptoConn::new_server(self.server_config.as_ref().unwrap().crypto.clone(), vec![])
                .unwrap();

        let local_cid = ConnectionID::new(self.cid_len, &mut self.rng);
        let internal_cid = self.current_icid();
        self.connections.insert(local_cid, internal_cid);

        self.connections_addresses.insert(internal_cid, remote);

        (
            ConnectionCore::new(
                local_cid,
                remote_cid,
                first_cid,
                Side::Server,
                crypto,
                transport_params,
            ),
            internal_cid,
        )
    }
}
