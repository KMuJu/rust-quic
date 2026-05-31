use std::{collections::HashMap, io::Cursor, net::SocketAddr};

use bytes::Bytes;
use rand::rngs::StdRng;

use crate::core::{connection::ConnectionID, packet::Header};

pub struct EndpointCore {
    rng: StdRng,
    cid_len: u8,
    connections: HashMap<ConnectionID, SocketAddr>,
}

pub enum EndpointOutput {
    RouteToConnection {
        dst_cid: ConnectionID,
        data: Bytes,
        migration: Option<SocketAddr>,
    },
    NewConnection {
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        data: Bytes,
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
        let header =
            Header::decode(&mut Cursor::new(&data), self.cid_len).expect("Could not decode header");
        let dst_cid = header.dst_cid();

        if let Some(old_remote) = self.connections.get(&dst_cid) {
            if old_remote == &remote {
                return EndpointOutput::RouteToConnection {
                    dst_cid,
                    data,
                    migration: None,
                };
            }

            return EndpointOutput::RouteToConnection {
                dst_cid,
                data,
                migration: Some(remote),
            };
        }

        if !header.is_initial() {
            // TODO: Stateless reset
            return EndpointOutput::Drop;
        }

        // TODO:
        // Server config must be some?
        // Do retry?
        // Validate token?

        self.connections.insert(dst_cid, remote);
        let src_cid = header.src_cid().unwrap();
        EndpointOutput::NewConnection {
            dst_cid,
            src_cid,
            data,
            remote,
        }
    }

    pub fn add_cid(&mut self, local_cid: ConnectionID, remote: SocketAddr) {
        self.connections.insert(local_cid, remote);
    }
}

impl Default for EndpointCore {
    fn default() -> Self {
        Self::new()
    }
}
