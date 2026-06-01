use std::sync::Arc;

use bytes::{Buf, BufMut};
use rustls::{ClientConfig, ServerConfig};
use rustls_pki_types::ServerName;
use thiserror::Error;

use crate::core::{connection::ConnectionID, transport_error::TransportError};

// TODO: This is just a mock, will later use rustls

#[derive(Debug)]
pub enum CryptoFrame {
    ClientHello,
    ServerHello,
    Finished,
}

impl CryptoFrame {
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        match self {
            CryptoFrame::ClientHello => buf.put_u8(0),
            CryptoFrame::ServerHello => buf.put_u8(1),
            CryptoFrame::Finished => buf.put_u8(2),
        }
    }

    pub fn decode<B: Buf>(buf: &mut B) -> CryptoFrame {
        let t = buf.get_u8();
        match t {
            0 => CryptoFrame::ClientHello,
            1 => CryptoFrame::ServerHello,
            2 => CryptoFrame::Finished,
            _ => panic!("Got invalid cryptoframe"),
        }
    }
}

enum CryptoConfig {
    Client(Arc<rustls::ClientConfig>),
    Server(Arc<rustls::ServerConfig>),
}

pub struct CryptoConn {
    conn: rustls::quic::Connection,
    next_secret: Option<rustls::quic::Secrets>,
    conf: CryptoConfig,
}

#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("Rustls error")]
    Tls(#[from] rustls::Error),
}

impl CryptoConn {
    pub fn new_server(config: Arc<ServerConfig>, params: Vec<u8>) -> Result<Self, CryptoError> {
        let conn =
            rustls::quic::ServerConnection::new(config.clone(), rustls::quic::Version::V1, params)?;

        Ok(Self {
            conn: rustls::quic::Connection::Server(conn),
            next_secret: None,
            conf: CryptoConfig::Server(config),
        })
    }

    pub fn new_client(
        config: Arc<ClientConfig>,
        name: &str,
        params: Vec<u8>,
    ) -> Result<Self, CryptoError> {
        let conn = rustls::quic::ClientConnection::new(
            config.clone(),
            rustls::quic::Version::V1,
            ServerName::try_from(name).unwrap().to_owned(),
            params,
        )?;

        Ok(Self {
            conn: rustls::quic::Connection::Client(conn),
            next_secret: None,
            conf: CryptoConfig::Client(config),
        })
    }

    pub fn read_handshake(&mut self, buf: &[u8]) -> Result<(), TransportError> {
        self.conn.read_hs(buf).map_err(|e| {
            if let Some(alert) = self.conn.alert() {
                TransportError
            } else {
                TransportError
            }
        })
    }

    pub fn write_crypto(&mut self, buf: &mut Vec<u8>) -> Option<rustls::quic::Keys> {
        let keys = match self.conn.write_hs(buf)? {
            rustls::quic::KeyChange::Handshake { keys } => keys,
            rustls::quic::KeyChange::OneRtt { keys, next } => {
                self.next_secret = Some(next);
                keys
            }
        };

        Some(keys)
    }

    pub fn is_handshaking(&self) -> bool {
        self.conn.is_handshaking()
    }

    pub fn initial_keys(&mut self, client_dst_connection_id: &[u8]) -> rustls::quic::Keys {
        let provider = match &self.conf {
            CryptoConfig::Client(client_config) => client_config.crypto_provider(),
            CryptoConfig::Server(server_config) => server_config.crypto_provider(),
        };

        let suite = initial_suite_from_provider(provider)
            .expect("Could not get TLS13_AES_128_GCM_SHA256 from provider");

        let side = match &self.conf {
            CryptoConfig::Client(_) => rustls::Side::Client,
            CryptoConfig::Server(_) => rustls::Side::Server,
        };

        suite.keys(client_dst_connection_id, side, rustls::quic::Version::V1)
    }
}

fn initial_suite_from_provider(
    provider: &Arc<rustls::crypto::CryptoProvider>,
) -> Option<rustls::quic::Suite> {
    provider
        .cipher_suites
        .iter()
        .find_map(|cs| match (cs.suite(), cs.tls13()) {
            (rustls::CipherSuite::TLS13_AES_128_GCM_SHA256, Some(suite)) => {
                Some(suite.quic_suite())
            }
            _ => None,
        })
        .flatten()
}
