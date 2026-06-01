use std::sync::Arc;

use bytes::{Buf, BufMut};
use rustls::{ClientConfig, ServerConfig};
use rustls_pki_types::ServerName;
use thiserror::Error;

use crate::core::transport_error::TransportError;

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

pub struct CryptoConn {
    conn: rustls::quic::Connection,
    next_secret: Option<rustls::quic::Secrets>,
}

#[derive(Error, Debug)]
pub enum CryptoError {
    #[error("Rustls error")]
    Tls(#[from] rustls::Error),
}

impl CryptoConn {
    pub fn new_server(config: Arc<ServerConfig>, params: Vec<u8>) -> Result<Self, CryptoError> {
        let conn = rustls::quic::ServerConnection::new(config, rustls::quic::Version::V1, params)?;

        Ok(Self {
            conn: rustls::quic::Connection::Server(conn),
            next_secret: None,
        })
    }

    pub fn new_client(
        config: Arc<ClientConfig>,
        name: &str,
        params: Vec<u8>,
    ) -> Result<Self, CryptoError> {
        let conn = rustls::quic::ClientConnection::new(
            config,
            rustls::quic::Version::V1,
            ServerName::try_from(name).unwrap().to_owned(),
            params,
        )?;

        Ok(Self {
            conn: rustls::quic::Connection::Client(conn),
            next_secret: None,
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
}
