use std::{io, sync::Arc};

use bytes::{Bytes, BytesMut};
use rustls::{ClientConfig, ServerConfig};
use rustls_pki_types::ServerName;
use thiserror::Error;

use crate::core::{
    frame::frame_type,
    packet::{DecodedPacketNumber, Packet, PacketNumber},
    transport_error::TransportError,
};

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

/// Cursor's position need to be at the start of the packet number
/// When returning the Cursor's position is after the end of the header
pub fn decrypt_header(
    keys: &rustls::quic::Keys,
    cursor: &mut io::Cursor<BytesMut>,
) -> PacketNumber {
    let pn_offset = cursor.position() as usize;
    let data = cursor.get_mut();

    let sample_offset = pn_offset + 4;
    let sample_len = keys.local.header.sample_len();

    let (header, sample) = data.split_at_mut(sample_offset);
    let (first, rest) = header.split_at_mut(1);

    keys.remote
        .header
        .decrypt_in_place(
            &sample[..sample_len],
            &mut first[0],
            &mut rest[pn_offset - 1..],
        )
        .unwrap();

    let pn_len = PacketNumber::decode_len(first[0]);

    PacketNumber::decode(cursor, pn_len)
}

/// Cursor posiion must be at the end of the header
pub fn decrypt_payload(
    keys: &rustls::quic::Keys,
    cursor: io::Cursor<BytesMut>,
    packet_number: u64,
) -> Bytes {
    let header_len = cursor.position(); // Is after the packet number
    let mut bytes = cursor.into_inner();
    let header_data = bytes.split_to(header_len as usize);
    let mut payload = bytes;

    let len = keys
        .remote
        .packet
        .decrypt_in_place(packet_number, &header_data, &mut payload)
        .unwrap()
        .len(); // Size of the decrypted payload is the size of the slice returned

    payload.truncate(len);

    payload.freeze()
}

/// Given a packet and keys, returns the encrypted packet
/// Uses the values from RFC 9001 section 5.4.2 to get the sample
pub fn encrypt_packet(
    packet: &Packet,
    keys: &rustls::quic::Keys,
    largest: DecodedPacketNumber,
) -> Bytes {
    let mut bytes = BytesMut::with_capacity(packet.size() + keys.local.packet.tag_len());
    packet.header.encode(&mut bytes).unwrap();

    let header_len = bytes.len();
    let pn_len = packet.packet_number().len() as usize;
    let pn_offset = header_len - pn_len; // Packet number is the last byte written

    let sample_len = keys.local.header.sample_len();
    let sample_offset = pn_offset + 4;

    packet
        .frames
        .iter()
        .for_each(|f| f.encode(&mut bytes).unwrap());

    pad_payload_to_len(&mut bytes, sample_offset + sample_len);

    let (header, payload) = bytes.split_at_mut(header_len);
    let pn = packet.packet_number().expand(largest + 1);
    let tag = keys
        .local
        .packet
        .encrypt_in_place(pn, header, payload)
        .unwrap();

    bytes.extend_from_slice(tag.as_ref());

    // Header encryption
    let (header, sample) = bytes.split_at_mut(sample_offset);
    let (first, rest) = header.split_at_mut(1);

    keys.local
        .header
        .encrypt_in_place(
            &sample[..sample_len],
            &mut first[0],
            &mut rest[pn_offset - 1..],
        )
        .unwrap();

    bytes.freeze()
}

fn pad_payload_to_len(bytes: &mut BytesMut, len: usize) {
    if bytes.len() < len {
        bytes.resize(len, frame_type::PADDING as u8);
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

pub fn handshake_type(buf: &[u8]) -> Option<&'static str> {
    match buf.first()? {
        1 => Some("ClientHello"),
        2 => Some("ServerHello"),
        8 => Some("EncryptedExtensions"),
        11 => Some("Certificate"),
        13 => Some("CertificateRequest"),
        15 => Some("CertificateVerify"),
        20 => Some("Finished"),
        _ => Some("Unknown"),
    }
}
