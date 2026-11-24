use std::sync::{Arc, atomic::AtomicU64};

use bytes::BytesMut;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::core::{
    connection::{ConnEvent, ConnectionCore},
    connection_id::ConnectionID,
    crypto::CryptoConn,
    packet::ProtectedPacket,
    space::PacketSpace,
    stream::Side,
    transport_parameters::TransportParam,
};

mod connection_ids;
mod timers;

// Will contain utils for the other tests
const CID_LEN: u8 = 4;
const MTU: usize = 1500;
static CLIENT_INTERNAL_CID: AtomicU64 = AtomicU64::new(0);
static SERVER_INTERNAL_CID: AtomicU64 = AtomicU64::new(0);
static CLIENT_CID_SEQ_NUM: AtomicU64 = AtomicU64::new(0);
static SERVER_CID_SEQ_NUM: AtomicU64 = AtomicU64::new(0);

pub(super) fn create_connections() -> (ConnectionCore, ConnectionCore) {
    let transport_params = Arc::new(TransportParam::new());
    let (client_cid, server_cid) = (new_cid(Side::Client), new_cid(Side::Server));
    let (client_crypto, server_crypto) = new_crypto_conns();

    let client = ConnectionCore::new(
        CLIENT_INTERNAL_CID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        client_cid,
        server_cid,
        server_cid,
        Side::Client,
        client_crypto,
        transport_params.clone(),
    );
    let server = ConnectionCore::new(
        SERVER_INTERNAL_CID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        server_cid,
        client_cid,
        server_cid,
        Side::Server,
        server_crypto,
        transport_params,
    );

    (client, server)
}

pub(super) fn do_handshake(client: &mut ConnectionCore, server: &mut ConnectionCore) {
    client.init();
    println!("[CLIENT] Sending from client");
    send_all_packets(client, server, MTU, false);

    loop {
        println!("[SERVER] Sending from server");
        send_all_packets(server, client, MTU, false);

        println!("[CLIENT] Sending from client");
        send_all_packets(client, server, MTU, false);

        if handshake_is_done(client) {
            println!("===HANDSHAKE IS DONE===");
            assert!(handshake_is_done(server));
            break;
        }
    }

    send_all_packets(server, client, MTU, false);
}

pub(super) fn send_all_packets(
    sender: &mut ConnectionCore,
    receiver: &mut ConnectionCore,
    mtu: usize,
    lost: bool,
) {
    for p_space in PacketSpace::iter() {
        while let Some(packet) = sender.get_encrypted_packet(p_space, mtu) {
            if lost {
                continue;
            }
            let bytes = BytesMut::from(packet);

            let protected = ProtectedPacket::new(bytes, CID_LEN).unwrap();
            receiver.handle_datagram(protected);
        }
    }
}

fn handshake_is_done(conn: &mut ConnectionCore) -> bool {
    while let Some(event) = conn.poll_conn_event() {
        if matches!(event, ConnEvent::HandshakeDone) {
            return true;
        }
    }

    false
}

pub(super) fn new_cid(side: Side) -> ConnectionID {
    let seq_num = match side {
        Side::Client => CLIENT_CID_SEQ_NUM.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
        Side::Server => SERVER_CID_SEQ_NUM.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
    };

    ConnectionID::new(seq_num, CID_LEN, &mut rand::make_rng())
}

fn new_crypto_conns() -> (CryptoConn, CryptoConn) {
    let name = "test";
    let (client_config, server_config) = crypto_conf(name);

    let client = CryptoConn::new_client(client_config, name, vec![]).unwrap();
    let server = CryptoConn::new_server(server_config, vec![]).unwrap();

    (client, server)
}

fn crypto_conf(name: &str) -> (Arc<rustls::ClientConfig>, Arc<rustls::ServerConfig>) {
    let cert = rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivateKeyDer::try_from(cert.key_pair.serialized_der().to_vec()).unwrap();

    let server_config = Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap(),
    );

    // Client trusts this specific cert directly
    let mut root_store = rustls::RootCertStore::empty();
    root_store.add(cert_der).unwrap();

    let client_config = Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    );

    (client_config, server_config)
}

#[test]
fn test_handshake() {
    let (mut client, mut server) = create_connections();

    do_handshake(&mut client, &mut server);
}
