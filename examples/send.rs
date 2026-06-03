use std::{fs, io, sync::Arc};

use rust_quic::{core::config::ClientConfig, io::endpoint::Endpoint};

fn client_tls_config() -> Arc<rustls::ClientConfig> {
    let cert = fs::read("examples/keys/cert.pem").expect("missing cert.pem — run gen_certs first");

    let mut root_store = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut cert.as_slice()) {
        root_store.add(cert.unwrap()).unwrap();
    }

    Arc::new(
        rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth(),
    )
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let client_config = ClientConfig::new(client_tls_config());
    let mut endpoint = Endpoint::client(([127, 0, 0, 1], 32030).into(), client_config).await?;
    let name = "localhost";
    let conn = endpoint.connect(name, ([127, 0, 0, 1], 42069).into()).await;
    println!("Created connection");

    for _ in 0..5 {
        let (mut recv, mut send) = conn.open_bidir();
        send.write(&[1, 2, 3, 4, 5]).unwrap();

        let mut buf = vec![0u8; 1024];
        let n = recv.read(&mut buf).await.unwrap();

        println!("Read from stream");
        println!("Data: {:?}", &buf[..n]);
        println!();
    }

    Ok(())
}
