use std::{fs, io, sync::Arc};

use rust_quic::{
    core::config::ServerConfig,
    io::{connection::Connection, endpoint::Endpoint},
};

fn server_tls_config() -> Arc<rustls::ServerConfig> {
    let cert = fs::read("examples/keys/cert.pem").expect("missing cert.pem — run gen_certs first");
    let key = fs::read("examples/keys/key.pem").expect("missing key.pem — run gen_certs first");

    let cert_der = rustls_pemfile::certs(&mut cert.as_slice())
        .map(|c| c.unwrap())
        .collect::<Vec<_>>();
    let key_der = rustls_pemfile::private_key(&mut key.as_slice())
        .unwrap()
        .unwrap();

    Arc::new(
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(cert_der, key_der)
            .unwrap(),
    )
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let server_config = ServerConfig::new(server_tls_config());
    let mut endpoint = Endpoint::server(([127, 0, 0, 1], 42069).into(), server_config).await?;

    while let Some(conn) = endpoint.accept().await {
        println!("Accepted incoming connection");
        let fut = handle_conn(conn);
        tokio::spawn(fut);
    }

    Ok(())
}

async fn handle_conn(conn: Connection) {
    async {
        loop {
            let (mut send, mut recv) = conn.accept_bidir().await;
            println!("Accepted bidir stream");
            let mut buf = vec![0u8; 1024];
            let n = recv.read(&mut buf).await.unwrap();

            println!("Read from stream");
            println!("Data: {:?}", &buf[..n]);

            send.write(&buf[..n]).unwrap();
            println!("Wrote to stream");
            println!();
        }
    }
    .await
}
