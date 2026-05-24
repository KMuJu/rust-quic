use std::io;

use rust_quic::{connection::Connection, endpoint::Endpoint};

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut endpoint = Endpoint::new(([127, 0, 0, 1], 42069).into()).await?;

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
