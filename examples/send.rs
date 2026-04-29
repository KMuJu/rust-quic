use std::io;

use rust_quic::io::endpoint::Endpoint;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut endpoint = Endpoint::new(([127, 0, 0, 1], 32030).into()).await?;
    let conn = endpoint.connect(([127, 0, 0, 1], 42069).into()).await;
    println!("Created connection: {conn:?}");

    for _ in 0..5 {
        let (mut recv, mut send) = conn.open_bidir();
        send.write(&[1, 2, 3, 4, 5]).unwrap();

        let mut buf = vec![0u8; 1024];
        let n = recv.read(&mut buf).await.unwrap();

        println!("Read from stream");
        println!("Data: {:?}", &buf[..n]);
    }

    Ok(())
}
