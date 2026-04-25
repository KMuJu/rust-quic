use std::io;

use rust_quic::io::endpoint::Endpoint;

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut endpoint = Endpoint::new(([127, 0, 0, 1], 32030).into()).await?;
    let conn = endpoint.connect(([127, 0, 0, 1], 42069).into()).await;
    println!("Created connection: {conn:?}");

    let (mut recv, mut send) = conn.open_bidir();

    // thread::sleep(Duration::from_secs(1));
    send.write(&[1, 2, 3, 4, 5]).unwrap();
    // println!("Wrote to stream");

    let mut buf = vec![0u8; 1024];
    let n = recv.read(&mut buf).await.unwrap();

    println!("Read from stream");
    println!("Data: {:?}", &buf[..n]);
    // poll_fn(|_| -> Poll<()> { Poll::Pending }).await;

    Ok(())
}
