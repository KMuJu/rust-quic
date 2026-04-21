use std::{future::poll_fn, io, task::Poll};

use bytes::Bytes;
use rust_quic::io::{
    connection::ConnectionID,
    encoding::VarInt,
    endpoint::Endpoint,
    packet::{Header, Packet, PacketNumber},
};
use tokio::net::UdpSocket;

#[tokio::main]
async fn main() -> io::Result<()> {
    // let socket = UdpSocket::bind("127.0.0.1:32030").await?;
    // socket.connect(("127.0.0.1", 42069)).await?;
    // send_packets(&socket).await?;

    let mut endpoint = Endpoint::new(([127, 0, 0, 1], 32030).into()).await?;
    let conn = endpoint.connect(([127, 0, 0, 1], 42069).into()).await;
    println!("Created connection: {conn:?}");

    let (mut recv, mut send) = conn.open_bidir();

    // thread::sleep(Duration::from_secs(1));
    send.write(&[1, 2, 3, 4, 5]).unwrap();
    println!("Wrote to stream");

    let mut buf = vec![0u8; 1024];
    let n = recv.read(&mut buf).await.unwrap();

    println!("Read from stream");
    println!("Data: {:?}", &buf[..n]);
    poll_fn(|_| -> Poll<()> { Poll::Pending }).await;

    Ok(())
}

async fn send_packets(socket: &UdpSocket) -> io::Result<()> {
    let packet = Packet {
        header: Header::Initial {
            version: 1,
            dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
            src_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1]), 1),
            token: None,
            length: VarInt(1),
            packet_number: PacketNumber::U8(1u8),
        },
        payload: Bytes::new(),
    };
    let mut buf: Vec<u8> = Vec::new();
    packet.encode(&mut buf).unwrap();

    socket.send(&buf).await?;

    println!("Sent inital packet");
    // thread::sleep(Duration::from_secs(1));

    let packet = Packet {
        header: Header::Short {
            dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
            spin: false,
            key_phase: false,
            packet_number: PacketNumber::U8(2u8),
        },
        payload: Bytes::from([12, 1, 0, 255, 255, 255].as_slice()),
    };
    let mut buf: Vec<u8> = Vec::new();
    packet.encode(&mut buf).unwrap();
    println!("Sent short packet");
    println!("Buf: {buf:?}");

    socket.send(&buf).await?;

    let packet = Packet {
        header: Header::Short {
            dst_cid: ConnectionID::from_buf(&mut io::Cursor::new(&[1, 2, 3, 4]), 4),
            spin: false,
            key_phase: false,
            packet_number: PacketNumber::U8(3u8),
        },
        payload: Bytes::from([12, 1, 3, 255, 255, 255].as_slice()),
    };

    let mut buf: Vec<u8> = Vec::new();
    packet.encode(&mut buf).unwrap();
    println!("Sent short packet");
    println!("Buf: {buf:?}");

    socket.send(&buf).await?;

    Ok(())
}
