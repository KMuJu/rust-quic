use std::{net::SocketAddr, time::Duration};

use rust_quic::{io::endpoint::Endpoint, mock_socket::mock::MockSocket};

// #[tokio::test]
// async fn test_handshake() {
//     let (client_sock, server_sock) = MockSocket::pair(0.0, 42);
//     let server_addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
//
//     let mut server = Endpoint::<MockSocket>::new_generic(server_sock);
//     let mut client = Endpoint::<MockSocket>::new_generic(client_sock);
//
//     // Spawn server to accept a connection
//     let server_handle = tokio::spawn(async move {
//         server.accept().await;
//     });
//
//     // Client connects
//     let _ = client.connect(server_addr).await;
//     server_handle.await.unwrap();
// }
//
// #[tokio::test]
// async fn test_stream_send_recv() {
//     let (client_sock, server_sock) = MockSocket::pair(0.0, 42);
//     let server_addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
//
//     let mut server = Endpoint::<MockSocket>::new_generic(server_sock);
//     let mut client = Endpoint::<MockSocket>::new_generic(client_sock);
//
//     let server_handle = tokio::spawn(async move {
//         let conn = server.accept().await.unwrap();
//         let (mut send, mut recv) = conn.accept_bidir().await;
//         // Echo back whatever we receive
//         let mut buf = vec![0u8; 1024];
//         let n = recv.read(&mut buf).await.unwrap();
//         send.write(&buf[..n]).unwrap();
//     });
//
//     let client_conn = client.connect(server_addr).await;
//     let (mut recv, mut send) = client_conn.open_bidir();
//
//     send.write(b"hello world").unwrap();
//     let mut buf = vec![0u8; 1024];
//     let n = recv.read(&mut buf).await.unwrap();
//     assert_eq!(&buf[..n], b"hello world");
//
//     server_handle.await.unwrap();
// }
//
// #[tokio::test]
// async fn test_stream_with_loss() {
//     tokio::time::timeout(Duration::from_secs(6), async {
//         let (client_sock, server_sock) = MockSocket::pair(0.2, 42); // 10% loss
//         let server_addr: SocketAddr = "127.0.0.1:5000".parse().unwrap();
//
//         let mut server = Endpoint::<MockSocket>::new_generic(server_sock);
//         let mut client = Endpoint::<MockSocket>::new_generic(client_sock);
//
//         let rounds = 0..5;
//         let rounds_server = rounds.clone();
//
//         let server_handle = tokio::spawn(async move {
//             let conn = server.accept().await.unwrap();
//             let (mut send, mut recv) = conn.accept_bidir().await;
//             // Echo back whatever we receive
//             for _ in rounds_server.clone() {
//                 let mut buf = vec![0u8; 1024];
//                 let n = recv.read(&mut buf).await.unwrap();
//                 send.write(&buf[..n]).unwrap();
//             }
//         });
//
//         let client_conn = client.connect(server_addr).await;
//         let (mut recv, mut send) = client_conn.open_bidir();
//
//         for _ in rounds {
//             send.write(b"hello world").unwrap();
//             println!("[TEST] Sent hello world");
//             let mut buf = vec![0u8; 1024];
//             let n = recv.read(&mut buf).await.unwrap();
//             assert_eq!(&buf[..n], b"hello world");
//             println!("[TEST] Read hello world\n");
//         }
//
//         server_handle.await.unwrap();
//     })
//     .await
//     .expect("test timed out after 6 seconds");
// }
