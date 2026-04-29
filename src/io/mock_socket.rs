pub mod mock {
    use std::{
        collections::VecDeque,
        io,
        net::SocketAddr,
        sync::{Arc, Mutex},
        task::Poll,
    };

    use futures::task::AtomicWaker;

    use crate::io::socket::Socket;

    type MockChannel = Arc<Mutex<VecDeque<(Vec<u8>, SocketAddr)>>>;

    pub struct MockSocket {
        incoming: MockChannel,
        outgoing: MockChannel,
        recv_waker: Arc<AtomicWaker>,
        send_waker: Arc<AtomicWaker>,
        loss_rate: f64,
    }

    impl MockSocket {
        pub fn pair(loss_rate: f64) -> (MockSocket, MockSocket) {
            let a_to_b = Arc::new(Mutex::new(VecDeque::new()));
            let b_to_a = Arc::new(Mutex::new(VecDeque::new()));
            let waker_a = Arc::new(AtomicWaker::new());
            let waker_b = Arc::new(AtomicWaker::new());

            let a = MockSocket {
                incoming: b_to_a.clone(),
                outgoing: a_to_b.clone(),
                recv_waker: waker_a.clone(),
                send_waker: waker_b.clone(),
                loss_rate,
            };
            let b = MockSocket {
                incoming: a_to_b.clone(),
                outgoing: b_to_a.clone(),
                recv_waker: waker_b.clone(),
                send_waker: waker_a.clone(),
                loss_rate,
            };
            (a, b)
        }
    }

    impl Socket for MockSocket {
        fn poll_recv_from(
            &self,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> Poll<io::Result<SocketAddr>> {
            self.recv_waker.register(cx.waker());
            if let Some((data, addr)) = self.incoming.lock().unwrap().pop_front() {
                buf.put_slice(&data);
                Poll::Ready(Ok(addr))
            } else {
                Poll::Pending
            }
        }

        fn poll_send_to(
            &self,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
            addr: SocketAddr,
        ) -> Poll<io::Result<usize>> {
            if rand::random::<f64>() >= self.loss_rate {
                self.outgoing
                    .lock()
                    .unwrap()
                    .push_back((buf.to_vec(), addr));
                self.send_waker.wake(); // wake the other socket's recv
            } else {
                println!("[MOCK] Lost packet in mock");
            }
            Poll::Ready(Ok(buf.len()))
        }
    }

    #[cfg(test)]
    mod tests {
        use std::future::poll_fn;

        use tokio::io::ReadBuf;

        use super::*;

        #[tokio::test]
        async fn test_mock_socket_send_recv() {
            let (a, b) = MockSocket::pair(0.0); // no loss
            let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();

            // Send from a
            poll_fn(|cx| a.poll_send_to(cx, b"hello", addr))
                .await
                .unwrap();

            // Receive on b
            let mut buf = vec![0u8; 64];
            let mut read_buf = ReadBuf::new(&mut buf);
            let from = poll_fn(|cx| b.poll_recv_from(cx, &mut read_buf))
                .await
                .unwrap();

            assert_eq!(read_buf.filled(), b"hello");
            assert_eq!(from, addr);
        }

        #[tokio::test]
        async fn test_mock_socket_loss() {
            let (a, b) = MockSocket::pair(1.0); // 100% loss
            let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();

            poll_fn(|cx| a.poll_send_to(cx, b"hello", addr))
                .await
                .unwrap();

            // Nothing should be in b's queue
            assert!(b.incoming.lock().unwrap().is_empty());
        }

        #[tokio::test]
        async fn test_mock_socket_bidirectional() {
            let (a, b) = MockSocket::pair(0.0);
            let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();

            // a -> b
            poll_fn(|cx| a.poll_send_to(cx, b"ping", addr))
                .await
                .unwrap();
            let mut buf = vec![0u8; 64];
            let mut read_buf = ReadBuf::new(&mut buf);
            poll_fn(|cx| b.poll_recv_from(cx, &mut read_buf))
                .await
                .unwrap();
            assert_eq!(read_buf.filled(), b"ping");

            // b -> a
            poll_fn(|cx| b.poll_send_to(cx, b"pong", addr))
                .await
                .unwrap();
            let mut buf = vec![0u8; 64];
            let mut read_buf = ReadBuf::new(&mut buf);
            poll_fn(|cx| a.poll_recv_from(cx, &mut read_buf))
                .await
                .unwrap();
            assert_eq!(read_buf.filled(), b"pong");
        }

        #[tokio::test]
        async fn test_mock_socket_waking() {
            let (a, b) = MockSocket::pair(0.0); // no loss
            let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();

            // Send from a
            let send_fut = poll_fn(|cx| a.poll_send_to(cx, b"hello", addr));

            // Receive on b
            let mut buf = vec![0u8; 64];
            let mut read_buf = ReadBuf::new(&mut buf);
            let read_fut = poll_fn(|cx| b.poll_recv_from(cx, &mut read_buf));

            send_fut.await.unwrap();
            let from = read_fut.await.unwrap();

            assert_eq!(read_buf.filled(), b"hello");
            assert_eq!(from, addr);
        }
    }
}
