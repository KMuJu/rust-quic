use std::{
    collections::{HashMap, VecDeque},
    io::{self, Cursor},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::BytesMut;
use rand::rngs::StdRng;
use thiserror::Error;
use tokio::{
    io::ReadBuf,
    net::UdpSocket,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};

use crate::io::{
    ConnectionToEndpoint, EndpointToConnection,
    connection::{Connection, ConnectionHandle, ConnectionID, Incoming, IncomingInner},
    packet::{Packet, PacketError},
    stream::Side,
};

pub struct EndpointState {
    socket: UdpSocket,
    connection_handles: HashMap<ConnectionID, ConnectionHandle>,
    pub(super) connections: HashMap<ConnectionID, Connection>,
    rng: StdRng,
    incoming_tx: UnboundedSender<IncomingInner>,
    stashed_packet: VecDeque<(BytesMut, SocketAddr)>,
    cid_len: u8,
    outgoing_rx: UnboundedReceiver<ConnectionToEndpoint>, // Receives data from connections
    outgoing_tx: UnboundedSender<ConnectionToEndpoint>,   // To be cloned into connections
}

pub struct EndpointInner {
    pub state: Mutex<EndpointState>,
}

pub struct Endpoint {
    inner: EndpointRef,
    incoming_rx: tokio::sync::Mutex<UnboundedReceiver<IncomingInner>>,
}

impl Endpoint {
    pub async fn new(addr: SocketAddr) -> io::Result<Self> {
        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
        let (outgoing_tx, outgoing_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = EndpointState {
            socket: UdpSocket::bind(addr).await?,
            connection_handles: HashMap::new(),
            connections: HashMap::new(),
            rng: rand::make_rng(),
            incoming_tx,
            stashed_packet: VecDeque::new(),
            cid_len: 4,
            outgoing_rx,
            outgoing_tx,
        };
        let endpoint = EndpointInner {
            state: Mutex::new(state),
        };
        let endpointref = EndpointRef(Arc::new(endpoint));

        let driver = EndpointDriver(endpointref.clone());
        tokio::spawn(async move {
            if let Err(e) = driver.await {
                eprintln!("Error in driver: {e}");
            }
        });

        Ok(Self {
            inner: endpointref,
            incoming_rx: tokio::sync::Mutex::new(incoming_rx),
        })
    }

    pub async fn accept(&mut self) -> Option<Connection> {
        let inner = self.incoming_rx.lock().await.recv().await?;
        let incoming = Incoming::new(inner, self.inner.clone());
        Some(incoming.await)
    }

    pub async fn connect(&mut self, addr: SocketAddr) -> Connection {
        let (conn, connected_recv) = {
            let mut state = self.inner.state.lock().unwrap();
            let peer_cid = ConnectionID::new(state.cid_len, &mut state.rng);
            let cid = ConnectionID::new(state.cid_len, &mut state.rng);

            let (incoming_data, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
            let (connected_send, connected_recv) = oneshot::channel();

            println!("Connecting to endpoint");
            let conn = Connection::new(
                cid,
                peer_cid,
                addr,
                Side::Client,
                connected_send,
                incoming_rx,
                state.outgoing_tx.clone(),
            );
            let handle = ConnectionHandle {
                addr,
                incoming_data,
            };

            state.connection_handles.insert(cid, handle);
            state.connections.insert(cid, conn.clone());

            (conn, connected_recv)
        };

        conn.init();

        // TODO: Wait for handshake to finish
        connected_recv.await.unwrap();

        conn
    }
}

impl EndpointState {
    fn insert_new_connection(
        &mut self,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        addr: SocketAddr,
    ) {
        let (incoming_data, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
        let (connected_send, connected_recv) = oneshot::channel();
        // TODO: Always server??
        let connection = Connection::new(
            dst_cid,
            src_cid,
            addr,
            Side::Server,
            connected_send,
            incoming_rx,
            self.outgoing_tx.clone(),
        );
        println!("Received new connection: {dst_cid:?}, {src_cid:?}");

        let handle = ConnectionHandle {
            addr,
            incoming_data,
        };

        self.connection_handles.insert(dst_cid, handle);
        self.connections.insert(dst_cid, connection);

        // TODO: Handle create incomming connection
        let incoming = IncomingInner::new(dst_cid, src_cid, connected_recv);
        self.incoming_tx.send(incoming).unwrap();
    }

    fn handle_datagram(&mut self, data: &[u8], addr: SocketAddr) {
        // println!("Datagram: {data:?}");
        // TODO: Swap with peek header thingy
        let packet = match Packet::decode(&mut Cursor::new(data), self.cid_len) {
            Ok(p) => p,
            Err(e) => {
                println!("Error when decoding: {e}");
                return;
            }
        };
        let cid = packet.dst_cid();
        if packet.is_initial() {
            let src_cid = packet.src_cid().unwrap();
            self.insert_new_connection(cid, src_cid, addr);
        }

        if let Some(handle) = self.connection_handles.get(&cid) {
            handle
                .incoming_data
                .send(EndpointToConnection {
                    data: data.to_vec(),
                    addr,
                })
                .unwrap();
        } else {
            eprintln!("Could not get connection handle to {cid:?}");
            eprintln!("{:?}", self.connection_handles);
        }
    }

    fn handle_recv(&mut self, cx: &mut Context<'_>) -> Result<bool, io::Error> {
        let mut b = [0u8; 1500];
        let mut buf = ReadBuf::new(&mut b);
        loop {
            match self.socket.poll_recv_from(cx, &mut buf) {
                Poll::Ready(Ok(addr)) => {
                    self.handle_datagram(buf.filled(), addr);
                    buf.clear();
                }
                // From quin-rs/quin
                // Ignore ECONNRESET as it's undefined in QUIC and may be injected by an
                // attacker
                // Is why there is no simple .await on recv_from
                Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::ConnectionReset => {
                    continue;
                }
                Poll::Ready(Err(e)) => {
                    return Err(e);
                }
                Poll::Pending => return Ok(false), // False because poll_recv_from will wake
            };
        }
    }

    fn handle_send(&mut self, cx: &mut Context<'_>) -> Result<bool, PacketError> {
        while let Some((packet, addr)) = self.stashed_packet.pop_front() {
            let Poll::Ready(_) = self.socket.poll_send_to(cx, &packet, addr) else {
                self.stashed_packet.push_front((packet, addr));
                return Ok(false); // False because poll_send_to will wake
            };
        }

        loop {
            let msg = match self.outgoing_rx.poll_recv(cx) {
                Poll::Pending => return Ok(false),
                Poll::Ready(msg) => msg,
            }
            .unwrap(); // WARN: Handle?

            if self
                .socket
                .poll_send_to(cx, &msg.data, msg.addr)
                .is_pending()
            {
                self.stashed_packet.push_back((msg.data, msg.addr));
            }
        }
    }
}

#[derive(Clone)]
pub struct EndpointRef(Arc<EndpointInner>);

impl std::ops::Deref for EndpointRef {
    type Target = EndpointInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct EndpointDriver(EndpointRef);

// Future that drives the IO for an endpoint
impl Future for EndpointDriver {
    type Output = Result<(), DriverError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // let Ok(mut endpoint) = self.0.state.try_lock() else {
        //     cx.waker().wake_by_ref();
        //     return Poll::Pending;
        // };

        let mut endpoint = self.0.state.lock().unwrap();

        let mut did_work = false;
        // Receive packets
        did_work |= endpoint.handle_recv(cx)?;
        // Send packets
        did_work |= endpoint.handle_send(cx)?;

        if did_work {
            drop(endpoint);
            cx.waker().wake_by_ref();
        }

        Poll::Pending
    }
}

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("Packet error: {0}")]
    Packet(#[from] PacketError),

    #[error("IO: {0}")]
    IO(#[from] io::Error),
}
