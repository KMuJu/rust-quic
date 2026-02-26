use core::task::Poll::Ready;
use std::{
    collections::HashMap,
    io::{self, Cursor},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use futures::task::AtomicWaker;
use rand::rngs::StdRng;
use thiserror::Error;
use tokio::{
    io::ReadBuf,
    net::UdpSocket,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};

use crate::io::{
    connection::{Connection, ConnectionID, Incoming, IncomingInner},
    packet::{Packet, PacketError},
    stream::Side,
};

pub struct EndpointState {
    pub socket: UdpSocket,
    pub connections: HashMap<ConnectionID, Connection>,
    rng: StdRng,
    incoming_tx: UnboundedSender<IncomingInner>,
    stashed_packet: Option<(Vec<u8>, ConnectionID)>,
    cid_to_ip: HashMap<ConnectionID, SocketAddr>,
    cid_len: u8,
    driver_waker: Arc<AtomicWaker>,
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
        let state = EndpointState {
            socket: UdpSocket::bind(addr).await?,
            connections: HashMap::new(),
            rng: rand::make_rng(),
            incoming_tx,
            stashed_packet: None,
            cid_to_ip: HashMap::new(),
            cid_len: 8,
            driver_waker: Arc::new(AtomicWaker::new()),
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
        let mut state = self.inner.state.lock().unwrap();
        let peer_cid = ConnectionID::new(state.cid_len, &mut state.rng);
        let cid = ConnectionID::new(state.cid_len, &mut state.rng);

        let conn = Connection::new(cid, peer_cid, Side::Client, state.driver_waker.clone());
        state.connections.insert(cid, conn.clone());
        state.cid_to_ip.insert(cid, addr);

        conn.init();

        // TODO: Wait for handshake to finish

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
        // TODO: Always server??
        let connection = Connection::new(dst_cid, src_cid, Side::Server, self.driver_waker.clone());
        self.connections.insert(dst_cid, connection.clone());

        // TODO: Handle create incomming connection
        let incoming = IncomingInner::new(dst_cid, src_cid);
        self.incoming_tx.send(incoming).unwrap();
        self.cid_to_ip.insert(dst_cid, addr);
    }

    fn handle_datagram(&mut self, data: &[u8], addr: SocketAddr) {
        println!("Datagram: {data:?}");
        let packet = match Packet::decode(&mut Cursor::new(data), self.cid_len) {
            Ok(p) => p,
            Err(e) => {
                println!("Error when decoding: {e}");
                return;
            }
        };
        if let Some(src_cid) = packet.src_cid() {
            self.insert_new_connection(packet.dst_cid(), src_cid, addr);
        }

        let cid = packet.dst_cid();
        let Some(connection) = self.connections.get_mut(&cid) else {
            println!("Could not get connection: {cid:?}");
            println!("Connections: {:?}", self.connections);
            return;
        };

        connection.handle_datagram(packet);
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
        if let Some((packet, cid)) = &self.stashed_packet {
            let Ready(_) = self
                .socket
                .poll_send_to(cx, packet, *self.cid_to_ip.get(cid).unwrap())
            else {
                return Ok(false); // False because poll_send_to will wake
            };
            self.stashed_packet = None;
        }

        println!("Handling send");
        let mut did_work = false;
        for (cid, conn) in self.connections.iter_mut() {
            let Some(packet) = conn.get_packet(1200) else {
                continue;
            };

            let mut buf: Vec<u8> = Vec::new();
            packet.encode(&mut buf)?;

            let Ready(_) = self
                .socket
                .poll_send_to(cx, &buf, *self.cid_to_ip.get(cid).unwrap())
            else {
                self.stashed_packet = Some((buf, *cid));
                return Ok(false); // False because poll_send_to will wake
            };
            did_work = true;
        }

        Ok(did_work)
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

        // println!("Waiting for endpoint");
        let mut endpoint = self.0.state.lock().unwrap();
        // println!("Got endpoint");

        endpoint.driver_waker.register(cx.waker());

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
