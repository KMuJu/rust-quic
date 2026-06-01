use std::{
    collections::{HashMap, VecDeque},
    io::{self},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
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

use crate::{
    core::{
        connection::{ConnectionCore, ConnectionID},
        endpoint::{EndpointCore, EndpointOutput},
        packet::PacketError,
        stream::Side,
        transport_parameters::TransportParam,
    },
    io::{
        ConnectionToEndpoint, EndpointToConnection,
        connection::{Connection, ConnectionHandle, IncomingInner},
    },
};
use crate::{io::connection::Incoming, socket::Socket};

pub struct EndpointIO<S: Socket> {
    socket: S,
    core: EndpointCore,
    // TODO: Multiple connectionids can point to the same connection, so add a map between
    // connectionid and internal handle
    connection_handles: HashMap<ConnectionID, ConnectionHandle>,
    pub(super) connections: HashMap<ConnectionID, Connection>,
    rng: StdRng,
    incoming_tx: UnboundedSender<IncomingInner>,
    stashed_packet: VecDeque<(Bytes, SocketAddr)>,
    cid_len: u8,
    outgoing_rx: UnboundedReceiver<ConnectionToEndpoint>, // Receives data from connections
    outgoing_tx: UnboundedSender<ConnectionToEndpoint>,   // To be cloned into connections
}

pub struct EndpointRef<S: Socket>(Arc<Mutex<EndpointIO<S>>>);

pub struct Endpoint<S: Socket> {
    inner: EndpointRef<S>,
    incoming_rx: tokio::sync::Mutex<UnboundedReceiver<IncomingInner>>,
}

impl Endpoint<UdpSocket> {
    pub async fn new(addr: SocketAddr) -> io::Result<Self> {
        Ok(Self::new_generic(UdpSocket::bind(addr).await?))
    }
}

impl<S: Socket> Endpoint<S> {
    pub fn new_generic(socket: S) -> Self {
        let (incoming_tx, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
        let (outgoing_tx, outgoing_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = EndpointIO {
            socket,
            core: EndpointCore::new(),
            connection_handles: HashMap::new(),
            connections: HashMap::new(),
            rng: rand::make_rng(),
            incoming_tx,
            stashed_packet: VecDeque::new(),
            cid_len: 4,
            outgoing_rx,
            outgoing_tx,
        };
        let endpointref = EndpointRef(Arc::new(Mutex::new(state)));

        let driver = EndpointDriver {
            endpoint: endpointref.clone(),
        };
        tokio::spawn(async move {
            if let Err(e) = driver.await {
                eprintln!("Error in driver: {e}");
            }
        });

        Self {
            inner: endpointref,
            incoming_rx: tokio::sync::Mutex::new(incoming_rx),
        }
    }

    pub async fn accept(&mut self) -> Option<Connection> {
        let inner = self.incoming_rx.lock().await.recv().await?;
        let incoming = Incoming::new(inner, self.inner.clone());
        Some(incoming.await)
    }

    pub async fn connect(&mut self, addr: SocketAddr) -> Connection {
        let (conn, connected_recv) = {
            let mut endpoint = self.inner.lock().unwrap();

            let (incoming_data, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
            let (connected_send, connected_recv) = oneshot::channel();

            let (core, cid) = endpoint.core.new_client_conn();
            println!("Connecting to endpoint");
            let conn = Connection::new(
                addr,
                core,
                connected_send,
                incoming_rx,
                endpoint.outgoing_tx.clone(),
            );
            let handle = ConnectionHandle {
                addr,
                incoming_data,
            };

            endpoint.connection_handles.insert(cid, handle);
            endpoint.connections.insert(cid, conn.clone());
            endpoint.core.add_cid(cid, addr);

            (conn, connected_recv)
        };

        conn.init();

        // Wait for handshake to finish
        connected_recv.await.unwrap();

        conn
    }
}

impl<S: Socket> EndpointIO<S> {
    fn insert_new_connection(
        &mut self,
        dst_cid: ConnectionID,
        src_cid: ConnectionID,
        addr: SocketAddr,
    ) {
        let (incoming_data, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
        let (connected_send, connected_recv) = oneshot::channel();
        let core = self.core.new_server_conn(dst_cid, src_cid);
        // TODO: Always server??
        let connection = Connection::new(
            addr,
            core,
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
        let incoming = IncomingInner::new(dst_cid, connected_recv);
        self.incoming_tx.send(incoming).unwrap();
    }

    fn handle_datagram(&mut self, data: Bytes, addr: SocketAddr) {
        match self.core.handle_datagram(data, addr) {
            EndpointOutput::RouteToConnection {
                dst_cid,
                protected,
                migration,
            } => {
                if let Some(handle) = self.connection_handles.get(&dst_cid) {
                    handle
                        .incoming_data
                        .send(EndpointToConnection { protected, addr })
                        .unwrap();
                } else {
                    eprintln!("Could not get connection handle to {dst_cid:?}");
                    eprintln!("{:?}", self.connection_handles);
                }
            }
            EndpointOutput::NewConnection {
                dst_cid,
                src_cid,
                protected,
                remote,
            } => {
                self.insert_new_connection(dst_cid, src_cid, remote);
                if let Some(handle) = self.connection_handles.get(&dst_cid) {
                    handle
                        .incoming_data
                        .send(EndpointToConnection { protected, addr })
                        .unwrap();
                } else {
                    eprintln!("Could not get connection handle to {dst_cid:?}");
                    eprintln!("{:?}", self.connection_handles);
                }
            }
            EndpointOutput::Drop => todo!(),
            EndpointOutput::SendDatagram => todo!(),
        }
    }

    fn handle_recv(&mut self, cx: &mut Context<'_>) -> Result<bool, io::Error> {
        let mut b = [0u8; 1500];
        let mut buf = ReadBuf::new(&mut b);
        loop {
            match self.socket.poll_recv_from(cx, &mut buf) {
                Poll::Ready(Ok(addr)) => {
                    let data = Bytes::copy_from_slice(buf.filled());
                    self.handle_datagram(data, addr);
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

impl<S: Socket> Clone for EndpointRef<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<S: Socket> std::ops::Deref for EndpointRef<S> {
    type Target = Arc<Mutex<EndpointIO<S>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct EndpointDriver<S: Socket> {
    endpoint: EndpointRef<S>,
}

// Future that drives the IO for an endpoint
impl<S: Socket> Future for EndpointDriver<S> {
    type Output = Result<(), DriverError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut endpoint = self.endpoint.lock().unwrap();

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
