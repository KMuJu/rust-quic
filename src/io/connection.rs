use std::{
    fmt::Debug,
    future::poll_fn,
    net::SocketAddr,
    ops::Deref,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll, ready},
};
use thiserror::Error;

use bytes::BytesMut;
use tokio::{
    io::ReadBuf,
    sync::{
        mpsc::{UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    time::Instant,
};

use crate::{
    ConnectionToEndpoint, EndpointToConnection,
    core::{
        connection::{ConnectionCore, ConnectionID},
        space::PacketSpace,
        stream::{Side, StreamID},
        transport_parameters::TransportParam,
    },
    io::endpoint::EndpointRef,
    socket::Socket,
};

#[derive(Debug)]
pub(crate) struct ConnectionHandle {
    pub(super) addr: SocketAddr,
    pub(super) incoming_data: UnboundedSender<EndpointToConnection>,
}

#[derive(Clone)]
pub struct ConnectionRef(Arc<Mutex<ConnectionIO>>);

impl Deref for ConnectionRef {
    type Target = Arc<Mutex<ConnectionIO>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Clone)]
pub struct Connection {
    inner: ConnectionRef,
}

impl Connection {
    pub(super) fn new(
        cid: ConnectionID,
        peer_cid: ConnectionID,
        addr: SocketAddr,
        side: Side,
        connected: oneshot::Sender<()>,
        incoming_data: UnboundedReceiver<EndpointToConnection>,
        outgoing_data: UnboundedSender<ConnectionToEndpoint>,
    ) -> Self {
        let io = ConnectionRef(Arc::new(Mutex::new(ConnectionIO::new(
            cid,
            peer_cid,
            addr,
            side,
            outgoing_data,
            connected,
        ))));
        let driver = ConnectionDriver {
            io: io.clone(),
            incoming_data,
            addr,
            timer: None,
        };

        tokio::spawn(driver);
        Connection { inner: io }
    }

    pub async fn accept_bidir(&self) -> (SendStreamHandle, RecvStreamHandle) {
        let id = poll_fn(|cx| {
            let mut inner = self.inner.lock().unwrap();
            inner.poll_accept_bidir(cx)
        })
        .await;
        (
            SendStreamHandle::new(id.clone(), self.inner.clone()),
            RecvStreamHandle::new(id.clone(), self.inner.clone()),
        )
    }

    pub async fn accept_uni(&self) -> RecvStreamHandle {
        let id = poll_fn(|cx| {
            let mut inner = self.inner.lock().unwrap();
            inner.poll_accept_unidir(cx)
        })
        .await;
        RecvStreamHandle::new(id.clone(), self.inner.clone())
    }

    pub fn open_bidir(&self) -> (RecvStreamHandle, SendStreamHandle) {
        let mut inner = self.inner.lock().unwrap();
        let id = inner.new_bidir();

        let recv_handle = RecvStreamHandle::new(id.clone(), self.inner.clone());
        let send_handle = SendStreamHandle::new(id.clone(), self.inner.clone());

        (recv_handle, send_handle)
    }

    // Send initial packet to peer
    pub(super) fn init(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.init();
    }
}

// impl Debug for Connection {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let inner = self.inner.lock().unwrap();
//         f.write_fmt(format_args!(
//             "Connection({:?}, {:?})",
//             inner.core.id, inner.core.peer_cid
//         ))
//     }
// }

pub struct IncomingInner {
    id: ConnectionID,
    // src_cid: ConnectionID,
    connected: oneshot::Receiver<()>,
}

impl IncomingInner {
    pub fn new(id: ConnectionID, connected: oneshot::Receiver<()>) -> Self {
        Self {
            id,
            // src_cid,
            connected,
        }
    }
}

pub struct Incoming<S: Socket> {
    inner: IncomingInner,
    endpoint: EndpointRef<S>,
}

impl<S: Socket> Incoming<S> {
    pub fn new(inner: IncomingInner, endpoint: EndpointRef<S>) -> Self {
        Self { inner, endpoint }
    }
}

impl<S: Socket> Future for Incoming<S> {
    type Output = Connection;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        println!("Waiting for connected notification");
        ready!(Pin::new(&mut this.inner.connected).poll(cx)).unwrap();

        let endpoint = this.endpoint.lock().unwrap();

        let connection = endpoint.connections.get(&this.inner.id).unwrap();
        Poll::Ready(connection.clone())
    }
}

pub struct SendStreamHandle {
    id: StreamID,
    connection: ConnectionRef,
}

pub struct RecvStreamHandle {
    id: StreamID,
    connection: ConnectionRef,
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("Closed stream")]
    ClosedStream,
    #[error("IO: {0}")]
    IO(#[from] std::io::Error),
}

#[derive(Debug, Error)]
pub enum SendError {
    #[error("Closed stream")]
    ClosedStream,
    #[error("Wrong state")]
    WrongState,
}

impl RecvStreamHandle {
    pub fn new(id: StreamID, connection: ConnectionRef) -> Self {
        Self { id, connection }
    }

    pub async fn read(&mut self, buf: &mut [u8]) -> Result<usize, ReadError> {
        let mut buf = ReadBuf::new(buf);
        poll_fn(|cx| {
            self.connection
                .lock()
                .unwrap()
                .poll_read(&mut buf, &self.id, cx)
        })
        .await
    }
}

impl SendStreamHandle {
    pub fn new(id: StreamID, connection: ConnectionRef) -> Self {
        Self { id, connection }
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize, SendError> {
        let mut inner = self.connection.lock().unwrap();
        inner.write(buf, self.id.clone())
    }
}

pub struct ConnectionIO {
    core: ConnectionCore,
    addr: SocketAddr,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
    outgoing_data: UnboundedSender<ConnectionToEndpoint>,
}

impl ConnectionIO {
    pub(crate) fn new(
        cid: ConnectionID,
        peer_cid: ConnectionID,
        addr: SocketAddr,
        side: Side,
        outgoing_data: UnboundedSender<ConnectionToEndpoint>,
        // TODO: Remove
        connected: oneshot::Sender<()>,
    ) -> Self {
        let transport_params = Arc::new(TransportParam::new());
        let core = ConnectionCore::new(cid, peer_cid, side, connected, transport_params);
        Self {
            core,
            addr,
            timer: None,
            outgoing_data,
        }
    }

    pub(crate) fn poll_accept_bidir(&mut self, cx: &mut Context<'_>) -> Poll<StreamID> {
        self.core.poll_accept_bidir(cx)
    }

    pub(crate) fn poll_accept_unidir(&mut self, cx: &mut Context<'_>) -> Poll<StreamID> {
        self.core.poll_accept_unidir(cx)
    }

    pub(crate) fn new_bidir(&mut self) -> StreamID {
        self.core.new_bidir()
    }

    pub(super) fn init(&mut self) {
        self.core.init()
    }

    pub(crate) fn poll_read(
        &mut self,
        buf: &mut ReadBuf,
        id: &StreamID,
        cx: &mut Context<'_>,
    ) -> Poll<Result<usize, ReadError>> {
        self.core.poll_read(buf, id, cx)
    }

    pub(crate) fn write(&mut self, buf: &[u8], id: StreamID) -> Result<usize, SendError> {
        self.core.write(buf, id)
    }

    pub(crate) fn handle_data(&mut self, data: Vec<EndpointToConnection>) {
        for data in data {
            self.core.handle_datagram(&data.data);
        }
    }

    pub(crate) fn check_timeout(&mut self, cx: &mut Context<'_>) {
        let timer_fired = self
            .timer
            .as_mut()
            .map(|timer| timer.as_mut().poll(cx).is_ready())
            .unwrap_or(false);

        if timer_fired {
            self.timer = None;
            self.core.handle_timeout(Instant::now());
        }
    }

    pub(crate) fn drive_transmit(&mut self) {
        let mtu = 1200;
        for p_space in PacketSpace::iter() {
            if self.core.is_discarded(p_space) {
                continue;
            }
            while let Some(packet) = self.core.get_packet(p_space, mtu) {
                let size = packet.size() as u64;

                // println!(
                //     "Sending packet({:?})[{:?}]: {packet:?}",
                //     self.core.state, self.core.side
                // );

                let mut buf = BytesMut::new();
                packet.encode(&mut buf).unwrap();
                self.outgoing_data
                    .send(ConnectionToEndpoint {
                        data: buf,
                        addr: self.addr,
                    })
                    .unwrap();

                let is_in_flight = packet.is_in_flight();
                let is_ack_eliciting = packet.is_ack_eliciting();
                let p_space = packet.packet_space();
                self.core.packet_sent(
                    p_space,
                    packet.packet_number().clone(),
                    packet.frames,
                    is_in_flight,
                    is_ack_eliciting,
                    size,
                );

                if is_in_flight {
                    self.core.set_loss_timer();
                }
            }
        }
    }

    pub(crate) fn rearm_timer(&mut self, cx: &mut Context<'_>) {
        let deadline = self.core.next_deadline();
        match (deadline, self.timer.as_mut()) {
            (Some(deadline), Some(timer)) => {
                timer.as_mut().reset(deadline);
            }
            (Some(deadline), None) => {
                let mut sleep = Box::pin(tokio::time::sleep_until(deadline));
                let _ = sleep.as_mut().poll(cx); // register waker
                self.timer = Some(sleep);
            }
            (None, _) => {
                self.timer = None;
            }
        }
    }
}

// Handles reeiving data from the endpoint and passing it to inner and seeing if pending data
// exists
struct ConnectionDriver {
    io: ConnectionRef,
    incoming_data: UnboundedReceiver<EndpointToConnection>,
    addr: SocketAddr,
    timer: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl Future for ConnectionDriver {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.io.lock().unwrap().core.register_waker(cx);

        // Handle incomming packets
        let mut buffer = Vec::new();
        loop {
            match this.incoming_data.poll_recv(cx) {
                Poll::Ready(Some(data)) => buffer.push(data),
                Poll::Ready(None) => return Poll::Ready(()), // channel closed
                Poll::Pending => break,
            }
        }

        let mut conn = this.io.lock().unwrap();

        conn.handle_data(buffer);

        // Handle timeout
        conn.check_timeout(cx);

        // Get packets from connection
        conn.drive_transmit();

        // Rearm timer
        conn.rearm_timer(cx);

        Poll::Pending
    }
}
