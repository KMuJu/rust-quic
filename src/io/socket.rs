use std::{
    io,
    net::SocketAddr,
    task::{Context, Poll},
};

use tokio::{io::ReadBuf, net::UdpSocket};

pub trait Socket: Send + 'static {
    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<SocketAddr>>;
    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> Poll<io::Result<usize>>;
}

impl Socket for UdpSocket {
    fn poll_recv_from(
        &self,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<SocketAddr>> {
        self.poll_recv_from(cx, buf)
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.poll_send_to(cx, buf, addr)
    }
}
