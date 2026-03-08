use bytes::BufMut;

// TODO: This is just a mock, will later use rustls

pub enum CryptoFrame {
    ClientHello,
    ServerHello,
    Finished,
}

impl CryptoFrame {
    pub fn encode<B: BufMut>(&self, buf: &mut B) {
        match self {
            CryptoFrame::ClientHello => buf.put_u8(0),
            CryptoFrame::ServerHello => buf.put_u8(1),
            CryptoFrame::Finished => buf.put_u8(2),
        }
    }
}
