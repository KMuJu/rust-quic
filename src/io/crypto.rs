use bytes::{Buf, BufMut};

// TODO: This is just a mock, will later use rustls

#[derive(Debug)]
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

    pub fn decode<B: Buf>(buf: &mut B) -> CryptoFrame {
        let t = buf.get_u8();
        match t {
            0 => CryptoFrame::ClientHello,
            1 => CryptoFrame::ServerHello,
            2 => CryptoFrame::Finished,
            _ => panic!("Got invalid cryptoframe"),
        }
    }
}
