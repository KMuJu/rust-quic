use std::sync::Arc;

pub struct ClientConfig {
    pub crypto: Arc<rustls::ClientConfig>,
    pub max_idle_timeout: usize,
}

pub struct ServerConfig {
    pub crypto: Arc<rustls::ServerConfig>,
    pub max_concurrent_bidir_streams: u64,
    pub max_concurrent_unidir_streams: u64,
    pub max_idle_timeout: usize,
}

impl ClientConfig {
    pub fn new(crypto: Arc<rustls::ClientConfig>) -> Self {
        Self {
            crypto,
            max_idle_timeout: 30_000,
        }
    }
}

impl ServerConfig {
    pub fn new(crypto: Arc<rustls::ServerConfig>) -> Self {
        Self {
            crypto,
            max_concurrent_bidir_streams: 1 << 32,
            max_concurrent_unidir_streams: 1 << 32,
            max_idle_timeout: 30_000,
        }
    }
}
