use std::{collections::HashMap, sync::Arc};

use pingora_core::{
    connectors::{BackendConnectionStats, BackendStatsView},
    protocols::l4::socket::SocketAddr,
    upstreams::peer::Peer,
};
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct Statistics {
    connections: Arc<RwLock<HashMap<SocketAddr, BackendStatsView>>>,
}

impl Statistics {
    pub fn new(peers: Vec<SocketAddr>) -> Self {
        let peers = peers
            .into_iter()
            .map(|b| {
                (
                    b,
                    BackendStatsView::new(Arc::new(BackendConnectionStats::new())),
                )
            })
            .collect();
        Self {
            connections: Arc::new(RwLock::new(peers)),
        }
    }
}
