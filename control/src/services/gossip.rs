use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeState {
    pub node_id: String,
    pub cpu_available: f64,
    pub mem_available_mb: f64,
    pub cached_images: Vec<String>,
}

pub struct GossipNetwork {
    peers: Arc<Mutex<HashMap<String, NodeState>>>,
}

impl GossipNetwork {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn update_peer(&self, state: NodeState) {
        if let Ok(mut peers) = self.peers.lock() {
            peers.insert(state.node_id.clone(), state);
        }
    }

    pub fn get_peers(&self) -> Vec<NodeState> {
        if let Ok(peers) = self.peers.lock() {
            peers.values().cloned().collect()
        } else {
            vec![]
        }
    }
}
