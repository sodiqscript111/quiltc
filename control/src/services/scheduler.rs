use rand::seq::SliceRandom;
use rand::thread_rng;
use serde_json::Value;

#[derive(Debug, Clone)]
pub struct NodeScore {
    pub node_id: String,
    pub score: f64,
}

pub fn score_node(
    node_id: &str,
    cpu_available: f64,
    mem_available_mb: f64,
    has_cached_image: bool,
) -> NodeScore {
    let mut score = cpu_available * 10.0 + (mem_available_mb / 1024.0) * 5.0;

    if has_cached_image {
        score += 100.0;
    }

    NodeScore {
        node_id: node_id.to_string(),
        score,
    }
}

pub fn jitter_top_nodes(mut nodes: Vec<NodeScore>, top_k: usize) -> Option<String> {
    if nodes.is_empty() {
        return None;
    }

    nodes.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    let top_nodes: Vec<NodeScore> = nodes.into_iter().take(top_k).collect();

    let mut rng = thread_rng();
    top_nodes.choose(&mut rng).map(|n| n.node_id.clone())
}

pub fn local_schedule_workload(_tenant_id: &str, _workload_id: &str) -> Option<String> {
    let simulated_nodes = vec![
        score_node("node-1", 4.0, 4096.0, false),
        score_node("node-2", 2.0, 2048.0, true),
        score_node("node-3", 8.0, 8192.0, false),
    ];

    jitter_top_nodes(simulated_nodes, 2)
}
