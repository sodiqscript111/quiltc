mod control_client;
mod overlay;
mod quilt_client;
mod types;

use anyhow::{Context, Result};
use clap::Parser;
use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use control_client::ControlClient;
use overlay::VxlanManager;
use quilt_client::QuiltClient;
use types::{
    NodeGpuRuntimeCapabilityRequest, NodeHeartbeatRequest, NodeVmmRuntimeCapabilityRequest,
    PeerInfo, RegisterNodeRequest, TlsConfig,
};

#[derive(Parser, Debug)]
#[command(name = "quilt-mesh-agent")]
#[command(about = "Quilt mesh node agent", long_about = None)]
struct Args {
    /// Quilt backend base URL
    #[arg(long, default_value = "http://127.0.0.1:8080")]
    backend_url: String,

    /// Cluster ID this node belongs to
    #[arg(long)]
    cluster_id: String,

    /// Cluster join token for initial registration
    #[arg(long, env = "QUILT_JOIN_TOKEN")]
    join_token: String,

    /// This node's host IP address used for mesh transport
    #[arg(long)]
    host_ip: String,

    /// Node name (defaults to system hostname)
    #[arg(long)]
    node_name: Option<String>,

    /// Optional public IP override
    #[arg(long)]
    public_ip: Option<String>,

    /// Optional private IP override
    #[arg(long)]
    private_ip: Option<String>,

    /// Local Quilt daemon gRPC endpoint
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    quilt_daemon: String,

    /// Agent version reported to the control plane
    #[arg(long, default_value = "quiltc-mesh-agent")]
    agent_version: String,

    /// Optional scheduler-visible labels as JSON object
    #[arg(long)]
    labels_json: Option<String>,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// CA certificate for verifying backend/daemon servers (PEM)
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Client certificate for mTLS (PEM)
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// Client private key for mTLS (PEM)
    #[arg(long)]
    tls_key: Option<PathBuf>,
}

struct AgentState {
    cluster_id: String,
    node_id: String,
    node_token: String,
    control_client: ControlClient,
    vxlan_manager: Arc<RwLock<VxlanManager>>,
    quilt_client: Arc<RwLock<QuiltClient>>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_logging(&args.log_level)?;

    info!("Starting Quilt mesh agent");

    #[cfg(feature = "dev-stubs")]
    warn!("Running in dev-stub mode - VXLAN operations are no-ops. NOT for production.");

    let host_ip: Ipv4Addr = args.host_ip.parse().context("Invalid host IP address")?;
    let node_name = if let Some(name) = args.node_name {
        name
    } else {
        hostname::get()
            .context("Failed to get system hostname")?
            .to_string_lossy()
            .to_string()
    };
    let labels = parse_labels(args.labels_json.as_deref())?;

    let tls_config = args.tls_ca.map(|ca| TlsConfig {
        ca_cert: ca,
        client_cert: args.tls_cert,
        client_key: args.tls_key,
    });

    let control_client = ControlClient::new(args.backend_url.clone(), tls_config.as_ref())
        .context("Failed to create backend client")?;

    let daemon_endpoint = if tls_config.is_some() && !args.quilt_daemon.starts_with("https://") {
        args.quilt_daemon.replace("http://", "https://")
    } else {
        args.quilt_daemon
    };
    let mut quilt_client = QuiltClient::new(daemon_endpoint, tls_config.as_ref())
        .await
        .context("Failed to create Quilt daemon client")?;

    let local_network = quilt_client
        .get_node_networking()
        .await
        .context("Failed to read local Quilt daemon networking")?;

    let register_req = RegisterNodeRequest {
        name: node_name,
        public_ip: args.public_ip.or_else(|| Some(host_ip.to_string())),
        private_ip: args.private_ip.or_else(|| Some(host_ip.to_string())),
        agent_version: Some(args.agent_version),
        labels,
        gpu_runtime: NodeGpuRuntimeCapabilityRequest::default(),
        vmm_runtime: NodeVmmRuntimeCapabilityRequest::default(),
        bridge_name: local_network.bridge_name.clone(),
        dns_port: i64::from(local_network.dns_port),
        egress_limit_mbit: 0,
        gpu_devices: Vec::new(),
    };

    info!("Registering node with backend at {}", args.backend_url);
    let registration = control_client
        .register_node(&args.cluster_id, &args.join_token, &register_req)
        .await
        .context("Failed to register node with backend")?;

    if registration.allocation.pod_cidr != local_network.pod_cidr {
        let _ = control_client
            .deregister(
                &args.cluster_id,
                &registration.node.id,
                &registration.node_token,
            )
            .await;
        anyhow::bail!(
            "local Quilt daemon PodCIDR {} does not match backend-assigned PodCIDR {}; start the node-local Quilt daemon with the assigned PodCIDR before joining the cluster",
            local_network.pod_cidr,
            registration.allocation.pod_cidr
        );
    }

    let vxlan_manager = VxlanManager::new(host_ip)
        .await
        .context("Failed to create VXLAN manager")?;
    vxlan_manager
        .setup_vxlan()
        .await
        .context("Failed to set up VXLAN interface")?;

    let state = Arc::new(AgentState {
        cluster_id: args.cluster_id,
        node_id: registration.node.id,
        node_token: registration.node_token,
        control_client,
        vxlan_manager: Arc::new(RwLock::new(vxlan_manager)),
        quilt_client: Arc::new(RwLock::new(quilt_client)),
    });

    send_ready_heartbeat(&state).await?;

    let cancel = CancellationToken::new();
    let heartbeat_state = state.clone();
    let heartbeat_cancel = cancel.clone();
    let heartbeat_handle =
        tokio::spawn(async move { heartbeat_loop(heartbeat_state, heartbeat_cancel).await });

    let peer_sync_state = state.clone();
    let peer_sync_cancel = cancel.clone();
    let peer_sync_handle =
        tokio::spawn(async move { peer_sync_loop(peer_sync_state, peer_sync_cancel).await });

    info!("Agent initialized successfully");
    shutdown_signal().await;
    cancel.cancel();

    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = heartbeat_handle.await;
        let _ = peer_sync_handle.await;
    })
    .await;

    if let Err(e) = graceful_shutdown(&state).await {
        error!("Error during shutdown cleanup: {}", e);
    }

    info!("Agent shutdown complete");
    Ok(())
}

fn init_logging(level: &str) -> Result<()> {
    let log_level = match level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

fn parse_labels(raw: Option<&str>) -> Result<HashMap<String, String>> {
    match raw {
        Some(raw) if !raw.trim().is_empty() => {
            serde_json::from_str(raw).with_context(|| format!("Invalid labels JSON: {}", raw))
        }
        _ => Ok(HashMap::new()),
    }
}

async fn send_ready_heartbeat(state: &AgentState) -> Result<()> {
    state
        .control_client
        .heartbeat(
            &state.cluster_id,
            &state.node_id,
            &state.node_token,
            &NodeHeartbeatRequest {
                state: "ready".to_string(),
                gpu_runtime: NodeGpuRuntimeCapabilityRequest::default(),
                vmm_runtime: NodeVmmRuntimeCapabilityRequest::default(),
                gpu_devices: Vec::new(),
            },
        )
        .await
        .context("Failed to mark node ready")
}

async fn graceful_shutdown(state: &AgentState) -> Result<()> {
    info!("Deregistering from backend...");
    if let Err(e) = state
        .control_client
        .deregister(&state.cluster_id, &state.node_id, &state.node_token)
        .await
    {
        warn!("Failed to deregister: {}", e);
    }

    let peer_subnets: Vec<String> = {
        let vxlan = state.vxlan_manager.read().await;
        vxlan.peers().keys().cloned().collect()
    };

    {
        let mut quilt = state.quilt_client.write().await;
        for subnet in &peer_subnets {
            if let Err(e) = quilt.delete_host_route(subnet.clone()).await {
                warn!("Failed to remove route for {}: {}", subnet, e);
            }
        }
    }

    {
        let mut vxlan = state.vxlan_manager.write().await;
        for subnet in &peer_subnets {
            if let Err(e) = vxlan.remove_peer(subnet).await {
                warn!("Failed to remove VXLAN peer {}: {}", subnet, e);
            }
        }
    }

    Ok(())
}

async fn heartbeat_loop(state: Arc<AgentState>, cancel: CancellationToken) -> Result<()> {
    info!("Starting heartbeat loop (every 10s)");
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            _ = cancel.cancelled() => return Ok(()),
        }

        if let Err(e) = send_ready_heartbeat(&state).await {
            warn!("Failed to send heartbeat: {}", e);
        }
    }
}

async fn peer_sync_loop(state: Arc<AgentState>, cancel: CancellationToken) -> Result<()> {
    info!("Starting peer sync loop (every 5s)");
    let mut known_peers: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {},
            _ = cancel.cancelled() => return Ok(()),
        }

        let peers_response = match state
            .control_client
            .list_peers(&state.cluster_id, &state.node_id, &state.node_token)
            .await
        {
            Ok(resp) => resp,
            Err(e) => {
                warn!("Failed to list peers: {}", e);
                continue;
            }
        };

        let current_peers: Vec<PeerInfo> = peers_response
            .peers
            .into_iter()
            .map(|peer| PeerInfo {
                node_id: peer.node_id,
                host_ip: peer.reachable_ip,
                subnet: peer.allocation.pod_cidr,
            })
            .collect();
        let current_subnets: HashSet<String> = current_peers
            .iter()
            .map(|peer| peer.subnet.clone())
            .collect();

        for peer in &current_peers {
            if known_peers.contains(&peer.subnet) {
                continue;
            }

            info!(
                "New peer discovered: node_id={}, subnet={}, host_ip={}",
                peer.node_id, peer.subnet, peer.host_ip
            );
            let Ok(peer_ip) = peer.host_ip.parse::<Ipv4Addr>() else {
                warn!("Invalid peer IP address: {}", peer.host_ip);
                continue;
            };

            {
                let mut vxlan = state.vxlan_manager.write().await;
                if let Err(e) = vxlan.add_peer(peer.subnet.clone(), peer_ip).await {
                    error!("Failed to add peer to VXLAN: {}", e);
                }
            }

            {
                let mut quilt = state.quilt_client.write().await;
                if let Err(e) = quilt
                    .upsert_host_route(peer.subnet.clone(), "vxlan100".to_string())
                    .await
                {
                    error!("Failed to add host route: {}", e);
                }
            }

            known_peers.insert(peer.subnet.clone());
        }

        let removed_subnets: Vec<String> =
            known_peers.difference(&current_subnets).cloned().collect();
        for subnet in removed_subnets {
            info!("Peer removed: subnet={}", subnet);

            {
                let mut vxlan = state.vxlan_manager.write().await;
                if let Err(e) = vxlan.remove_peer(&subnet).await {
                    error!("Failed to remove VXLAN peer: {}", e);
                }
            }

            {
                let mut quilt = state.quilt_client.write().await;
                if let Err(e) = quilt.delete_host_route(subnet.clone()).await {
                    error!("Failed to remove host route: {}", e);
                }
            }

            known_peers.remove(&subnet);
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
