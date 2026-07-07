use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub ca_cert: PathBuf,
    pub client_cert: Option<PathBuf>,
    pub client_key: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegisterNodeRequest {
    pub name: String,
    pub public_ip: Option<String>,
    pub private_ip: Option<String>,
    pub agent_version: Option<String>,
    pub labels: HashMap<String, String>,
    pub gpu_runtime: NodeGpuRuntimeCapabilityRequest,
    pub vmm_runtime: NodeVmmRuntimeCapabilityRequest,
    pub bridge_name: String,
    pub dns_port: i64,
    pub egress_limit_mbit: i64,
    pub gpu_devices: Vec<NodeGpuDeviceRequest>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NodeHeartbeatRequest {
    pub state: String,
    pub gpu_runtime: NodeGpuRuntimeCapabilityRequest,
    pub vmm_runtime: NodeVmmRuntimeCapabilityRequest,
    pub gpu_devices: Vec<NodeGpuDeviceRequest>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct NodeGpuRuntimeCapabilityRequest {
    pub accelerator_vendor: Option<String>,
    pub execution_mode: Option<String>,
    pub qgpu_version: Option<String>,
    pub runtime_compatible: bool,
    pub compatibility_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct NodeVmmRuntimeCapabilityRequest {
    pub kvm_available: bool,
    pub vm_runtime_version: Option<String>,
    pub supported_guest_architectures: Vec<String>,
    pub supported_firmware_profiles: Vec<String>,
    pub confidential_capabilities: Vec<String>,
    pub max_vcpu: Option<i64>,
    pub max_memory_mb: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeGpuDeviceRequest {
    pub id: String,
    pub vendor: String,
    pub card_index: i64,
    pub device_path: String,
    pub model: Option<String>,
    pub driver_version: Option<String>,
    pub major: i64,
    pub minor: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NodeResponse {
    pub id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct NodeAllocationResponse {
    pub pod_cidr: String,
    pub bridge_name: String,
    pub dns_port: i64,
    pub egress_limit_mbit: i64,
    pub allocated_at: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RegisterNodeResponse {
    pub node: NodeResponse,
    pub allocation: NodeAllocationResponse,
    pub node_token: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentPeer {
    pub node_id: String,
    pub reachable_ip: String,
    pub allocation: NodeAllocationResponse,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ListAgentPeersResponse {
    pub peers: Vec<AgentPeer>,
}

#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub node_id: String,
    pub host_ip: String,
    pub subnet: String,
}
