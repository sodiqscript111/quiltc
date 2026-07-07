mod config;
mod error;
mod http_client;
mod snapshot_types;

use anyhow::{Context, Result};
use base64::Engine;
use clap::{Args as ClapArgs, Parser, Subcommand, ValueEnum};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use reqwest::Method;
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;
use tracing::Level;
use tracing_subscriber::FmtSubscriber;
use uuid::Uuid;

use crate::config::{default_config_path, node_token_key, Config};
use crate::http_client::{header_kv, Client, TenantAuth};
use crate::snapshot_types::{CapabilityMatrix, OperationResponse, OperationStatus};

const EXIT_VALIDATION_FAILURE: i32 = 2;
const EXIT_APPLY_FAILURE: i32 = 3;
const EXIT_TRANSPORT_OR_AUTH_FAILURE: i32 = 4;
const EXIT_GENERIC_FAILURE: i32 = 1;

#[derive(Parser, Debug)]
#[command(name = "quiltc")]
#[command(about = "Quilt control-plane CLI", long_about = None)]
struct Args {
    /// Base URL for Quilt backend
    #[arg(long, env = "QUILT_BASE_URL", default_value = "https://quilt.sh")]
    base_url: String,

    /// Tenant JWT (Authorization: Bearer ...)
    #[arg(long, env = "QUILT_JWT")]
    jwt: Option<String>,

    /// Tenant API key (X-Api-Key)
    #[arg(long, env = "QUILT_API_KEY")]
    api_key: Option<String>,

    /// Load config from this path
    #[arg(long)]
    config: Option<PathBuf>,

    /// Save provided auth (jwt/api-key) into config
    #[arg(long, default_value_t = false)]
    save_auth: bool,

    /// HTTP timeout seconds
    #[arg(long, default_value_t = 60)]
    timeout_secs: u64,

    /// Retry count (429 always retried; 5xx retried for GET/DELETE)
    #[arg(long, default_value_t = 2)]
    retries: u32,

    /// Log level
    #[arg(long, default_value = "warn")]
    log_level: String,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Cluster control-plane (tenant auth required)
    Clusters {
        #[command(subcommand)]
        cmd: ClusterCmd,
    },

    /// Auth endpoints (tenant)
    Auth {
        #[command(subcommand)]
        cmd: AuthCmd,
    },

    /// Tenant API keys
    ApiKeys {
        #[command(subcommand)]
        cmd: ApiKeyCmd,
    },

    /// Volumes
    Volumes {
        #[command(subcommand)]
        cmd: VolumeCmd,
    },

    /// Agent control-plane (join token + node token)
    Agent {
        #[command(subcommand)]
        cmd: AgentCmd,
    },

    /// Container runtime endpoints (tenant auth required)
    Containers {
        #[command(subcommand)]
        cmd: ContainerCmd,
    },

    /// Snapshot lifecycle endpoints
    Snapshots {
        #[command(subcommand)]
        cmd: SnapshotCmd,
    },

    /// Long-running operation endpoints
    Operations {
        #[command(subcommand)]
        cmd: OperationCmd,
    },

    /// Stream events (SSE)
    Events {
        #[arg(long)]
        operation_id: Option<String>,
        #[arg(long)]
        snapshot_id: Option<String>,
        #[arg(long)]
        container_id: Option<String>,
    },

    /// Make an arbitrary HTTP request (escape hatch for new endpoints)
    Request {
        /// Method (GET/POST/PUT/PATCH/DELETE)
        method: String,
        /// Path, e.g. /api/clusters
        path: String,
        /// JSON request body (string)
        #[arg(long)]
        json: Option<String>,
        /// Add header (repeatable), e.g. --header 'X-Foo: bar'
        #[arg(long)]
        header: Vec<String>,

        /// Stream response body to stdout (no buffering / no JSON pretty printing)
        #[arg(long, default_value_t = false)]
        stream: bool,

        /// Write response body to file (GET only)
        #[arg(long)]
        out: Option<PathBuf>,
    },

    /// Kubernetes manifest workflows (backend-driven; no client-side translation)
    K8s {
        #[command(subcommand)]
        cmd: K8sCmd,
    },
}

#[derive(Subcommand, Debug)]
enum AuthCmd {
    Register {
        #[arg(long)]
        email: Option<String>,
        #[arg(long)]
        password: Option<String>,
        /// Raw JSON (string) or @/path/to/file.json
        #[arg(long)]
        json: Option<String>,
    },
    Login {
        #[arg(long)]
        email: Option<String>,
        #[arg(long)]
        password: Option<String>,
        /// Raw JSON (string) or @/path/to/file.json
        #[arg(long)]
        json: Option<String>,
    },
    Refresh {
        /// Raw JSON (string) or @/path/to/file.json (backend-specific; may be empty)
        #[arg(long)]
        json: Option<String>,
    },
    Logout,
    Me,
}

#[derive(Subcommand, Debug)]
enum ApiKeyCmd {
    List,
    Create {
        /// Raw JSON (string) or @/path/to/file.json
        spec: String,
    },
    Delete {
        id: String,
    },
    ContainerList {
        container_id: String,
    },
    ContainerCreate {
        container_id: String,
        /// Raw JSON (string) or @/path/to/file.json
        spec: String,
    },
}

#[derive(Subcommand, Debug)]
enum VolumeCmd {
    List,
    Create {
        /// Raw JSON (string) or @/path/to/file.json
        spec: String,
    },
    Get {
        name: String,
    },
    Delete {
        name: String,
    },
    Upload {
        name: String,
        /// Local file path to upload
        file: PathBuf,
        /// Destination path in the volume (default: /<basename of local file>)
        #[arg(long)]
        path: Option<String>,
        /// File mode (decimal). Default: 420 (0644).
        #[arg(long, default_value_t = 420)]
        mode: u32,
    },
    Download {
        name: String,
        /// Source path in the volume (default: <basename of out>)
        #[arg(long)]
        path: Option<String>,
        /// Output file path
        out: PathBuf,
    },
    ArchiveUpload {
        name: String,
        /// Local .tar.gz file to upload and extract
        archive: PathBuf,
        /// Destination directory in the volume (default: /)
        #[arg(long, default_value = "/")]
        path: String,
        /// How many leading path components to strip during extraction (default: 0)
        #[arg(long, default_value_t = 0)]
        strip_components: u32,
    },
}

#[derive(Subcommand, Debug)]
enum ClusterCmd {
    Create {
        #[arg(long)]
        name: String,
        #[arg(long)]
        pod_cidr: String,
        #[arg(long)]
        node_cidr_prefix: u8,
    },
    List,
    Get {
        cluster_id: String,
    },
    Delete {
        cluster_id: String,
    },
    Reconcile {
        cluster_id: String,
    },
    Nodes {
        cluster_id: String,
    },
    NodeGet {
        cluster_id: String,
        node_id: String,
    },
    NodeDrain {
        cluster_id: String,
        node_id: String,
    },
    NodeDelete {
        cluster_id: String,
        node_id: String,
    },
    WorkloadCreate {
        cluster_id: String,
        /// Workload spec JSON (string) or @/path/to/file.json
        spec: String,
    },
    Workloads {
        cluster_id: String,
    },
    WorkloadGet {
        cluster_id: String,
        workload_id: String,
    },
    WorkloadUpdate {
        cluster_id: String,
        workload_id: String,
        /// Workload spec JSON (string) or @/path/to/file.json
        spec: String,
    },
    WorkloadDelete {
        cluster_id: String,
        workload_id: String,
    },
    Placements {
        cluster_id: String,
    },
    JoinTokenCreate {
        cluster_id: String,
        /// Token TTL in seconds (optional). Valid range: 60..=86400.
        #[arg(long)]
        ttl_secs: Option<u64>,
        /// Maximum uses (optional). Valid range: 1..=1000.
        #[arg(long)]
        max_uses: Option<u64>,
    },
    Capabilities {
        cluster_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum AgentCmd {
    Register {
        cluster_id: String,
        /// Cluster join token (sent as X-Quilt-Join-Token). If omitted, uses env QUILT_JOIN_TOKEN.
        #[arg(long, env = "QUILT_JOIN_TOKEN")]
        join_token: Option<String>,
        #[arg(long)]
        name: String,
        #[arg(long)]
        public_ip: Option<String>,
        #[arg(long)]
        private_ip: Option<String>,
        #[arg(long)]
        agent_version: Option<String>,
        #[arg(long)]
        labels_json: Option<String>,
        #[arg(long)]
        taints_json: Option<String>,
        #[arg(long)]
        bridge_name: Option<String>,
        #[arg(long)]
        dns_port: Option<u16>,
        #[arg(long)]
        egress_limit_mbit: Option<u32>,
    },
    Heartbeat {
        cluster_id: String,
        node_id: String,
        /// State string (registered|ready|not_ready|draining|deleted)
        #[arg(long)]
        state: String,
        #[arg(long)]
        public_ip: Option<String>,
        #[arg(long)]
        private_ip: Option<String>,
        #[arg(long)]
        agent_version: Option<String>,
        #[arg(long)]
        labels_json: Option<String>,
        #[arg(long)]
        taints_json: Option<String>,
    },
    Allocation {
        cluster_id: String,
        node_id: String,
    },
    Placements {
        cluster_id: String,
        node_id: String,
    },
    Report {
        cluster_id: String,
        node_id: String,
        placement_id: String,
        #[arg(long)]
        state: String,
        #[arg(long)]
        container_id: Option<String>,
        #[arg(long)]
        message: Option<String>,
    },
    Deregister {
        cluster_id: String,
        node_id: String,
    },
}

#[derive(Subcommand, Debug)]
enum ContainerCmd {
    List,
    Create {
        /// Container create JSON (string) or @/path/to/file.json
        spec: String,
        /// Optional async mode override (default backend behavior is async=true)
        #[arg(long)]
        async_mode: Option<bool>,
    },
    BatchCreate {
        /// Batch create JSON (string) or @/path/to/file.json.
        /// Accepts either {"items":[...]} or a raw array of item objects.
        spec: String,
        /// Optional async mode override (defaults to true for batch)
        #[arg(long)]
        async_mode: Option<bool>,
    },
    Get {
        id: String,
    },
    Delete {
        id: String,
        /// Optional execution mode override (maps to ?execution=async when true)
        #[arg(long)]
        async_mode: Option<bool>,
    },
    Start {
        id: String,
        /// Optional async mode override
        #[arg(long)]
        async_mode: Option<bool>,
    },
    Stop {
        id: String,
        /// Optional execution mode override (maps to ?execution=async when true)
        #[arg(long)]
        async_mode: Option<bool>,
    },
    Kill {
        id: String,
    },
    Exec {
        id: String,
        /// Command to execute. Use `--` before the command to allow args like `-lc`.
        /// Example: `quiltc containers exec <id> -- sh -lc 'ip route'`
        #[arg(trailing_var_arg = true)]
        command: Vec<String>,
    },
    Logs {
        id: String,
    },
    Metrics {
        id: String,
    },
    NetworkGet {
        id: String,
    },
    NetworkPut {
        id: String,
        /// Network config JSON (string) or @/path/to/file.json
        spec: String,
    },
    NetworkSetup {
        id: String,
    },
    RouteAdd {
        id: String,
        #[arg(long)]
        destination: String,
    },
    RouteDel {
        id: String,
        #[arg(long)]
        destination: String,
    },
    Snapshot {
        id: String,
        #[command(flatten)]
        options: LifecycleRequestArgs,
    },
    Fork {
        id: String,
        #[command(flatten)]
        options: LifecycleRequestArgs,
    },
    Resume {
        id: String,
        #[command(flatten)]
        options: LifecycleRequestArgs,
    },
}

#[derive(Subcommand, Debug)]
enum SnapshotCmd {
    List {
        #[arg(long)]
        container_id: Option<String>,
        #[arg(long)]
        label: Vec<String>,
    },
    Get {
        id: String,
    },
    Lineage {
        id: String,
    },
    Delete {
        id: String,
        #[command(flatten)]
        options: MutationControlArgs,
    },
    Pin {
        id: String,
        #[command(flatten)]
        options: MutationControlArgs,
    },
    Unpin {
        id: String,
        #[command(flatten)]
        options: MutationControlArgs,
    },
    Clone {
        id: String,
        #[command(flatten)]
        options: LifecycleRequestArgs,
    },
}

#[derive(Subcommand, Debug)]
enum OperationCmd {
    Get {
        operation_id: String,
    },
    Watch {
        operation_id: String,
        #[arg(long, default_value_t = 300)]
        timeout_secs: u64,
    },
}

#[derive(Subcommand, Debug)]
enum K8sCmd {
    Validate {
        #[arg(short = 'f', long = "file", required = true)]
        sources: Vec<String>,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, default_value_t = false)]
        strict: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
    Apply {
        #[arg(short = 'f', long = "file", required = true)]
        sources: Vec<String>,
        #[arg(long)]
        cluster_id: String,
        #[arg(long, default_value = "default")]
        application: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, default_value_t = false)]
        no_validate: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long, default_value_t = false)]
        prune: bool,
        #[arg(long, default_value_t = false)]
        strict: bool,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = 300)]
        timeout_secs: u64,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
    Diff {
        #[arg(short = 'f', long = "file", required = true)]
        sources: Vec<String>,
        #[arg(long)]
        cluster_id: String,
        #[arg(long, default_value = "default")]
        application: String,
        #[arg(long)]
        namespace: Option<String>,
        #[arg(long, default_value_t = false)]
        strict: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
    Status {
        #[arg(long = "operation")]
        operation_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long, default_value_t = false)]
        follow: bool,
        #[arg(long, default_value_t = 300)]
        timeout_secs: u64,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
    Get {
        #[command(subcommand)]
        cmd: K8sGetCmd,
    },
    Delete {
        resource_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
    Export {
        #[arg(long)]
        cluster_id: String,
        #[arg(long, default_value = "default")]
        application: String,
        #[arg(short = 'o', long = "output", default_value = "yaml")]
        output: K8sExportFormat,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
    Schema {
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
}

#[derive(Subcommand, Debug)]
enum K8sGetCmd {
    Resources {
        #[arg(long)]
        cluster_id: String,
        #[arg(long)]
        application: Option<String>,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        include_secrets: Option<bool>,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
    Resource {
        resource_id: String,
        #[arg(long)]
        cluster_id: String,
        #[arg(long, default_value_t = false)]
        json: bool,
        #[arg(long, default_value_t = false)]
        non_interactive: bool,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum K8sExportFormat {
    Yaml,
    Json,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ConsistencyMode {
    CrashConsistent,
    AppConsistent,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum NetworkMode {
    Reset,
    PreserveNs,
    PreserveConnBestEffort,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum VolumeMode {
    Exclude,
    IncludeNamed,
    IncludeAllAllowed,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum ResumePolicy {
    Manual,
    Immediate,
}

#[derive(ClapArgs, Debug, Clone)]
struct MutationControlArgs {
    #[arg(long)]
    idempotency_key: Option<String>,
    #[arg(long, default_value_t = false)]
    wait: bool,
    #[arg(long, default_value_t = 300)]
    timeout_secs: u64,
    #[arg(long)]
    cluster_id: Option<String>,
    #[arg(long)]
    require_capability: Vec<String>,
}

#[derive(ClapArgs, Debug, Clone)]
struct LifecycleRequestArgs {
    #[arg(long, value_enum, default_value = "crash-consistent")]
    consistency_mode: ConsistencyMode,
    #[arg(long, value_enum, default_value = "reset")]
    network_mode: NetworkMode,
    #[arg(long, value_enum, default_value = "exclude")]
    volume_mode: VolumeMode,
    #[arg(long, value_enum, default_value = "manual")]
    resume_policy: ResumePolicy,
    #[arg(long)]
    placement_hint_json: Option<String>,
    #[arg(long)]
    ttl_seconds: Option<u64>,
    #[arg(long)]
    label: Vec<String>,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
    #[arg(long)]
    async_mode: Option<bool>,
    #[command(flatten)]
    mutation: MutationControlArgs,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Logging
    let lvl = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::WARN,
    };
    let subscriber = FmtSubscriber::builder().with_max_level(lvl).finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cfg_path = args.config.unwrap_or_else(default_config_path);
    let mut cfg = Config::load(&cfg_path)?;

    // Merge auth: CLI args override config.
    if cfg.base_url.is_none() {
        cfg.base_url = Some(args.base_url.clone());
    }
    if args.save_auth {
        if let Some(jwt) = &args.jwt {
            cfg.tenant_jwt = Some(jwt.clone());
        }
        if let Some(k) = &args.api_key {
            cfg.tenant_api_key = Some(k.clone());
        }
        cfg.base_url = Some(args.base_url.clone());
        cfg.save(&cfg_path)?;
    }

    let tenant_auth = if let Some(jwt) = args.jwt.or_else(|| cfg.tenant_jwt.clone()) {
        Some(TenantAuth::Jwt(jwt))
    } else if let Some(k) = args.api_key.or_else(|| cfg.tenant_api_key.clone()) {
        Some(TenantAuth::ApiKey(k))
    } else {
        None
    };

    let tenant_client = Client::new(
        &args.base_url,
        tenant_auth,
        Duration::from_secs(args.timeout_secs),
        args.retries,
    )?;

    let agent_client = Client::new(
        &args.base_url,
        None,
        Duration::from_secs(args.timeout_secs),
        args.retries,
    )?;

    match args.cmd {
        Command::Clusters { cmd } => run_clusters(&tenant_client, cmd).await,
        Command::Auth { cmd } => run_auth(&tenant_client, cmd).await,
        Command::ApiKeys { cmd } => run_api_keys(&tenant_client, cmd).await,
        Command::Volumes { cmd } => run_volumes(&tenant_client, cmd).await,
        Command::Agent { cmd } => run_agent(&agent_client, &mut cfg, &cfg_path, cmd).await,
        Command::Containers { cmd } => run_containers(&tenant_client, cmd).await,
        Command::Snapshots { cmd } => run_snapshots(&tenant_client, cmd).await,
        Command::Operations { cmd } => run_operations(&tenant_client, cmd).await,
        Command::Events {
            operation_id,
            snapshot_id,
            container_id,
        } => {
            let path = build_events_path(operation_id, snapshot_id, container_id);
            tenant_client
                .stream_to_stdout(Method::GET, &path, Default::default(), None)
                .await
        }
        Command::Request {
            method,
            path,
            json,
            header,
            stream,
            out,
        } => run_request(&tenant_client, method, path, json, header, stream, out).await,
        Command::K8s { cmd } => match run_k8s(&tenant_client, cmd).await {
            Ok(()) => Ok(()),
            Err(f) => {
                eprintln!("{}", f.message);
                std::process::exit(f.exit_code);
            }
        },
    }
}

async fn run_clusters(client: &Client, cmd: ClusterCmd) -> Result<()> {
    match cmd {
        ClusterCmd::Create {
            name,
            pod_cidr,
            node_cidr_prefix,
        } => {
            let body = serde_json::json!({
                "name": name,
                "pod_cidr": pod_cidr,
                "node_cidr_prefix": node_cidr_prefix,
            });
            client
                .send_json(
                    Method::POST,
                    "/api/clusters",
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ClusterCmd::List => {
            client
                .send_json(Method::GET, "/api/clusters", Default::default(), None)
                .await
        }
        ClusterCmd::Get { cluster_id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/clusters/{}", cluster_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::Delete { cluster_id } => {
            client
                .send_json(
                    Method::DELETE,
                    &format!("/api/clusters/{}", cluster_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::Reconcile { cluster_id } => {
            client
                .send_json(
                    Method::POST,
                    &format!("/api/clusters/{}/reconcile", cluster_id),
                    Default::default(),
                    Some(serde_json::json!({})),
                )
                .await
        }
        ClusterCmd::Nodes { cluster_id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/clusters/{}/nodes", cluster_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::NodeGet {
            cluster_id,
            node_id,
        } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/clusters/{}/nodes/{}", cluster_id, node_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::NodeDrain {
            cluster_id,
            node_id,
        } => {
            client
                .send_json(
                    Method::POST,
                    &format!("/api/clusters/{}/nodes/{}/drain", cluster_id, node_id),
                    Default::default(),
                    Some(serde_json::json!({})),
                )
                .await
        }
        ClusterCmd::NodeDelete {
            cluster_id,
            node_id,
        } => {
            client
                .send_json(
                    Method::DELETE,
                    &format!("/api/clusters/{}/nodes/{}", cluster_id, node_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::WorkloadCreate { cluster_id, spec } => {
            let body = parse_json_arg(&spec)?;
            client
                .send_json(
                    Method::POST,
                    &format!("/api/clusters/{}/workloads", cluster_id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ClusterCmd::Workloads { cluster_id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/clusters/{}/workloads", cluster_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::WorkloadGet {
            cluster_id,
            workload_id,
        } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/clusters/{}/workloads/{}", cluster_id, workload_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::WorkloadUpdate {
            cluster_id,
            workload_id,
            spec,
        } => {
            let body = parse_json_arg(&spec)?;
            client
                .send_json(
                    Method::PUT,
                    &format!("/api/clusters/{}/workloads/{}", cluster_id, workload_id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ClusterCmd::WorkloadDelete {
            cluster_id,
            workload_id,
        } => {
            client
                .send_json(
                    Method::DELETE,
                    &format!("/api/clusters/{}/workloads/{}", cluster_id, workload_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::Placements { cluster_id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/clusters/{}/placements", cluster_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ClusterCmd::JoinTokenCreate {
            cluster_id,
            ttl_secs,
            max_uses,
        } => {
            let mut map = serde_json::Map::new();
            if let Some(v) = ttl_secs {
                map.insert("ttl_secs".to_string(), serde_json::Value::from(v));
            }
            if let Some(v) = max_uses {
                map.insert("max_uses".to_string(), serde_json::Value::from(v));
            }
            let body = serde_json::Value::Object(map);
            client
                .send_json(
                    Method::POST,
                    &format!("/api/clusters/{}/join-tokens", cluster_id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ClusterCmd::Capabilities { cluster_id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/clusters/{}/capabilities", cluster_id),
                    Default::default(),
                    None,
                )
                .await
        }
    }
}

async fn run_auth(client: &Client, cmd: AuthCmd) -> Result<()> {
    match cmd {
        AuthCmd::Register {
            email,
            password,
            json,
        } => {
            let body = if let Some(j) = json {
                parse_json_arg(&j)?
            } else {
                serde_json::json!({
                    "email": email.context("Missing --email (or use --json)")?,
                    "password": password.context("Missing --password (or use --json)")?,
                })
            };
            client
                .send_json(
                    Method::POST,
                    "/api/auth/register",
                    Default::default(),
                    Some(body),
                )
                .await
        }
        AuthCmd::Login {
            email,
            password,
            json,
        } => {
            let body = if let Some(j) = json {
                parse_json_arg(&j)?
            } else {
                serde_json::json!({
                    "email": email.context("Missing --email (or use --json)")?,
                    "password": password.context("Missing --password (or use --json)")?,
                })
            };
            client
                .send_json(
                    Method::POST,
                    "/api/auth/login",
                    Default::default(),
                    Some(body),
                )
                .await
        }
        AuthCmd::Refresh { json } => {
            let body = json.map(|j| parse_json_arg(&j)).transpose()?;
            client
                .send_json(Method::POST, "/api/auth/refresh", Default::default(), body)
                .await
        }
        AuthCmd::Logout => {
            client
                .send_json(
                    Method::POST,
                    "/api/auth/logout",
                    Default::default(),
                    Some(serde_json::json!({})),
                )
                .await
        }
        AuthCmd::Me => {
            client
                .send_json(Method::GET, "/api/auth/me", Default::default(), None)
                .await
        }
    }
}

async fn run_api_keys(client: &Client, cmd: ApiKeyCmd) -> Result<()> {
    match cmd {
        ApiKeyCmd::List => {
            client
                .send_json(Method::GET, "/api/api-keys", Default::default(), None)
                .await
        }
        ApiKeyCmd::Create { spec } => {
            let body = parse_json_arg(&spec)?;
            client
                .send_json(
                    Method::POST,
                    "/api/api-keys",
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ApiKeyCmd::Delete { id } => {
            client
                .send_json(
                    Method::DELETE,
                    &format!("/api/api-keys/{}", id),
                    Default::default(),
                    None,
                )
                .await
        }
        ApiKeyCmd::ContainerList { container_id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/containers/{}/api-keys", container_id),
                    Default::default(),
                    None,
                )
                .await
        }
        ApiKeyCmd::ContainerCreate { container_id, spec } => {
            let body = parse_json_arg(&spec)?;
            client
                .send_json(
                    Method::POST,
                    &format!("/api/containers/{}/api-keys", container_id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
    }
}

async fn run_volumes(client: &Client, cmd: VolumeCmd) -> Result<()> {
    match cmd {
        VolumeCmd::List => {
            client
                .send_json(Method::GET, "/api/volumes", Default::default(), None)
                .await
        }
        VolumeCmd::Create { spec } => {
            let body = parse_json_arg(&spec)?;
            client
                .send_json(Method::POST, "/api/volumes", Default::default(), Some(body))
                .await
        }
        VolumeCmd::Get { name } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/volumes/{}", name),
                    Default::default(),
                    None,
                )
                .await
        }
        VolumeCmd::Delete { name } => {
            client
                .send_json(
                    Method::DELETE,
                    &format!("/api/volumes/{}", name),
                    Default::default(),
                    None,
                )
                .await
        }
        VolumeCmd::Upload {
            name,
            file,
            path,
            mode,
        } => {
            let bytes = fs::read(&file).with_context(|| format!("Failed to read {:?}", file))?;
            let content = base64::engine::general_purpose::STANDARD.encode(bytes);

            let filename = file.file_name().and_then(|s| s.to_str()).unwrap_or("file");
            let dest = path.unwrap_or_else(|| format!("/{}", filename));

            let body = serde_json::json!({
                "path": dest,
                "content": content,
                "mode": mode,
            });

            client
                .send_json(
                    Method::POST,
                    &format!("/api/volumes/{}/files", name),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        VolumeCmd::Download { name, path, out } => {
            let guess = out
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or("file")
                .to_string();
            let src = path.unwrap_or(guess);
            let src = src.strip_prefix('/').unwrap_or(&src);
            let src_enc = encode_path(src);

            let bytes = client
                .send_json_bytes(
                    Method::GET,
                    &format!("/api/volumes/{}/files/{}", name, src_enc),
                    Default::default(),
                    None,
                )
                .await?;
            let v: serde_json::Value =
                serde_json::from_slice(&bytes).context("Failed to parse file response JSON")?;
            let content_b64 = v
                .get("content")
                .and_then(|x| x.as_str())
                .context("file response missing content")?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(content_b64)
                .context("Failed to base64-decode file content")?;
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)
                    .with_context(|| format!("Failed to create dir {:?}", parent))?;
            }
            fs::write(&out, &decoded).with_context(|| format!("Failed to write {:?}", out))?;

            let resp = serde_json::json!({
                "success": true,
                "path": v.get("path").and_then(|x| x.as_str()).unwrap_or(src),
                "out": out.to_string_lossy(),
                "size": decoded.len(),
            });
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(())
        }
        VolumeCmd::ArchiveUpload {
            name,
            archive,
            path,
            strip_components,
        } => {
            let bytes =
                fs::read(&archive).with_context(|| format!("Failed to read {:?}", archive))?;
            let content = base64::engine::general_purpose::STANDARD.encode(bytes);
            let body = serde_json::json!({
                "content": content,
                "path": path,
                "strip_components": strip_components,
            });
            client
                .send_json(
                    Method::POST,
                    &format!("/api/volumes/{}/archive", name),
                    Default::default(),
                    Some(body),
                )
                .await
        }
    }
}

async fn run_agent(
    client: &Client,
    cfg: &mut Config,
    cfg_path: &std::path::Path,
    cmd: AgentCmd,
) -> Result<()> {
    match cmd {
        AgentCmd::Register {
            cluster_id,
            join_token,
            name,
            public_ip,
            private_ip,
            agent_version,
            labels_json,
            taints_json,
            bridge_name,
            dns_port,
            egress_limit_mbit,
        } => {
            let join_token = join_token.context(
                "Missing join token (set --join-token or QUILT_JOIN_TOKEN; mint via `quiltc clusters join-token-create <cluster_id> ...`)",
            )?;
            let body = serde_json::json!({
                "name": name,
                "public_ip": public_ip,
                "private_ip": private_ip,
                "agent_version": agent_version,
                "labels_json": labels_json,
                "taints_json": taints_json,
                "bridge_name": bridge_name,
                "dns_port": dns_port,
                "egress_limit_mbit": egress_limit_mbit,
            });

            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::HeaderName::from_static("x-quilt-join-token"),
                reqwest::header::HeaderValue::from_str(&join_token)?,
            );

            // Print response and also persist node_token.
            let url = format!("/api/agent/clusters/{}/nodes/register", cluster_id);
            let resp_bytes = client
                .send_json_bytes(Method::POST, &url, headers, Some(body))
                .await?;

            let v: serde_json::Value = serde_json::from_slice(&resp_bytes)
                .context("Failed to parse register response JSON")?;
            println!("{}", serde_json::to_string_pretty(&v)?);

            // Backend shape: either { node_id, node_token } or { node: { id }, node_token }.
            let node_id = v
                .get("node_id")
                .and_then(|x| x.as_str())
                .or_else(|| {
                    v.get("node")
                        .and_then(|n| n.get("id"))
                        .and_then(|x| x.as_str())
                })
                .context("register response missing node_id")?;
            let node_token = v
                .get("node_token")
                .and_then(|x| x.as_str())
                .context("register response missing node_token")?;

            let key = node_token_key(client.base_url().as_str(), &cluster_id, node_id);
            cfg.node_tokens.insert(key, node_token.to_string());
            cfg.save(cfg_path)?;
            Ok(())
        }
        AgentCmd::Heartbeat {
            cluster_id,
            node_id,
            state,
            public_ip,
            private_ip,
            agent_version,
            labels_json,
            taints_json,
        } => {
            let node_token =
                load_node_token(cfg, client.base_url().as_str(), &cluster_id, &node_id)?;
            let body = serde_json::json!({
                "state": state,
                "public_ip": public_ip,
                "private_ip": private_ip,
                "agent_version": agent_version,
                "labels_json": labels_json,
                "taints_json": taints_json,
            });
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::HeaderName::from_static("x-quilt-node-token"),
                reqwest::header::HeaderValue::from_str(&node_token)?,
            );
            client
                .send_json(
                    Method::POST,
                    &format!(
                        "/api/agent/clusters/{}/nodes/{}/heartbeat",
                        cluster_id, node_id
                    ),
                    headers,
                    Some(body),
                )
                .await
        }
        AgentCmd::Allocation {
            cluster_id,
            node_id,
        } => {
            let node_token =
                load_node_token(cfg, client.base_url().as_str(), &cluster_id, &node_id)?;
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::HeaderName::from_static("x-quilt-node-token"),
                reqwest::header::HeaderValue::from_str(&node_token)?,
            );
            client
                .send_json(
                    Method::GET,
                    &format!(
                        "/api/agent/clusters/{}/nodes/{}/allocation",
                        cluster_id, node_id
                    ),
                    headers,
                    None,
                )
                .await
        }
        AgentCmd::Placements {
            cluster_id,
            node_id,
        } => {
            let node_token =
                load_node_token(cfg, client.base_url().as_str(), &cluster_id, &node_id)?;
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::HeaderName::from_static("x-quilt-node-token"),
                reqwest::header::HeaderValue::from_str(&node_token)?,
            );
            client
                .send_json(
                    Method::GET,
                    &format!(
                        "/api/agent/clusters/{}/nodes/{}/placements",
                        cluster_id, node_id
                    ),
                    headers,
                    None,
                )
                .await
        }
        AgentCmd::Report {
            cluster_id,
            node_id,
            placement_id,
            state,
            container_id,
            message,
        } => {
            let node_token =
                load_node_token(cfg, client.base_url().as_str(), &cluster_id, &node_id)?;
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::HeaderName::from_static("x-quilt-node-token"),
                reqwest::header::HeaderValue::from_str(&node_token)?,
            );
            let body = serde_json::json!({
                "container_id": container_id,
                "state": state,
                "message": message,
            });
            client
                .send_json(
                    Method::POST,
                    &format!(
                        "/api/agent/clusters/{}/nodes/{}/placements/{}/report",
                        cluster_id, node_id, placement_id
                    ),
                    headers,
                    Some(body),
                )
                .await
        }
        AgentCmd::Deregister {
            cluster_id,
            node_id,
        } => {
            let node_token =
                load_node_token(cfg, client.base_url().as_str(), &cluster_id, &node_id)?;
            let mut headers = reqwest::header::HeaderMap::new();
            headers.insert(
                reqwest::header::HeaderName::from_static("x-quilt-node-token"),
                reqwest::header::HeaderValue::from_str(&node_token)?,
            );
            client
                .send_json(
                    Method::POST,
                    &format!(
                        "/api/agent/clusters/{}/nodes/{}/deregister",
                        cluster_id, node_id
                    ),
                    headers,
                    Some(serde_json::json!({})),
                )
                .await
        }
    }
}

async fn run_containers(client: &Client, cmd: ContainerCmd) -> Result<()> {
    match cmd {
        ContainerCmd::List => {
            client
                .send_json(Method::GET, "/api/containers", Default::default(), None)
                .await
        }
        ContainerCmd::Create { spec, async_mode } => {
            let mut body = parse_json_arg(&spec)?;
            maybe_set_async_mode(&mut body, async_mode, false)?;
            client
                .send_json(
                    Method::POST,
                    "/api/containers",
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ContainerCmd::BatchCreate { spec, async_mode } => {
            let parsed = parse_json_arg(&spec)?;
            let mut body = match parsed {
                serde_json::Value::Array(items) => {
                    serde_json::json!({
                        "items": items,
                    })
                }
                serde_json::Value::Object(_) => parsed,
                _ => anyhow::bail!("batch create spec must be an object or array"),
            };

            let obj = body
                .as_object_mut()
                .context("batch create request must be a JSON object")?;
            if !obj.contains_key("items") {
                anyhow::bail!("batch create request missing 'items' field");
            }
            if let Some(mode) = async_mode {
                obj.insert("async_mode".to_string(), serde_json::Value::Bool(mode));
            } else if !obj.contains_key("async_mode") {
                obj.insert("async_mode".to_string(), serde_json::Value::Bool(true));
            }

            client
                .send_json(
                    Method::POST,
                    "/api/containers/batch",
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ContainerCmd::Get { id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/containers/{}", id),
                    Default::default(),
                    None,
                )
                .await
        }
        ContainerCmd::Delete { id, async_mode } => {
            let path = if async_mode == Some(true) {
                format!("/api/containers/{}?execution=async", id)
            } else {
                format!("/api/containers/{}", id)
            };
            client
                .send_json(Method::DELETE, &path, Default::default(), None)
                .await
        }
        ContainerCmd::Start { id, async_mode } => {
            let body = async_mode.map(|m| serde_json::json!({ "async_mode": m }));
            client
                .send_json(
                    Method::POST,
                    &format!("/api/containers/{}/start", id),
                    Default::default(),
                    body,
                )
                .await
        }
        ContainerCmd::Stop { id, async_mode } => {
            let path = if async_mode == Some(true) {
                format!("/api/containers/{}/stop?execution=async", id)
            } else {
                format!("/api/containers/{}/stop", id)
            };
            client
                .send_json(Method::POST, &path, Default::default(), None)
                .await
        }
        ContainerCmd::Kill { id } => {
            client
                .send_json(
                    Method::POST,
                    &format!("/api/containers/{}/kill", id),
                    Default::default(),
                    Some(serde_json::json!({})),
                )
                .await
        }
        ContainerCmd::Exec { id, command } => {
            let body = serde_json::json!({ "command": command });
            client
                .send_json(
                    Method::POST,
                    &format!("/api/containers/{}/exec", id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ContainerCmd::Logs { id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/containers/{}/logs", id),
                    Default::default(),
                    None,
                )
                .await
        }
        ContainerCmd::Metrics { id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/containers/{}/metrics", id),
                    Default::default(),
                    None,
                )
                .await
        }
        ContainerCmd::NetworkGet { id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/containers/{}/network", id),
                    Default::default(),
                    None,
                )
                .await
        }
        ContainerCmd::NetworkPut { id, spec } => {
            let body = parse_json_arg(&spec)?;
            client
                .send_json(
                    Method::PUT,
                    &format!("/api/containers/{}/network", id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ContainerCmd::NetworkSetup { id } => {
            client
                .send_json(
                    Method::POST,
                    &format!("/api/containers/{}/network/setup", id),
                    Default::default(),
                    Some(serde_json::json!({})),
                )
                .await
        }
        ContainerCmd::RouteAdd { id, destination } => {
            let body = serde_json::json!({ "destination": destination });
            client
                .send_json(
                    Method::POST,
                    &format!("/api/containers/{}/routes", id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ContainerCmd::RouteDel { id, destination } => {
            let body = serde_json::json!({ "destination": destination });
            client
                .send_json(
                    Method::DELETE,
                    &format!("/api/containers/{}/routes", id),
                    Default::default(),
                    Some(body),
                )
                .await
        }
        ContainerCmd::Snapshot { id, options } => {
            run_lifecycle_mutation(
                client,
                Method::POST,
                &format!("/api/containers/{}/snapshot", id),
                options,
            )
            .await
        }
        ContainerCmd::Fork { id, options } => {
            run_lifecycle_mutation(
                client,
                Method::POST,
                &format!("/api/containers/{}/fork", id),
                options,
            )
            .await
        }
        ContainerCmd::Resume { id, options } => {
            run_lifecycle_mutation(
                client,
                Method::POST,
                &format!("/api/containers/{}/resume", id),
                options,
            )
            .await
        }
    }
}

async fn run_snapshots(client: &Client, cmd: SnapshotCmd) -> Result<()> {
    match cmd {
        SnapshotCmd::List {
            container_id,
            label,
        } => {
            let mut query = Vec::new();
            if let Some(v) = container_id {
                query.push(format!("container_id={}", encode_query(&v)));
            }
            for l in label {
                query.push(format!("label={}", encode_query(&l)));
            }
            let path = if query.is_empty() {
                "/api/snapshots".to_string()
            } else {
                format!("/api/snapshots?{}", query.join("&"))
            };
            client
                .send_json(Method::GET, &path, Default::default(), None)
                .await
        }
        SnapshotCmd::Get { id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/snapshots/{}", id),
                    Default::default(),
                    None,
                )
                .await
        }
        SnapshotCmd::Lineage { id } => {
            client
                .send_json(
                    Method::GET,
                    &format!("/api/snapshots/{}/lineage", id),
                    Default::default(),
                    None,
                )
                .await
        }
        SnapshotCmd::Delete { id, options } => {
            run_operation_mutation(
                client,
                Method::DELETE,
                &format!("/api/snapshots/{}", id),
                Some(serde_json::json!({})),
                options,
            )
            .await
        }
        SnapshotCmd::Pin { id, options } => {
            run_operation_mutation(
                client,
                Method::POST,
                &format!("/api/snapshots/{}/pin", id),
                Some(serde_json::json!({})),
                options,
            )
            .await
        }
        SnapshotCmd::Unpin { id, options } => {
            run_operation_mutation(
                client,
                Method::POST,
                &format!("/api/snapshots/{}/unpin", id),
                Some(serde_json::json!({})),
                options,
            )
            .await
        }
        SnapshotCmd::Clone { id, options } => {
            run_lifecycle_mutation(
                client,
                Method::POST,
                &format!("/api/snapshots/{}/clone", id),
                options,
            )
            .await
        }
    }
}

async fn run_operations(client: &Client, cmd: OperationCmd) -> Result<()> {
    match cmd {
        OperationCmd::Get { operation_id } => {
            let bytes = client
                .send_json_bytes(
                    Method::GET,
                    &format!("/api/operations/{}", operation_id),
                    Default::default(),
                    None,
                )
                .await?;
            let v: serde_json::Value = serde_json::from_slice(&bytes)
                .context("Failed to parse operation response JSON")?;
            let op = parse_operation_response(&v);
            print_json_value(&operation_to_json(&op, Some(&v))?)?;
            if let Some(status) = &op.status {
                if status.is_terminal_failure() {
                    anyhow::bail!(
                        "operation {} terminated with status={}",
                        operation_id,
                        status.as_str()
                    );
                }
            }
            Ok(())
        }
        OperationCmd::Watch {
            operation_id,
            timeout_secs,
        } => {
            let v =
                wait_for_operation_value(client, &operation_id, Duration::from_secs(timeout_secs))
                    .await?;
            let op = parse_operation_response(&v);
            print_json_value(&operation_to_json(&op, Some(&v))?)?;
            if let Some(status) = &op.status {
                if status.is_terminal_failure() {
                    anyhow::bail!(
                        "operation {} terminated with status={}",
                        operation_id,
                        status.as_str()
                    );
                }
            }
            Ok(())
        }
    }
}

async fn run_lifecycle_mutation(
    client: &Client,
    method: Method,
    path: &str,
    options: LifecycleRequestArgs,
) -> Result<()> {
    if matches!(options.network_mode, NetworkMode::PreserveConnBestEffort) && !options.dry_run {
        anyhow::bail!(
            "network_mode=preserve-conn-best-effort is risky; rerun with --dry-run first or choose a safer mode"
        );
    }

    let labels = parse_labels(&options.label)?;
    let placement_hint = options
        .placement_hint_json
        .as_deref()
        .map(parse_json_arg)
        .transpose()?;
    let idempotency_key = options
        .mutation
        .idempotency_key
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let body = serde_json::json!({
        "consistency_mode": consistency_mode_str(options.consistency_mode),
        "network_mode": network_mode_str(options.network_mode),
        "volume_mode": volume_mode_str(options.volume_mode),
        "resume_policy": resume_policy_str(options.resume_policy),
        "placement_hint": placement_hint,
        "ttl_seconds": options.ttl_seconds,
        "labels": labels,
        "idempotency_key": idempotency_key,
        "dry_run": options.dry_run,
        "async_mode": options.async_mode,
    });

    run_operation_mutation(client, method, path, Some(body), options.mutation).await
}

async fn run_operation_mutation(
    client: &Client,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
    options: MutationControlArgs,
) -> Result<()> {
    preflight_capabilities(
        client,
        options.cluster_id.as_deref(),
        &options.require_capability,
    )
    .await?;

    let idempotency_key = options
        .idempotency_key
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::HeaderName::from_static("idempotency-key"),
        reqwest::header::HeaderValue::from_str(&idempotency_key)?,
    );

    let bytes = client.send_json_bytes(method, path, headers, body).await?;
    let response_value: serde_json::Value = if bytes.is_empty() {
        serde_json::json!({})
    } else {
        serde_json::from_slice(&bytes).context("Failed to parse operation response JSON")?
    };

    let mut response: OperationResponse = parse_operation_response(&response_value);
    if response.operation_id.is_none() {
        response.operation_id = response_value
            .get("operation_id")
            .and_then(|v| v.as_str())
            .map(str::to_string);
    }

    if let Some(op_id) = response.operation_id.clone() {
        print_json_value(&serde_json::json!({
            "operation_id": op_id,
            "status": response
                .status
                .as_ref()
                .map(|s| s.as_str().to_string())
                .unwrap_or_else(|| "accepted".to_string()),
        }))?;

        if options.wait {
            let op = wait_for_operation(client, &op_id, Duration::from_secs(options.timeout_secs))
                .await?;
            let json = operation_to_json(&op, None)?;
            print_json_value(&json)?;
            if let Some(status) = &op.status {
                if status.is_terminal_failure() {
                    let hint = reason_code_hint(op.reason_code.as_deref());
                    if let Some(h) = hint {
                        anyhow::bail!(
                            "operation {} failed: status={} reason_code={:?} hint={}",
                            op_id,
                            status.as_str(),
                            op.reason_code,
                            h
                        );
                    }
                    anyhow::bail!(
                        "operation {} failed: status={} reason_code={:?}",
                        op_id,
                        status.as_str(),
                        op.reason_code
                    );
                }
            }
        } else {
            print_json_value(&response_value)?;
        }
    } else {
        // Some mutation endpoints are synchronous and return terminal payloads
        // without operation ids. Treat these as valid success responses.
        print_json_value(&response_value)?;
        if options.wait {
            print_json_value(&serde_json::json!({
                "status": "succeeded",
                "note": "endpoint returned synchronous response without operation_id"
            }))?;
        }
    }

    Ok(())
}

async fn wait_for_operation(
    client: &Client,
    operation_id: &str,
    timeout: Duration,
) -> Result<OperationResponse> {
    let v = wait_for_operation_value(client, operation_id, timeout).await?;
    Ok(parse_operation_response(&v))
}

async fn wait_for_operation_value(
    client: &Client,
    operation_id: &str,
    timeout: Duration,
) -> Result<serde_json::Value> {
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() > timeout {
            anyhow::bail!("operation {} timed out after {:?}", operation_id, timeout);
        }
        let bytes = client
            .send_json_bytes(
                Method::GET,
                &format!("/api/operations/{}", operation_id),
                Default::default(),
                None,
            )
            .await?;
        let v: serde_json::Value =
            serde_json::from_slice(&bytes).context("Failed to parse operation JSON")?;
        let op = parse_operation_response(&v);
        if let Some(status) = &op.status {
            if status.is_terminal() {
                return Ok(v);
            }
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
}

async fn preflight_capabilities(
    client: &Client,
    cluster_id: Option<&str>,
    required: &[String],
) -> Result<()> {
    if required.is_empty() {
        return Ok(());
    }
    let cluster_id =
        cluster_id.context("--require-capability needs --cluster-id to run preflight checks")?;

    let bytes = match client
        .send_json_bytes(
            Method::GET,
            &format!("/api/clusters/{}/capabilities", cluster_id),
            Default::default(),
            None,
        )
        .await
    {
        Ok(bytes) => bytes,
        Err(_) => {
            client
                .send_json_bytes(
                    Method::GET,
                    &format!("/api/capabilities?cluster_id={}", encode_query(cluster_id)),
                    Default::default(),
                    None,
                )
                .await?
        }
    };
    let raw: serde_json::Value =
        serde_json::from_slice(&bytes).context("Failed to parse capabilities JSON")?;
    let matrix = parse_capability_matrix(raw);

    let mut missing = Vec::new();
    for cap in required {
        if !matrix.get(cap).copied().unwrap_or(false) {
            missing.push(cap.clone());
        }
    }
    if !missing.is_empty() {
        anyhow::bail!(
            "UNSUPPORTED_NODE_CAPABILITY: cluster_id={} missing_capabilities={}",
            cluster_id,
            missing.join(",")
        );
    }
    Ok(())
}

fn parse_capability_matrix(v: serde_json::Value) -> std::collections::BTreeMap<String, bool> {
    let mut out = std::collections::BTreeMap::new();
    let matrix: Option<CapabilityMatrix> = serde_json::from_value(v.clone()).ok();
    if let Some(m) = matrix {
        if let Some(c) = m.capabilities {
            return c;
        }
    }
    if let Some(obj) = v.get("capabilities").and_then(|x| x.as_object()) {
        for (k, v) in obj {
            out.insert(k.clone(), v.as_bool().unwrap_or(false));
        }
        return out;
    }
    if let Some(obj) = v.as_object() {
        for (k, v) in obj {
            if let Some(b) = v.as_bool() {
                out.insert(k.clone(), b);
            }
        }
    }
    out
}

fn build_events_path(
    operation_id: Option<String>,
    snapshot_id: Option<String>,
    container_id: Option<String>,
) -> String {
    let mut parts = Vec::new();
    if let Some(v) = operation_id {
        parts.push(format!("operation_id={}", encode_query(&v)));
    }
    if let Some(v) = snapshot_id {
        parts.push(format!("snapshot_id={}", encode_query(&v)));
    }
    if let Some(v) = container_id {
        parts.push(format!("container_id={}", encode_query(&v)));
    }
    if parts.is_empty() {
        "/api/events".to_string()
    } else {
        format!("/api/events?{}", parts.join("&"))
    }
}

fn parse_labels(labels: &[String]) -> Result<std::collections::BTreeMap<String, String>> {
    let mut out = std::collections::BTreeMap::new();
    for entry in labels {
        let (k, v) = entry
            .split_once('=')
            .with_context(|| format!("invalid --label '{}', expected key=value", entry))?;
        if k.trim().is_empty() {
            anyhow::bail!("invalid --label '{}': key cannot be empty", entry);
        }
        out.insert(k.trim().to_string(), v.trim().to_string());
    }
    Ok(out)
}

fn operation_to_json(
    op: &OperationResponse,
    raw: Option<&serde_json::Value>,
) -> Result<serde_json::Value> {
    let mut v = if let Some(raw) = raw {
        raw.clone()
    } else {
        serde_json::to_value(op)?
    };
    if let Some(reason_code) = &op.reason_code {
        if let Some(hint) = reason_code_hint(Some(reason_code)) {
            if let Some(obj) = v.as_object_mut() {
                obj.insert(
                    "action_hint".to_string(),
                    serde_json::Value::String(hint.to_string()),
                );
            }
        }
    }
    Ok(v)
}

fn parse_operation_response(v: &serde_json::Value) -> OperationResponse {
    let status = v.get("status").and_then(|s| s.as_str()).map(|s| match s {
        "accepted" => OperationStatus::Accepted,
        "queued" => OperationStatus::Queued,
        "running" => OperationStatus::Running,
        "succeeded" => OperationStatus::Succeeded,
        "failed" => OperationStatus::Failed,
        "cancelled" | "canceled" => OperationStatus::Cancelled,
        "timed_out" => OperationStatus::TimedOut,
        other => OperationStatus::Unknown(other.to_string()),
    });

    OperationResponse {
        operation_id: v
            .get("operation_id")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        status,
        operation_type: v
            .get("operation_type")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        snapshot_id: v
            .get("snapshot_id")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        container_id: v
            .get("container_id")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        result_container_id: v
            .get("result_container_id")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        warnings: v.get("warnings").and_then(|x| {
            x.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
        }),
        capability_downgrades: v.get("capability_downgrades").and_then(|x| {
            x.as_array().map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
        }),
        reason_code: v
            .get("reason_code")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        reason_message: v
            .get("reason_message")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        progress_pct: v.get("progress_pct").cloned(),
        started_at: v.get("started_at").cloned(),
        updated_at: v.get("updated_at").cloned(),
        completed_at: v.get("completed_at").cloned(),
    }
}

fn reason_code_hint(reason_code: Option<&str>) -> Option<&'static str> {
    match reason_code {
        Some("UNSUPPORTED_NODE_CAPABILITY") => {
            Some("Check cluster capabilities and rerun with a compatible mode or node selector.")
        }
        Some("KERNEL_INCOMPATIBLE") => Some("Target a node pool with compatible kernel features."),
        Some("CGROUP_MODE_MISMATCH") => Some("Align cgroup mode between source and target nodes."),
        Some("SECCOMP_PROFILE_INCOMPATIBLE") => {
            Some("Use a compatible seccomp profile or relax policy for this workflow.")
        }
        Some("APPARMOR_PROFILE_INCOMPATIBLE") => {
            Some("Use a compatible apparmor profile or choose a different target.")
        }
        Some("ARCH_MISMATCH") => Some("Target nodes with matching CPU architecture."),
        Some("SNAPSHOT_NOT_FOUND") => Some("Verify snapshot ID and tenant scope."),
        Some("SNAPSHOT_PINNED") => Some("Unpin snapshot before deleting."),
        Some("SNAPSHOT_IN_USE") => Some("Stop dependent containers/workloads, then retry."),
        Some("POLICY_DENIED") => Some("Check org policy or runtime policy bindings."),
        Some("TENANT_ISOLATION_VIOLATION") => {
            Some("Use resources within the same tenant boundary.")
        }
        Some("ARTIFACT_INTEGRITY_FAILURE") => {
            Some("Recreate snapshot and validate artifact storage.")
        }
        Some("STORAGE_QUOTA_EXCEEDED") => Some("Increase quota or delete unused snapshots."),
        Some("IDEMPOTENCY_CONFLICT") => {
            Some("Reuse the original idempotency key intent or rotate key.")
        }
        Some("TIMEOUT") => Some("Retry with longer timeout and inspect operation/event logs."),
        _ => None,
    }
}

fn print_json_value(v: &serde_json::Value) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(v)?);
    Ok(())
}

fn consistency_mode_str(v: ConsistencyMode) -> &'static str {
    match v {
        ConsistencyMode::CrashConsistent => "crash-consistent",
        ConsistencyMode::AppConsistent => "app-consistent",
    }
}

fn network_mode_str(v: NetworkMode) -> &'static str {
    match v {
        NetworkMode::Reset => "reset",
        NetworkMode::PreserveNs => "preserve_ns",
        NetworkMode::PreserveConnBestEffort => "preserve_conn_best_effort",
    }
}

fn volume_mode_str(v: VolumeMode) -> &'static str {
    match v {
        VolumeMode::Exclude => "exclude",
        VolumeMode::IncludeNamed => "include_named",
        VolumeMode::IncludeAllAllowed => "include_all_allowed",
    }
}

fn resume_policy_str(v: ResumePolicy) -> &'static str {
    match v {
        ResumePolicy::Manual => "manual",
        ResumePolicy::Immediate => "immediate",
    }
}

#[derive(Debug)]
struct K8sFailure {
    exit_code: i32,
    message: String,
}

impl K8sFailure {
    fn new(exit_code: i32, message: impl Into<String>) -> Self {
        Self {
            exit_code,
            message: message.into(),
        }
    }
}

async fn run_k8s(client: &Client, cmd: K8sCmd) -> std::result::Result<(), K8sFailure> {
    match cmd {
        K8sCmd::Validate {
            sources,
            namespace,
            strict,
            json,
            non_interactive,
        } => {
            let _ = non_interactive;
            let manifests = collect_manifest_sources(&sources)
                .await
                .map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))?;
            let body = serde_json::json!({
                "manifest": k8s_manifest_string(&manifests),
                "namespace": namespace,
                "strict": strict
            });
            let response = k8s_call(client, Method::POST, "/api/k8s/validate", Some(body), None)
                .await
                .map_err(|e| map_k8s_transport_or_auth(e, EXIT_VALIDATION_FAILURE))?;
            k8s_emit_response(&response, json, true)?;
            if strict && k8s_has_warnings(&response) {
                return Err(K8sFailure::new(
                    EXIT_VALIDATION_FAILURE,
                    "validation produced warnings and --strict is enabled",
                ));
            }
            Ok(())
        }
        K8sCmd::Apply {
            sources,
            cluster_id,
            application,
            namespace,
            no_validate,
            dry_run,
            prune,
            strict,
            follow,
            timeout_secs,
            json,
            non_interactive,
        } => {
            let _ = non_interactive;
            let manifests = collect_manifest_sources(&sources)
                .await
                .map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))?;
            let manifest = k8s_manifest_string(&manifests);

            if !no_validate {
                let validate_body = serde_json::json!({
                    "manifest": manifest,
                    "namespace": namespace,
                    "strict": strict
                });
                let validate_resp = k8s_call(
                    client,
                    Method::POST,
                    "/api/k8s/validate",
                    Some(validate_body),
                    None,
                )
                .await
                .map_err(|e| map_k8s_transport_or_auth(e, EXIT_VALIDATION_FAILURE))?;
                k8s_emit_response(&validate_resp, json, true)?;
                if strict && k8s_has_warnings(&validate_resp) {
                    return Err(K8sFailure::new(
                        EXIT_VALIDATION_FAILURE,
                        "validation produced warnings and --strict is enabled",
                    ));
                }
            }

            if dry_run {
                let diff_body = serde_json::json!({
                    "manifest": manifest,
                    "cluster_id": cluster_id,
                    "application": application,
                    "namespace": namespace,
                    "strict": strict
                });
                let diff_resp =
                    k8s_call(client, Method::POST, "/api/k8s/diff", Some(diff_body), None)
                        .await
                        .map_err(|e| map_k8s_transport_or_auth(e, EXIT_VALIDATION_FAILURE))?;
                k8s_emit_response(&diff_resp, json, true)?;
                if strict && k8s_has_warnings(&diff_resp) {
                    return Err(K8sFailure::new(
                        EXIT_VALIDATION_FAILURE,
                        "diff produced warnings and --strict is enabled",
                    ));
                }
                return Ok(());
            }

            let apply_body = serde_json::json!({
                "manifest": manifest,
                "cluster_id": cluster_id,
                "application": application,
                "namespace": namespace,
                "strict": strict,
                "dry_run": false,
                "prune": prune
            });
            let apply_resp = k8s_call(
                client,
                Method::POST,
                "/api/k8s/apply",
                Some(apply_body),
                None,
            )
            .await
            .map_err(|e| map_k8s_transport_or_auth(e, EXIT_APPLY_FAILURE))?;
            k8s_emit_response(&apply_resp, json, true)?;

            if strict && k8s_has_warnings(&apply_resp) {
                return Err(K8sFailure::new(
                    EXIT_APPLY_FAILURE,
                    "apply produced warnings and --strict is enabled",
                ));
            }

            if let Some(op_id) = extract_operation_id(&apply_resp) {
                println!("operation_id: {}", op_id);
                if follow {
                    let op = k8s_wait_for_apply_operation(
                        client,
                        &op_id,
                        &cluster_id,
                        Duration::from_secs(timeout_secs),
                    )
                    .await?;
                    k8s_emit_response(&op, json, false)?;
                    if k8s_operation_failed(&op) {
                        return Err(K8sFailure::new(
                            EXIT_APPLY_FAILURE,
                            format!("operation {} terminated with failure", op_id),
                        ));
                    }
                }
            }
            Ok(())
        }
        K8sCmd::Diff {
            sources,
            cluster_id,
            application,
            namespace,
            strict,
            json,
            non_interactive,
        } => {
            let _ = non_interactive;
            let manifests = collect_manifest_sources(&sources)
                .await
                .map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))?;
            let body = serde_json::json!({
                "manifest": k8s_manifest_string(&manifests),
                "cluster_id": cluster_id,
                "application": application,
                "namespace": namespace,
                "strict": strict
            });
            let response = k8s_call(client, Method::POST, "/api/k8s/diff", Some(body), None)
                .await
                .map_err(|e| map_k8s_transport_or_auth(e, EXIT_VALIDATION_FAILURE))?;
            k8s_emit_response(&response, json, true)?;
            if strict && k8s_has_warnings(&response) {
                return Err(K8sFailure::new(
                    EXIT_VALIDATION_FAILURE,
                    "diff produced warnings and --strict is enabled",
                ));
            }
            Ok(())
        }
        K8sCmd::Status {
            operation_id,
            cluster_id,
            follow,
            timeout_secs,
            json,
            non_interactive,
        } => {
            let _ = non_interactive;
            if follow {
                let op = k8s_wait_for_apply_operation(
                    client,
                    &operation_id,
                    &cluster_id,
                    Duration::from_secs(timeout_secs),
                )
                .await?;
                k8s_emit_response(&op, json, false)?;
                if k8s_operation_failed(&op) {
                    return Err(K8sFailure::new(
                        EXIT_APPLY_FAILURE,
                        format!("operation {} terminated with failure", operation_id),
                    ));
                }
                return Ok(());
            }
            let response = k8s_call(
                client,
                Method::GET,
                &format!(
                    "/api/k8s/applies/{}?cluster_id={}",
                    operation_id,
                    encode_query(&cluster_id)
                ),
                None,
                None,
            )
            .await
            .map_err(|e| map_k8s_transport_or_auth(e, EXIT_APPLY_FAILURE))?;
            k8s_emit_response(&response, json, false)?;
            if k8s_operation_failed(&response) {
                return Err(K8sFailure::new(
                    EXIT_APPLY_FAILURE,
                    format!("operation {} terminated with failure", operation_id),
                ));
            }
            Ok(())
        }
        K8sCmd::Get { cmd } => match cmd {
            K8sGetCmd::Resources {
                cluster_id,
                application,
                kind,
                include_secrets,
                json,
                non_interactive,
            } => {
                let _ = non_interactive;
                let mut query = vec![format!("cluster_id={}", encode_query(&cluster_id))];
                if let Some(v) = application {
                    query.push(format!("application={}", encode_query(&v)));
                }
                if let Some(v) = kind {
                    query.push(format!("kind={}", encode_query(&v)));
                }
                if let Some(v) = include_secrets {
                    query.push(format!("include_secrets={}", v));
                }
                let path = format!("/api/k8s/resources?{}", query.join("&"));
                let response = k8s_call(client, Method::GET, &path, None, None)
                    .await
                    .map_err(|e| map_k8s_transport_or_auth(e, EXIT_GENERIC_FAILURE))?;
                k8s_emit_response(&response, json, false)?;
                Ok(())
            }
            K8sGetCmd::Resource {
                resource_id,
                cluster_id,
                json,
                non_interactive,
            } => {
                let _ = non_interactive;
                let path = format!(
                    "/api/k8s/resources/{}?cluster_id={}",
                    resource_id,
                    encode_query(&cluster_id)
                );
                let response = k8s_call(client, Method::GET, &path, None, None)
                    .await
                    .map_err(|e| map_k8s_transport_or_auth(e, EXIT_GENERIC_FAILURE))?;
                k8s_emit_response(&response, json, false)?;
                Ok(())
            }
        },
        K8sCmd::Delete {
            resource_id,
            cluster_id,
            json,
            non_interactive,
        } => {
            let _ = non_interactive;
            let path = format!(
                "/api/k8s/resources/{}?cluster_id={}",
                resource_id,
                encode_query(&cluster_id)
            );
            let response = k8s_call(client, Method::DELETE, &path, None, None)
                .await
                .map_err(|e| map_k8s_transport_or_auth(e, EXIT_APPLY_FAILURE))?;
            if json {
                print_json_value(&response)
                    .map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))?;
            } else if response.as_object().map(|o| o.is_empty()).unwrap_or(false) {
                println!(
                    "{{\"deleted\":true,\"resource_id\":\"{}\",\"cluster_id\":\"{}\"}}",
                    resource_id, cluster_id
                );
            } else {
                k8s_emit_response(&response, false, false)?;
            }
            Ok(())
        }
        K8sCmd::Export {
            cluster_id,
            application,
            output,
            json,
            non_interactive,
        } => {
            let _ = non_interactive;
            let response = k8s_call(
                client,
                Method::POST,
                "/api/k8s/export",
                Some(serde_json::json!({
                    "cluster_id": cluster_id,
                    "application": application,
                    "format": match output {
                        K8sExportFormat::Yaml => "yaml",
                        K8sExportFormat::Json => "json",
                    }
                })),
                None,
            )
            .await
            .map_err(|e| map_k8s_transport_or_auth(e, EXIT_GENERIC_FAILURE))?;
            if json {
                print_json_value(&response)
                    .map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))?;
            } else {
                match output {
                    K8sExportFormat::Yaml => {
                        if let Some(yaml) = response.get("output").and_then(|v| v.as_str()) {
                            println!("{}", yaml);
                        } else {
                            print_json_value(&response).map_err(|e| {
                                K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string())
                            })?;
                        }
                    }
                    K8sExportFormat::Json => {
                        print_json_value(&response)
                            .map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))?;
                    }
                }
            }
            Ok(())
        }
        K8sCmd::Schema {
            json,
            non_interactive,
        } => {
            let _ = non_interactive;
            let response = k8s_call(client, Method::GET, "/api/k8s/schema", None, None)
                .await
                .map_err(|e| map_k8s_transport_or_auth(e, EXIT_GENERIC_FAILURE))?;
            k8s_emit_response(&response, json, false)?;
            Ok(())
        }
    }
}

async fn k8s_call(
    client: &Client,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
    headers: Option<reqwest::header::HeaderMap>,
) -> Result<serde_json::Value> {
    let bytes = client
        .send_json_bytes(method, path, headers.unwrap_or_default(), body)
        .await?;
    if bytes.is_empty() {
        return Ok(serde_json::json!({}));
    }
    serde_json::from_slice(&bytes).context("Failed to parse backend JSON response")
}

fn k8s_emit_response(
    v: &serde_json::Value,
    json_mode: bool,
    render_summary: bool,
) -> std::result::Result<(), K8sFailure> {
    if json_mode {
        print_json_value(v).map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))?;
        return Ok(());
    }

    if render_summary {
        print_k8s_summary(v);
    }
    print_k8s_messages(v);
    print_json_value(v).map_err(|e| K8sFailure::new(EXIT_GENERIC_FAILURE, e.to_string()))
}

fn print_k8s_summary(v: &serde_json::Value) {
    let mut counts: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    if let Some(resources) = v.get("resources").and_then(|x| x.as_array()) {
        for r in resources {
            let kind = r
                .get("kind")
                .and_then(|x| x.as_str())
                .unwrap_or("Unknown")
                .to_string();
            let action = r
                .get("action")
                .or_else(|| r.get("result"))
                .and_then(|x| x.as_str())
                .unwrap_or("unknown")
                .to_string();
            *counts.entry(kind).or_default().entry(action).or_default() += 1;
        }
    }
    if let Some(summary) = v.get("summary").and_then(|x| x.as_object()) {
        for (action, by_kind) in summary {
            if let Some(kind_map) = by_kind.as_object() {
                for (kind, n) in kind_map {
                    if let Some(c) = n.as_u64() {
                        *counts
                            .entry(kind.to_string())
                            .or_default()
                            .entry(action.to_string())
                            .or_default() += c;
                    }
                }
            }
        }
    }
    if counts.is_empty() {
        return;
    }
    println!("summary:");
    for (kind, actions) in counts {
        let mut parts = Vec::new();
        for (action, count) in actions {
            parts.push(format!("{}={}", action, count));
        }
        println!("  {}: {}", kind, parts.join(", "));
    }
}

fn print_k8s_messages(v: &serde_json::Value) {
    if let Some(warnings) = v.get("warnings").and_then(|x| x.as_array()) {
        for w in warnings {
            print_backend_message("warning", w);
        }
    }
    if let Some(errors) = v.get("errors").and_then(|x| x.as_array()) {
        for e in errors {
            print_backend_message("error", e);
        }
    }
}

fn print_backend_message(level: &str, value: &serde_json::Value) {
    if let Some(obj) = value.as_object() {
        let code = obj.get("code").and_then(|x| x.as_str()).unwrap_or("");
        let field_path = obj.get("field_path").and_then(|x| x.as_str()).unwrap_or("");
        let message = obj
            .get("message")
            .and_then(|x| x.as_str())
            .unwrap_or_else(|| value.as_str().unwrap_or(""));
        let hint = obj.get("hint").and_then(|x| x.as_str()).unwrap_or("");
        let docs_url = obj.get("docs_url").and_then(|x| x.as_str()).unwrap_or("");
        println!(
            "{}: code={} field_path={} message={} hint={} docs_url={}",
            level, code, field_path, message, hint, docs_url
        );
    } else if let Some(s) = value.as_str() {
        println!("{}: {}", level, s);
    } else {
        println!("{}: {}", level, value);
    }
}

fn k8s_has_warnings(v: &serde_json::Value) -> bool {
    v.get("warnings")
        .and_then(|x| x.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false)
}

fn k8s_operation_failed(v: &serde_json::Value) -> bool {
    let status = v
        .get("status")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_lowercase();
    matches!(
        status.as_str(),
        "failed" | "error" | "cancelled" | "canceled"
    )
}

fn extract_operation_id(v: &serde_json::Value) -> Option<String> {
    v.get("operation_id")
        .and_then(|x| x.as_str())
        .map(str::to_string)
}

async fn k8s_wait_for_apply_operation(
    client: &Client,
    operation_id: &str,
    cluster_id: &str,
    timeout: Duration,
) -> std::result::Result<serde_json::Value, K8sFailure> {
    let started = std::time::Instant::now();
    loop {
        if started.elapsed() > timeout {
            return Err(K8sFailure::new(
                EXIT_APPLY_FAILURE,
                format!("operation {} timed out after {:?}", operation_id, timeout),
            ));
        }
        let value = k8s_call(
            client,
            Method::GET,
            &format!(
                "/api/k8s/applies/{}?cluster_id={}",
                operation_id,
                encode_query(cluster_id)
            ),
            None,
            None,
        )
        .await
        .map_err(|e| map_k8s_transport_or_auth(e, EXIT_APPLY_FAILURE))?;
        let status = value
            .get("status")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_lowercase();
        if matches!(
            status.as_str(),
            "succeeded" | "success" | "failed" | "error" | "cancelled" | "canceled"
        ) {
            return Ok(value);
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

fn map_k8s_transport_or_auth(err: anyhow::Error, fallback: i32) -> K8sFailure {
    let msg = err.to_string();
    if is_transport_or_auth_error(&msg) {
        return K8sFailure::new(EXIT_TRANSPORT_OR_AUTH_FAILURE, msg);
    }
    K8sFailure::new(fallback, msg)
}

fn is_transport_or_auth_error(msg: &str) -> bool {
    msg.contains("status=401")
        || msg.contains("status=403")
        || msg.contains("status=407")
        || msg.contains("Request failed")
        || msg.contains("connection")
        || msg.contains("timed out")
        || msg.contains("dns")
}

#[derive(Debug, Clone)]
struct ManifestSource {
    content: String,
}

fn k8s_manifest_string(manifests: &[ManifestSource]) -> String {
    manifests
        .iter()
        .map(|m| m.content.as_str())
        .collect::<Vec<_>>()
        .join("\n---\n")
}

async fn collect_manifest_sources(inputs: &[String]) -> Result<Vec<ManifestSource>> {
    let mut out = Vec::new();
    for src in inputs {
        if src.starts_with("http://") || src.starts_with("https://") {
            let content = reqwest::get(src)
                .await
                .with_context(|| format!("Failed to fetch URL source {}", src))?
                .error_for_status()
                .with_context(|| format!("HTTP error fetching {}", src))?
                .text()
                .await
                .with_context(|| format!("Failed to read URL response {}", src))?;
            out.push(ManifestSource { content });
            continue;
        }

        let path = PathBuf::from(src);
        let meta = fs::metadata(&path).with_context(|| format!("Source not found: {}", src))?;
        if meta.is_file() {
            let content = read_manifest_file(&path)?;
            out.push(ManifestSource { content });
            continue;
        }
        if meta.is_dir() {
            let files = collect_manifest_files_in_dir(&path)?;
            for f in files {
                let content = read_manifest_file(&f)?;
                out.push(ManifestSource { content });
            }
            continue;
        }
        anyhow::bail!("Unsupported manifest source: {}", src);
    }

    if out.is_empty() {
        anyhow::bail!("No manifest documents collected from inputs");
    }
    Ok(out)
}

fn collect_manifest_files_in_dir(dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut stack = vec![dir.to_path_buf()];
    let mut files = Vec::new();
    while let Some(current) = stack.pop() {
        let mut entries = fs::read_dir(&current)
            .with_context(|| format!("Failed to read directory {:?}", current))?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            let meta = fs::metadata(&path)
                .with_context(|| format!("Failed to read metadata for {:?}", path))?;
            if meta.is_dir() {
                stack.push(path);
                continue;
            }
            if meta.is_file() && is_manifest_extension(&path) {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn is_manifest_extension(path: &std::path::Path) -> bool {
    matches!(
        path.extension().and_then(|x| x.to_str()).unwrap_or(""),
        "yaml" | "yml" | "json"
    )
}

fn read_manifest_file(path: &std::path::Path) -> Result<String> {
    let mut f = fs::File::open(path).with_context(|| format!("Failed to open {:?}", path))?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)
        .with_context(|| format!("Failed to read UTF-8 text from {:?}", path))?;
    Ok(buf)
}

async fn run_request(
    client: &Client,
    method: String,
    path: String,
    json: Option<String>,
    header: Vec<String>,
    stream: bool,
    out: Option<PathBuf>,
) -> Result<()> {
    let method = Method::from_bytes(method.to_uppercase().as_bytes()).context("Invalid method")?;
    let body = json.as_deref().map(parse_json_arg).transpose()?;

    let mut headers = reqwest::header::HeaderMap::new();
    for h in header {
        let (k, v) = h
            .split_once(':')
            .context("Header must be in 'Name: value' format")?;
        let (name, value) = header_kv(k.trim(), v.trim())?;
        headers.insert(name, value);
    }

    if let Some(out_path) = out {
        if method != Method::GET {
            anyhow::bail!("--out is only supported with GET");
        }
        return client.download_to_file(&path, headers, &out_path).await;
    }

    if stream {
        client.stream_to_stdout(method, &path, headers, body).await
    } else {
        client.send_json(method, &path, headers, body).await
    }
}

fn parse_json_arg(s: &str) -> Result<serde_json::Value> {
    if let Some(path) = s.strip_prefix('@') {
        let bytes = std::fs::read(path).with_context(|| format!("Failed to read {}", path))?;
        let v =
            serde_json::from_slice(&bytes).with_context(|| format!("Invalid JSON in {}", path))?;
        return Ok(v);
    }
    let v = serde_json::from_str(s).context("Invalid JSON")?;
    Ok(v)
}

fn maybe_set_async_mode(
    body: &mut serde_json::Value,
    async_mode: Option<bool>,
    default_true: bool,
) -> Result<()> {
    let obj = body
        .as_object_mut()
        .context("request spec must be a JSON object")?;
    match async_mode {
        Some(v) => {
            obj.insert("async_mode".to_string(), serde_json::Value::Bool(v));
        }
        None if default_true && !obj.contains_key("async_mode") => {
            obj.insert("async_mode".to_string(), serde_json::Value::Bool(true));
        }
        None => {}
    }
    Ok(())
}

fn encode_path(path: &str) -> String {
    // Conservative encoding: encode each path segment to keep slashes as delimiters.
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|seg| percent_encode(seg.as_bytes(), NON_ALPHANUMERIC).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn encode_query(value: &str) -> String {
    percent_encode(value.as_bytes(), NON_ALPHANUMERIC).to_string()
}

fn load_node_token(
    cfg: &Config,
    base_url: &str,
    cluster_id: &str,
    node_id: &str,
) -> Result<String> {
    let key = node_token_key(base_url, cluster_id, node_id);
    cfg.node_tokens
        .get(&key)
        .cloned()
        .with_context(|| format!("No node_token found for cluster_id={} node_id={} (run `quiltc agent register ...` first)", cluster_id, node_id))
}
