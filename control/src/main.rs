mod api;
mod db;
mod services;
mod tls;
mod types;

use anyhow::Result;
use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use api::AppState;
use services::orchestrator::{self, ExecutionConfig};

#[derive(Parser, Debug)]
#[command(name = "quilt-mesh-control")]
#[command(about = "Quilt Mesh control plane", long_about = None)]
struct Args {
    /// Bind address for HTTP server
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,

    /// Database file path
    #[arg(long)]
    db_path: Option<PathBuf>,

    /// Log level
    #[arg(long, default_value = "info")]
    log_level: String,

    /// TLS certificate file (PEM)
    #[arg(long)]
    tls_cert: Option<PathBuf>,

    /// TLS private key file (PEM)
    #[arg(long)]
    tls_key: Option<PathBuf>,

    /// CA certificate for client verification (enables mTLS)
    #[arg(long)]
    tls_ca: Option<PathBuf>,

    /// Elasticity control base URL
    #[arg(long, env = "CONTROL_BASE_URL")]
    control_base_url: Option<String>,

    /// Elasticity control API key (sent as X-Api-Key)
    #[arg(long, env = "CONTROL_API_KEY")]
    control_api_key: Option<String>,

    /// Enable local P2P gossip-based scheduler (replaces centralized placement)
    #[arg(long)]
    local_scheduler: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Initialize logging
    let log_level = match args.log_level.to_lowercase().as_str() {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "info" => Level::INFO,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();

    tracing::subscriber::set_global_default(subscriber)?;

    info!("Starting Quilt Mesh Control Plane");

    // Initialize database
    let db = db::init_db(args.db_path)?;

    // Create application state
    let state = Arc::new(AppState { db: db.clone() });

    let execution_config = ExecutionConfig {
        control_base_url: args.control_base_url,
        control_api_key: args.control_api_key,
        local_scheduler: args.local_scheduler,
    };

    orchestrator::start_loops(db, execution_config).await?;

    // Create router
    let app = api::create_router(state);

    // Parse bind address
    let addr: SocketAddr = args.bind.parse()?;

    // Start server (with or without TLS)
    if let (Some(cert_path), Some(key_path)) = (&args.tls_cert, &args.tls_key) {
        info!("Starting HTTPS server on {} (TLS enabled)", addr);

        let tls_config = tls::load_server_config(cert_path, key_path, args.tls_ca.as_deref())?;

        let rustls_config =
            axum_server::tls_rustls::RustlsConfig::from_config(Arc::new(tls_config));

        axum_server::bind_rustls(addr, rustls_config)
            .serve(app.into_make_service())
            .await?;
    } else {
        info!("Listening on http://{}", addr);

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;
    }

    info!("Control plane shutdown complete");

    Ok(())
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

    info!("Shutdown signal received, draining connections...");
}
