use anyhow::{Context, Result};
use reqwest::Client;
use tracing::{debug, info};

use crate::types::{
    ListAgentPeersResponse, NodeHeartbeatRequest, RegisterNodeRequest, RegisterNodeResponse,
    TlsConfig,
};

pub struct ControlClient {
    base_url: String,
    client: Client,
}

impl ControlClient {
    pub fn new(base_url: String, tls: Option<&TlsConfig>) -> Result<Self> {
        let mut builder = Client::builder().timeout(std::time::Duration::from_secs(10));

        if let Some(tls) = tls {
            let ca_pem = std::fs::read(&tls.ca_cert)
                .with_context(|| format!("Failed to read CA cert: {:?}", tls.ca_cert))?;
            let ca_cert = reqwest::Certificate::from_pem(&ca_pem)
                .context("Failed to parse CA certificate")?;
            builder = builder.add_root_certificate(ca_cert);

            if let (Some(cert_path), Some(key_path)) = (&tls.client_cert, &tls.client_key) {
                let cert_pem = std::fs::read(cert_path)
                    .with_context(|| format!("Failed to read client cert: {:?}", cert_path))?;
                let key_pem = std::fs::read(key_path)
                    .with_context(|| format!("Failed to read client key: {:?}", key_path))?;
                let mut identity_pem = cert_pem;
                identity_pem.extend_from_slice(&key_pem);
                let identity = reqwest::Identity::from_pem(&identity_pem)
                    .context("Failed to parse client identity")?;
                builder = builder.identity(identity);
            }
        }

        Ok(Self {
            base_url,
            client: builder.build().context("Failed to create HTTP client")?,
        })
    }

    pub async fn register_node(
        &self,
        cluster_id: &str,
        join_token: &str,
        req: &RegisterNodeRequest,
    ) -> Result<RegisterNodeResponse> {
        let url = format!(
            "{}/api/agent/clusters/{}/nodes/register",
            self.base_url, cluster_id
        );
        info!("Registering node at {}", url);

        let resp = self
            .client
            .post(&url)
            .header("X-Quilt-Join-Token", join_token)
            .json(req)
            .send()
            .await
            .context("Failed to send registration request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Registration failed ({}): {}", status, body);
        }

        resp.json::<RegisterNodeResponse>()
            .await
            .context("Failed to parse registration response")
    }

    pub async fn heartbeat(
        &self,
        cluster_id: &str,
        node_id: &str,
        node_token: &str,
        req: &NodeHeartbeatRequest,
    ) -> Result<()> {
        let url = format!(
            "{}/api/agent/clusters/{}/nodes/{}/heartbeat",
            self.base_url, cluster_id, node_id
        );
        debug!("Sending heartbeat to {}", url);

        let resp = self
            .client
            .post(&url)
            .header("X-Quilt-Node-Token", node_token)
            .json(req)
            .send()
            .await
            .context("Failed to send heartbeat")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Heartbeat failed ({}): {}", status, body);
        }

        Ok(())
    }

    pub async fn list_peers(
        &self,
        cluster_id: &str,
        node_id: &str,
        node_token: &str,
    ) -> Result<ListAgentPeersResponse> {
        let url = format!(
            "{}/api/agent/clusters/{}/nodes/{}/peers",
            self.base_url, cluster_id, node_id
        );
        debug!("Listing peers from {}", url);

        let resp = self
            .client
            .get(&url)
            .header("X-Quilt-Node-Token", node_token)
            .send()
            .await
            .context("Failed to list peers")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("List peers failed ({}): {}", status, body);
        }

        resp.json::<ListAgentPeersResponse>()
            .await
            .context("Failed to parse peers response")
    }

    pub async fn deregister(
        &self,
        cluster_id: &str,
        node_id: &str,
        node_token: &str,
    ) -> Result<()> {
        let url = format!(
            "{}/api/agent/clusters/{}/nodes/{}/deregister",
            self.base_url, cluster_id, node_id
        );
        info!("Deregistering node at {}", url);

        let resp = self
            .client
            .post(&url)
            .header("X-Quilt-Node-Token", node_token)
            .json(&serde_json::json!({}))
            .send()
            .await
            .context("Failed to send deregister request")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Deregister failed ({}): {}", status, body);
        }

        Ok(())
    }
}
