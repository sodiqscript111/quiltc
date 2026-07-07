use anyhow::{bail, Context, Result};
use tracing::info;

use crate::types::TlsConfig;

pub mod quilt {
    tonic::include_proto!("quilt");
}

use quilt::quilt_service_client::QuiltServiceClient;
use quilt::{
    DeleteHostRouteRequest, GetNodeNetworkingRequest, GetNodeNetworkingResponse,
    UpsertHostRouteRequest,
};

pub struct QuiltClient {
    client: QuiltServiceClient<tonic::transport::Channel>,
}

impl QuiltClient {
    pub async fn new(quilt_endpoint: String, tls: Option<&TlsConfig>) -> Result<Self> {
        info!("Connecting to Quilt daemon at {}", quilt_endpoint);

        let channel = if let Some(tls) = tls {
            let ca_pem = std::fs::read(&tls.ca_cert)
                .with_context(|| format!("Failed to read CA cert: {:?}", tls.ca_cert))?;
            let ca = tonic::transport::Certificate::from_pem(ca_pem);

            let mut tls_config = tonic::transport::ClientTlsConfig::new().ca_certificate(ca);

            if let (Some(cert_path), Some(key_path)) = (&tls.client_cert, &tls.client_key) {
                let cert_pem = std::fs::read(cert_path)
                    .with_context(|| format!("Failed to read client cert: {:?}", cert_path))?;
                let key_pem = std::fs::read(key_path)
                    .with_context(|| format!("Failed to read client key: {:?}", key_path))?;
                let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
                tls_config = tls_config.identity(identity);
            }

            tonic::transport::Channel::from_shared(quilt_endpoint)?
                .tls_config(tls_config)?
                .connect()
                .await
                .context("Failed to connect to Quilt daemon (TLS)")?
        } else {
            tonic::transport::Channel::from_shared(quilt_endpoint)?
                .connect()
                .await
                .context("Failed to connect to Quilt daemon")?
        };

        Ok(Self {
            client: QuiltServiceClient::new(channel),
        })
    }

    pub async fn get_node_networking(&mut self) -> Result<GetNodeNetworkingResponse> {
        info!("Calling GetNodeNetworking()");
        let resp = self
            .client
            .get_node_networking(tonic::Request::new(GetNodeNetworkingRequest {}))
            .await?
            .into_inner();
        if !resp.success {
            bail!("GetNodeNetworking failed: {}", resp.error);
        }
        Ok(resp)
    }

    pub async fn upsert_host_route(
        &mut self,
        destination: String,
        via_interface: String,
    ) -> Result<()> {
        info!(
            "Calling UpsertHostRoute(destination={}, via={})",
            destination, via_interface
        );
        let resp = self
            .client
            .upsert_host_route(tonic::Request::new(UpsertHostRouteRequest {
                destination,
                via_interface,
            }))
            .await?
            .into_inner();
        if !resp.success {
            bail!("UpsertHostRoute failed: {}", resp.error);
        }
        Ok(())
    }

    pub async fn delete_host_route(&mut self, destination: String) -> Result<()> {
        info!("Calling DeleteHostRoute(destination={})", destination);
        let resp = self
            .client
            .delete_host_route(tonic::Request::new(DeleteHostRouteRequest { destination }))
            .await?
            .into_inner();
        if !resp.success {
            bail!("DeleteHostRoute failed: {}", resp.error);
        }
        Ok(())
    }
}
