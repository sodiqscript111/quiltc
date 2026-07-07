#[cfg(all(not(target_os = "linux"), not(feature = "dev-stubs")))]
compile_error!(
    "quilt-mesh-agent requires Linux for VXLAN support. \
     Use `cargo build --features dev-stubs` for macOS development."
);

use anyhow::Result;

#[cfg(target_os = "linux")]
use anyhow::Context;
#[cfg(target_os = "linux")]
use futures::TryStreamExt;
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::net::IpAddr;
use std::net::Ipv4Addr;
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
use netlink_packet_route::{
    neighbour::{NeighbourAddress, NeighbourAttribute, NeighbourMessage},
    AddressFamily,
};
#[cfg(target_os = "linux")]
use rtnetlink::{new_connection, Handle};

const VXLAN_INTERFACE: &str = "vxlan100";
const VXLAN_VNI: u32 = 100;
const BRIDGE_INTERFACE: &str = "quilt0";

#[cfg(target_os = "linux")]
pub struct VxlanManager {
    handle: Handle,
    peers: HashMap<String, Ipv4Addr>, // subnet -> peer_host_ip
}

#[cfg(all(not(target_os = "linux"), feature = "dev-stubs"))]
pub struct VxlanManager {
    peers: HashMap<String, Ipv4Addr>,
}

#[cfg(target_os = "linux")]
impl VxlanManager {
    /// Create a new VXLAN manager
    pub async fn new(_local_ip: Ipv4Addr) -> Result<Self> {
        let (connection, handle, _) =
            new_connection().context("Failed to create netlink connection")?;

        // Spawn netlink connection in background
        tokio::spawn(connection);

        Ok(Self {
            handle,
            peers: HashMap::new(),
        })
    }

    /// Set up VXLAN interface (create if doesn't exist)
    pub async fn setup_vxlan(&self) -> Result<()> {
        info!("Setting up VXLAN interface: {}", VXLAN_INTERFACE);

        // Check if interface already exists
        if self.interface_exists(VXLAN_INTERFACE).await? {
            info!("VXLAN interface {} already exists", VXLAN_INTERFACE);
            return Ok(());
        }

        // Create VXLAN interface
        self.create_vxlan_interface().await?;

        // Bring interface up
        self.set_link_up(VXLAN_INTERFACE).await?;

        // Attach to bridge (if bridge exists)
        if self.interface_exists(BRIDGE_INTERFACE).await? {
            self.attach_to_bridge().await?;
        } else {
            warn!(
                "Bridge {} not found - VXLAN interface created but not bridged",
                BRIDGE_INTERFACE
            );
        }

        info!("VXLAN interface {} created successfully", VXLAN_INTERFACE);
        Ok(())
    }

    /// Add a peer to the VXLAN FDB
    pub async fn add_peer(&mut self, subnet: String, peer_host_ip: Ipv4Addr) -> Result<()> {
        info!(
            "Adding VXLAN peer: subnet={}, host_ip={}",
            subnet, peer_host_ip
        );

        // Add default FDB entry (all MACs go to this peer for this subnet)
        self.add_fdb_entry(peer_host_ip).await?;

        self.peers.insert(subnet, peer_host_ip);
        Ok(())
    }

    /// Remove a peer from the VXLAN FDB
    pub async fn remove_peer(&mut self, subnet: &str) -> Result<()> {
        if let Some(peer_host_ip) = self.peers.remove(subnet) {
            info!(
                "Removing VXLAN peer: subnet={}, host_ip={}",
                subnet, peer_host_ip
            );
            self.remove_fdb_entry(peer_host_ip).await?;
        }
        Ok(())
    }

    /// Check if a network interface exists
    async fn interface_exists(&self, name: &str) -> Result<bool> {
        let mut links = self
            .handle
            .link()
            .get()
            .match_name(name.to_string())
            .execute();

        Ok(links.try_next().await?.is_some())
    }

    /// Create VXLAN interface using rtnetlink
    async fn create_vxlan_interface(&self) -> Result<()> {
        debug!("Creating VXLAN interface with VNI={}", VXLAN_VNI);

        // Create VXLAN link
        self.handle
            .link()
            .add()
            .vxlan(VXLAN_INTERFACE.to_string(), VXLAN_VNI)
            .execute()
            .await
            .context("Failed to create VXLAN interface")?;

        Ok(())
    }

    /// Set link up
    async fn set_link_up(&self, name: &str) -> Result<()> {
        debug!("Setting link {} up", name);

        let mut links = self
            .handle
            .link()
            .get()
            .match_name(name.to_string())
            .execute();
        let link = links.try_next().await?.context("Interface not found")?;

        self.handle
            .link()
            .set(link.header.index)
            .up()
            .execute()
            .await
            .context("Failed to set link up")?;

        Ok(())
    }

    /// Attach VXLAN interface to bridge
    async fn attach_to_bridge(&self) -> Result<()> {
        debug!(
            "Attaching {} to bridge {}",
            VXLAN_INTERFACE, BRIDGE_INTERFACE
        );

        // Get bridge index
        let mut links = self
            .handle
            .link()
            .get()
            .match_name(BRIDGE_INTERFACE.to_string())
            .execute();
        let bridge = links.try_next().await?.context("Bridge not found")?;
        let bridge_index = bridge.header.index;

        // Get VXLAN index
        let mut links = self
            .handle
            .link()
            .get()
            .match_name(VXLAN_INTERFACE.to_string())
            .execute();
        let vxlan = links
            .try_next()
            .await?
            .context("VXLAN interface not found")?;
        let vxlan_index = vxlan.header.index;

        // Set master to bridge
        self.handle
            .link()
            .set(vxlan_index)
            .controller(bridge_index)
            .execute()
            .await
            .context("Failed to attach to bridge")?;

        info!("VXLAN interface attached to bridge {}", BRIDGE_INTERFACE);
        Ok(())
    }

    /// Add FDB entry for a peer
    async fn add_fdb_entry(&self, peer_host_ip: Ipv4Addr) -> Result<()> {
        debug!("Adding FDB entry for peer {}", peer_host_ip);

        // Get VXLAN interface index
        let mut links = self
            .handle
            .link()
            .get()
            .match_name(VXLAN_INTERFACE.to_string())
            .execute();
        let link = links
            .try_next()
            .await?
            .context("VXLAN interface not found")?;
        let if_index = link.header.index;

        // Add default FDB entry (00:00:00:00:00:00 -> peer_host_ip)
        // This makes VXLAN forward all unknown MACs to this peer
        self.handle
            .neighbours()
            .add_bridge(if_index, &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
            .destination(IpAddr::V4(peer_host_ip))
            .execute()
            .await
            .context("Failed to add FDB entry")?;

        debug!("FDB entry added for {}", peer_host_ip);
        Ok(())
    }

    /// Remove FDB entry for a peer
    async fn remove_fdb_entry(&self, peer_host_ip: Ipv4Addr) -> Result<()> {
        debug!("Removing FDB entry for peer {}", peer_host_ip);

        // Get VXLAN interface index
        let mut links = self
            .handle
            .link()
            .get()
            .match_name(VXLAN_INTERFACE.to_string())
            .execute();
        let link = links
            .try_next()
            .await?
            .context("VXLAN interface not found")?;
        let if_index = link.header.index;

        let mut message = NeighbourMessage::default();
        message.header.family = AddressFamily::Bridge;
        message.header.ifindex = if_index;
        message
            .attributes
            .push(NeighbourAttribute::LinkLocalAddress(vec![
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            ]));
        message
            .attributes
            .push(NeighbourAttribute::Destination(NeighbourAddress::Inet(
                peer_host_ip,
            )));

        self.handle
            .neighbours()
            .del(message)
            .execute()
            .await
            .context("Failed to remove FDB entry")?;

        debug!("FDB entry removed for {}", peer_host_ip);
        Ok(())
    }

    /// Get current peers
    pub fn peers(&self) -> &HashMap<String, Ipv4Addr> {
        &self.peers
    }
}

// ============================================================================
// Non-Linux stub implementation
// ============================================================================

#[cfg(all(not(target_os = "linux"), feature = "dev-stubs"))]
impl VxlanManager {
    pub async fn new(_local_ip: Ipv4Addr) -> Result<Self> {
        warn!("VXLAN is only supported on Linux - running in stub mode");
        Ok(Self {
            peers: HashMap::new(),
        })
    }

    pub async fn setup_vxlan(&self) -> Result<()> {
        warn!("VXLAN setup skipped (not on Linux)");
        Ok(())
    }

    pub async fn add_peer(&mut self, subnet: String, peer_host_ip: Ipv4Addr) -> Result<()> {
        info!(
            "Stub: Would add VXLAN peer subnet={}, host_ip={}",
            subnet, peer_host_ip
        );
        self.peers.insert(subnet, peer_host_ip);
        Ok(())
    }

    pub async fn remove_peer(&mut self, subnet: &str) -> Result<()> {
        info!("Stub: Would remove VXLAN peer subnet={}", subnet);
        self.peers.remove(subnet);
        Ok(())
    }

    pub fn peers(&self) -> &HashMap<String, Ipv4Addr> {
        &self.peers
    }
}

// Cleanup on drop
impl Drop for VxlanManager {
    fn drop(&mut self) {
        // Note: We don't delete the VXLAN interface on drop
        // It should persist for debugging and may be reused
        debug!("VxlanManager dropped");
    }
}
