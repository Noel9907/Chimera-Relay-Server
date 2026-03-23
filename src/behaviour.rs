// ── Relay Server Behaviour ──
//
// Defines what protocols the relay server speaks.
// Much simpler than the desktop app's ChimeraBehaviour — no chunk/DAG protocols.
//
// The relay server does 4 things:
//   1. Relay:     Accept relay reservations, forward traffic for NAT-ed peers
//   2. Kademlia:  Participate in the DHT (store records, answer queries)
//   3. Identify:  Exchange peer info when someone connects
//   4. Ping:      Respond to liveness checks

use libp2p::swarm::NetworkBehaviour;
use libp2p::{identify, kad, ping, relay};

/// The relay server's network behaviour.
///
/// Key difference from the desktop app's ChimeraBehaviour:
///   - Uses `relay::Behaviour` (SERVER) instead of `relay::client::Behaviour` (CLIENT)
///   - No chunk_proto or dag_proto — the relay doesn't store or serve content
#[derive(NetworkBehaviour)]
pub struct RelayServerBehaviour {
    /// Circuit Relay v2 SERVER — accepts relay reservations from NAT-ed peers
    /// and forwards traffic between them. This is the relay's main job.
    pub relay: relay::Behaviour,

    /// Kademlia DHT — same protocol ID as the desktop app (/chimera/kad/1.0.0).
    /// Stores site name → root CID mappings and provider records.
    /// Since the relay is always online, it's a reliable DHT storage node.
    pub kademlia: kad::Behaviour<kad::store::MemoryStore>,

    /// Identify — peers exchange info (PeerId, protocols, listen addresses)
    /// automatically when they connect. We use this to learn peer addresses
    /// and add them to Kademlia, so other peers can find them.
    pub identify: identify::Behaviour,

    /// Ping — simple liveness check. Peers ping us to see if we're alive.
    pub ping: ping::Behaviour,
}
