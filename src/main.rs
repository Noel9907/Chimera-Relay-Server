// ── Chimera Relay Server ──
//
// A headless libp2p node that serves two purposes:
//
//   1. BOOTSTRAP NODE — the first peer that Chimera desktop apps connect to.
//      When a new desktop app starts, it dials us to join the network.
//      We add it to our Kademlia routing table, so future peers can discover it.
//
//   2. CIRCUIT RELAY — peers behind NAT can't accept incoming connections.
//      They "reserve" a slot on us, and other peers reach them through us.
//      Think of it as a postal forwarding service.
//
// What this does NOT do:
//   - No chunk/DAG storage or serving (the relay has no content)
//   - No SQLite, no filesystem chunk store
//   - No GUI, no Tauri, no React
//
// Configuration via environment variables:
//   CHIMERA_PORT     — TCP port to listen on (default: 4001)
//   CHIMERA_DATA_DIR — where to store the keypair (default: ~/.chimera-relay/)
//   RUST_LOG         — log level (default: info)

mod behaviour;
mod identity;

use std::num::NonZero;
use std::path::PathBuf;
use std::time::Duration;

use libp2p::futures::StreamExt;
use libp2p::swarm::SwarmEvent;
use libp2p::{identify, kad, relay, StreamProtocol, Swarm};
use tracing::{debug, info};

use behaviour::{RelayServerBehaviour, RelayServerBehaviourEvent};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── Logging setup ──
    // Default to "info" level. Override with RUST_LOG env var.
    // Example: RUST_LOG=debug cargo run
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    // ── Configuration ──
    // Simple env var config — no config file needed for a relay server.
    let port: u16 = std::env::var("CHIMERA_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4001);

    let data_dir = std::env::var("CHIMERA_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join(".chimera-relay")
        });

    // ── Identity ──
    // Load or generate Ed25519 keypair. This MUST persist across restarts —
    // if the PeerId changes, every desktop app's bootstrap config breaks.
    let keypair_path = data_dir.join("identity").join("keypair.bin");
    let keypair = identity::load_or_generate_keypair(&keypair_path)?;
    let local_peer_id = keypair.public().to_peer_id();

    // ── Build swarm ──
    // The swarm combines transport (TCP + Noise + Yamux) with our behaviour.
    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(keypair)
        .with_tokio()
        .with_tcp(
            libp2p::tcp::Config::default(),
            libp2p::noise::Config::new,
            libp2p::yamux::Config::default,
        )?
        .with_behaviour(|key| {
            let peer_id = key.public().to_peer_id();

            // Circuit Relay v2 SERVER.
            // Desktop apps use relay::client::Behaviour (they USE relays).
            // We use relay::Behaviour (we ARE the relay).
            let relay_server = relay::Behaviour::new(peer_id, relay::Config::default());

            // Kademlia DHT — MUST use the same protocol ID as the desktop app.
            // If these don't match, peers will ignore each other's DHT messages.
            let kademlia = {
                let store = kad::store::MemoryStore::new(peer_id);
                let mut config = kad::Config::new(StreamProtocol::new("/chimera/kad/1.0.0"));
                config.set_replication_factor(NonZero::new(3).unwrap());
                let mut kad_behaviour = kad::Behaviour::with_config(peer_id, store, config);
                // Force Kademlia into server mode. By default, Kademlia waits to confirm
                // it's publicly reachable before serving records. Since we're on a public
                // IP (EC2), we skip that detection and immediately accept/serve DHT queries.
                kad_behaviour.set_mode(Some(kad::Mode::Server));
                kad_behaviour
            };

            // Identify — same protocol version string as the desktop app.
            let identify = identify::Behaviour::new(identify::Config::new(
                "/chimera/id/1.0.0".to_string(),
                key.public(),
            ));

            // Ping — automatic liveness checks.
            let ping = libp2p::ping::Behaviour::new(libp2p::ping::Config::new());

            RelayServerBehaviour {
                relay: relay_server,
                kademlia,
                identify,
                ping,
            }
        })?
        .with_swarm_config(|c| {
            // Longer idle timeout than the desktop app (120s vs 60s).
            // Relay connections can be longer-lived.
            c.with_idle_connection_timeout(Duration::from_secs(120))
        })
        .build();

    // ── Start listening ──
    // Listen on all interfaces (0.0.0.0) so peers from any network can reach us.
    // On EC2, this means the public IP is reachable.
    let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", port)
        .parse()
        .expect("Valid multiaddr");
    swarm.listen_on(listen_addr)?;

    println!("═══════════════════════════════════════════════════════════");
    println!("  Chimera Relay Server");
    println!("  PeerId: {}", local_peer_id);
    println!();
    println!("  Add this to your desktop app's bootstrap_nodes config:");
    println!("  /ip4/<YOUR_PUBLIC_IP>/tcp/{}/p2p/{}", port, local_peer_id);
    println!("═══════════════════════════════════════════════════════════");

    // ── Event loop ──
    // Process swarm events forever. No IPC commands to handle (unlike the desktop app) —
    // the relay just listens, relays, and participates in the DHT.
    run_event_loop(&mut swarm).await;

    Ok(())
}

// ═══════════════════════════════════════════════════════════════════
// Event loop — the relay's main loop
// ═══════════════════════════════════════════════════════════════════

async fn run_event_loop(swarm: &mut Swarm<RelayServerBehaviour>) {
    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                info!("Listening on: {}/p2p/{}", address, swarm.local_peer_id());
            }

            SwarmEvent::ConnectionEstablished {
                peer_id,
                num_established,
                ..
            } => {
                info!(
                    "Peer connected: {} (now {} connections to this peer)",
                    peer_id, num_established
                );
            }

            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                info!(
                    "Peer disconnected: {} ({} connections remaining to this peer)",
                    peer_id, num_established
                );
            }

            SwarmEvent::Behaviour(event) => {
                handle_behaviour_event(swarm, event);
            }

            other => {
                debug!("Swarm event: {:?}", other);
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// Behaviour event handlers
// ═══════════════════════════════════════════════════════════════════

fn handle_behaviour_event(
    swarm: &mut Swarm<RelayServerBehaviour>,
    event: RelayServerBehaviourEvent,
) {
    match event {
        RelayServerBehaviourEvent::Identify(event) => {
            handle_identify(swarm, event);
        }
        RelayServerBehaviourEvent::Kademlia(event) => {
            handle_kademlia(event);
        }
        RelayServerBehaviourEvent::Relay(event) => {
            handle_relay(event);
        }
        RelayServerBehaviourEvent::Ping(event) => {
            debug!("Ping from {}: {:?}", event.peer, event.result);
        }
    }
}

/// When a peer connects and identifies itself, we add its addresses to Kademlia.
/// This is how the DHT learns about peers:
///   1. Peer A connects to us and identifies (tells us its addresses)
///   2. We add Peer A's addresses to our Kademlia routing table
///   3. Peer B connects, does a Kademlia lookup → we tell it about Peer A
///   4. Peer B can now connect directly to Peer A
fn handle_identify(swarm: &mut Swarm<RelayServerBehaviour>, event: identify::Event) {
    match event {
        identify::Event::Received { peer_id, info, .. } => {
            info!(
                "Identified peer {}: {} addresses, protocols: {:?}",
                peer_id,
                info.listen_addrs.len(),
                info.protocols
            );

            // Add every address this peer reports to Kademlia.
            // This is critical — without this, the relay can't tell other peers
            // how to reach this peer.
            for addr in info.listen_addrs {
                swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&peer_id, addr);
            }
        }

        identify::Event::Sent { peer_id, .. } => {
            debug!("Sent identify info to {}", peer_id);
        }

        other => {
            debug!("Identify event: {:?}", other);
        }
    }
}

/// Log Kademlia events. The relay doesn't initiate DHT queries —
/// it just stores records and responds to queries from desktop apps.
fn handle_kademlia(event: kad::Event) {
    match event {
        kad::Event::RoutingUpdated { peer, .. } => {
            info!("Kademlia: peer {} added to routing table", peer);
        }

        kad::Event::InboundRequest { request } => {
            debug!("Kademlia: inbound request: {:?}", request);
        }

        kad::Event::OutboundQueryProgressed { result, .. } => {
            match &result {
                kad::QueryResult::Bootstrap(Ok(result)) => {
                    info!(
                        "Kademlia: bootstrap step complete ({} remaining)",
                        result.num_remaining
                    );
                }
                _ => {
                    debug!("Kademlia: query result: {:?}", result);
                }
            }
        }

        other => {
            debug!("Kademlia: {:?}", other);
        }
    }
}

/// Log relay events. The relay behaviour handles everything internally —
/// we just log so we can monitor activity.
fn handle_relay(event: relay::Event) {
    // Log all relay events at info level — these are interesting
    // for monitoring how many peers are using us as a relay.
    info!("Relay: {:?}", event);
}
