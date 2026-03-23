// ── Identity Persistence ──
//
// The relay server's identity (Ed25519 keypair) must persist across restarts.
// If the PeerId changes, every desktop app's bootstrap config becomes invalid.
//
// Format: 32-byte Ed25519 seed saved to disk.
// Same format as the desktop app — they're interoperable.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use libp2p::identity;
use tracing::info;

/// Load an existing keypair from disk, or generate and save a new one.
///
/// The file stores the 32-byte Ed25519 secret seed.
/// From this seed, the full keypair (and PeerId) can be reconstructed deterministically.
pub fn load_or_generate_keypair(path: &Path) -> Result<identity::Keypair> {
    if path.exists() {
        load_keypair(path)
    } else {
        generate_and_save_keypair(path)
    }
}

fn load_keypair(path: &Path) -> Result<identity::Keypair> {
    let bytes = fs::read(path)
        .with_context(|| format!("Failed to read keypair from {}", path.display()))?;

    // Handle both 64-byte (full keypair) and 32-byte (seed only) files.
    // The desktop app also handles this — old versions saved 64 bytes.
    let seed = if bytes.len() == 64 {
        bytes[..32].to_vec()
    } else {
        bytes
    };

    let keypair = identity::Keypair::ed25519_from_bytes(seed)
        .map_err(|e| anyhow::anyhow!("Invalid keypair file: {}", e))?;

    info!("Loaded existing keypair from {}", path.display());
    Ok(keypair)
}

fn generate_and_save_keypair(path: &Path) -> Result<identity::Keypair> {
    let keypair = identity::Keypair::generate_ed25519();

    // Create parent directories (e.g., ~/.chimera-relay/identity/)
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create directory {}", parent.display()))?;
    }

    // Extract the Ed25519 variant so we can get the raw seed bytes
    let ed25519_kp = keypair
        .clone()
        .try_into_ed25519()
        .map_err(|e| anyhow::anyhow!("Not an Ed25519 keypair: {}", e))?;

    // Save only the 32-byte seed (not the full 64-byte keypair).
    // ed25519_from_bytes() can reconstruct the full keypair from just the seed.
    let bytes = ed25519_kp.to_bytes();
    fs::write(path, &bytes[..32])
        .with_context(|| format!("Failed to save keypair to {}", path.display()))?;

    info!("Generated new keypair, saved to {}", path.display());
    Ok(keypair)
}
