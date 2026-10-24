//! Installation-control/SSH entry point, never a model-facing authority API.
use anyhow::{Context, Result, ensure};
use std::io::Read;
fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("--manifest") {
        println!(
            "{}",
            serde_json::to_string(&shenko_node::running_manifest()?)?
        );
        return Ok(());
    }
    let dir = std::env::args()
        .nth(1)
        .context("usage: shenko-node PRIVATE_DATA_DIR < request.json")?;
    let mut bytes = Vec::new();
    std::io::stdin()
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 1024 * 1024, "request exceeds 1MiB");
    let request = serde_json::from_slice(&bytes)?;
    let mut node = shenko_node::Node::open(std::path::Path::new(&dir))?;
    println!("{}", node.handle(request)?);
    Ok(())
}
