use std::path::Path;

use anyhow::{Result, bail};
use tokio::fs;

pub async fn verify_pack(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path).await?;
    if !metadata.is_file() {
        bail!("pack path {path:?} is not a file");
    }
    Ok(())
}
