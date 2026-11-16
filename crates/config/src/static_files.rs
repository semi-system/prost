use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StaticFiles {
    pub root: PathBuf,
    pub index: Vec<String>,
    pub strip_prefix: bool,
    pub max_open_files: usize,
    pub load_to_ram: bool,
    pub max_ram_bytes: u64,
    pub max_ram_files: usize,
    pub ram_headroom_bytes: u64,
    /// Disk mode caches complete small files on demand; zero disables that cache.
    pub cache_max_file_bytes: u64,
}
impl Default for StaticFiles {
    fn default() -> Self {
        Self {
            root: PathBuf::new(),
            index: vec!["index.html".into()],
            strip_prefix: false,
            max_open_files: 256,
            load_to_ram: false,
            max_ram_bytes: 64 * 1024 * 1024,
            max_ram_files: 10_000,
            ram_headroom_bytes: 64 * 1024 * 1024,
            cache_max_file_bytes: 1024 * 1024,
        }
    }
}
impl StaticFiles {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.root.is_absolute(), "static root must be absolute");
        ensure!(
            (1..=65536).contains(&self.max_open_files),
            "invalid static max_open_files"
        );
        ensure!(self.index.len() <= 16, "too many static index names");
        ensure!(
            (1..=(1_u64 << 40)).contains(&self.max_ram_bytes),
            "static max_ram_bytes must be 1..1TiB"
        );
        ensure!(
            (1..=100_000).contains(&self.max_ram_files),
            "invalid static max_ram_files"
        );
        ensure!(
            self.ram_headroom_bytes <= (1_u64 << 40),
            "invalid static RAM headroom"
        );
        ensure!(
            self.cache_max_file_bytes <= (1_u64 << 40),
            "invalid static cache_max_file_bytes"
        );
        for index in &self.index {
            ensure!(
                !index.is_empty()
                    && index.len() <= 255
                    && !index.contains(['/', '\\', '\0'])
                    && index != "."
                    && index != "..",
                "index must be a plain file name"
            );
        }
        Ok(())
    }
}
