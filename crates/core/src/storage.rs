use anyhow::{Context, Result};
use prost_config::Config;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub struct ConfigLock(File);
impl Drop for ConfigLock {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self.0);
    }
}
pub fn lock(path: &Path) -> Result<ConfigLock> {
    let p = path.with_extension("toml.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .mode(0o600)
        .open(p)?;
    fs2::FileExt::try_lock_exclusive(&file)
        .context("configuration already managed by another daemon")?;
    Ok(ConfigLock(file))
}
/// Only pre-rename failures return Err. A post-rename directory fsync error is
/// reported separately: disk content has changed and runtime must publish it.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<bool> {
    atomic_write_with(path, bytes, |_| Ok(()))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum WritePhase {
    DirectoryBefore,
    Create,
    Write,
    FileSync,
    Rename,
    DirectoryAfter,
    Complete,
}
fn atomic_write_with(
    path: &Path,
    bytes: &[u8],
    mut checkpoint: impl FnMut(WritePhase) -> std::io::Result<()>,
) -> Result<bool> {
    let parent = path.parent().context("path has no parent")?;
    let dir = File::open(parent)?;
    checkpoint(WritePhase::DirectoryBefore)?;
    dir.sync_all()?;
    let mut random = [0u8; 8];
    getrandom::fill(&mut random).map_err(|e| anyhow::anyhow!("temporary filename: {e}"))?;
    let filename = path.file_name().context("missing filename")?;
    // Leave room for the nonce even when the destination is close to NAME_MAX.
    let mut name = if filename.as_encoded_bytes().len() <= 128 {
        filename.to_os_string()
    } else {
        hash(filename.as_encoded_bytes()).into()
    };
    name.push(format!(
        ".{}.{:016x}.tmp",
        std::process::id(),
        u64::from_ne_bytes(random)
    ));
    let temp = parent.join(name);
    let mut created = false;
    let result = (|| {
        checkpoint(WritePhase::Create)?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        created = true;
        checkpoint(WritePhase::Write)?;
        file.write_all(bytes)?;
        checkpoint(WritePhase::FileSync)?;
        file.sync_all()?;
        checkpoint(WritePhase::Rename)?;
        fs::rename(&temp, path)?;
        let durable = checkpoint(WritePhase::DirectoryAfter)
            .and_then(|_| dir.sync_all())
            .is_ok();
        let observed = checkpoint(WritePhase::Complete).is_ok();
        Ok::<_, anyhow::Error>(durable && observed)
    })();
    if result.is_err() && created {
        let _ = fs::remove_file(temp);
    }
    result
}
fn read_artifact(path: &Path, managed: bool) -> Result<Vec<u8>> {
    const LIMIT: u64 = 16 * 1024 * 1024;
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NONBLOCK | if managed { libc::O_NOFOLLOW } else { 0 })
        .open(path)
        .with_context(|| format!("read artifact {}", path.display()))?;
    let meta = file.metadata()?;
    anyhow::ensure!(meta.is_file(), "artifact must be a regular file");
    anyhow::ensure!(meta.len() <= LIMIT, "artifact exceeds 16 MiB");
    let mut bytes = Vec::with_capacity(meta.len() as usize);
    file.take(LIMIT + 1).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64 <= LIMIT, "artifact exceeds 16 MiB");
    Ok(bytes)
}
fn artifact(path: &mut PathBuf, dir: &Path) -> Result<()> {
    let managed = path
        .file_name()
        .and_then(|n| n.to_str())
        .is_some_and(artifacts::is_hash)
        && fs::canonicalize(
            path.parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )? == dir;
    let bytes = read_artifact(path, managed)?;
    let digest = hash(&bytes);
    if managed {
        anyhow::ensure!(
            path.file_name().and_then(|n| n.to_str()) == Some(digest.as_str()),
            "managed artifact hash mismatch"
        );
    }
    let destination = dir.join(digest);
    if fs::symlink_metadata(&destination).is_ok() {
        anyhow::ensure!(
            read_artifact(&destination, true)? == bytes,
            "artifact hash collision or corruption"
        );
    } else {
        anyhow::ensure!(
            atomic_write(&destination, &bytes)?,
            "artifact directory fsync failed"
        );
    }
    *path = destination;
    Ok(())
}
pub fn stage(mut c: Config, store: &ArtifactStore) -> Result<Config> {
    #[cfg(test)]
    if let Some((entered, release)) = store.stage_pause.lock().unwrap().take() {
        let _ = entered.send(());
        let _ = release.recv();
    }
    store.verify()?;
    let dir = &store.path;
    for u in c.upstreams.values_mut() {
        if let Some(tls) = &mut u.health_check.tls {
            if let Some(path) = &mut tls.ca_certificate {
                artifact(path, dir)?;
            }
            if let Some(id) = &mut tls.identity {
                artifact(&mut id.certificate, dir)?;
                artifact(&mut id.private_key, dir)?;
            }
        }
        if let Some(id) = u.tls.as_mut().and_then(|t| t.identity.as_mut()) {
            artifact(&mut id.certificate, dir)?;
            artifact(&mut id.private_key, dir)?;
        }
        if let Some(path) = u.tls.as_mut().and_then(|t| t.ca_certificate.as_mut()) {
            artifact(path, dir)?;
        }
    }
    for l in &mut c.listeners {
        if let Some(tls) = &mut l.tls {
            if let Some(path) = &mut tls.client_ca {
                artifact(path, dir)?;
            }
            artifact(&mut tls.certificate, dir)?;
            artifact(&mut tls.private_key, dir)?;
            for cert in &mut tls.certificates {
                artifact(&mut cert.certificate, dir)?;
                artifact(&mut cert.private_key, dir)?;
            }
        }
        for p in l
            .plugins
            .iter_mut()
            .chain(l.routes.iter_mut().flat_map(|r| r.plugins.iter_mut()))
        {
            if let Some(path) = &mut p.module {
                artifact(path, dir)?;
            }
        }
    }
    store.verify()?;
    Ok(c)
}

mod artifacts;
pub(crate) use artifacts::references;
pub use artifacts::{ArtifactStore, GcOptions, GcReport};

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bounded_artifact_reads_and_managed_symlinks() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let large = dir.path().join("large");
        File::create(&large)?.set_len(1024 * 1024 * 1024)?;
        assert!(read_artifact(&large, false)
            .unwrap_err()
            .to_string()
            .contains("16 MiB"));
        assert!(read_artifact(dir.path(), false).is_err());
        let source = dir.path().join("cert");
        fs::write(&source, b"data")?;
        let link = dir.path().join("link");
        std::os::unix::fs::symlink(&source, &link)?;
        assert_eq!(read_artifact(&link, false)?, b"data");
        assert!(read_artifact(&link, true).is_err());
        Ok(())
    }
    #[test]
    fn stale_pid_temp_does_not_prevent_restart_or_get_removed() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("config.toml");
        let stale = path.with_extension(format!("{}.tmp", std::process::id()));
        fs::write(&stale, b"other writer or crashed process")?;
        assert!(atomic_write(&path, b"new config")?);
        assert_eq!(fs::read(&path)?, b"new config");
        assert_eq!(fs::read(&stale)?, b"other writer or crashed process");
        let long = dir.path().join("x".repeat(240));
        assert!(atomic_write(&long, b"long filename")?);
        assert_eq!(fs::read(long)?, b"long filename");
        Ok(())
    }
    #[test]
    fn corrupted_managed_source_is_not_imported_as_a_new_artifact() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = ArtifactStore::open(&dir.path().join("store"))?;
        let mut path = store.path.join(hash(b"original"));
        fs::write(&path, b"corrupted")?;
        assert!(artifact(&mut path, &store.path)
            .unwrap_err()
            .to_string()
            .contains("hash mismatch"));
        assert!(!store.path.join(hash(b"corrupted")).exists());
        Ok(())
    }
}
