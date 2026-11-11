use anyhow::{Context, Result};
use prost_config::Config;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

pub fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub fn lock(path: &Path) -> Result<File> {
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
    Ok(file)
}
/// Only pre-rename failures return Err. A post-rename directory fsync error is
/// reported separately: disk content has changed and runtime must publish it.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<bool> {
    let parent = path.parent().context("path has no parent")?;
    let dir = File::open(parent)?;
    dir.sync_all()?;
    let temp = path.with_extension(format!("{}.tmp", std::process::id()));
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, path)?;
        Ok::<_, anyhow::Error>(dir.sync_all().is_ok())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}
fn artifact(path: &mut PathBuf, dir: &Path) -> Result<()> {
    let bytes = fs::read(&*path).with_context(|| format!("read artifact {}", path.display()))?;
    anyhow::ensure!(bytes.len() <= 16 * 1024 * 1024, "artifact exceeds 16 MiB");
    let destination = dir.join(hash(&bytes));
    if destination.exists() {
        anyhow::ensure!(
            fs::read(&destination)? == bytes,
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
pub fn stage(mut c: Config) -> Result<Config> {
    use std::os::unix::fs::PermissionsExt;
    fs::create_dir_all(&c.runtime.artifact_dir)?;
    fs::set_permissions(&c.runtime.artifact_dir, fs::Permissions::from_mode(0o700))?;
    let dir = c.runtime.artifact_dir.clone();
    for u in c.upstreams.values_mut() {
        if let Some(path) = u.tls.as_mut().and_then(|t| t.ca_certificate.as_mut()) {
            artifact(path, &dir)?;
        }
    }
    for l in &mut c.listeners {
        if let Some(tls) = &mut l.tls {
            artifact(&mut tls.certificate, &dir)?;
            artifact(&mut tls.private_key, &dir)?;
            for cert in &mut tls.certificates {
                artifact(&mut cert.certificate, &dir)?;
                artifact(&mut cert.private_key, &dir)?;
            }
        }
        for p in l
            .plugins
            .iter_mut()
            .chain(l.routes.iter_mut().flat_map(|r| r.plugins.iter_mut()))
        {
            if let Some(path) = &mut p.module {
                artifact(path, &dir)?;
            }
        }
    }
    Ok(c)
}
