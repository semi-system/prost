use super::*;
use anyhow::ensure;
use serde_json::json;
use std::{
    collections::HashSet,
    ffi::OsString,
    os::{
        fd::AsRawFd,
        unix::fs::{MetadataExt, PermissionsExt},
    },
    time::{Duration, SystemTime},
};

/// One daemon owns a store. GC never races a second daemon using another config.
pub struct ArtifactStore {
    pub path: PathBuf,
    directory: File,
    _lock: File,
    #[cfg(test)]
    pub fault: std::sync::Mutex<Option<(WritePhase, bool)>>,
    #[cfg(test)]
    pub stage_pause: std::sync::Mutex<
        Option<(
            std::sync::mpsc::SyncSender<()>,
            std::sync::mpsc::Receiver<()>,
        )>,
    >,
}
impl Drop for ArtifactStore {
    fn drop(&mut self) {
        let _ = fs2::FileExt::unlock(&self._lock);
    }
}
impl ArtifactStore {
    pub fn open(path: &Path) -> Result<Self> {
        fs::create_dir_all(path)?;
        let path = fs::canonicalize(path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path.join(".prost-store.lock"))?;
        ensure!(
            lock.metadata()?.is_file(),
            "artifact lock must be a regular file"
        );
        fs2::FileExt::try_lock_exclusive(&lock)
            .context("artifact store already managed by another daemon")?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700))?;
        fs::set_permissions(
            path.join(".prost-store.lock"),
            fs::Permissions::from_mode(0o600),
        )?;
        let directory = File::open(&path)?;
        Ok(Self {
            path,
            directory,
            _lock: lock,
            #[cfg(test)]
            fault: std::sync::Mutex::new(None),
            #[cfg(test)]
            stage_pause: std::sync::Mutex::new(None),
        })
    }
    pub fn verify(&self) -> Result<()> {
        let current = fs::metadata(&self.path)?;
        let opened = self.directory.metadata()?;
        ensure!(
            current.is_dir() && current.dev() == opened.dev() && current.ino() == opened.ino(),
            "artifact directory was replaced; restart required"
        );
        let lock = fs::symlink_metadata(self.path.join(".prost-store.lock"))?;
        let held = self._lock.metadata()?;
        ensure!(
            lock.is_file() && lock.dev() == held.dev() && lock.ino() == held.ino(),
            "artifact lock was replaced; restart required"
        );
        Ok(())
    }
    pub fn persist(&self, path: &Path, bytes: &[u8]) -> Result<bool> {
        #[cfg(test)]
        {
            let fault = self.fault.lock().unwrap().take();
            super::atomic_write_with(path, bytes, |phase| {
                if fault.is_some_and(|(at, _)| at == phase) {
                    if fault.unwrap().1 {
                        std::process::exit(73);
                    }
                    return Err(std::io::Error::from_raw_os_error(libc::ENOSPC));
                }
                Ok(())
            })
        }
        #[cfg(not(test))]
        super::atomic_write(path, bytes)
    }
    pub fn collect(
        &self,
        protected: &HashSet<OsString>,
        options: GcOptions,
        generation: u64,
        live_generations: usize,
    ) -> Result<GcReport> {
        self.verify()?;
        ensure!(
            options.min_age_seconds <= 31_536_000,
            "min_age_seconds must be 0..31536000"
        );
        ensure!(
            (1..=1_000_000).contains(&options.max_entries)
                && (1..=100_000).contains(&options.max_delete_files),
            "invalid GC scan/delete limit"
        );
        let mut report = GcReport {
            dry_run: options.dry_run,
            generation,
            live_generations,
            scanned_entries: 0,
            referenced_files: 0,
            referenced_bytes: 0,
            eligible_files: 0,
            eligible_bytes: 0,
            deleted_files: 0,
            deleted_bytes: 0,
            skipped_entries: 0,
            scan_limited: false,
            delete_limited: false,
            durable: true,
            errors: 0,
            first_error: None,
        };
        // Anchor traversal/unlink to the directory we locked, not a replaceable path.
        let anchor = PathBuf::from(format!("/proc/self/fd/{}", self.directory.as_raw_fd()));
        let now = SystemTime::now();
        for entry in fs::read_dir(anchor)?.take(options.max_entries + 1) {
            if report.scanned_entries == options.max_entries {
                report.scan_limited = true;
                break;
            }
            report.scanned_entries += 1;
            let entry = match entry {
                Ok(v) => v,
                Err(e) => {
                    report.error(e);
                    continue;
                }
            };
            let name = entry.file_name();
            let Some(text) = name.to_str() else {
                report.skipped_entries += 1;
                continue;
            };
            if !is_hash(text) && !is_temp(text) {
                report.skipped_entries += 1;
                continue;
            }
            let meta = match fs::symlink_metadata(entry.path()) {
                Ok(v) => v,
                Err(e) => {
                    report.error(e);
                    continue;
                }
            };
            if !meta.is_file() {
                report.skipped_entries += 1;
                continue;
            }
            if protected.contains(&name) {
                report.referenced_files += 1;
                report.referenced_bytes += meta.len();
                continue;
            }
            let modified = match meta.modified() {
                Ok(v) => v,
                Err(e) => {
                    report.error(e);
                    continue;
                }
            };
            if now.duration_since(modified).is_err()
                || now.duration_since(modified).unwrap_or_default()
                    < Duration::from_secs(options.min_age_seconds)
            {
                report.skipped_entries += 1;
                continue;
            }
            report.eligible_files += 1;
            report.eligible_bytes += meta.len();
            if options.dry_run {
                continue;
            }
            if report.deleted_files == options.max_delete_files {
                report.delete_limited = true;
                continue;
            }
            match fs::remove_file(entry.path()) {
                Ok(()) => {
                    report.deleted_files += 1;
                    report.deleted_bytes += meta.len();
                }
                Err(e) => report.error(e),
            }
        }
        if !options.dry_run && report.deleted_files > 0 {
            if let Err(e) = self.directory.sync_all() {
                report.durable = false;
                report.error(e);
            }
        }
        Ok(report)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GcOptions {
    pub dry_run: bool,
    pub min_age_seconds: u64,
    pub max_entries: usize,
    pub max_delete_files: u64,
}
impl Default for GcOptions {
    fn default() -> Self {
        Self {
            dry_run: true,
            min_age_seconds: 3600,
            max_entries: 100_000,
            max_delete_files: 1024,
        }
    }
}
#[derive(Debug, serde::Serialize)]
pub struct GcReport {
    pub dry_run: bool,
    pub generation: u64,
    pub live_generations: usize,
    pub scanned_entries: usize,
    pub referenced_files: u64,
    pub referenced_bytes: u64,
    pub eligible_files: u64,
    pub eligible_bytes: u64,
    pub deleted_files: u64,
    pub deleted_bytes: u64,
    pub skipped_entries: u64,
    pub scan_limited: bool,
    pub delete_limited: bool,
    pub durable: bool,
    pub errors: u64,
    pub first_error: Option<String>,
}
impl GcReport {
    fn error(&mut self, error: std::io::Error) {
        self.errors += 1;
        if self.first_error.is_none() {
            self.first_error = Some(error.to_string());
        }
    }
    pub fn json(&self) -> serde_json::Value {
        json!(self)
    }
}
pub(super) fn is_hash(name: &str) -> bool {
    name.len() == 64
        && name
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn is_temp(name: &str) -> bool {
    let parts: Vec<_> = name.split('.').collect();
    match parts.as_slice() {
        [hash, pid, "tmp"] => {
            is_hash(hash) && !pid.is_empty() && pid.bytes().all(|b| b.is_ascii_digit())
        }
        [hash, pid, nonce, "tmp"] => {
            is_hash(hash)
                && !pid.is_empty()
                && pid.bytes().all(|b| b.is_ascii_digit())
                && nonce.len() == 16
                && nonce.bytes().all(|b| b.is_ascii_hexdigit())
        }
        _ => false,
    }
}
pub(crate) fn references(config: &Config) -> HashSet<OsString> {
    let mut paths = Vec::new();
    if let Some(ca) = config
        .runtime
        .s3
        .as_ref()
        .and_then(|s| s.ca_certificate.as_ref())
    {
        paths.push(ca);
    }
    for u in config.upstreams.values() {
        if let Some(tls) = &u.health_check.tls {
            if let Some(path) = &tls.ca_certificate {
                paths.push(path);
            }
            if let Some(id) = &tls.identity {
                paths.extend([&id.certificate, &id.private_key]);
            }
        }
        if let Some(id) = u.tls.as_ref().and_then(|t| t.identity.as_ref()) {
            paths.extend([&id.certificate, &id.private_key]);
        }
        if let Some(path) = u.tls.as_ref().and_then(|tls| tls.ca_certificate.as_ref()) {
            paths.push(path);
        }
    }
    for l in &config.listeners {
        if let Some(tls) = &l.tls {
            if let Some(path) = &tls.client_ca {
                paths.push(path);
            }
            paths.extend([&tls.certificate, &tls.private_key]);
            for cert in &tls.certificates {
                paths.extend([&cert.certificate, &cert.private_key]);
            }
        }
        for p in l
            .plugins
            .iter()
            .chain(l.routes.iter().flat_map(|r| &r.plugins))
        {
            if let Some(path) = &p.module {
                paths.push(path);
            }
        }
    }
    paths
        .into_iter()
        .filter_map(|p| p.file_name())
        .filter(|n| n.to_str().is_some_and(is_hash))
        .map(OsString::from)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn old(path: &Path) -> Result<()> {
        File::options().write(true).open(path)?.set_times(
            std::fs::FileTimes::new().set_modified(SystemTime::now() - Duration::from_secs(7200)),
        )?;
        Ok(())
    }
    #[test]
    fn dry_run_age_live_roots_limits_and_owned_temps() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let store = ArtifactStore::open(&dir.path().join("store"))?;
        let orphan = store.path.join("a".repeat(64));
        fs::write(&orphan, b"orphan")?;
        old(&orphan)?;
        let retained = store.path.join("b".repeat(64));
        fs::write(&retained, b"retained")?;
        old(&retained)?;
        let fresh = store.path.join("c".repeat(64));
        fs::write(&fresh, b"young")?;
        let target = dir.path().join("external");
        fs::write(&target, b"outside")?;
        let link = store.path.join("d".repeat(64));
        std::os::unix::fs::symlink(&target, &link)?;
        let folder = store.path.join("e".repeat(64));
        fs::create_dir(&folder)?;
        fs::write(store.path.join("notes.txt"), b"not managed")?;
        let legacy = store.path.join(format!("{}.123.tmp", "f".repeat(64)));
        fs::write(&legacy, b"stale")?;
        old(&legacy)?;
        let temp = store
            .path
            .join(format!("{}.123.0123456789abcdef.tmp", "0".repeat(64)));
        fs::write(&temp, b"stale")?;
        old(&temp)?;
        let protected = HashSet::from([retained.file_name().unwrap().to_os_string()]);
        let report = store.collect(&protected, GcOptions::default(), 9, 2)?;
        assert_eq!(report.eligible_files, 3);
        assert_eq!(report.referenced_files, 1);
        assert_eq!(report.deleted_files, 0);
        assert!(orphan.exists() && legacy.exists() && temp.exists());
        let report = store.collect(
            &protected,
            GcOptions {
                dry_run: false,
                max_delete_files: 1,
                ..GcOptions::default()
            },
            9,
            2,
        )?;
        assert_eq!(report.deleted_files, 1);
        assert!(report.delete_limited && report.durable);
        let report = store.collect(
            &protected,
            GcOptions {
                dry_run: false,
                min_age_seconds: 0,
                ..GcOptions::default()
            },
            9,
            2,
        )?;
        assert_eq!(report.deleted_files, 3);
        assert!(
            retained.exists()
                && link.symlink_metadata()?.file_type().is_symlink()
                && folder.is_dir()
        );
        assert_eq!(fs::read(target)?, b"outside");
        assert!(store.path.join("notes.txt").exists());
        let report = store.collect(
            &protected,
            GcOptions {
                max_entries: 1,
                ..GcOptions::default()
            },
            9,
            2,
        )?;
        assert!(report.scan_limited);
        assert_eq!(report.scanned_entries, 1);
        Ok(())
    }
    #[test]
    fn exclusive_store_lock_and_directory_replacement() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("store");
        let store = ArtifactStore::open(&path)?;
        assert!(ArtifactStore::open(&path).is_err());
        let orphan = path.join("a".repeat(64));
        fs::write(&orphan, b"keep")?;
        let moved = dir.path().join("moved");
        fs::rename(&path, &moved)?;
        fs::create_dir(&path)?;
        assert!(store
            .collect(
                &HashSet::new(),
                GcOptions {
                    dry_run: false,
                    min_age_seconds: 0,
                    ..GcOptions::default()
                },
                0,
                1
            )
            .is_err());
        assert!(moved.join("a".repeat(64)).exists());
        drop(store);
        let store = ArtifactStore::open(&moved)?;
        drop(store);
        Ok(())
    }
}
