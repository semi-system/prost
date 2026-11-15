use crate::config::{Certificate, Config};
use anyhow::{ensure, Context, Result};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{symlink, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
};

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
pub fn private_read(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let meta = file.metadata()?;
    ensure!(
        meta.is_file()
            && meta.nlink() == 1
            && meta.permissions().mode() & 0o077 == 0
            && meta.len() <= limit,
        "unsafe or oversized agent private file"
    );
    let mut bytes = Vec::new();
    (&mut file).take(limit + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= limit,
        "agent private file grew beyond limit"
    );
    Ok(bytes)
}
pub fn atomic(path: &Path, data: &[u8], mode: u32) -> Result<()> {
    let mut random = [0u8; 8];
    getrandom::fill(&mut random).map_err(|_| anyhow::anyhow!("entropy unavailable"))?;
    let temporary = path.with_extension(format!("tmp-{:016x}", u64::from_le_bytes(random)));
    let result = (|| -> Result<()> {
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&temporary)?;
        f.write_all(data)?;
        f.sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().context("agent file parent")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Bundle {
    pub id: String,
    pub domains: Vec<String>,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub not_after: i64,
    pub bootstrap: bool,
}
pub struct Store {
    pub state: PathBuf,
    pub certificates: PathBuf,
    shared: bool,
    _lock: File,
}
impl Store {
    pub fn open(config: &Config) -> Result<Self> {
        fs::create_dir_all(&config.state_dir)?;
        fs::set_permissions(&config.state_dir, fs::Permissions::from_mode(0o700))?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(config.state_dir.join("agent.lock"))?;
        lock.try_lock_exclusive()
            .context("another certificate agent owns state")?;
        fs::create_dir_all(&config.certificates_dir)?;
        fs::set_permissions(
            &config.certificates_dir,
            fs::Permissions::from_mode(if config.shared_certificate_group {
                0o750
            } else {
                0o700
            }),
        )?;
        Ok(Self {
            state: fs::canonicalize(&config.state_dir)?,
            certificates: fs::canonicalize(&config.certificates_dir)?,
            shared: config.shared_certificate_group,
            _lock: lock,
        })
    }
    pub fn load(&self, id: &str) -> Result<Option<Bundle>> {
        let path = self.state.join(format!("{id}.json"));
        if !path.exists() {
            return Ok(None);
        }
        let bundle: Bundle = serde_json::from_slice(&private_read(&path, 1024 * 1024)?)?;
        ensure!(
            bundle.id == id
                && bundle.certificate.starts_with(&self.certificates)
                && bundle.private_key.starts_with(&self.certificates),
            "invalid stored certificate paths"
        );
        prost_transport::server_tls(
            &prost_config::ServerTls {
                certificate: bundle.certificate.clone(),
                private_key: bundle.private_key.clone(),
                ..Default::default()
            },
            false,
        )?;
        let info = prost_transport::certificate_info(&fs::read(&bundle.certificate)?)?;
        ensure!(
            info.not_after == bundle.not_after,
            "stored certificate metadata mismatch"
        );
        Ok(Some(bundle))
    }
    fn link_current(&self, bundle: &Bundle) -> Result<()> {
        let parent = self.certificates.join(&bundle.id);
        let version = bundle
            .certificate
            .parent()
            .and_then(Path::file_name)
            .context("bundle version")?;
        let temporary = parent.join(".current-new");
        if fs::symlink_metadata(&temporary).is_ok() {
            fs::remove_file(&temporary)?;
        }
        symlink(version, &temporary)?;
        fs::rename(&temporary, parent.join("current"))?;
        File::open(&parent)?.sync_all()?;
        Ok(())
    }
    pub fn install(
        &self,
        spec: &Certificate,
        certificate: &str,
        key: &str,
        bootstrap: bool,
    ) -> Result<Bundle> {
        ensure!(
            certificate.len() <= 1024 * 1024 && key.len() <= 64 * 1024,
            "certificate bundle too large"
        );
        let info = prost_transport::certificate_info(certificate.as_bytes())?;
        ensure!(
            info.not_before <= now() + 300
                && info.not_after > now() + 60
                && spec
                    .domains
                    .iter()
                    .all(|name| info.dns_names.contains(name)),
            "issued certificate dates/SAN do not match requested domains"
        );
        let version = format!("{:x}", Sha256::digest(certificate.as_bytes()));
        let parent = self.certificates.join(&spec.id);
        fs::create_dir_all(&parent)?;
        fs::set_permissions(
            &parent,
            fs::Permissions::from_mode(if self.shared { 0o750 } else { 0o700 }),
        )?;
        let mut random = [0; 8];
        getrandom::fill(&mut random).map_err(|_| anyhow::anyhow!("entropy unavailable"))?;
        let staging = parent.join(format!(".staging-{:016x}", u64::from_le_bytes(random)));
        fs::create_dir(&staging)?;
        fs::set_permissions(
            &staging,
            fs::Permissions::from_mode(if self.shared { 0o750 } else { 0o700 }),
        )?;
        let prepared = (|| -> Result<()> {
            atomic(
                &staging.join("fullchain.pem"),
                certificate.as_bytes(),
                if self.shared { 0o640 } else { 0o600 },
            )?;
            atomic(
                &staging.join("privkey.pem"),
                key.as_bytes(),
                if self.shared { 0o640 } else { 0o600 },
            )?;
            prost_transport::server_tls(
                &prost_config::ServerTls {
                    certificate: staging.join("fullchain.pem"),
                    private_key: staging.join("privkey.pem"),
                    ..Default::default()
                },
                false,
            )?;
            File::open(&staging)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = prepared {
            let _ = fs::remove_dir_all(&staging);
            return Err(error);
        }
        let directory = parent.join(&version);
        if directory.exists() {
            fs::remove_dir_all(&staging)?;
        } else {
            fs::rename(&staging, &directory)?;
            File::open(&parent)?.sync_all()?;
        }
        let cert_path = directory.join("fullchain.pem");
        let key_path = directory.join("privkey.pem");
        prost_transport::server_tls(
            &prost_config::ServerTls {
                certificate: cert_path.clone(),
                private_key: key_path.clone(),
                ..Default::default()
            },
            false,
        )?;
        let bundle = Bundle {
            id: spec.id.clone(),
            domains: spec.domains.clone(),
            certificate: cert_path,
            private_key: key_path,
            not_after: info.not_after,
            bootstrap,
        };
        atomic(
            &self.state.join(format!("{}.json", spec.id)),
            &serde_json::to_vec_pretty(&bundle)?,
            0o600,
        )?;
        self.link_current(&bundle)?;
        Ok(bundle)
    }
    pub fn bootstrap(&self, spec: &Certificate) -> Result<Bundle> {
        if let Some(bundle) = self.load(&spec.id)? {
            ensure!(
                bundle.domains == spec.domains,
                "certificate domain changes require a new group id"
            );
            self.link_current(&bundle)?;
            return Ok(bundle);
        }
        let cert = rcgen::generate_simple_self_signed(spec.domains.clone())?;
        self.install(
            spec,
            &cert.cert.pem(),
            &cert.signing_key.serialize_pem(),
            true,
        )
    }
    pub fn prune(&self, current: &Bundle) -> Result<()> {
        let parent = self.certificates.join(&current.id);
        let active = current.certificate.parent().context("bundle parent")?;
        let mut versions = Vec::new();
        for entry in fs::read_dir(&parent)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if entry.file_type()?.is_dir()
                && name.len() == 64
                && name.bytes().all(|b| b.is_ascii_hexdigit())
                && entry.path() != active
            {
                versions.push((entry.metadata()?.modified()?, entry.path()));
            }
        }
        versions.sort_by_key(|(time, _)| std::cmp::Reverse(*time));
        for (_, path) in versions.into_iter().skip(7) {
            fs::remove_dir_all(path)?;
        }
        File::open(parent)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bootstrap_restart_invalid_key_keeps_current_and_private_state() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let text=format!("directory_url='https://ca.test/dir'\ncontact='mailto:ops@example.test'\nterms_agreed=true\nstate_dir={:?}\ncertificates_dir={:?}\nadmin_socket='/run/prost/admin.sock'\nchallenge_address='127.0.0.1:8088'\n[[certificates]]\nid='mail'\ndomains=['mail.example.test']\nlisteners=['imaps']\n",dir.path().join("state"),dir.path().join("certs"));
        let config = Config::parse(&text)?;
        let store = Store::open(&config)?;
        assert!(Store::open(&config).is_err());
        let bundle = store.bootstrap(&config.certificates[0])?;
        assert!(bundle.bootstrap);
        assert_eq!(
            fs::metadata(&bundle.private_key)?.permissions().mode() & 0o777,
            0o600
        );
        let before = fs::read(&bundle.private_key)?;
        let other = rcgen::generate_simple_self_signed(config.certificates[0].domains.clone())?;
        assert!(store
            .install(
                &config.certificates[0],
                &fs::read_to_string(&bundle.certificate)?,
                &other.signing_key.serialize_pem(),
                false
            )
            .is_err());
        assert_eq!(fs::read(&bundle.private_key)?, before);
        assert_eq!(
            fs::read(store.certificates.join("mail/current/privkey.pem"))?,
            before
        );
        drop(store);
        let store = Store::open(&config)?;
        let restored = store.bootstrap(&config.certificates[0])?;
        assert_eq!(restored.certificate, bundle.certificate);
        assert!(Config::parse(&text.replace("mail.example.test", "*.example.test")).is_err());
        assert!(Config::parse(&text.replace("https://ca.test", "http://ca.test")).is_err());
        Ok(())
    }
}
