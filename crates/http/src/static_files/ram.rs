//! A bounded immutable RAM image, prepared entirely before configuration commit.
use super::{open_beneath, File};
use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use std::{
    io::Read,
    os::unix::fs::MetadataExt,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::SystemTime,
};

/// Shared across active, candidate and retiring configuration generations.
pub struct RamBudget {
    limit: u64,
    used: AtomicU64,
}
impl RamBudget {
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            used: AtomicU64::new(0),
        }
    }
    pub fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }
    pub fn limit(&self) -> u64 {
        self.limit
    }
    pub(super) fn reserve(self: &Arc<Self>, bytes: u64) -> Result<Arc<Reservation>> {
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.limit)
            })
            .map_err(|_| {
                anyhow::anyhow!(
                    "static RAM process budget exceeded: need {bytes}, used {}, limit {}",
                    self.used(),
                    self.limit
                )
            })?;
        Ok(Arc::new(Reservation {
            budget: self.clone(),
            bytes,
        }))
    }
}
pub(super) struct Reservation {
    budget: Arc<RamBudget>,
    bytes: u64,
}
impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.used.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}
pub(super) struct RamImage {
    entries: Vec<Entry>,
    _reservation: Arc<Reservation>,
}
struct Entry {
    path: String,
    content: Option<Bytes>,
    size: u64,
    modified: SystemTime,
    identity: [i128; 6],
}
struct Owner {
    data: Vec<u8>,
    _reservation: Arc<Reservation>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
pub(super) struct RamFile {
    pub content: Bytes,
    pub size: u64,
    pub modified: SystemTime,
    pub name: String,
    pub redirect: bool,
    pub identity: [i128; 6],
}
#[derive(PartialEq)]
struct Spec {
    path: String,
    directory: bool,
    size: u64,
    identity: [i128; 6],
    modified: SystemTime,
}
fn spec(path: String, metadata: &std::fs::Metadata) -> Spec {
    Spec {
        path,
        directory: metadata.is_dir(),
        size: if metadata.is_file() {
            metadata.len()
        } else {
            0
        },
        identity: identity(metadata),
        modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
    }
}
pub(super) fn identity(metadata: &std::fs::Metadata) -> [i128; 6] {
    [
        metadata.dev().into(),
        metadata.ino().into(),
        metadata.mtime().into(),
        metadata.mtime_nsec().into(),
        metadata.ctime().into(),
        metadata.ctime_nsec().into(),
    ]
}

impl RamImage {
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn reserved_bytes(&self) -> u64 {
        self._reservation.bytes
    }
    pub fn load(
        root: &File,
        config: &prost_config::StaticFiles,
        budget: &Arc<RamBudget>,
    ) -> Result<Self> {
        ensure_memory(1024, config.ram_headroom_bytes)?;
        let (planned, cost) = manifest(root, config)?;
        ensure!(
            cost <= config.max_ram_bytes,
            "static RAM route budget exceeded: need {cost}, limit {}",
            config.max_ram_bytes
        );
        ensure_memory(cost, config.ram_headroom_bytes)?;
        let reservation = budget.reserve(cost)?;
        let mut entries = Vec::new();
        entries
            .try_reserve_exact(planned.len())
            .context("cannot allocate static RAM metadata")?;
        let mut remaining = cost;
        for item in &planned {
            let content = if item.directory {
                None
            } else {
                ensure_memory(remaining, config.ram_headroom_bytes)?;
                let parts = item.path.split('/').map(str::to_owned).collect::<Vec<_>>();
                let file = open_beneath(root, &parts).context("open static RAM file")?;
                ensure!(
                    spec(item.path.clone(), &file.metadata()?) == *item,
                    "static tree changed during preparation: {}",
                    item.path
                );
                let capacity = usize::try_from(
                    item.size
                        .checked_add(1)
                        .context("static file size overflow")?,
                )
                .context("static file does not fit address space")?;
                let mut data = Vec::new();
                data.try_reserve_exact(capacity)
                    .context("cannot allocate static file RAM")?;
                let mut bounded = file.take(item.size + 1);
                bounded
                    .read_to_end(&mut data)
                    .context("read static RAM file")?;
                ensure!(
                    data.len() as u64 == item.size
                        && spec(item.path.clone(), &bounded.get_ref().metadata()?) == *item,
                    "static file changed while loading: {}",
                    item.path
                );
                Some(Bytes::from_owner(Owner {
                    data,
                    _reservation: reservation.clone(),
                }))
            };
            remaining = remaining.saturating_sub(entry_cost(item)?);
            entries.push(Entry {
                path: item.path.clone(),
                content,
                size: item.size,
                modified: item.modified,
                identity: item.identity,
            });
        }
        // A new/deleted/renamed file invalidates the whole candidate as well.
        ensure!(
            manifest(root, config)?.0 == planned,
            "static tree changed during RAM preparation"
        );
        Ok(Self {
            entries,
            _reservation: reservation,
        })
    }
    pub fn open(
        &self,
        parts: &[String],
        indexes: &[String],
        slash: bool,
    ) -> std::io::Result<RamFile> {
        let path = parts.join("/");
        let entry = self.find(&path).ok_or(std::io::ErrorKind::NotFound)?;
        let selected = if entry.content.is_none() {
            if !slash {
                return Ok(RamFile {
                    content: Bytes::new(),
                    size: 0,
                    modified: entry.modified,
                    name: String::new(),
                    redirect: true,
                    identity: entry.identity,
                });
            }
            indexes
                .iter()
                .find_map(|name| {
                    let key = if path.is_empty() {
                        name.clone()
                    } else {
                        format!("{path}/{name}")
                    };
                    self.find(&key).filter(|entry| entry.content.is_some())
                })
                .ok_or(std::io::ErrorKind::PermissionDenied)?
        } else {
            entry
        };
        Ok(RamFile {
            content: selected.content.clone().unwrap(),
            size: selected.size,
            modified: selected.modified,
            name: selected.path.rsplit('/').next().unwrap_or("").into(),
            redirect: false,
            identity: selected.identity,
        })
    }
    fn find(&self, path: &str) -> Option<&Entry> {
        self.entries
            .binary_search_by(|entry| entry.path.as_str().cmp(path))
            .ok()
            .map(|i| &self.entries[i])
    }
}
fn entry_cost(spec: &Spec) -> Result<u64> {
    // Charge bounded metadata/path overhead as well as payload, not just files.
    spec.size
        .checked_add(1024)
        .and_then(|n| n.checked_add(spec.path.len() as u64 * 2))
        .context("static RAM size overflow")
}
fn manifest(root: &File, config: &prost_config::StaticFiles) -> Result<(Vec<Spec>, u64)> {
    fn walk(
        dir: &File,
        prefix: &str,
        depth: usize,
        result: &mut Vec<Spec>,
        cost: &mut u64,
        config: &prost_config::StaticFiles,
    ) -> Result<()> {
        ensure!(depth <= 64, "static RAM directory depth exceeds 64");
        ensure!(
            result.len() < config.max_ram_files,
            "static RAM file/directory count exceeds {}",
            config.max_ram_files
        );
        let item = spec(prefix.into(), &dir.metadata()?);
        *cost = cost
            .checked_add(entry_cost(&item)?)
            .context("static RAM size overflow")?;
        ensure!(
            *cost <= config.max_ram_bytes,
            "static RAM route budget exceeded: need at least {cost}, limit {}",
            config.max_ram_bytes
        );
        let growth = if result.len() == result.capacity() {
            result
                .capacity()
                .max(4)
                .checked_mul(2)
                .and_then(|n| n.checked_mul(std::mem::size_of::<Spec>()))
                .context("static manifest capacity overflow")? as u64
        } else {
            0
        };
        ensure_memory(
            growth.saturating_add(1024 + prefix.len() as u64 * 2),
            config.ram_headroom_bytes,
        )?;
        result
            .try_reserve(1)
            .context("cannot allocate static RAM manifest")?;
        result.push(item);
        if !dir.metadata()?.is_dir() {
            return Ok(());
        }
        use std::os::fd::AsRawFd;
        for entry in std::fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("static RAM file names must be UTF-8"))?;
            let path = if prefix.is_empty() {
                name.clone()
            } else {
                format!("{prefix}/{name}")
            };
            ensure!(path.len() <= 4096, "static RAM path exceeds 4096 bytes");
            let child = open_beneath(dir, std::slice::from_ref(&name))
                .with_context(|| format!("static RAM rejects symlink/inaccessible entry {path}"))?;
            let metadata = child.metadata()?;
            ensure!(
                metadata.is_file() || metadata.is_dir(),
                "static RAM only accepts regular files/directories: {path}"
            );
            walk(&child, &path, depth + 1, result, cost, config)?;
        }
        Ok(())
    }
    let mut result = Vec::new();
    let mut cost = 0;
    walk(root, "", 0, &mut result, &mut cost, config)?;
    result.sort_unstable_by(|a, b| a.path.cmp(&b.path));
    Ok((result, cost))
}

pub(super) fn ensure_memory(bytes: u64, headroom: u64) -> Result<()> {
    check_memory(bytes, headroom, memory_available()?)
}
fn check_memory(bytes: u64, headroom: u64, available: u64) -> Result<()> {
    ensure!(bytes.checked_add(headroom).is_some_and(|needed|needed<=available),
        "insufficient available RAM for static image: need {bytes} plus {headroom} headroom, available {available}");
    Ok(())
}
fn memory_available() -> Result<u64> {
    let mem = std::fs::read_to_string("/proc/meminfo").context("cannot verify available RAM")?;
    let mut available = mem
        .lines()
        .find_map(|line| line.strip_prefix("MemAvailable:"))
        .and_then(|v| v.split_whitespace().next())
        .and_then(|v| v.parse::<u64>().ok())
        .and_then(|kb| kb.checked_mul(1024))
        .context("cannot determine MemAvailable")?;
    let mount = Path::new("/sys/fs/cgroup");
    // The namespace root covers containers. Also inspect the current v2 path
    // and every parent for a tighter systemd/service/ancestor limit.
    let group =
        std::fs::read_to_string("/proc/self/cgroup").context("cannot verify cgroup RAM limits")?;
    if let Some(path) = group.lines().find_map(|line| line.strip_prefix("0::")) {
        let mut targets = vec![mount.to_path_buf()];
        if Path::new(path)
            .components()
            .all(|p| !matches!(p, std::path::Component::ParentDir))
        {
            let mut current = mount.join(path.trim_start_matches('/'));
            while current.starts_with(mount) && current != mount {
                targets.push(current.clone());
                current.pop();
            }
        }
        let mut found = false;
        for target in targets {
            if !target.join("memory.max").exists() {
                continue;
            }
            found = true;
            let maximum = std::fs::read_to_string(target.join("memory.max"))
                .context("cannot read cgroup memory.max")?;
            if maximum.trim() == "max" {
                continue;
            }
            let maximum = maximum
                .trim()
                .parse::<u64>()
                .context("invalid cgroup memory.max")?;
            let used = std::fs::read_to_string(target.join("memory.current"))
                .context("cannot read cgroup memory.current")?
                .trim()
                .parse::<u64>()?;
            available = available.min(maximum.saturating_sub(used));
        }
        ensure!(found, "cannot verify current cgroup v2 memory controller");
    } else {
        // Do not fall back to host-wide free RAM on a v1 memory-limited host.
        bail!("static RAM preload requires a verifiable cgroup v2 memory controller");
    }
    let mut limits = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // A finite process address-space limit can be lower than available RAM.
    if unsafe { libc::getrlimit(libc::RLIMIT_AS, &mut limits) } != 0 {
        bail!("cannot verify RLIMIT_AS");
    }
    if limits.rlim_cur != libc::RLIM_INFINITY {
        let status = std::fs::read_to_string("/proc/self/status")?;
        let used = status
            .lines()
            .find_map(|l| l.strip_prefix("VmSize:"))
            .and_then(|s| s.split_whitespace().next())
            .context("cannot read VmSize")?
            .parse::<u64>()?
            .checked_mul(1024)
            .context("VmSize overflow")?;
        available = available.min(limits.rlim_cur.saturating_sub(used));
    }
    Ok(available)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn ram_budget_counts_retained_bytes_and_refuses_insufficient_headroom() {
        let budget = Arc::new(RamBudget::new(10));
        let lease = budget.reserve(8).unwrap();
        assert!(budget.reserve(3).is_err());
        let clone = lease.clone();
        drop(lease);
        assert_eq!(budget.used(), 8);
        let bytes = Bytes::from_owner(Owner {
            data: vec![0; 8],
            _reservation: clone,
        });
        let retained = bytes.slice(1..3);
        drop(bytes);
        assert_eq!(budget.used(), 8);
        assert!(budget.reserve(3).is_err());
        drop(retained);
        assert_eq!(budget.used(), 0);
        assert!(check_memory(8, 3, 10).is_err());
        assert!(check_memory(u64::MAX, 1, u64::MAX).is_err());
        assert!(check_memory(8, 2, 10).is_ok());
    }
}
