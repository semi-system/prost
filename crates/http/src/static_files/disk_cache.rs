//! Metadata-validated, bounded demand cache for disk-backed static files.
//! Cache pressure is a bypass, never a reason to reject disk traffic.
use super::{ram, RamBudget};
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::io::AsyncReadExt;

#[derive(Clone, PartialEq, Eq)]
pub(super) struct Stamp {
    len: u64,
    identity: [i128; 6],
}
impl Stamp {
    pub fn new(m: &std::fs::Metadata) -> Self {
        Self {
            len: m.len(),
            identity: ram::identity(m),
        }
    }
}
struct Entry {
    stamp: Stamp,
    data: Bytes,
}
#[derive(Default)]
struct State {
    entries: HashMap<String, Entry>,
    order: VecDeque<String>,
    pending: HashSet<String>,
    hits: u64,
    misses: u64,
}
pub(super) struct DiskCache {
    state: Mutex<State>,
    budget: Arc<RamBudget>,
    global: Arc<RamBudget>,
    max_file: u64,
    max_entries: usize,
    headroom: u64,
}
impl DiskCache {
    pub fn new(c: &prost_config::StaticFiles, global: &Arc<RamBudget>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State::default()),
            budget: Arc::new(RamBudget::new(c.max_ram_bytes)),
            global: global.clone(),
            max_file: c.cache_max_file_bytes,
            max_entries: c.max_ram_files,
            headroom: c.ram_headroom_bytes,
        })
    }
    pub fn lookup(&self, key: &str, stamp: &Stamp) -> Option<Bytes> {
        let mut state = self.state.lock().unwrap();
        if let Some(entry) = state.entries.get(key) {
            if entry.stamp == *stamp {
                let data = entry.data.clone();
                state.hits += 1;
                return Some(data);
            }
            state.entries.remove(key);
            state.order.retain(|name| name != key);
        }
        state.misses += 1;
        None
    }
    pub fn stats(&self) -> super::StaticStats {
        let state = self.state.lock().unwrap();
        super::StaticStats {
            mode: "disk",
            entries: state.entries.len(),
            reserved_bytes: self.budget.used(),
            hits: state.hits,
            misses: state.misses,
        }
    }
    fn begin(self: &Arc<Self>, key: &str, stamp: &Stamp) -> Option<Fill> {
        if self.max_file == 0 || stamp.len > self.max_file {
            return None;
        }
        let cost = stamp
            .len
            .checked_add(1024)?
            .checked_add(key.len() as u64 * 2)?;
        if cost > self.budget.limit() {
            return None;
        }
        let mut state = self.state.lock().unwrap();
        if state.pending.contains(key) {
            return None;
        }
        while state.entries.len() + state.pending.len() >= self.max_entries
            || self
                .budget
                .used()
                .checked_add(cost)
                .is_none_or(|v| v > self.budget.limit())
        {
            let old = state.order.pop_front()?;
            state.entries.remove(&old);
        }
        let global = self.global.reserve(cost).ok()?;
        let local = self.budget.reserve(cost).ok()?;
        state.pending.insert(key.into());
        Some(Fill {
            cache: self.clone(),
            key: key.into(),
            stamp: stamp.clone(),
            global,
            local,
        })
    }
    pub async fn read(
        self: &Arc<Self>,
        file: &mut tokio::fs::File,
        key: &str,
        stamp: &Stamp,
    ) -> std::io::Result<Option<Bytes>> {
        let Some(fill) = self.begin(key, stamp) else {
            return Ok(None);
        };
        // Probe outside the cache mutex. Failure merely streams from disk.
        if ram::ensure_memory(stamp.len + 1024, self.headroom).is_err() {
            return Ok(None);
        }
        let Ok(capacity) = usize::try_from(stamp.len.saturating_add(1)) else {
            return Ok(None);
        };
        let mut data = Vec::new();
        if data.try_reserve_exact(capacity).is_err() {
            return Ok(None);
        }
        (&mut *file)
            .take(stamp.len + 1)
            .read_to_end(&mut data)
            .await?;
        if data.len() as u64 != stamp.len || Stamp::new(&file.metadata().await?) != *stamp {
            return Ok(None);
        }
        let bytes = Bytes::from_owner(Owner {
            data,
            _global: fill.global.clone(),
            _local: fill.local.clone(),
        });
        let mut state = self.state.lock().unwrap();
        state.entries.insert(
            key.into(),
            Entry {
                stamp: fill.stamp.clone(),
                data: bytes.clone(),
            },
        );
        state.order.retain(|name| name != key);
        state.order.push_back(key.into());
        Ok(Some(bytes))
    }
}
struct Fill {
    cache: Arc<DiskCache>,
    key: String,
    stamp: Stamp,
    global: Arc<ram::Reservation>,
    local: Arc<ram::Reservation>,
}
impl Drop for Fill {
    fn drop(&mut self) {
        self.cache.state.lock().unwrap().pending.remove(&self.key);
    }
}
struct Owner {
    data: Vec<u8>,
    _global: Arc<ram::Reservation>,
    _local: Arc<ram::Reservation>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}
