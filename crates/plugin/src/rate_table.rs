//! Administrative access to bounded native limiter state, outside traffic hooks.
use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::IpAddr;

pub const MAX_ROWS: usize = 1024;
fn default_limit() -> usize {
    100
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyEncoding {
    #[default]
    Text,
    Hex,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableKey {
    pub value: String,
    #[serde(default)]
    pub encoding: KeyEncoding,
}
impl TableKey {
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            encoding: KeyEncoding::Text,
        }
    }
    pub(crate) fn bytes(&self) -> Result<Vec<u8>> {
        match self.encoding {
            KeyEncoding::Text => {
                ensure!(self.value.len() <= 256, "rate table key exceeds 256 bytes");
                Ok(self.value.as_bytes().to_vec())
            }
            KeyEncoding::Hex => {
                ensure!(
                    self.value.len() <= 512 && self.value.len().is_multiple_of(2),
                    "invalid rate table hex key"
                );
                self.value
                    .as_bytes()
                    .chunks_exact(2)
                    .map(|pair| {
                        let digit = |v: u8| {
                            (v as char)
                                .to_digit(16)
                                .ok_or_else(|| anyhow::anyhow!("invalid rate table hex key"))
                        };
                        Ok(((digit(pair[0])? << 4) | digit(pair[1])?) as u8)
                    })
                    .collect()
            }
        }
    }
    pub(crate) fn ip(&self) -> Result<IpAddr> {
        ensure!(
            matches!(self.encoding, KeyEncoding::Text),
            "IP keys require text encoding"
        );
        ensure!(self.value.len() <= 45, "invalid IP rate table key");
        Ok(canonical_ip(self.value.parse()?))
    }
    pub(crate) fn header(bytes: &[u8]) -> Self {
        use std::fmt::Write;
        let mut value = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            write!(value, "{byte:02x}").unwrap();
        }
        Self {
            value,
            encoding: KeyEncoding::Hex,
        }
    }
}
pub(crate) fn canonical_ip(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v) => v.to_ipv4_mapped().map(IpAddr::V4).unwrap_or(IpAddr::V6(v)),
        v => v,
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TableQuery {
    pub key: Option<TableKey>,
    pub limit: usize,
}
impl Default for TableQuery {
    fn default() -> Self {
        Self {
            key: None,
            limit: default_limit(),
        }
    }
}
impl TableQuery {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            (1..=MAX_ROWS).contains(&self.limit),
            "rate table limit must be 1..=1024"
        );
        if let Some(key) = &self.key {
            key.bytes()?;
        }
        Ok(())
    }
}
/// No filesystem access or async traffic work. Implementations lock one shard
/// at a time; views and clear-all are deliberately not a global traffic barrier.
pub trait RateTable: Send + Sync {
    fn inspect(&self, query: &TableQuery) -> Result<Value>;
    fn clear(&self, key: Option<&TableKey>) -> Result<usize>;
}
pub(crate) fn view(
    algorithm: &str,
    key_kind: &str,
    capacity: usize,
    retained: usize,
    entries: Vec<Value>,
    filtered: bool,
) -> Value {
    json!({"algorithm":algorithm,"key_kind":key_kind,"capacity":capacity,
           "retained_entries":retained,"returned":entries.len(),
           "truncated":!filtered && retained>entries.len(),"entries":entries,
           "atomic_snapshot":false})
}
