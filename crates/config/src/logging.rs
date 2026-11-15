use super::*;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AccessLog {
    pub sink: LogSink,
    pub format: LogFormat,
    pub statuses: Vec<u16>,
    pub include_paths: Vec<String>,
    pub exclude_paths: Vec<String>,
    pub path: PathBuf,
    pub queue_capacity: usize,
    pub sample_every: u64,
    pub include_query: bool,
    pub max_path_bytes: usize,
    pub flush_interval_ms: u64,
}
impl Default for AccessLog {
    fn default() -> Self {
        Self {
            sink: LogSink::File,
            format: LogFormat::Json,
            statuses: Vec::new(),
            include_paths: Vec::new(),
            exclude_paths: Vec::new(),
            path: PathBuf::new(),
            queue_capacity: 2048,
            sample_every: 1,
            include_query: false,
            max_path_bytes: 1024,
            flush_interval_ms: 1000,
        }
    }
}
impl AccessLog {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.statuses.len() <= 500 && self.statuses.iter().all(|s| (100..=599).contains(s)),
            "invalid access log statuses"
        );
        ensure!(
            self.include_paths.len() <= 64
                && self.exclude_paths.len() <= 64
                && self
                    .include_paths
                    .iter()
                    .chain(&self.exclude_paths)
                    .all(|s| s.starts_with('/') && s.len() <= 4096),
            "invalid access log path filters"
        );
        ensure!(
            match self.sink {
                LogSink::File => !self.path.as_os_str().is_empty(),
                LogSink::Stdout => self.path.as_os_str().is_empty(),
            },
            "file access log requires path; stdout access log must omit path"
        );
        ensure!(
            (1..=65536).contains(&self.queue_capacity),
            "access log queue_capacity must be 1..65536"
        );
        ensure!(
            (1..=1_000_000).contains(&self.sample_every),
            "access log sample_every must be 1..1000000"
        );
        ensure!(
            (8..=4096).contains(&self.max_path_bytes),
            "access log max_path_bytes must be 8..4096"
        );
        ensure!(
            (10..=60000).contains(&self.flush_interval_ms),
            "access log flush_interval_ms must be 10..60000"
        );
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogSink {
    #[default]
    File,
    Stdout,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogFormat {
    #[default]
    Json,
    Compact,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn log_schema_roundtrip_and_budgets() -> Result<()> {
        let c = AccessLog {
            path: "/var/log/prost/access.jsonl".into(),
            ..Default::default()
        };
        c.validate()?;
        let decoded: AccessLog = toml::from_str(&toml::to_string(&c)?)?;
        assert_eq!(c, decoded);
        for text in [
            "path='x'\nqueue_capacity=0",
            "path='x'\nqueue_capacity=65537",
            "path='x'\nsample_every=0",
            "path='x'\nsample_every=1000001",
            "path='x'\nmax_path_bytes=7",
            "path='x'\nmax_path_bytes=4097",
            "path='x'\nflush_interval_ms=9",
            "path='x'\nflush_interval_ms=60001",
            "path=''",
        ] {
            assert!(
                toml::from_str::<AccessLog>(text)?.validate().is_err(),
                "{text}"
            );
        }
        assert!(toml::from_str::<AccessLog>("path='x'\nunknown=true").is_err());
        toml::from_str::<AccessLog>("sink='stdout'")?.validate()?;
        assert!(
            toml::from_str::<AccessLog>("sink='stdout'\npath='/dev/stdout'")?
                .validate()
                .is_err()
        );
        Ok(())
    }
}
