use super::{HeaderContext, Value};
use anyhow::{ensure, Context, Result};
use http::{HeaderMap, HeaderName, HeaderValue};
use regex::{Regex, RegexBuilder};
use serde::Deserialize;
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
};

pub(super) type Names = BTreeMap<String, usize>;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MapConfig {
    source: String,
    #[serde(default)]
    default: String,
    #[serde(default)]
    case_sensitive: bool,
    #[serde(default)]
    volatile: bool,
    #[serde(default)]
    entries: Vec<EntryConfig>,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    #[default]
    Exact,
    Prefix,
    Suffix,
    Regex,
    RegexI,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryConfig {
    pattern: String,
    value: String,
    #[serde(default)]
    kind: Kind,
}
pub(super) fn regex(pattern: &str, insensitive: bool, budget: &mut usize) -> Result<Regex> {
    ensure!(pattern.len() <= 1024, "regex pattern exceeds 1 KiB");
    *budget += 1;
    ensure!(*budget <= 8, "at most 8 regex rules per header plugin");
    Ok(RegexBuilder::new(pattern)
        .case_insensitive(insensitive)
        .size_limit(256 * 1024)
        .dfa_size_limit(256 * 1024)
        .nest_limit(64)
        .build()?)
}
pub(super) struct Map {
    source: Value,
    default: Value,
    exact: HashMap<String, Value>,
    prefixes: Vec<(String, Value)>,
    suffixes: Vec<(String, Value)>,
    regexes: Vec<(Regex, Value)>,
    sensitive: bool,
    volatile: bool,
    needs: u8,
    dependencies: Vec<usize>,
}
impl Map {
    fn values(&self) -> impl Iterator<Item = &Value> {
        std::iter::once(&self.source)
            .chain(std::iter::once(&self.default))
            .chain(self.exact.values())
            .chain(self.prefixes.iter().map(|(_, v)| v))
            .chain(self.suffixes.iter().map(|(_, v)| v))
            .chain(self.regexes.iter().map(|(_, v)| v))
    }
    pub fn requirements(&self, upstream: bool, names: &mut Vec<HeaderName>) {
        for value in self.values() {
            value.inputs(upstream, names);
        }
    }
}
pub(super) fn compile(
    config: BTreeMap<String, MapConfig>,
    budget: &mut usize,
) -> Result<(Names, Vec<Map>)> {
    ensure!(config.len() <= 16, "at most 16 maps per header plugin");
    ensure!(
        config.values().map(|m| m.entries.len()).sum::<usize>() <= 256,
        "at most 256 map entries per header plugin"
    );
    let mut names = Names::new();
    for name in config.keys() {
        ensure!(
            !name.is_empty()
                && name.len() <= 64
                && name
                    .bytes()
                    .enumerate()
                    .all(|(i, b)| b.is_ascii_alphabetic()
                        || b == b'_'
                        || i > 0 && b.is_ascii_digit()),
            "invalid map variable name"
        );
        ensure!(
            !matches!(
                name.as_str(),
                "remote_addr"
                    | "scheme"
                    | "host"
                    | "request_method"
                    | "request_uri"
                    | "uri"
                    | "upstream_addr"
                    | "status"
                    | "request_id"
            ) && !name.starts_with("http_")
                && !name.starts_with("upstream_http_")
                && !name.starts_with("sent_http_"),
            "map variable shadows a built-in variable"
        );
        names.insert(name.clone(), names.len());
    }
    let mut maps = Vec::with_capacity(config.len());
    for (_, config) in config {
        let (source, mut needs) = Value::compile_in(config.source, &names, false)?;
        let (default, bits) = Value::compile_in(config.default, &names, false)?;
        needs |= bits;
        let mut map = Map {
            source,
            default,
            exact: HashMap::new(),
            prefixes: Vec::new(),
            suffixes: Vec::new(),
            regexes: Vec::new(),
            sensitive: config.case_sensitive,
            volatile: config.volatile,
            needs,
            dependencies: Vec::new(),
        };
        for entry in config.entries {
            ensure!(entry.pattern.len() <= 1024, "map pattern exceeds 1 KiB");
            let (value, bits) = Value::compile_in(
                entry.value,
                &names,
                matches!(entry.kind, Kind::Regex | Kind::RegexI),
            )?;
            map.needs |= bits;
            let pattern = if config.case_sensitive {
                entry.pattern.clone()
            } else {
                entry.pattern.to_ascii_lowercase()
            };
            match entry.kind {
                Kind::Exact => ensure!(
                    map.exact.insert(pattern, value).is_none(),
                    "duplicate exact map entry"
                ),
                Kind::Prefix => map.prefixes.push((pattern, value)),
                Kind::Suffix => map.suffixes.push((pattern, value)),
                Kind::Regex | Kind::RegexI => map.regexes.push((
                    regex(&entry.pattern, matches!(entry.kind, Kind::RegexI), budget)?,
                    value,
                )),
            }
        }
        map.prefixes
            .sort_by_key(|(p, _)| std::cmp::Reverse(p.len()));
        map.suffixes
            .sort_by_key(|(p, _)| std::cmp::Reverse(p.len()));
        let mut dependencies = Vec::new();
        for value in map.values() {
            value.maps(&mut dependencies);
        }
        dependencies.sort_unstable();
        dependencies.dedup();
        map.dependencies = dependencies;
        maps.push(map);
    }
    let mut state = vec![0u8; maps.len()];
    fn visit(index: usize, maps: &[Map], state: &mut [u8]) -> Result<()> {
        ensure!(state[index] != 1, "cyclic map variables");
        if state[index] == 2 {
            return Ok(());
        }
        state[index] = 1;
        for &dep in &maps[index].dependencies {
            visit(dep, maps, state)?;
        }
        state[index] = 2;
        Ok(())
    }
    for index in 0..maps.len() {
        visit(index, &maps, &mut state)?;
    }
    Ok((names, maps))
}
pub(super) fn requirements(
    maps: &[Map],
    roots: &[usize],
    request: &mut Vec<HeaderName>,
    upstream: &mut Vec<HeaderName>,
) -> u8 {
    fn visit(
        i: usize,
        maps: &[Map],
        seen: &mut [bool; 16],
        request: &mut Vec<HeaderName>,
        upstream: &mut Vec<HeaderName>,
    ) -> u8 {
        if seen[i] {
            return 0;
        }
        seen[i] = true;
        maps[i].requirements(false, request);
        maps[i].requirements(true, upstream);
        maps[i].needs
            | maps[i]
                .dependencies
                .iter()
                .fold(0, |bits, &i| bits | visit(i, maps, seen, request, upstream))
    }
    let mut seen = [false; 16];
    roots.iter().fold(0, |bits, &i| {
        bits | visit(i, maps, &mut seen, request, upstream)
    })
}
pub(super) struct Eval<'a> {
    maps: &'a [Map],
    results: Vec<Option<HeaderValue>>,
    captures: Vec<HeaderValue>,
    bytes: usize,
    evaluations: usize,
}
impl<'a> Eval<'a> {
    pub fn new(maps: &'a [Map]) -> Self {
        Self {
            maps,
            results: Vec::new(),
            captures: Vec::new(),
            bytes: 0,
            evaluations: 0,
        }
    }
    pub fn capture(&self, index: usize) -> HeaderValue {
        self.captures
            .get(index)
            .cloned()
            .unwrap_or_else(|| HeaderValue::from_static(""))
    }
    pub fn get(
        &mut self,
        index: usize,
        status: u16,
        ctx: Option<&HeaderContext>,
        headers: &HeaderMap,
    ) -> Result<HeaderValue> {
        let maps = self.maps;
        let map = &maps[index];
        if !map.volatile {
            if let Some(Some(value)) = self.results.get(index) {
                return Ok(value.clone());
            }
        }
        self.evaluations += 1;
        ensure!(self.evaluations <= 128, "map evaluation budget exceeded");
        let source = map.source.render(status, ctx, headers, self)?;
        let source = source.to_str().context("map source is not text")?;
        let key = if !map.sensitive && source.bytes().any(|b| b.is_ascii_uppercase()) {
            Cow::Owned(source.to_ascii_lowercase())
        } else {
            Cow::Borrowed(source)
        };
        let mut selected = map.exact.get(key.as_ref());
        if selected.is_none() {
            selected = map
                .prefixes
                .iter()
                .find(|(p, _)| key.starts_with(p))
                .map(|(_, v)| v);
        }
        if selected.is_none() {
            selected = map
                .suffixes
                .iter()
                .find(|(p, _)| key.ends_with(p))
                .map(|(_, v)| v);
        }
        let mut captures = None;
        if selected.is_none() {
            for (regex, value) in &map.regexes {
                if !value.captures() {
                    if regex.is_match(source) {
                        selected = Some(value);
                        break;
                    }
                    continue;
                }
                if let Some(found) = regex.captures(source) {
                    let values = (0..found.len().min(10))
                        .map(|i| HeaderValue::from_str(found.get(i).map_or("", |m| m.as_str())))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    ensure!(
                        values.iter().map(|v| v.as_bytes().len()).sum::<usize>() <= 32768,
                        "map captures exceed 32 KiB"
                    );
                    captures = Some(values);
                    selected = Some(value);
                    break;
                }
            }
        }
        let previous = captures.map(|values| std::mem::replace(&mut self.captures, values));
        let result = selected
            .unwrap_or(&map.default)
            .render(status, ctx, headers, self);
        if let Some(previous) = previous {
            self.captures = previous;
        }
        let value = result?;
        if !map.volatile {
            if self.results.is_empty() {
                self.results.resize_with(maps.len(), || None);
            }
            self.bytes += value.as_bytes().len();
            ensure!(self.bytes <= 32768, "map cache exceeds 32 KiB");
            self.results[index] = Some(value.clone());
        }
        Ok(value)
    }
}
