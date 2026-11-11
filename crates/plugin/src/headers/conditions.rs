use super::{maps, Eval, HeaderContext, Names, Value};
use anyhow::{ensure, Context, Result};
use http::{HeaderMap, HeaderValue};
use regex::Regex;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    source: String,
    #[serde(default)]
    equals: Option<String>,
    #[serde(default)]
    not_equals: Option<String>,
    #[serde(default)]
    matches: Option<String>,
    #[serde(default)]
    not_matches: Option<String>,
    #[serde(default)]
    not_empty: Option<bool>,
    #[serde(default)]
    truthy: Option<bool>,
}
impl Config {
    pub fn bytes(&self) -> usize {
        self.source.len()
            + [
                &self.equals,
                &self.not_equals,
                &self.matches,
                &self.not_matches,
            ]
            .into_iter()
            .filter_map(|v| v.as_ref())
            .map(String::len)
            .sum::<usize>()
    }
}
pub(super) struct Condition {
    source: Value,
    equals: Option<(Value, bool)>,
    regex: Option<(Regex, bool)>,
    not_empty: Option<bool>,
    truthy: Option<bool>,
}
impl Condition {
    pub fn compile(config: Config, names: &Names, budget: &mut usize) -> Result<(Self, u8)> {
        let count = [
            config.equals.is_some(),
            config.not_equals.is_some(),
            config.matches.is_some(),
            config.not_matches.is_some(),
            config.not_empty.is_some(),
            config.truthy.is_some(),
        ]
        .into_iter()
        .filter(|yes| *yes)
        .count();
        ensure!(count == 1, "condition requires exactly one comparison");
        let (source, mut needs) = Value::compile_in(config.source, names, false)?;
        let equals = if let Some(value) = config.equals {
            let (v, b) = Value::compile_in(value, names, false)?;
            needs |= b;
            Some((v, false))
        } else if let Some(value) = config.not_equals {
            let (v, b) = Value::compile_in(value, names, false)?;
            needs |= b;
            Some((v, true))
        } else {
            None
        };
        let regex = if let Some(pattern) = config.matches {
            Some((maps::regex(&pattern, false, budget)?, false))
        } else if let Some(pattern) = config.not_matches {
            Some((maps::regex(&pattern, false, budget)?, true))
        } else {
            None
        };
        Ok((
            Self {
                source,
                equals,
                regex,
                not_empty: config.not_empty,
                truthy: config.truthy,
            },
            needs,
        ))
    }
    pub fn values(&self) -> impl Iterator<Item = &Value> {
        std::iter::once(&self.source).chain(self.equals.iter().map(|(v, _)| v))
    }
    pub fn matches(
        &self,
        status: u16,
        ctx: Option<&HeaderContext>,
        headers: &HeaderMap,
        eval: &mut Eval<'_>,
    ) -> Result<bool> {
        let value: HeaderValue = self.source.render(status, ctx, headers, eval)?;
        Ok(if let Some((right, invert)) = &self.equals {
            (value == right.render(status, ctx, headers, eval)?) ^ invert
        } else if let Some((regex, invert)) = &self.regex {
            regex.is_match(value.to_str().context("condition source is not text")?) ^ invert
        } else if let Some(want) = self.not_empty {
            value.is_empty() != want
        } else {
            (!value.is_empty() && value.as_bytes() != b"0") == self.truthy.unwrap_or(true)
        })
    }
}
