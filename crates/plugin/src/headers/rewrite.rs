use super::{conditions, maps, Condition, Eval, Names, Part, Value, Variable};
use crate::{FilterCall, FilterDecision, FilterHook, NativeFilter};
use anyhow::{bail, ensure, Context, Result};
use http::{HeaderMap, HeaderName, HeaderValue, Uri};
use regex::Regex;
use serde::Deserialize;
use std::net::IpAddr;
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    rules: Vec<RuleConfig>,
}
#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    Rewrite,
    Redirect,
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Query {
    #[default]
    Preserve,
    Drop,
    Append,
}
fn stop() -> bool {
    true
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleConfig {
    action: Action,
    to: String,
    #[serde(default)]
    exact: Option<String>,
    #[serde(default)]
    prefix: Option<String>,
    #[serde(default)]
    regex: Option<String>,
    #[serde(default)]
    when: Option<conditions::Config>,
    #[serde(default)]
    status: Option<u16>,
    #[serde(default)]
    query: Query,
    #[serde(default = "stop")]
    stop: bool,
}
enum Matcher {
    Any,
    Exact(String),
    Prefix(String),
    Regex(Regex),
}
struct Rule {
    action: Action,
    to: Value,
    matcher: Matcher,
    when: Option<Condition>,
    status: u16,
    query: Query,
    stop: bool,
    literal_path: Option<http::uri::PathAndQuery>,
    literal_redirect: Option<Arc<crate::Redirect>>,
}
pub(crate) struct Rewrite {
    rules: Vec<Rule>,
    needs: u8,
    inputs: Vec<HeaderName>,
}
impl Rewrite {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let config: Config = serde_json::from_value(value)?;
        ensure!(
            !config.rules.is_empty() && config.rules.len() <= 32,
            "rewrite needs 1..32 rules"
        );
        let names = Names::new();
        let (mut rules, mut inputs, mut needs, mut budget, mut bytes) =
            (Vec::new(), Vec::new(), 0, 0, 0);
        for c in config.rules {
            bytes += c.to.len()
                + c.exact.as_ref().map_or(0, String::len)
                + c.prefix.as_ref().map_or(0, String::len)
                + c.regex.as_ref().map_or(0, String::len)
                + c.when.as_ref().map_or(0, conditions::Config::bytes);
            ensure!(bytes <= 32768, "rewrite rules exceed 32 KiB");
            ensure!(
                usize::from(c.exact.is_some())
                    + usize::from(c.prefix.is_some())
                    + usize::from(c.regex.is_some())
                    <= 1,
                "rewrite rule accepts only one of exact, prefix, regex"
            );
            let matcher = if let Some(p) = c.exact {
                path_match(&p)?;
                Matcher::Exact(p)
            } else if let Some(p) = c.prefix {
                path_match(&p)?;
                Matcher::Prefix(p)
            } else if let Some(p) = c.regex {
                Matcher::Regex(maps::regex(&p, false, &mut budget)?)
            } else {
                Matcher::Any
            };
            let (to, bits) = Value::compile_in(c.to, &names, matches!(matcher, Matcher::Regex(_)))?;
            needs |= bits;
            let mut literal_path = None;
            if let Value::Literal(value) = &to {
                valid_target(value.to_str()?, c.action)?;
                if matches!(c.action, Action::Rewrite) {
                    literal_path = Some(value.to_str()?.parse()?);
                }
            }
            let when = c
                .when
                .map(|c| -> Result<_> {
                    let (condition, bits) = Condition::compile(c, &names, &mut budget)?;
                    needs |= bits;
                    Ok(condition)
                })
                .transpose()?;
            for value in std::iter::once(&to).chain(when.iter().flat_map(Condition::values)) {
                if let Value::Template(parts) = value {
                    ensure!(
                        !parts.iter().any(|p| matches!(
                            p,
                            Part::Variable(
                                Variable::Status
                                    | Variable::UpstreamAddr
                                    | Variable::Upstream(_)
                                    | Variable::Sent(_)
                            )
                        )),
                        "response variables cannot be used in rewrite rules"
                    );
                }
                value.inputs(false, &mut inputs);
            }
            let status = c.status.unwrap_or(302);
            match c.action {
                Action::Rewrite => {
                    ensure!(c.status.is_none(), "rewrite action does not accept status")
                }
                Action::Redirect => ensure!(
                    matches!(status, 301 | 302 | 303 | 307 | 308),
                    "invalid redirect status"
                ),
            }
            let literal_redirect = if matches!(c.action, Action::Redirect) {
                if let Value::Literal(value) = &to {
                    Some(Arc::new(crate::Redirect {
                        status,
                        location: value.clone(),
                    }))
                } else {
                    None
                }
            } else {
                None
            };
            rules.push(Rule {
                action: c.action,
                to,
                matcher,
                when,
                status,
                query: c.query,
                stop: c.stop,
                literal_path,
                literal_redirect,
            });
        }
        inputs.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        inputs.dedup();
        Ok(Self {
            rules,
            needs,
            inputs,
        })
    }
}
fn path_match(path: &str) -> Result<()> {
    ensure!(
        path.starts_with('/')
            && path.len() <= 1024
            && !path.contains(['?', '#'])
            && !path.bytes().any(|b| b <= 32 || b >= 127),
        "invalid rewrite path matcher"
    );
    Ok(())
}
fn valid_target(target: &str, action: Action) -> Result<()> {
    ensure!(
        !target.is_empty()
            && target.len() <= 8192
            && !target.bytes().any(|b| b <= 32 || b >= 127 || b == b'\\'),
        "invalid rewrite/redirect target"
    );
    let base = target.split_once('#').map_or(target, |(base, _)| base);
    match action {
        Action::Rewrite => {
            ensure!(
                target.starts_with('/') && !target.starts_with("//") && !target.contains('#'),
                "rewrite target must be an origin path without fragment"
            );
            let _: http::uri::PathAndQuery = target.parse()?;
        }
        Action::Redirect => {
            if base.starts_with('/') {
                ensure!(
                    !base.starts_with("//"),
                    "protocol-relative redirect is unsupported"
                );
                let _: http::uri::PathAndQuery = base.parse()?;
            } else {
                let uri: Uri = base.parse()?;
                ensure!(
                    matches!(uri.scheme_str(), Some("http" | "https"))
                        && uri.authority().is_some()
                        && !uri.authority().unwrap().as_str().contains('@'),
                    "redirect requires an http(s) URL or absolute path"
                );
            }
        }
    }
    Ok(())
}
fn target(value: &str, suffix: &str, old_query: Option<&str>, query: Query) -> Result<String> {
    let (base, fragment) = value
        .split_once('#')
        .map_or((value, None), |(b, f)| (b, Some(f)));
    let (path, new_query) = base
        .split_once('?')
        .map_or((base, None), |(p, q)| (p, Some(q)));
    let inherited = match query {
        Query::Drop => None,
        Query::Preserve if new_query.is_some() => None,
        Query::Append if new_query.is_some() => old_query.filter(|s| !s.is_empty()),
        _ => old_query,
    };
    let needs_slash = !suffix.is_empty()
        && !suffix.starts_with('/')
        && path
            .split_once("://")
            .is_some_and(|(_, rest)| !rest.contains('/'));
    let length = value
        .len()
        .saturating_add(suffix.len())
        .saturating_add(inherited.map_or(0, |s| s.len() + 1))
        .saturating_add(usize::from(needs_slash));
    ensure!(length <= 8192, "expanded rewrite target exceeds 8 KiB");
    let mut text = String::with_capacity(length);
    text.push_str(path);
    if needs_slash {
        text.push('/');
    }
    text.push_str(suffix);
    if let Some(q) = new_query {
        text.push('?');
        text.push_str(q);
        if matches!(query, Query::Append) {
            if let Some(old) = old_query.filter(|s| !s.is_empty()) {
                if !q.is_empty() {
                    text.push('&');
                }
                text.push_str(old);
            }
        }
    } else if !matches!(query, Query::Drop) {
        if let Some(old) = old_query {
            text.push('?');
            text.push_str(old);
        }
    }
    if let Some(fragment) = fragment {
        text.push('#');
        text.push_str(fragment);
    }
    ensure!(text.len() <= 8192, "expanded rewrite target exceeds 8 KiB");
    Ok(text)
}
impl NativeFilter for Rewrite {
    fn uses_request_uri(&self) -> bool {
        true
    }
    fn context_needs(&self) -> u8 {
        self.needs
    }
    fn header_inputs(&self, upstream: bool) -> &[HeaderName] {
        if upstream {
            &[]
        } else {
            &self.inputs
        }
    }
    fn applies_to(&self, hook: FilterHook) -> bool {
        hook == FilterHook::RequestHeaders
    }
    fn filter(
        &self,
        _: FilterHook,
        _: IpAddr,
        _: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        bail!("rewrite requires a request URI")
    }
    fn filter_request(
        &self,
        call: &FilterCall<'_>,
        uri: &mut Uri,
        headers: &mut HeaderMap,
    ) -> Result<FilterDecision> {
        for rule in &self.rules {
            let mut eval = Eval::new(&[]);
            eval.current_uri = Some(uri);
            let suffix = match &rule.matcher {
                Matcher::Any => "",
                Matcher::Exact(p) => {
                    if uri.path() != p {
                        continue;
                    }
                    ""
                }
                Matcher::Prefix(p) => {
                    let Some(suffix) = uri.path().strip_prefix(p) else {
                        continue;
                    };
                    suffix
                }
                Matcher::Regex(regex) => {
                    if rule.to.captures() {
                        let Some(captures) = regex.captures(uri.path()) else {
                            continue;
                        };
                        eval.set_captures(captures)?;
                    } else if !regex.is_match(uri.path()) {
                        continue;
                    }
                    ""
                }
            };
            if !rule
                .when
                .as_ref()
                .map(|c| c.matches(200, call.context, headers, &mut eval))
                .transpose()?
                .unwrap_or(true)
            {
                continue;
            }
            if suffix.is_empty() && (uri.query().is_none() || matches!(rule.query, Query::Drop)) {
                if let Value::Literal(_) = &rule.to {
                    match rule.action {
                        Action::Redirect => {
                            return Ok(FilterDecision::Redirect(
                                rule.literal_redirect.as_ref().unwrap().clone(),
                            ))
                        }
                        Action::Rewrite => {
                            set_uri(uri, rule.literal_path.as_ref().unwrap().clone())?;
                            if rule.stop {
                                break;
                            } else {
                                continue;
                            }
                        }
                    }
                }
            }
            let value = rule.to.render(200, call.context, headers, &mut eval)?;
            let output = target(value.to_str()?, suffix, uri.query(), rule.query)?;
            valid_target(&output, rule.action)?;
            match rule.action {
                Action::Redirect => {
                    return Ok(FilterDecision::Redirect(Arc::new(crate::Redirect {
                        status: rule.status,
                        location: HeaderValue::try_from(output)?,
                    })))
                }
                Action::Rewrite => {
                    set_uri(uri, output.try_into()?)?;
                    if rule.stop {
                        break;
                    }
                }
            }
        }
        Ok(FilterDecision::Continue)
    }
}
fn set_uri(uri: &mut Uri, path: http::uri::PathAndQuery) -> Result<()> {
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(path);
    *uri = Uri::from_parts(parts).context("construct rewritten URI")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn apply(rules: serde_json::Value, path: &str) -> Result<(Uri, FilterDecision)> {
        let filter = Rewrite::new(serde_json::json!({"rules":rules}))?;
        let mut uri: Uri = path.parse()?;
        let decision = filter.filter_request(
            &FilterCall {
                hook: FilterHook::RequestHeaders,
                peer: "127.0.0.1".parse()?,
                status: 200,
                context: None,
                inherited_skip: 0,
            },
            &mut uri,
            &mut HeaderMap::new(),
        )?;
        Ok((uri, decision))
    }
    #[test]
    fn ordered_prefix_regex_query_and_current_uri() -> Result<()> {
        let (uri, _) = apply(
            serde_json::json!([
                {"action":"rewrite","prefix":"/old/","to":"/v1/","stop":false},
                {"action":"rewrite","regex":"^/v1/(.*)$","to":"/v2/$1?new=2","query":"append"}
            ]),
            "/old/a%2Fb?x=1",
        )?;
        assert_eq!(uri, "/v2/a%2Fb?new=2&x=1");
        for (query, expected) in [
            ("preserve", "/next?old=1"),
            ("drop", "/next"),
            ("append", "/next?old=1"),
        ] {
            let (uri, _) = apply(
                serde_json::json!([{"action":"rewrite","exact":"/old","to":"/next","query":query}]),
                "/old?old=1",
            )?;
            assert_eq!(uri, expected);
        }
        let (uri, _) = apply(
            serde_json::json!([
                {"action":"rewrite","exact":"/old","to":"/new","stop":false},
                {"action":"rewrite","to":"$uri/final","when":{"source":"$uri","equals":"/new"}}
            ]),
            "/old",
        )?;
        assert_eq!(uri, "/new/final");
        assert_eq!(
            target("/next?", "", Some("old=1"), Query::Preserve)?,
            "/next?"
        );
        assert!(target("/next", "", Some(&"a".repeat(8192)), Query::Preserve).is_err());
        Ok(())
    }
    #[test]
    fn redirect_codes_fragments_and_prefix_cannot_change_authority() -> Result<()> {
        for status in [301, 302, 303, 307, 308] {
            let (_, decision) = apply(
                serde_json::json!([{"action":"redirect","prefix":"/old/","to":"https://new.example/path/?new=1#section","status":status,"query":"append"}]),
                "/old/file?old=2",
            )?;
            let FilterDecision::Redirect(redirect) = decision else {
                panic!("redirect missing")
            };
            assert_eq!(redirect.status, status);
            assert_eq!(
                redirect.location,
                "https://new.example/path/file?new=1&old=2#section"
            );
        }
        let (_, decision) = apply(
            serde_json::json!([{"action":"redirect","prefix":"/old/","to":"https://new.example"}]),
            "/old/.evil.test",
        )?;
        let FilterDecision::Redirect(redirect) = decision else {
            panic!("redirect missing")
        };
        assert_eq!(redirect.location, "https://new.example/.evil.test");
        Ok(())
    }
    #[test]
    fn invalid_rewrite_rules_fail_preparation() {
        for rule in [
            serde_json::json!({"action":"rewrite","to":"https://upstream/path"}),
            serde_json::json!({"action":"rewrite","to":"//other/path"}),
            serde_json::json!({"action":"rewrite","to":"/path#fragment"}),
            serde_json::json!({"action":"rewrite","to":"/new","status":301}),
            serde_json::json!({"action":"redirect","to":"javascript:alert(1)"}),
            serde_json::json!({"action":"redirect","to":"//other/path"}),
            serde_json::json!({"action":"redirect","to":"https://user@host/path"}),
            serde_json::json!({"action":"redirect","to":"/path","status":200}),
            serde_json::json!({"action":"rewrite","to":"/path","exact":"/","prefix":"/"}),
            serde_json::json!({"action":"rewrite","to":"/path","regex":"["}),
            serde_json::json!({"action":"rewrite","to":"$upstream_http_location"}),
            serde_json::json!({"action":"rewrite","to":"$sent_http_location"}),
            serde_json::json!({"action":"rewrite","to":"$unknown"}),
            serde_json::json!({"action":"rewrite","to":"/$1"}),
            serde_json::json!({"action":"rewrite","to":"/path","when":{"source":"$status","equals":"200"}}),
        ] {
            assert!(Rewrite::new(serde_json::json!({"rules":[rule]})).is_err());
        }
    }
}
