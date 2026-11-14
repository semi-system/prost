use anyhow::{Context, Result};
use http::{HeaderMap, HeaderName, Method};
use prost_config::Route;
use regex::bytes::{Regex, RegexBuilder};

pub(crate) struct RouteMatch {
    methods: Vec<Method>,
    headers: Vec<HeaderRule>,
}
struct HeaderRule {
    name: HeaderName,
    comparison: Comparison,
    inverted: bool,
    ignore_case: bool,
}
enum Comparison {
    Equal(Vec<u8>),
    Prefix(Vec<u8>),
    Regex(Regex),
    Present(bool),
}
impl RouteMatch {
    pub fn compile(route: &Route) -> Result<Self> {
        let methods = route
            .methods
            .iter()
            .map(|s| Method::from_bytes(s.as_bytes()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let headers = route
            .headers
            .iter()
            .map(|rule| {
                let comparison = if let Some(value) =
                    rule.equals.as_ref().or(rule.not_equals.as_ref())
                {
                    Comparison::Equal(value.as_bytes().to_vec())
                } else if let Some(value) = &rule.prefix {
                    Comparison::Prefix(value.as_bytes().to_vec())
                } else if let Some(pattern) = rule.matches.as_ref().or(rule.not_matches.as_ref()) {
                    Comparison::Regex(
                        RegexBuilder::new(pattern)
                            .unicode(false)
                            .case_insensitive(rule.ignore_case)
                            .size_limit(65536)
                            .dfa_size_limit(65536)
                            .nest_limit(32)
                            .build()
                            .with_context(|| {
                                format!(
                                    "invalid or over-budget route header regex for {}",
                                    rule.name
                                )
                            })?,
                    )
                } else {
                    Comparison::Present(rule.present.context("route header comparison missing")?)
                };
                Ok(HeaderRule {
                    name: HeaderName::from_bytes(rule.name.as_bytes())?,
                    comparison,
                    inverted: rule.not_equals.is_some() || rule.not_matches.is_some(),
                    ignore_case: rule.ignore_case,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { methods, headers })
    }
    pub fn matches(&self, method: &Method, headers: &HeaderMap) -> bool {
        (self.methods.is_empty() || self.methods.contains(method))
            && self.headers.iter().all(|rule| {
                let present = headers.contains_key(&rule.name);
                if let Comparison::Present(want) = rule.comparison {
                    return present == want;
                }
                if !present {
                    return false;
                }
                let matches =
                    headers
                        .get_all(&rule.name)
                        .iter()
                        .any(|value| match &rule.comparison {
                            Comparison::Equal(want) => {
                                if rule.ignore_case {
                                    value.as_bytes().eq_ignore_ascii_case(want)
                                } else {
                                    value.as_bytes() == want
                                }
                            }
                            Comparison::Prefix(want) => {
                                value.as_bytes().get(..want.len()).is_some_and(|prefix| {
                                    if rule.ignore_case {
                                        prefix.eq_ignore_ascii_case(want)
                                    } else {
                                        prefix == want
                                    }
                                })
                            }
                            Comparison::Regex(regex) => regex.is_match(value.as_bytes()),
                            Comparison::Present(_) => unreachable!(),
                        });
                matches != rule.inverted
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn compile(methods: &[&str], headers: serde_json::Value) -> Result<RouteMatch> {
        RouteMatch::compile(&serde_json::from_value(
            serde_json::json!({"upstream":"app","methods":methods,"headers":headers}),
        )?)
    }
    #[test]
    fn methods_header_and_or_presence_case_and_duplicate_semantics() -> Result<()> {
        let rule = compile(
            &["GET", "HEAD"],
            serde_json::json!([
            {"name":"X-Tenant","equals":"alice","ignore_case":true},
            {"name":"x-version","prefix":"v2"},{"name":"x-disabled","present":false}]),
        )?;
        let mut headers = HeaderMap::new();
        headers.append("x-tenant", "bob".parse()?);
        headers.append("x-tenant", "ALICE".parse()?);
        headers.insert("x-version", "v2.3".parse()?);
        assert!(rule.matches(&Method::GET, &headers));
        assert!(rule.matches(&Method::HEAD, &headers));
        assert!(!rule.matches(&Method::POST, &headers));
        assert!(!rule.matches(&Method::from_bytes(b"get")?, &headers));
        headers.insert("x-disabled", "".parse()?);
        assert!(!rule.matches(&Method::GET, &headers));
        assert!(compile(
            &[],
            serde_json::json!([{"name":"x-disabled","present":true}])
        )?
        .matches(&Method::POST, &headers));
        let negative = compile(
            &[],
            serde_json::json!([{"name":"x-tenant","not_equals":"ALICE"}]),
        )?;
        assert!(!negative.matches(&Method::GET, &headers));
        headers.remove("x-tenant");
        assert!(!negative.matches(&Method::GET, &headers));
        headers.insert("x-tenant", "alice".parse()?);
        assert!(negative.matches(&Method::GET, &headers));
        Ok(())
    }
    #[test]
    fn byte_regexes_are_precompiled_bounded_and_do_not_require_utf8_headers() -> Result<()> {
        let rule = compile(&[], serde_json::json!([{"name":"x","matches":"^\\x80$"}]))?;
        let mut headers = HeaderMap::new();
        headers.insert("x", http::HeaderValue::from_bytes(&[0x80])?);
        assert!(rule.matches(&Method::GET, &headers));
        assert!(!compile(
            &[],
            serde_json::json!([{"name":"x","not_matches":"^\\x80$"}])
        )?
        .matches(&Method::GET, &headers));
        headers.insert("x", "ABC".parse()?);
        assert!(compile(
            &[],
            serde_json::json!([{"name":"x","matches":"^abc$","ignore_case":true}])
        )?
        .matches(&Method::GET, &headers));
        for pattern in ["(", "a{1000000}"] {
            assert!(compile(&[], serde_json::json!([{"name":"x","matches":pattern}])).is_err());
        }
        Ok(())
    }
}
