use crate::{FilterCall, FilterDecision, FilterHook, NativeFilter};
use anyhow::{bail, ensure, Context, Result};
use http::{header, HeaderMap, HeaderName, HeaderValue, Method, Uri};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    net::IpAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub const REQUEST_HEADERS: u8 = 1;
pub const UPSTREAM_HEADERS: u8 = 2;
pub const REQUEST_ID: u8 = 4;
const BASIC: u8 = 8;
pub struct HeaderContext {
    pub remote_addr: HeaderValue,
    pub scheme: &'static str,
    pub method: Method,
    pub uri: Uri,
    pub host: HeaderValue,
    pub request_id: Option<HeaderValue>,
    pub request_headers: HeaderMap,
    pub upstream_addr: Option<HeaderValue>,
    pub upstream_headers: HeaderMap,
}

enum Variable {
    Remote,
    Scheme,
    Host,
    Method,
    RequestUri,
    Uri,
    UpstreamAddr,
    Status,
    RequestId,
    Http(HeaderName),
    Upstream(HeaderName),
    Sent(HeaderName),
}
enum Part {
    Text(String),
    Variable(Variable),
}
enum Value {
    Literal(HeaderValue),
    Template(Vec<Part>),
}
impl Value {
    fn inputs(&self, upstream: bool, target: &mut Vec<HeaderName>) {
        if let Self::Template(parts) = self {
            for part in parts {
                match part {
                    Part::Variable(Variable::Http(name)) if !upstream => target.push(name.clone()),
                    Part::Variable(Variable::Upstream(name)) if upstream => {
                        target.push(name.clone())
                    }
                    _ => {}
                }
            }
        }
    }
}
impl Value {
    fn compile(text: String) -> Result<(Self, u8)> {
        ensure!(text.len() <= 8192, "header value exceeds 8 KiB");
        HeaderValue::from_str(&text)?;
        if !text.contains('$') {
            return Ok((Self::Literal(HeaderValue::from_str(&text)?), 0));
        }
        let mut parts = Vec::new();
        let mut rest = text.as_str();
        let mut needs = 0;
        while let Some(at) = rest.find('$') {
            if at > 0 {
                parts.push(Part::Text(rest[..at].into()));
            }
            rest = &rest[at + 1..];
            if let Some(tail) = rest.strip_prefix('$') {
                parts.push(Part::Text("$".into()));
                rest = tail;
                continue;
            }
            let name = if let Some(tail) = rest.strip_prefix('{') {
                let end = tail.find('}').context("unclosed header variable")?;
                let name = &tail[..end];
                rest = &tail[end + 1..];
                name
            } else {
                let end = rest
                    .bytes()
                    .take_while(|b| b.is_ascii_alphanumeric() || *b == b'_')
                    .count();
                let name = &rest[..end];
                rest = &rest[end..];
                name
            };
            ensure!(
                !name.is_empty(),
                "empty header variable; use $$ for literal dollar"
            );
            let (variable, bits) = match name {
                "remote_addr" => (Variable::Remote, BASIC),
                "scheme" => (Variable::Scheme, BASIC),
                "host" => (Variable::Host, BASIC),
                "request_method" => (Variable::Method, BASIC),
                "request_uri" => (Variable::RequestUri, BASIC),
                "uri" => (Variable::Uri, BASIC),
                "upstream_addr" => (Variable::UpstreamAddr, BASIC),
                "status" => (Variable::Status, 0),
                "request_id" => (Variable::RequestId, REQUEST_ID),
                s if s.starts_with("http_") => {
                    (Variable::Http(variable_header(&s[5..])?), REQUEST_HEADERS)
                }
                s if s.starts_with("upstream_http_") => (
                    Variable::Upstream(variable_header(&s[14..])?),
                    UPSTREAM_HEADERS,
                ),
                s if s.starts_with("sent_http_") => (Variable::Sent(variable_header(&s[10..])?), 0),
                _ => bail!("unknown header variable ${name}"),
            };
            needs |= bits;
            parts.push(Part::Variable(variable));
        }
        if !rest.is_empty() {
            parts.push(Part::Text(rest.into()));
        }
        Ok((Self::Template(parts), needs))
    }
    fn render(
        &self,
        status: u16,
        ctx: Option<&HeaderContext>,
        headers: &HeaderMap,
    ) -> Result<HeaderValue> {
        let Self::Template(parts) = self else {
            if let Self::Literal(v) = self {
                return Ok(v.clone());
            }
            unreachable!()
        };
        // Common single-variable values share the already prepared HeaderValue.
        if let [Part::Variable(variable)] = parts.as_slice() {
            let borrowed = match variable {
                Variable::Remote => ctx.map(|c| &c.remote_addr),
                Variable::Host => ctx.map(|c| &c.host),
                Variable::RequestId => ctx.and_then(|c| c.request_id.as_ref()),
                Variable::UpstreamAddr => ctx.and_then(|c| c.upstream_addr.as_ref()),
                Variable::Http(n) => ctx.and_then(|c| single_header(&c.request_headers, n)),
                Variable::Upstream(n) => ctx.and_then(|c| single_header(&c.upstream_headers, n)),
                Variable::Sent(n) => single_header(headers, n),
                _ => None,
            };
            if let Some(value) = borrowed {
                return Ok(value.clone());
            }
            if matches!(variable, Variable::Status) {
                return Ok(HeaderValue::from_str(
                    http::StatusCode::from_u16(status)?.as_str(),
                )?);
            }
            if matches!(variable, Variable::Scheme) {
                return Ok(if ctx.is_some_and(|c| c.scheme == "https") {
                    HeaderValue::from_static("https")
                } else {
                    HeaderValue::from_static("http")
                });
            }
        }
        let mut text = String::with_capacity(64);
        for part in parts {
            match part {
                Part::Text(v) => text.push_str(v),
                Part::Variable(variable) => match variable {
                    Variable::Status => text.push_str(&status.to_string()),
                    Variable::Sent(name) => push_header(&mut text, headers, name),
                    v => {
                        if let Some(c) = ctx {
                            match v {
                                Variable::Remote => push_value(&mut text, Some(&c.remote_addr)),
                                Variable::Scheme => text.push_str(c.scheme),
                                Variable::Host => push_value(&mut text, Some(&c.host)),
                                Variable::Method => text.push_str(c.method.as_str()),
                                Variable::RequestUri => text
                                    .push_str(c.uri.path_and_query().map_or("/", |v| v.as_str())),
                                Variable::Uri => text.push_str(c.uri.path()),
                                Variable::RequestId => push_value(&mut text, c.request_id.as_ref()),
                                Variable::UpstreamAddr => {
                                    push_value(&mut text, c.upstream_addr.as_ref())
                                }
                                Variable::Http(name) => {
                                    push_header(&mut text, &c.request_headers, name)
                                }
                                Variable::Upstream(name) => {
                                    push_header(&mut text, &c.upstream_headers, name)
                                }
                                _ => {}
                            }
                        }
                    }
                },
            }
            ensure!(text.len() <= 8192, "expanded header exceeds 8 KiB");
        }
        Ok(HeaderValue::from_str(&text)?)
    }
}
fn variable_header(name: &str) -> Result<HeaderName> {
    Ok(HeaderName::from_bytes(name.replace('_', "-").as_bytes())?)
}
fn push_value(out: &mut String, value: Option<&HeaderValue>) {
    if let Some(v) = value.and_then(|v| v.to_str().ok()) {
        out.push_str(v);
    }
}
fn single_header<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Option<&'a HeaderValue> {
    let mut values = headers.get_all(name).iter();
    let first = values.next()?;
    values.next().is_none().then_some(first)
}
fn push_header(out: &mut String, headers: &HeaderMap, name: &HeaderName) {
    for (i, v) in headers.get_all(name).iter().enumerate() {
        if i > 0 {
            out.push_str(if name == header::COOKIE { "; " } else { ", " });
        }
        push_value(out, Some(v));
    }
}
fn name(text: String, trailer: bool) -> Result<HeaderName> {
    let n = HeaderName::from_bytes(text.as_bytes())?;
    ensure!(
        !(matches!(
            n.as_str(),
            "content-length"
                | "transfer-encoding"
                | "connection"
                | "keep-alive"
                | "proxy-connection"
                | "upgrade"
                | "te"
                | "trailer"
        ) || trailer
            && matches!(
                n.as_str(),
                "host"
                    | "authorization"
                    | "www-authenticate"
                    | "content-encoding"
                    | "content-type"
                    | "content-range"
            )),
        "header filter cannot change framing or forbidden trailer field"
    );
    Ok(n)
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Rules {
    set: BTreeMap<String, String>,
    append: BTreeMap<String, Vec<String>>,
    remove: Vec<String>,
    add: Vec<Addition>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Addition {
    name: String,
    value: String,
    #[serde(default)]
    always: bool,
}
struct Add {
    name: HeaderName,
    value: Value,
    always: bool,
}
struct Compiled {
    set: Vec<(HeaderName, Value)>,
    append: Vec<(HeaderName, Value)>,
    remove: Vec<HeaderName>,
    add: Vec<Add>,
}
impl Compiled {
    fn inputs(&self, upstream: bool, target: &mut Vec<HeaderName>) {
        for value in self
            .set
            .iter()
            .map(|(_, v)| v)
            .chain(self.append.iter().map(|(_, v)| v))
            .chain(self.add.iter().map(|a| &a.value))
        {
            value.inputs(upstream, target);
        }
    }
    fn new(rules: Rules, needs: &mut u8) -> Result<Self> {
        ensure!(
            rules.set.len()
                + rules.append.values().map(Vec::len).sum::<usize>()
                + rules.remove.len()
                + rules.add.len()
                <= 64,
            "too many header rules"
        );
        let mut bytes = rules.remove.iter().map(String::len).sum::<usize>();
        let mut compile = |n: String, v: String| -> Result<_> {
            bytes += n.len() + v.len();
            ensure!(bytes <= 32768, "header rules exceed 32 KiB");
            let (v, b) = Value::compile(v)?;
            *needs |= b;
            Ok((name(n, false)?, v))
        };
        let set = rules
            .set
            .into_iter()
            .map(|(n, v)| compile(n, v))
            .collect::<Result<_>>()?;
        let append = rules
            .append
            .into_iter()
            .flat_map(|(n, vs)| vs.into_iter().map(move |v| (n.clone(), v)))
            .map(|(n, v)| compile(n, v))
            .collect::<Result<_>>()?;
        let add = rules
            .add
            .into_iter()
            .map(|a| {
                let (n, v) = compile(a.name, a.value)?;
                Ok(Add {
                    name: n,
                    value: v,
                    always: a.always,
                })
            })
            .collect::<Result<_>>()?;
        Ok(Self {
            set,
            append,
            add,
            remove: rules
                .remove
                .into_iter()
                .map(|n| name(n, false))
                .collect::<Result<_>>()?,
        })
    }
    fn empty(&self) -> bool {
        self.set.is_empty()
            && self.append.is_empty()
            && self.remove.is_empty()
            && self.add.is_empty()
    }
    fn apply(
        &self,
        status: u16,
        ctx: Option<&HeaderContext>,
        headers: &mut HeaderMap,
    ) -> Result<()> {
        let mut bytes = 0;
        for n in &self.remove {
            headers.remove(n);
        }
        for (n, v) in &self.set {
            let v = v.render(status, ctx, headers)?;
            bytes += v.as_bytes().len();
            ensure!(bytes <= 32768, "header output exceeds 32 KiB");
            headers.insert(n.clone(), v);
        }
        for (n, v) in &self.append {
            let v = v.render(status, ctx, headers)?;
            bytes += v.as_bytes().len();
            ensure!(bytes <= 32768, "header output exceeds 32 KiB");
            headers.append(n.clone(), v);
        }
        for a in &self.add {
            if a.always || normal_status(status) {
                let v = a.value.render(status, ctx, headers)?;
                bytes += v.as_bytes().len();
                ensure!(bytes <= 32768, "header output exceeds 32 KiB");
                if !v.is_empty() {
                    headers.append(a.name.clone(), v);
                }
            }
        }
        Ok(())
    }
}
fn normal_status(status: u16) -> bool {
    matches!(
        status,
        200 | 201 | 204 | 206 | 301 | 302 | 303 | 304 | 307 | 308
    )
}
#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Inherit {
    #[default]
    On,
    Off,
    Merge,
}
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    request: Rules,
    response: Rules,
    trailers: Vec<Addition>,
    expires: Option<String>,
    inherit: Inherit,
    trailer_inherit: Inherit,
}
pub(crate) struct Headers {
    request: Compiled,
    response: Compiled,
    trailers: Vec<Add>,
    expires: Option<Expiry>,
    needs: u8,
    inherit: Inherit,
    trailer_inherit: Inherit,
    request_inputs: Vec<HeaderName>,
    upstream_inputs: Vec<HeaderName>,
}
enum Expiry {
    Static(Expiration),
    Dynamic(Value),
}
impl Headers {
    pub fn new(value: serde_json::Value) -> Result<Self> {
        let c: Config = serde_json::from_value(value)?;
        let mut needs = 0;
        let request = Compiled::new(c.request, &mut needs)?;
        let response = Compiled::new(c.response, &mut needs)?;
        ensure!(c.trailers.len() <= 64, "too many trailer rules");
        ensure!(
            c.trailers
                .iter()
                .map(|a| a.name.len() + a.value.len())
                .sum::<usize>()
                <= 32768,
            "trailer rules exceed 32 KiB"
        );
        let trailers: Vec<Add> = c
            .trailers
            .into_iter()
            .map(|a| {
                let (v, b) = Value::compile(a.value)?;
                needs |= b;
                Ok(Add {
                    name: name(a.name, true)?,
                    value: v,
                    always: a.always,
                })
            })
            .collect::<Result<_>>()?;
        let expires = c
            .expires
            .map(|v| -> Result<_> {
                let expr = Value::compile(v)?;
                needs |= expr.1;
                Ok(match expr.0 {
                    Value::Literal(v) => Expiry::Static(Expiration::parse(v.to_str()?)?),
                    v => Expiry::Dynamic(v),
                })
            })
            .transpose()?;
        let inputs = |upstream| {
            let mut names = Vec::new();
            request.inputs(upstream, &mut names);
            response.inputs(upstream, &mut names);
            for a in &trailers {
                a.value.inputs(upstream, &mut names);
            }
            if let Some(Expiry::Dynamic(v)) = &expires {
                v.inputs(upstream, &mut names);
            }
            names.sort_by(|a, b| a.as_str().cmp(b.as_str()));
            names.dedup();
            names
        };
        let request_inputs = inputs(false);
        let upstream_inputs = inputs(true);
        Ok(Self {
            request,
            response,
            trailers,
            expires,
            needs,
            inherit: c.inherit,
            trailer_inherit: c.trailer_inherit,
            request_inputs,
            upstream_inputs,
        })
    }
}
impl NativeFilter for Headers {
    fn header_inputs(&self, upstream: bool) -> &[HeaderName] {
        if upstream {
            &self.upstream_inputs
        } else {
            &self.request_inputs
        }
    }
    fn context_needs(&self) -> u8 {
        self.needs
    }
    fn parent_override(&self) -> Option<(u8, bool)> {
        Some((
            (if matches!(self.inherit, Inherit::Off)
                || matches!(self.inherit, Inherit::On) && !self.request.empty()
            {
                1
            } else {
                0
            }) | (if matches!(self.inherit, Inherit::Off)
                || matches!(self.inherit, Inherit::On) && !self.response.empty()
            {
                2
            } else {
                0
            }) | (if self.expires.is_some() { 4 } else { 0 }),
            matches!(self.trailer_inherit, Inherit::Off)
                || matches!(self.trailer_inherit, Inherit::On) && !self.trailers.is_empty(),
        ))
    }
    fn applies_to(&self, h: FilterHook) -> bool {
        match h {
            FilterHook::RequestHeaders => !self.request.empty(),
            FilterHook::ResponseHeaders => {
                !self.response.empty() || self.expires.is_some() || !self.trailers.is_empty()
            }
            _ => false,
        }
    }
    fn filter(
        &self,
        h: FilterHook,
        peer: IpAddr,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        self.filter_with_context(
            &FilterCall {
                hook: h,
                peer,
                status: 200,
                context: None,
                inherited_skip: 0,
            },
            headers,
        )
    }
    fn filter_with_context(
        &self,
        call: &FilterCall<'_>,
        headers: Option<&mut HeaderMap>,
    ) -> Result<FilterDecision> {
        if let Some(headers) = headers {
            match call.hook {
                FilterHook::RequestHeaders => {
                    if call.inherited_skip & 1 == 0 {
                        self.request.apply(200, call.context, headers)?;
                    }
                }
                FilterHook::ResponseHeaders => {
                    if normal_status(call.status) && call.inherited_skip & 4 == 0 {
                        if let Some(e) = &self.expires {
                            match e {
                                Expiry::Static(e) => e.apply(headers, SystemTime::now())?,
                                Expiry::Dynamic(v) => {
                                    let value = v.render(call.status, call.context, headers)?;
                                    if let Ok(e) = Expiration::parse(value.to_str()?) {
                                        e.apply(headers, SystemTime::now())?;
                                    }
                                }
                            }
                        }
                    }
                    if call.inherited_skip & 2 == 0 {
                        self.response.apply(call.status, call.context, headers)?;
                    }
                }
                _ => {}
            }
        }
        Ok(FilterDecision::Continue)
    }
    fn trailers(
        &self,
        status: u16,
        ctx: Option<&HeaderContext>,
        headers: &HeaderMap,
    ) -> Result<Option<HeaderMap>> {
        if self.trailers.is_empty() {
            return Ok(None);
        }
        let mut result = HeaderMap::new();
        let mut bytes = 0;
        for a in &self.trailers {
            if a.always || normal_status(status) && !matches!(status, 204 | 304) {
                let v = a.value.render(status, ctx, headers)?;
                bytes += v.as_bytes().len();
                ensure!(bytes <= 32768, "trailers exceed 32 KiB");
                if !v.is_empty() {
                    result.append(a.name.clone(), v);
                }
            }
        }
        Ok((!result.is_empty()).then_some(result))
    }
}

enum Expiration {
    Off,
    Epoch,
    Max,
    Offset { seconds: i64, modified: bool },
    Daily(u64),
}
impl Expiration {
    fn parse(text: &str) -> Result<Self> {
        Ok(match text {
            "off" => Self::Off,
            "epoch" => Self::Epoch,
            "max" => Self::Max,
            _ => {
                let (modified, value) = text
                    .strip_prefix("modified ")
                    .map_or((false, text), |s| (true, s));
                if let Some(v) = value.strip_prefix('@') {
                    ensure!(!modified, "modified daily expires invalid");
                    let n = duration(v)?;
                    ensure!((0..=86400).contains(&n), "daily expires exceeds 24h");
                    Self::Daily(n as u64)
                } else {
                    Self::Offset {
                        seconds: duration(value)?,
                        modified,
                    }
                }
            }
        })
    }
    fn apply(&self, headers: &mut HeaderMap, now: SystemTime) -> Result<()> {
        let (now_s, offset) = (
            now.duration_since(UNIX_EPOCH)?.as_secs() as i64,
            match self {
                Self::Offset { seconds, .. } => *seconds,
                _ => 0,
            },
        );
        let (target, cache) = match self {
            Self::Off => return Ok(()),
            Self::Epoch => (1, "no-cache".into()),
            Self::Max => (2145916555, "max-age=315360000".into()),
            Self::Daily(at) => {
                let mut target = now_s / 86400 * 86400 + *at as i64;
                if target <= now_s {
                    target += 86400;
                }
                (target, format!("max-age={}", target - now_s))
            }
            Self::Offset { modified, .. } => {
                let base = if *modified {
                    headers
                        .get(header::LAST_MODIFIED)
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| httpdate::parse_http_date(v).ok())
                        .and_then(|v| v.duration_since(UNIX_EPOCH).ok())
                        .map_or(now_s, |v| v.as_secs() as i64)
                } else {
                    now_s
                };
                let target = (base + offset).clamp(0, 253402300799);
                let age = target - now_s;
                (
                    target,
                    if age < 0 || offset < 0 {
                        "no-cache".into()
                    } else {
                        format!("max-age={age}")
                    },
                )
            }
        };
        headers.insert(
            header::EXPIRES,
            HeaderValue::from_str(&httpdate::fmt_http_date(
                UNIX_EPOCH + Duration::from_secs(target as u64),
            ))?,
        );
        headers.insert(header::CACHE_CONTROL, HeaderValue::from_str(&cache)?);
        Ok(())
    }
}
fn duration(text: &str) -> Result<i64> {
    let (negative, mut text) = if let Some(s) = text.strip_prefix('-') {
        (true, s)
    } else {
        (false, text.strip_prefix('+').unwrap_or(text))
    };
    ensure!(!text.is_empty(), "empty expires interval");
    let mut total = 0i64;
    while !text.is_empty() {
        let n = text.bytes().take_while(u8::is_ascii_digit).count();
        ensure!(n > 0, "invalid expires interval");
        let value = text[..n].parse::<i64>()?;
        text = &text[n..];
        let (unit, tail) = if text.is_empty() {
            (1, "")
        } else {
            let unit = match text.as_bytes()[0] {
                b's' => 1,
                b'm' => 60,
                b'h' => 3600,
                b'd' => 86400,
                b'w' => 604800,
                b'M' => 2592000,
                b'y' => 31536000,
                _ => bail!("invalid expires unit"),
            };
            (unit, &text[1..])
        };
        total = total
            .checked_add(value.checked_mul(unit).context("expires overflow")?)
            .context("expires overflow")?;
        ensure!(total <= 3153600000, "expires exceeds 100 years");
        text = tail;
    }
    Ok(if negative { -total } else { total })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn apply(value: serde_json::Value, status: u16) -> Result<HeaderMap> {
        let filter = Headers::new(value)?;
        let mut headers = HeaderMap::new();
        filter.filter_with_context(
            &FilterCall {
                hook: FilterHook::ResponseHeaders,
                peer: "127.0.0.1".parse()?,
                status,
                context: None,
                inherited_skip: 0,
            },
            Some(&mut headers),
        )?;
        Ok(headers)
    }
    #[test]
    fn status_always_empty_and_multivalue_rules() -> Result<()> {
        for status in [
            200, 201, 204, 206, 301, 302, 303, 304, 307, 308, 400, 404, 500, 503,
        ] {
            let h = apply(
                serde_json::json!({"response":{"add":[{"name":"x-regular","value":"ok"},{"name":"x-always","value":"$status","always":true},{"name":"x-empty","value":"","always":true}],"append":{"set-cookie":["a=1","b=2"]}}}),
                status,
            )?;
            assert_eq!(h.contains_key("x-regular"), normal_status(status));
            assert_eq!(h["x-always"], status.to_string());
            assert!(!h.contains_key("x-empty"));
            assert_eq!(h.get_all("set-cookie").iter().count(), 2);
        }
        Ok(())
    }
    #[test]
    fn expiration_modes_modified_and_daily_utc() -> Result<()> {
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for (mode, cache) in [
            ("1h30m", "max-age=5400"),
            ("-1s", "no-cache"),
            ("0", "max-age=0"),
            ("epoch", "no-cache"),
            ("max", "max-age=315360000"),
        ] {
            let mut h = HeaderMap::new();
            Expiration::parse(mode)?.apply(&mut h, now)?;
            assert_eq!(h[header::CACHE_CONTROL], cache);
            if mode == "max" {
                assert_eq!(h[header::EXPIRES], "Thu, 31 Dec 2037 23:55:55 GMT");
            }
            if mode == "epoch" {
                assert_eq!(h[header::EXPIRES], "Thu, 01 Jan 1970 00:00:01 GMT");
            }
        }
        let mut h = HeaderMap::new();
        h.insert(
            header::LAST_MODIFIED,
            HeaderValue::from_str(&httpdate::fmt_http_date(now - Duration::from_secs(60)))?,
        );
        Expiration::parse("modified +2m")?.apply(&mut h, now)?;
        assert_eq!(h[header::CACHE_CONTROL], "max-age=60");
        Expiration::parse("@15h30m")?.apply(&mut h, now)?;
        let target = httpdate::parse_http_date(h[header::EXPIRES].to_str()?)?;
        let diff = target.duration_since(now)?.as_secs();
        assert!(diff > 0 && diff <= 86400);
        for invalid in [
            "bad",
            "10000000000000000000y",
            "1ms",
            "@25h",
            "modified @2h",
        ] {
            assert!(Expiration::parse(invalid).is_err());
        }
        Ok(())
    }
    #[test]
    fn validation_and_template_compilation_are_bounded() -> Result<()> {
        for value in [
            serde_json::json!({"response":{"set":{"bad:name":"x"}}}),
            serde_json::json!({"response":{"set":{"x-test":"$unknown"}}}),
            serde_json::json!({"response":{"set":{"x-test":"${host"}}}),
            serde_json::json!({"trailers":[{"name":"content-length","value":"1"}]}),
            serde_json::json!({"expires":"garbage"}),
        ] {
            assert!(Headers::new(value).is_err());
        }
        let h = apply(
            serde_json::json!({"response":{"set":{"x-dollar":"$$cost-${status}"}}}),
            201,
        )?;
        assert_eq!(h["x-dollar"], "$cost-201");
        let literal = Headers::new(serde_json::json!({"response":{"set":{"x-static":"value"}}}))?;
        assert_eq!(literal.context_needs(), 0);
        Ok(())
    }
}
