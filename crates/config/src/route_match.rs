use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteHeader {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub equals: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_equals: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matches: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub not_matches: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub present: Option<bool>,
    #[serde(default)]
    pub ignore_case: bool,
}
impl RouteHeader {
    pub fn has_regex(&self) -> bool {
        self.matches.is_some() || self.not_matches.is_some()
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.name.len() <= 128,
            "route header name exceeds 128 bytes"
        );
        http::HeaderName::from_bytes(self.name.as_bytes()).context("invalid route header name")?;
        ensure!(
            [
                self.equals.is_some(),
                self.not_equals.is_some(),
                self.prefix.is_some(),
                self.matches.is_some(),
                self.not_matches.is_some(),
                self.present.is_some()
            ]
            .into_iter()
            .filter(|v| *v)
            .count()
                == 1,
            "route header requires exactly one comparison"
        );
        ensure!(
            self.present.is_none() || !self.ignore_case,
            "ignore_case does not apply to header presence"
        );
        for value in [&self.equals, &self.not_equals, &self.prefix]
            .into_iter()
            .flatten()
        {
            ensure!(
                value.len() <= 4096,
                "route header literal exceeds 4096 bytes"
            );
            http::HeaderValue::from_bytes(value.as_bytes())
                .context("invalid route header value")?;
        }
        for pattern in [&self.matches, &self.not_matches].into_iter().flatten() {
            ensure!(
                pattern.len() <= 2048,
                "route header regex exceeds 2048 bytes"
            );
        }
        Ok(())
    }
}
impl Route {
    pub(crate) fn validate_match(&self) -> Result<()> {
        ensure!(
            self.methods.len() <= 32 && self.headers.len() <= 16,
            "route permits at most 32 methods and 16 header predicates"
        );
        let mut seen = HashSet::new();
        for method in &self.methods {
            ensure!(
                !method.is_empty() && method.len() <= 64,
                "invalid route method length"
            );
            http::Method::from_bytes(method.as_bytes()).context("invalid route method")?;
            ensure!(seen.insert(method), "duplicate route method");
        }
        for header in &self.headers {
            header.validate()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn route(methods: serde_json::Value, headers: serde_json::Value) -> Route {
        serde_json::from_value(
            serde_json::json!({"upstream":"app","methods":methods,"headers":headers}),
        )
        .unwrap()
    }
    #[test]
    fn route_match_schema_rejects_ambiguous_invalid_and_unbounded_inputs() -> Result<()> {
        route(
            serde_json::json!(["GET", "HEAD", "get", "CUSTOM"]),
            serde_json::json!([{ "name":"X-Tenant","equals":"alice" }]),
        )
        .validate_match()?;
        for methods in [
            serde_json::json!(["GET", "GET"]),
            serde_json::json!([""]),
            serde_json::json!(["GE T"]),
            serde_json::json!(["GET\r\n"]),
            serde_json::json!(["X".repeat(65)]),
        ] {
            assert!(route(methods, serde_json::json!([]))
                .validate_match()
                .is_err());
        }
        for header in [
            serde_json::json!({"name":":method","equals":"GET"}),
            serde_json::json!({"name":"x"}),
            serde_json::json!({"name":"x","equals":"a","present":true}),
            serde_json::json!({"name":"x","equals":"a\r\nb"}),
            serde_json::json!({"name":"x","present":false,"ignore_case":true}),
            serde_json::json!({"name":"x","matches":"a".repeat(2049)}),
        ] {
            assert!(route(serde_json::json!([]), serde_json::json!([header]))
                .validate_match()
                .is_err());
        }
        let legacy = serde_json::from_value::<Route>(serde_json::json!({"upstream":"app"}))?;
        let encoded = toml::to_string(&legacy)?;
        assert!(!encoded.contains("methods") && !encoded.contains("headers"));
        Ok(())
    }
}
