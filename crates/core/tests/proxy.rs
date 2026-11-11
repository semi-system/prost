use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{Request, Response};
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::{
    body::{Frame, Incoming},
    service::service_fn,
};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use prost_config::{Config, Endpoint, Protocol};
use prost_core::{ProxyBuilder, RunningProxy};
use std::{convert::Infallible, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};

struct Server {
    address: SocketAddr,
    task: JoinHandle<()>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_pool_connections_close_without_another_request() -> Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let closed = Arc::new(tokio::sync::Notify::new());
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let signal = closed.clone();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let closed = signal.clone();
            tokio::spawn(async move {
                let served = Arc::new(AtomicBool::new(false));
                let request_seen = served.clone();
                let service = service_fn(move |_: Request<Incoming>| {
                    request_seen.store(true, Ordering::Relaxed);
                    async {
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"idle"))))
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(io), service)
                    .await;
                if served.load(Ordering::Relaxed) {
                    closed.notify_one();
                }
            });
        }
    });
    let _server = Server { address, task };
    let mut fixture = Fixture::new(Protocol::Http, address);
    let upstream = fixture.config.upstreams.get_mut("main").unwrap();
    upstream.pool_idle_timeout_ms = 50;
    upstream.health_interval_ms = 60000;
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "idle");
    timeout(Duration::from_secs(2), closed.notified()).await?;
    assert_eq!(p.metrics.requests.get(), 1);
    let mut invalid = p.snapshot().config.clone();
    invalid
        .upstreams
        .get_mut("main")
        .unwrap()
        .http1_max_buf_size = 4096;
    assert!(p.apply(invalid.encode()?, 0).await.is_err());
    assert_eq!(p.snapshot().config.generation, 0);
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "idle");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

fn builtin(name: &str, config: serde_json::Value) -> Result<prost_config::Plugin> {
    Ok(serde_json::from_value(
        serde_json::json!({"name":name,"config":config}),
    )?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_header_variables_h1_h2_trailers_and_bodyless_responses() -> Result<()> {
    let backend = http_server("headers").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.upstreams.get_mut("main").unwrap().http2 = true;
    fixture.config.listeners[0].plugins = vec![builtin(
        "headers",
        serde_json::json!({
            "request":{"set":{"x-test-plugin":"$request_id"}},
            "response":{"add":[{"name":"x-id","value":"$request_id","always":true},{"name":"x-seen","value":"$upstream_http_x_seen_plugin"},{"name":"x-location","value":"$scheme://$host$request_uri"},{"name":"x-regular","value":"yes"},{"name":"x-status","value":"$status","always":true}]},
            "expires":"1h",
            "trailers":[{"name":"x-end","value":"$request_id:$status"}]
        }),
    )?];
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut h1, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    for (method, path, status) in [
        (http::Method::GET, "/", 200),
        (http::Method::GET, "/status/404", 404),
        (http::Method::HEAD, "/", 200),
        (http::Method::GET, "/status/204", 204),
        (http::Method::GET, "/status/304", 304),
    ] {
        let response = h1
            .send_request(
                Request::builder()
                    .method(method.clone())
                    .uri(path)
                    .header("host", "Example.Test:8080")
                    .header("te", "trailers")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), status);
        let id = response.headers()["x-id"].to_str()?.to_string();
        assert_eq!(id.len(), 32);
        assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_eq!(response.headers()["x-status"], status.to_string());
        if status == 200 {
            assert_eq!(response.headers()["x-seen"], id);
            assert_eq!(
                response.headers()["x-location"],
                format!("http://example.test{path}")
            );
            assert_eq!(response.headers()["cache-control"], "max-age=3600");
        }
        if status == 404 {
            assert!(!response.headers().contains_key("x-regular"));
            assert!(!response.headers().contains_key("expires"));
        }
        let collected = response.into_body().collect().await?;
        if method == http::Method::HEAD || matches!(status, 204 | 304) {
            assert!(collected.trailers().is_none());
            assert!(collected.to_bytes().is_empty());
        } else if status == 200 {
            assert_eq!(
                collected.trailers().context("native H1 trailers missing")?["x-end"],
                format!("{id}:200")
            );
            assert_eq!(collected.to_bytes(), "headers");
        }
    }
    drop(h1);
    driver.abort();
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut h2, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    let response = h2
        .send_request(
            Request::builder()
                .uri("http://example.test/grpc")
                .header("te", "trailers")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    let id = response.headers()["x-id"].to_str()?.to_string();
    let collected = response.into_body().collect().await?;
    assert_eq!(
        collected.trailers().context("H2 trailers missing")?["grpc-status"],
        "0"
    );
    assert_eq!(collected.trailers().unwrap()["x-end"], format!("{id}:200"));
    assert_eq!(
        collected.to_bytes(),
        Bytes::from_static(b"\0\0\0\0\x04test")
    );
    drop(h2);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn native_header_inheritance_and_always_on_local_rejections() -> Result<()> {
    let backend = http_server("inherit").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.listeners[0].plugins = vec![builtin(
        "headers",
        serde_json::json!({"response":{"add":[{"name":"x-parent","value":"parent","always":true}]},"expires":"epoch","trailers":[{"name":"x-parent-end","value":"parent","always":true}]}),
    )?];
    fixture.config.listeners[0].routes.push(serde_json::from_value(serde_json::json!({"path_prefix":"/child","upstream":"main","plugins":[{"name":"headers","config":{"response":{"set":{"x-child":"child"}},"trailers":[{"name":"x-child-end","value":"child","always":true}]}}]}))?);
    fixture.config.listeners[0].routes.push(serde_json::from_value(serde_json::json!({"path_prefix":"/merge","upstream":"main","plugins":[{"name":"headers","config":{"inherit":"merge","trailer_inherit":"merge","expires":"off","response":{"set":{"x-child":"child"}},"trailers":[{"name":"x-child-end","value":"child","always":true}]}}]}))?);
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    for path in ["/child", "/merge"] {
        let response = client
            .send_request(
                Request::builder()
                    .uri(path)
                    .header("host", "localhost")
                    .header("te", "trailers")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.headers()["x-child"], "child");
        assert_eq!(
            response.headers().contains_key("x-parent"),
            path == "/merge"
        );
        assert_eq!(response.headers().contains_key("expires"), path == "/child");
        let collected = response.into_body().collect().await?;
        let trailers = collected.trailers().context("inherit trailers missing")?;
        assert_eq!(trailers["x-child-end"], "child");
        assert_eq!(trailers.contains_key("x-parent-end"), path == "/merge");
    }
    let mut next = p.snapshot().config.clone();
    next.listeners[0].plugins.push(builtin(
        "ip_acl",
        serde_json::json!({"deny":["127.0.0.0/8"]}),
    )?);
    p.apply(next.encode()?, 0).await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.status(), 403);
    assert_eq!(response.headers()["x-parent"], "parent");
    assert!(!response.headers().contains_key("expires"));
    response.into_body().collect().await?;
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_headers_hot_update_and_plugin_ordering() -> Result<()> {
    let backend = http_server("native").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    let headers = builtin(
        "headers",
        serde_json::json!({
            "request":{"set":{"x-test-plugin":"native"}},
            "response":{"set":{"x-prost-plugin":"before"},"remove":["x-seen-forwarded"],"append":{"set-cookie":["a=1","b=2"]}}
        }),
    )?;
    fixture.config.listeners[0].plugins = vec![headers.clone()];
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    for expected in ["before", "after"] {
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["x-seen-plugin"], "native");
        assert_eq!(response.headers()["x-prost-plugin"], expected);
        assert!(!response.headers().contains_key("x-seen-forwarded"));
        assert_eq!(response.headers().get_all("set-cookie").iter().count(), 2);
        assert_eq!(response.into_body().collect().await?.to_bytes(), "native");
        if expected == "before" {
            let mut config = p.snapshot().config.clone();
            #[cfg(feature = "wasm")]
            {
                let wasm = fixture.dir.path().join("headers.wat");
                std::fs::write(&wasm, include_str!("../../../examples/add-header.wat"))?;
                config.listeners[0].plugins.push(prost_config::Plugin {
                    name: "wasm".into(),
                    module: Some(wasm),
                    hooks: Some(vec!["response_headers".into()]),
                    config: Default::default(),
                });
            }
            config.listeners[0].plugins.push(builtin(
                "headers",
                serde_json::json!({"response":{"set":{"x-prost-plugin":"after"}}}),
            )?);
            p.apply(config.encode()?, 0).await?;
        }
    }
    let mut invalid = p.snapshot().config.clone();
    invalid.listeners[0].plugins[0] = builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-bad":"invalid\r\nheader"}}}),
    )?;
    assert!(p.apply(invalid.encode()?, 1).await.is_err());
    invalid = p.snapshot().config.clone();
    invalid.listeners[0].plugins[0].hooks = Some(vec!["unknown".into()]);
    assert!(p.apply(invalid.encode()?, 1).await.is_err());
    assert_eq!(p.snapshot().config.generation, 1);
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[cfg(not(feature = "wasm"))]
#[tokio::test]
async fn wasm_disabled_rejects_apply_without_changing_generation() -> Result<()> {
    let backend = http_server("native").await;
    let fixture = Fixture::new(Protocol::Http, backend.address);
    let p = fixture.start().await;
    let module = fixture.dir.path().join("module.wat");
    std::fs::write(&module, include_str!("../../../examples/add-header.wat"))?;
    let original = std::fs::read(&fixture.path)?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].plugins.push(serde_json::from_value(
        serde_json::json!({"name":"wasm","module":module}),
    )?);
    let error = p.apply(config.encode()?, 0).await.unwrap_err();
    assert!(format!("{error:#}").contains("WASM support is not enabled"));
    assert_eq!(p.snapshot().config.generation, 0);
    assert_eq!(std::fs::read(&fixture.path)?, original);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn native_acl_http_ignores_spoofed_forwarded_address_and_tcp_closes() -> Result<()> {
    let backend = http_server("allowed").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.listeners[0].plugins.push(builtin(
        "ip_acl",
        serde_json::json!({"allow":["10.0.0.0/8"]}),
    )?);
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .header("x-forwarded-for", "10.0.0.1")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.status(), 403);
    response.into_body().collect().await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].plugins = vec![builtin(
        "ip_acl",
        serde_json::json!({"allow":["127.0.0.0/8"]}),
    )?];
    p.apply(next.encode()?, 0).await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.status(), 200);
    response.into_body().collect().await?;
    drop(client);
    driver.abort();
    p.shutdown().await;

    let echo = echo().await;
    let mut fixture = Fixture::new(Protocol::Tcp, echo.address);
    fixture.config.listeners[0].plugins.push(builtin(
        "ip_acl",
        serde_json::json!({"deny":["127.0.0.0/8"]}),
    )?);
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let mut io = TcpStream::connect(fixture.address()).await?;
    assert!(matches!(
        timeout(Duration::from_secs(2), io.read(&mut [0u8; 1])).await?,
        Ok(0) | Err(_)
    ));
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mapped_headers_apply_on_live_h1_h2_and_invalid_update_rolls_back() -> Result<()> {
    let backend = http_server("mapped").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    let rules = serde_json::json!({
        "maps": {
            "policy": {"source":"$http_origin", "default":"", "entries":[
                {"pattern":"https://app.example", "value":"trusted"},
                {"pattern":"^https://([a-z]+)\\.example$", "kind":"regex", "value":"tenant-$1"}
            ]},
            "ttl": {"source":"$policy", "default":"off", "entries":[
                {"pattern":"trusted", "value":"1h"}
            ]}
        },
        "request":{"set":{"x-test-plugin":"$policy"}},
        "response":{
            "add":[{"name":"x-policy","value":"$policy","always":true,
                "when":{"source":"$policy","not_empty":true}}],
            "rules":[{"name":"x-seen","action":"set","value":"$upstream_http_x_seen_plugin",
                "when":{"source":"$status","equals":"200"}}]
        },
        "expires":"$ttl"
    });
    fixture.config.listeners[0].plugins = vec![builtin("headers", rules.clone())?];
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    for (origin, policy, expiry) in [
        ("https://app.example", "trusted", true),
        ("https://bob.example", "tenant-bob", false),
        ("https://other.invalid", "", false),
    ] {
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .header("origin", origin)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["x-seen"], policy);
        assert_eq!(
            response.headers().contains_key("x-policy"),
            !policy.is_empty()
        );
        assert_eq!(response.headers().contains_key("expires"), expiry);
        if expiry {
            assert_eq!(response.headers()["cache-control"], "max-age=3600");
        }
        assert_eq!(response.into_body().collect().await?.to_bytes(), "mapped");
    }
    let durable = std::fs::read(&fixture.path)?;
    let mut invalid = p.snapshot().config.clone();
    let mut cyclic = rules.clone();
    cyclic["maps"]["policy"]["source"] = serde_json::json!("$ttl");
    invalid.listeners[0].plugins[0] = builtin("headers", cyclic)?;
    assert!(p.apply(invalid.encode()?, 0).await.is_err());
    assert_eq!(std::fs::read(&fixture.path)?, durable);
    assert_eq!(p.snapshot().config.generation, 0);
    let mut bad_regex = rules.clone();
    bad_regex["maps"]["policy"]["entries"][1]["pattern"] = serde_json::json!("[");
    invalid.listeners[0].plugins[0] = builtin("headers", bad_regex)?;
    assert!(p.apply(invalid.encode()?, 0).await.is_err());
    assert_eq!(std::fs::read(&fixture.path)?, durable);
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut h2, h2conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let h2driver = tokio::spawn(h2conn);
    let mut next = p.snapshot().config.clone();
    let mut v2 = rules;
    v2["maps"]["policy"]["entries"][0]["value"] = serde_json::json!("v2");
    next.listeners[0].plugins[0] = builtin("headers", v2)?;
    p.apply(next.encode()?, 0).await?;
    let request = || {
        Request::builder()
            .uri("http://localhost/")
            .header("origin", "https://app.example")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    for response in [
        client.send_request(request()).await?,
        h2.send_request(request()).await?,
    ] {
        assert_eq!(response.headers()["x-policy"], "v2");
        assert_eq!(response.headers()["x-seen"], "v2");
        assert!(!response.headers().contains_key("expires"));
        assert_eq!(response.into_body().collect().await?.to_bytes(), "mapped");
    }
    drop(client);
    drop(h2);
    driver.abort();
    h2driver.abort();
    p.shutdown().await;
    // Committed maps are part of the managed TOML and remain valid at restart.
    drop(p);
    let restarted = fixture.start().await;
    assert_eq!(restarted.snapshot().config.generation, 1);
    restarted.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_rate_limit_shares_workers_and_survives_neighbor_update() -> Result<()> {
    let backend = http_server("keyed").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.runtime.io_workers = 2;
    fixture.config.listeners[0].plugins = vec![
        builtin(
            "rate_limit",
            serde_json::json!({"requests_per_second":1,"burst":1,
            "key":"$http_x_api_key","max_keys":64,"idle_timeout_ms":60000}),
        )?,
        builtin(
            "headers",
            serde_json::json!({"response":{"set":{"x-policy":"v1"}}}),
        )?,
    ];
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    async fn request(address: SocketAddr, key: &str) -> Result<(u16, String)> {
        let io = TcpStream::connect(address).await?;
        let (mut client, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(conn);
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .header("x-api-key", key)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        let status = response.status().as_u16();
        let policy = response.headers()["x-policy"].to_str()?.to_owned();
        response.into_body().collect().await?;
        drop(client);
        driver.abort();
        Ok((status, policy))
    }
    let concurrent =
        futures_util::future::join_all((0..16).map(|_| request(fixture.address(), "alice"))).await;
    let statuses: Vec<_> = concurrent.into_iter().collect::<Result<Vec<_>>>()?;
    assert_eq!(statuses.iter().filter(|(s, _)| *s == 200).count(), 1);
    assert_eq!(statuses.iter().filter(|(s, _)| *s == 429).count(), 15);
    assert_eq!(request(fixture.address(), "bob").await?.0, 200);
    let mut next = p.snapshot().config.clone();
    next.listeners[0].plugins[1] = builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-policy":"v2"}}}),
    )?;
    p.apply(next.encode()?, 0).await?;
    assert_eq!(
        request(fixture.address(), "alice").await?,
        (429, "v2".into())
    );
    assert_eq!(request(fixture.address(), "bob").await?.0, 429);
    assert_eq!(request(fixture.address(), "carol").await?.0, 200);
    assert_eq!(p.metrics.errors.get(), 0);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_rate_limit_returns_429_and_survives_pool_update() -> Result<()> {
    let backend = http_server("limited").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.runtime.io_workers = 2;
    fixture.config.listeners[0].plugins.push(builtin(
        "rate_limit",
        serde_json::json!({"requests_per_second":1,"burst":1}),
    )?);
    fixture.config.listeners[0].plugins.push(builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-policy":"v1"}}}),
    )?);
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    async fn request(address: SocketAddr, status: u16) -> Result<String> {
        let io = TcpStream::connect(address).await?;
        let (mut client, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), status);
        if status == 429 {
            assert_eq!(response.headers()["retry-after"], "1");
        }
        let policy = response.headers()["x-policy"].to_str()?.to_owned();
        response.into_body().collect().await?;
        drop(client);
        driver.abort();
        Ok(policy)
    }
    request(fixture.address(), 200).await?;
    request(fixture.address(), 429).await?;
    let mut next = p.snapshot().config.clone();
    next.upstreams
        .get_mut("main")
        .unwrap()
        .pool_max_idle_per_host = 32;
    p.apply(next.encode()?, 0).await?;
    request(fixture.address(), 429).await?;
    let mut next = p.snapshot().config.clone();
    next.listeners[0].plugins[1] = builtin(
        "headers",
        serde_json::json!({"response":{"set":{"x-policy":"v2"}}}),
    )?;
    p.apply(next.encode()?, 1).await?;
    assert_eq!(request(fixture.address(), 429).await?, "v2");
    tokio::time::sleep(Duration::from_millis(1100)).await;
    request(fixture.address(), 200).await?;
    assert_eq!(p.metrics.errors.get(), 0);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_health_detects_application_failure_timeout_and_recovers() -> Result<()> {
    use std::sync::atomic::{AtomicU32, Ordering};
    let mode = Arc::new(AtomicU32::new(0));
    let socket = TcpListener::bind("127.0.0.1:0").await?;
    let address = socket.local_addr()?;
    let state = mode.clone();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = socket.accept().await {
            let state = state.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let mode = state.load(Ordering::Relaxed);
                    async move {
                        let status = if req.uri().path() == "/ready" {
                            assert_eq!(req.headers()["host"], "health.internal");
                            if mode == 2 {
                                tokio::time::sleep(Duration::from_millis(300)).await;
                            }
                            if mode == 1 {
                                503
                            } else {
                                200
                            }
                        } else {
                            200
                        };
                        Ok::<_, Infallible>(
                            Response::builder()
                                .status(status)
                                .body(Full::new(Bytes::from_static(b"ok")))
                                .unwrap(),
                        )
                    }
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(io), service)
                    .await;
            });
        }
    });
    let _backend = Server { address, task };
    // Cover the separate HTTP/1 and prior-knowledge HTTP/2 probe drivers.
    for h2 in [false, true] {
        mode.store(0, Ordering::Relaxed);
        let mut fixture = Fixture::new(Protocol::Http, address);
        let upstream = fixture.config.upstreams.get_mut("main").unwrap();
        upstream.http2 = h2;
        upstream.health_check = prost_config::HealthCheck {
            path: Some("/ready".into()),
            host: Some("health.internal".into()),
            timeout_ms: 50,
            fall: 2,
            rise: 2,
            ..Default::default()
        };
        std::fs::write(&fixture.path, fixture.config.encode()?)?;
        let p = fixture.start().await;
        async fn wait_health(p: &Arc<RunningProxy>, healthy: bool) -> Result<()> {
            timeout(Duration::from_secs(4), async {
                while p.status()["upstreams"]["main"][0]["healthy"] != healthy {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            Ok(())
        }
        mode.store(1, Ordering::Relaxed);
        wait_health(&p, false).await?;
        // A pool-only update retains health without sharing counters between generations.
        let mut next = p.snapshot().config.clone();
        next.upstreams
            .get_mut("main")
            .unwrap()
            .pool_max_idle_per_host = 64;
        p.apply(next.encode()?, 0).await?;
        assert_eq!(p.status()["upstreams"]["main"][0]["healthy"], false);
        let io = TcpStream::connect(fixture.address()).await?;
        let (mut client, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), 503);
        response.into_body().collect().await?;
        mode.store(0, Ordering::Relaxed);
        wait_health(&p, true).await?;
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.into_body().collect().await?.to_bytes(), "ok");
        mode.store(2, Ordering::Relaxed);
        wait_health(&p, false).await?;
        mode.store(0, Ordering::Relaxed);
        wait_health(&p, true).await?;
        let mut bad = p.snapshot().config.clone();
        bad.upstreams.get_mut("main").unwrap().health_check.path = Some("/bad\r\ninjected".into());
        assert!(p.apply(bad.encode()?, 1).await.is_err());
        assert_eq!(p.status()["generation"], 1);
        drop(client);
        driver.abort();
        p.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_runtime_fallback_serves_http_with_exact_metrics() -> Result<()> {
    let backend = http_server("fallback").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.runtime.worker_local = false;
    fixture.config.runtime.io_workers = 0;
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "fallback");
    assert_eq!(p.metrics.requests.get(), 1);
    assert_eq!(p.metrics.latency.get_sample_count(), 1);
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_small_and_split_bodies_are_complete() -> Result<()> {
    let backend = http_server("ready").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.runtime.io_workers = 2;
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let mut tasks = Vec::new();
    for index in 0..32 {
        let address = fixture.address();
        tasks.push(tokio::spawn(async move {
            let io = TcpStream::connect(address).await?;
            let (mut client, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
            let driver = tokio::spawn(connection);
            for n in 0..10 {
                let split = (n + index) % 2 == 0;
                let response = client
                    .send_request(
                        Request::builder()
                            .uri(if split { "/split" } else { "/" })
                            .header("host", "localhost")
                            .body(Full::new(Bytes::new()))?,
                    )
                    .await?;
                assert_eq!(
                    response.into_body().collect().await?.to_bytes(),
                    if split { "onetwo" } else { "ready" }
                );
            }
            drop(client);
            driver.abort();
            Ok::<_, anyhow::Error>(())
        }));
    }
    for task in tasks {
        timeout(Duration::from_secs(5), task).await???;
    }
    p.shutdown().await;
    assert_eq!(p.metrics.requests.get(), 320);
    assert_eq!(p.metrics.latency.get_sample_count(), 320);
    Ok(())
}

#[tokio::test]
async fn upgraded_connections_hold_admission_until_tunnel_closes() -> Result<()> {
    let l = TcpListener::bind("127.0.0.1:0").await?;
    let address = l.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(io).await {
                    while let Some(Ok(message)) = ws.next().await {
                        if ws.send(message).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    });
    let server = Server { address, task };
    let mut fixture = Fixture::new(Protocol::Http, server.address);
    fixture.config.runtime.max_connections = 1;
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/", fixture.address())).await?;
    ws.send(tokio_tungstenite::tungstenite::Message::text("held"))
        .await?;
    assert_eq!(ws.next().await.unwrap()?.into_text()?, "held");
    assert_eq!(p.metrics.connections.get(), 1);
    assert!(timeout(
        Duration::from_secs(2),
        tokio_tungstenite::connect_async(format!("ws://{}/", fixture.address()))
    )
    .await?
    .is_err());
    drop(ws);
    timeout(Duration::from_secs(2), async {
        while p.metrics.connections.get() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn compiled_routes_preserve_precedence_and_refresh_upstream_bindings() -> Result<()> {
    let a = http_server("a").await;
    let b = http_server("b").await;
    let mut fixture = Fixture::new(Protocol::Http, a.address);
    fixture
        .config
        .upstreams
        .insert("other".into(), fixture.config.upstreams["main"].clone());
    fixture.config.upstreams.get_mut("other").unwrap().endpoints[0].address = b.address.to_string();
    let route = |host: Option<&str>, prefix: &str, priority, upstream: &str| prost_config::Route {
        host: host.map(str::to_owned),
        path_prefix: prefix.into(),
        priority,
        upstream: upstream.into(),
        plugins: vec![],
    };
    fixture.config.listeners[0].routes = vec![
        route(None, "/api", 0, "main"),
        route(Some("example.com"), "/api/long", 0, "other"),
        route(None, "/api/high", 10, "other"),
    ];
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    for (path, host, expected) in [
        ("/api/long/x", "EXAMPLE.COM:8080", "b"),
        ("/api/long/x", "other.test", "a"),
        ("/api/high/x", "other.test", "b"),
        ("/", "other.test", "a"),
    ] {
        let response = client
            .send_request(
                Request::builder()
                    .uri(path)
                    .header("host", host)
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.into_body().collect().await?.to_bytes(), expected);
    }
    let mut config = p.snapshot().config.clone();
    config.upstreams.get_mut("other").unwrap().endpoints[0].address = a.address.to_string();
    p.apply(config.encode()?, 0).await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("/api/high/x")
                .header("host", "example.com")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "a");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn streamed_response_timeout_releases_request_capacity() -> Result<()> {
    let backend = http_server("ready").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.runtime.max_requests = 1;
    fixture
        .config
        .upstreams
        .get_mut("main")
        .unwrap()
        .idle_timeout_ms = 100;
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    async fn get(address: SocketAddr, path: &str) -> Result<hyper::Response<Incoming>> {
        let io = TcpStream::connect(address).await?;
        let (mut client, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        tokio::spawn(connection);
        Ok(client
            .send_request(
                Request::builder()
                    .uri(path)
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?)
    }
    let slow = get(fixture.address(), "/slow").await?;
    let blocked = get(fixture.address(), "/").await?;
    assert_eq!(blocked.status(), 503);
    blocked.into_body().collect().await?;
    assert!(timeout(Duration::from_secs(2), slow.into_body().collect())
        .await?
        .is_err());
    let response = get(fixture.address(), "/").await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.into_body().collect().await?.to_bytes(), "ready");
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn plugin_end_hook_waits_for_body_and_survives_fast_path_changes() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Recorder(Arc<AtomicUsize>);
    impl prost_core::prost_plugin::NativePlugin for Recorder {
        fn on_event(
            &self,
            event: &prost_core::prost_plugin::Event,
        ) -> Result<prost_core::prost_plugin::Action> {
            if event.hook == "end" {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Default::default())
        }
    }
    let backend = http_server("body").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.listeners[0].routes = vec![prost_config::Route {
        host: None,
        path_prefix: "/".into(),
        priority: 0,
        upstream: "main".into(),
        plugins: vec![prost_config::Plugin {
            name: "recorder".into(),
            module: None,
            hooks: None,
            config: Default::default(),
        }],
    }];
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let count = Arc::new(AtomicUsize::new(0));
    let mut registry = prost_core::prost_plugin::PluginRegistry::new()?;
    registry.register("recorder".into(), Arc::new(Recorder(count.clone())));
    let p = ProxyBuilder::new(&fixture.path)?
        .plugins(registry)
        .start()
        .await?;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let response = client
        .send_request(
            Request::builder()
                .uri("/slow")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(count.load(Ordering::Relaxed), 0);
    assert_eq!(response.into_body().collect().await?.to_bytes(), "late");
    timeout(Duration::from_secs(2), async {
        while count.load(Ordering::Relaxed) != 1 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn connection_limit_is_not_reset_by_upstream_update() -> Result<()> {
    let a = echo().await;
    let b = echo().await;
    let mut fixture = Fixture::new(Protocol::Tcp, a.address);
    fixture.config.listeners[0].plugins = vec![prost_config::Plugin {
        name: "connection_limit".into(),
        module: None,
        hooks: None,
        config: std::collections::BTreeMap::from([("max_connections".into(), 1.into())]),
    }];
    fixture.config.listeners[0].plugins.push(builtin(
        "ip_acl",
        serde_json::json!({"deny":["192.0.2.0/24"]}),
    )?);
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let mut old = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut old, b"held").await?;
    let mut config = p.snapshot().config.clone();
    config.upstreams.get_mut("main").unwrap().endpoints[0].address = b.address.to_string();
    p.apply(config.encode()?, 0).await?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].plugins[1] =
        builtin("ip_acl", serde_json::json!({"deny":["198.51.100.0/24"]}))?;
    p.apply(config.encode()?, 1).await?;
    let mut second = TcpStream::connect(fixture.address()).await?;
    let mut byte = [0];
    assert!(matches!(
        timeout(Duration::from_secs(2), second.read(&mut byte)).await?,
        Ok(0) | Err(_)
    ));
    roundtrip(&mut old, b"still held").await?;
    drop(old);
    drop(second);
    p.shutdown().await;
    Ok(())
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn echo() -> Server {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = l.local_addr().unwrap();
    let task = tokio::spawn(async move {
        while let Ok((mut io, _)) = l.accept().await {
            tokio::spawn(async move {
                let (mut r, mut w) = io.split();
                let _ = tokio::io::copy(&mut r, &mut w).await;
            });
        }
    });
    Server { address, task }
}
async fn http_server(label: &'static str) -> Server {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = l.local_addr().unwrap();
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            tokio::spawn(async move {
                let service = service_fn(move |request: Request<Incoming>| async move {
                    let seen = request.headers().get("x-test-plugin").cloned();
                    let forwarded = request.headers().get("x-forwarded-for").cloned();
                    let body: http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible> =
                        if request.uri().path() == "/grpc" {
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", http::HeaderValue::from_static("0"));
                            let frames = vec![
                                Ok::<_, Infallible>(Frame::data(Bytes::from_static(
                                    b"\0\0\0\0\x04test",
                                ))),
                                Ok(Frame::trailers(trailers)),
                            ];
                            StreamBody::new(futures_util::stream::iter(frames))
                                .map_err(|e| match e {})
                                .boxed_unsync()
                        } else if request.uri().path() == "/split" {
                            let first = futures_util::stream::once(async {
                                Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"one")))
                            });
                            let second = futures_util::stream::once(async {
                                tokio::time::sleep(Duration::from_millis(30)).await;
                                Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"two")))
                            });
                            StreamBody::new(first.chain(second)).boxed_unsync()
                        } else if request.uri().path() == "/slow" {
                            StreamBody::new(futures_util::stream::once(async {
                                tokio::time::sleep(Duration::from_millis(300)).await;
                                Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"late")))
                            }))
                            .boxed_unsync()
                        } else {
                            Full::new(Bytes::from_static(label.as_bytes()))
                                .map_err(|e| match e {})
                                .boxed_unsync()
                        };
                    let path = request.uri().path();
                    let mut response = Response::new(body);
                    if let Some(status) = path
                        .strip_prefix("/status/")
                        .and_then(|v| v.parse::<u16>().ok())
                    {
                        *response.status_mut() = http::StatusCode::from_u16(status).unwrap();
                    }
                    if path == "/slow" {
                        response
                            .headers_mut()
                            .insert("content-length", http::HeaderValue::from_static("4"));
                    }
                    if path == "/split" {
                        response
                            .headers_mut()
                            .insert("content-length", http::HeaderValue::from_static("6"));
                    }
                    response.headers_mut().insert(
                        "content-type",
                        http::HeaderValue::from_static("application/grpc"),
                    );
                    if let Some(v) = seen {
                        response.headers_mut().insert("x-seen-plugin", v);
                    }
                    if let Some(v) = forwarded {
                        response.headers_mut().insert("x-seen-forwarded", v);
                    }
                    Ok::<_, Infallible>(response)
                });
                let _ = auto::Builder::new(TokioExecutor::new())
                    .serve_connection_with_upgrades(TokioIo::new(io), service)
                    .await;
            });
        }
    });
    Server { address, task }
}
fn free_address() -> SocketAddr {
    // Keep reserved fixture ports outside Linux's ephemeral range. Backend
    // listeners using :0 must not take a port between config creation and bind.
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(10000);
    loop {
        let port = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        assert!(port < 32768, "test port range exhausted");
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        if std::net::TcpListener::bind(address).is_ok() {
            return address;
        }
    }
}
struct Fixture {
    dir: TempDir,
    path: PathBuf,
    config: Config,
}
impl Fixture {
    fn new(protocol: Protocol, endpoint: SocketAddr) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prost.toml");
        let text = format!(
            r#"
schema_version=1
[runtime]
admin_socket={socket:?}
artifact_dir={artifacts:?}
drain_seconds=1
worker_local=true
io_workers=1
[upstreams.main]
health_interval_ms=100
idle_timeout_ms=2000
[[upstreams.main.endpoints]]
id="one"
address={endpoint:?}
[[listeners]]
id="main"
address={address:?}
protocol={protocol:?}
upstream="main"
"#,
            socket = dir.path().join("admin.sock").to_str().unwrap(),
            artifacts = dir.path().join("artifacts").to_str().unwrap(),
            endpoint = endpoint.to_string(),
            address = free_address().to_string(),
            protocol = if protocol == Protocol::Tcp {
                "tcp"
            } else {
                "http"
            }
        );
        let config = Config::parse(&text).unwrap();
        std::fs::write(&path, config.encode().unwrap()).unwrap();
        Self { dir, path, config }
    }
    async fn start(&self) -> Arc<RunningProxy> {
        ProxyBuilder::new(&self.path)
            .unwrap()
            .start()
            .await
            .unwrap()
    }
    fn address(&self) -> SocketAddr {
        self.config.listeners[0].address
    }
}
async fn roundtrip(
    io: &mut (impl tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin),
    data: &[u8],
) -> Result<()> {
    timeout(Duration::from_secs(3), async {
        io.write_all(data).await?;
        let mut response = vec![0; data.len()];
        io.read_exact(&mut response).await?;
        assert_eq!(response, data);
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    Ok(())
}

#[tokio::test]
async fn tcp_update_preserves_session_and_half_close() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    let mut old = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut old, b"before").await?;
    let mut next = p.snapshot().config.clone();
    next.upstreams.get_mut("main").unwrap().endpoints[0].enabled = false;
    p.apply(next.encode()?, 0).await?;
    roundtrip(&mut old, b"after").await?;
    let mut new = TcpStream::connect(fixture.address()).await?;
    new.write_all(b"blocked").await?;
    let mut buf = [0; 16];
    assert!(matches!(
        timeout(Duration::from_secs(3), new.read(&mut buf)).await?,
        Ok(0) | Err(_)
    ));
    old.write_all(b"half-close").await?;
    old.shutdown().await?;
    let mut last = Vec::new();
    timeout(Duration::from_secs(3), old.read_to_end(&mut last)).await??;
    assert_eq!(last, b"half-close");
    drop(old);
    drop(new);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn transaction_conflict_bind_failure_and_restart() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    let occupied = TcpListener::bind("127.0.0.1:0").await?;
    let before = std::fs::read(&fixture.path)?;
    let mut bad = p.snapshot().config.clone();
    let mut listener = bad.listeners[0].clone();
    listener.id = "occupied".into();
    listener.address = occupied.local_addr()?;
    bad.listeners.push(listener);
    assert!(p.apply(bad.encode()?, 0).await.is_err());
    assert_eq!(std::fs::read(&fixture.path)?, before);
    assert_eq!(p.snapshot().config.generation, 0);
    let config = p.snapshot().config.encode()?;
    let (a, b) = tokio::join!(p.apply(config.clone(), 0), p.apply(config, 0));
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(p.apply("schema_version = 99".into(), 1).await.is_err());
    assert_eq!(p.snapshot().config.generation, 1);
    let hash = p.snapshot().hash.clone();
    p.shutdown().await;
    drop(p);
    let p = fixture.start().await;
    assert_eq!(p.snapshot().config.generation, 1);
    assert_eq!(p.snapshot().hash, hash);
    let mut io = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut io, b"restart").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn file_edits_do_not_change_running_config() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    std::fs::write(&fixture.path, "invalid config")?;
    let mut io = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut io, b"no watcher").await?;
    assert_eq!(p.snapshot().config.generation, 0);
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn keepalive_requests_use_new_generation_and_release_old() -> Result<()> {
    let a = http_server("old").await;
    let b = http_server("new").await;
    let fixture = Fixture::new(Protocol::Http, a.address);
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let send = |label: &str| {
        Request::builder()
            .uri("/")
            .header("host", label)
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    let old = Arc::downgrade(&p.snapshot());
    let response = client.send_request(send("localhost")).await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "old");
    let mut config = p.snapshot().config.clone();
    config.upstreams.get_mut("main").unwrap().endpoints[0].address = b.address.to_string();
    p.apply(config.encode()?, 0).await?;
    let response = client.send_request(send("localhost")).await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "new");
    timeout(Duration::from_secs(3), async {
        while old.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn http2_grpc_trailers_and_untrusted_forwarded_headers() -> Result<()> {
    let backend = http_server("h2").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fixture.config.upstreams.get_mut("main").unwrap().http2 = true;
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let request = Request::builder()
        .uri(format!("http://{}/grpc", fixture.address()))
        .header("te", "trailers")
        .header("x-forwarded-for", "evil")
        .body(Full::new(Bytes::new()))?;
    let response = client.send_request(request).await?;
    assert_eq!(response.headers()["x-seen-forwarded"], "127.0.0.1");
    let body = response.into_body().collect().await?;
    assert_eq!(body.trailers().unwrap()["grpc-status"], "0");
    assert_eq!(body.to_bytes(), Bytes::from_static(b"\0\0\0\0\x04test"));
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

fn certificate(dir: &std::path::Path) -> Result<prost_config::ServerTls> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let certificate = dir.join("cert.pem");
    let private_key = dir.join("key.pem");
    std::fs::write(&certificate, cert.cert.pem())?;
    std::fs::write(&private_key, cert.key_pair.serialize_pem())?;
    Ok(prost_config::ServerTls {
        certificate,
        private_key,
    })
}
#[tokio::test]
async fn tls_termination_rotation_and_artifact_restart() -> Result<()> {
    let backend = echo().await;
    let mut fixture = Fixture::new(Protocol::Tcp, backend.address);
    let tls = certificate(fixture.dir.path())?;
    fixture.config.listeners[0].tls = Some(tls.clone());
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let client = prost_transport::client_tls(Some(&prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate.clone()),
    }))?;
    let connector = tokio_rustls::TlsConnector::from(client);
    let mut io = connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(fixture.address()).await?,
        )
        .await?;
    roundtrip(&mut io, b"tls old").await?;
    let old_cert = io.get_ref().1.peer_certificates().unwrap()[0].clone();
    let replacement_dir = fixture.dir.path().join("replacement");
    std::fs::create_dir(&replacement_dir)?;
    let replacement = certificate(&replacement_dir)?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].tls = Some(replacement.clone());
    p.apply(config.encode()?, 0).await?;
    roundtrip(&mut io, b"still old session").await?;
    let client = prost_transport::client_tls(Some(&prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(replacement.certificate.clone()),
    }))?;
    let connector = tokio_rustls::TlsConnector::from(client);
    let mut new = connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(fixture.address()).await?,
        )
        .await?;
    assert_ne!(new.get_ref().1.peer_certificates().unwrap()[0], old_cert);
    roundtrip(&mut new, b"new cert").await?;
    // Source files can disappear; committed artifacts remain usable after restart.
    std::fs::remove_file(replacement.certificate)?;
    std::fs::remove_file(replacement.private_key)?;
    drop(io);
    drop(new);
    p.shutdown().await;
    drop(p);
    let p = fixture.start().await;
    let mut io = connector
        .connect(
            "localhost".try_into()?,
            TcpStream::connect(fixture.address()).await?,
        )
        .await?;
    roundtrip(&mut io, b"artifact").await?;
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn tls_upstream_verified_and_bad_certificate_rejected() -> Result<()> {
    let backend = echo().await;
    let mut fixture = Fixture::new(Protocol::Tcp, backend.address);
    let tls = certificate(fixture.dir.path())?;
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&tls, false)?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(mut io) = acceptor.accept(io).await {
                    let (mut r, mut w) = tokio::io::split(&mut io);
                    let _ = tokio::io::copy(&mut r, &mut w).await;
                }
            });
        }
    });
    let server = Server { address, task };
    let upstream = fixture.config.upstreams.get_mut("main").unwrap();
    upstream.endpoints[0].address = server.address.to_string();
    upstream.tls = Some(prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate),
    });
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let mut io = TcpStream::connect(fixture.address()).await?;
    roundtrip(&mut io, b"verified TLS upstream").await?;
    let mut bad = p.snapshot().config.clone();
    bad.listeners[0].tls = Some(prost_config::ServerTls {
        certificate: fixture.dir.path().join("missing"),
        private_key: fixture.dir.path().join("missing-key"),
    });
    assert!(p.apply(bad.encode()?, 0).await.is_err());
    assert_eq!(p.snapshot().config.generation, 0);
    drop(io);
    p.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn https_upstream_protocol_and_alpn_follow_config_after_apply() -> Result<()> {
    let unused = echo().await;
    let mut fixture = Fixture::new(Protocol::Http, unused.address);
    let tls = certificate(fixture.dir.path())?;
    let acceptor = tokio_rustls::TlsAcceptor::from(prost_transport::server_tls(&tls, true)?);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = listener.accept().await {
            let acceptor = acceptor.clone();
            tokio::spawn(async move {
                if let Ok(io) = acceptor.accept(io).await {
                    let h2 = io.get_ref().1.alpn_protocol() == Some(b"h2");
                    let service = service_fn(move |request: Request<Incoming>| async move {
                        Ok::<_, Infallible>(
                            Response::builder()
                                .header(
                                    "x-version",
                                    if request.version() == http::Version::HTTP_2 {
                                        "h2"
                                    } else {
                                        "h1"
                                    },
                                )
                                .header("x-alpn", if h2 { "h2" } else { "h1" })
                                .body(Full::new(Bytes::from_static(b"protocol")))
                                .unwrap(),
                        )
                    });
                    let _ = auto::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(io), service)
                        .await;
                }
            });
        }
    });
    let _server = Server { address, task };
    let upstream = fixture.config.upstreams.get_mut("main").unwrap();
    upstream.endpoints[0].address = address.to_string();
    upstream.tls = Some(prost_config::ClientTls {
        server_name: "localhost".into(),
        ca_certificate: Some(tls.certificate),
    });
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, conn) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(conn);
    for (generation, h2) in [false, true, false].into_iter().enumerate() {
        if generation > 0 {
            let mut next = p.snapshot().config.clone();
            next.upstreams.get_mut("main").unwrap().http2 = h2;
            p.apply(next.encode()?, generation as u64 - 1).await?;
        }
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()["x-version"],
            if h2 { "h2" } else { "h1" }
        );
        assert_eq!(response.headers()["x-alpn"], if h2 { "h2" } else { "h1" });
        assert_eq!(response.into_body().collect().await?.to_bytes(), "protocol");
    }
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn websocket_tunnel_survives_config_update() -> Result<()> {
    let l = TcpListener::bind("127.0.0.1:0").await?;
    let address = l.local_addr()?;
    let task = tokio::spawn(async move {
        while let Ok((io, _)) = l.accept().await {
            tokio::spawn(async move {
                if let Ok(mut ws) = tokio_tungstenite::accept_async(io).await {
                    while let Some(Ok(message)) = ws.next().await {
                        if ws.send(message).await.is_err() {
                            break;
                        }
                    }
                }
            });
        }
    });
    let server = Server { address, task };
    let fixture = Fixture::new(Protocol::Http, server.address);
    let p = fixture.start().await;
    let (mut ws, _) =
        tokio_tungstenite::connect_async(format!("ws://{}/", fixture.address())).await?;
    ws.send(tokio_tungstenite::tungstenite::Message::text("before"))
        .await?;
    assert_eq!(
        timeout(Duration::from_secs(3), ws.next())
            .await?
            .unwrap()?
            .into_text()?,
        "before"
    );
    p.set_enabled("main/one".into(), false, 0).await?;
    ws.send(tokio_tungstenite::tungstenite::Message::text("after"))
        .await?;
    assert_eq!(
        timeout(Duration::from_secs(3), ws.next())
            .await?
            .unwrap()?
            .into_text()?,
        "after"
    );
    drop(ws);
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn listener_add_remove_and_force_shutdown() -> Result<()> {
    let backend = echo().await;
    let fixture = Fixture::new(Protocol::Tcp, backend.address);
    let p = fixture.start().await;
    let mut config = p.snapshot().config.clone();
    let mut second = config.listeners[0].clone();
    second.id = "second".into();
    second.address = free_address();
    let address = second.address;
    config.listeners.push(second);
    p.apply(config.encode()?, 0).await?;
    let mut old = TcpStream::connect(address).await?;
    roundtrip(&mut old, b"added").await?;
    let mut config = p.snapshot().config.clone();
    config.listeners.retain(|l| l.id != "second");
    p.apply(config.encode()?, 1).await?;
    roundtrip(&mut old, b"removed still live").await?;
    // The live stream forces the configured one-second drain deadline.
    timeout(Duration::from_secs(3), p.shutdown()).await?;
    let mut b = [0];
    assert_eq!(old.read(&mut b).await?, 0);
    Ok(())
}

#[cfg(feature = "wasm")]
#[tokio::test]
async fn wasm_header_plugin_updates_and_rejects_invalid_module() -> Result<()> {
    let backend = http_server("plugin").await;
    let mut fixture = Fixture::new(Protocol::Http, backend.address);
    fn module(value: &str) -> String {
        let output = format!(r#"{{"set_headers":{{"x-test-plugin":"{value}"}}}}"#);
        let bytes = output
            .as_bytes()
            .iter()
            .map(|b| format!("\\{b:02x}"))
            .collect::<String>();
        let packed = ((1024u64) << 32) | output.len() as u64;
        format!(
            r#"(module (memory (export "memory") 1) (data (i32.const 1024) "{bytes}") (func (export "prost_abi_version") (result i32) i32.const 1) (func (export "alloc") (param i32) (result i32) i32.const 4096) (func (export "handle") (param i32 i32) (result i64) i64.const {packed}))"#
        )
    }
    let wasm = fixture.dir.path().join("plugin.wat");
    std::fs::write(&wasm, module("v1"))?;
    fixture.config.listeners[0]
        .plugins
        .push(prost_config::Plugin {
            name: "wasm".into(),
            module: Some(wasm.clone()),
            hooks: None,
            config: Default::default(),
        });
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    async fn check(address: SocketAddr, expected: &str) -> Result<()> {
        let io = TcpStream::connect(address).await?;
        let (mut client, connection) =
            hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
        let driver = tokio::spawn(connection);
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        assert_eq!(response.status(), 200);
        assert_eq!(response.headers()["x-seen-plugin"], expected);
        response.into_body().collect().await?;
        drop(client);
        driver.abort();
        Ok(())
    }
    check(fixture.address(), "v1").await?;
    std::fs::write(&wasm, module("v2"))?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].plugins[0].module = Some(wasm.clone());
    p.apply(config.encode()?, 0).await?;
    check(fixture.address(), "v2").await?;
    std::fs::write(&wasm, "invalid wasm")?;
    let mut config = p.snapshot().config.clone();
    config.listeners[0].plugins[0].module = Some(wasm);
    assert!(p.apply(config.encode()?, 1).await.is_err());
    check(fixture.address(), "v2").await?;
    p.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn weighted_round_robin_and_endpoint_disable() -> Result<()> {
    let a = http_server("a").await;
    let b = http_server("b").await;
    let mut fixture = Fixture::new(Protocol::Http, a.address);
    let u = fixture.config.upstreams.get_mut("main").unwrap();
    u.endpoints[0].weight = 2;
    u.endpoints.push(Endpoint {
        id: "two".into(),
        address: b.address.to_string(),
        weight: 1,
        enabled: true,
    });
    std::fs::write(&fixture.path, fixture.config.encode()?)?;
    let p = fixture.start().await;
    let io = TcpStream::connect(fixture.address()).await?;
    let (mut client, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io)).await?;
    let driver = tokio::spawn(connection);
    let mut results = Vec::new();
    for _ in 0..6 {
        let response = client
            .send_request(
                Request::builder()
                    .uri("/")
                    .header("host", "localhost")
                    .body(Full::new(Bytes::new()))?,
            )
            .await?;
        results.push(response.into_body().collect().await?.to_bytes());
    }
    assert_eq!(results.iter().filter(|v| v.as_ref() == b"a").count(), 4);
    assert_eq!(results.iter().filter(|v| v.as_ref() == b"b").count(), 2);
    p.set_enabled("main/one".into(), false, 0).await?;
    let response = client
        .send_request(
            Request::builder()
                .uri("/")
                .header("host", "localhost")
                .body(Full::new(Bytes::new()))?,
        )
        .await?;
    assert_eq!(response.into_body().collect().await?.to_bytes(), "b");
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
