use super::*;
use prost_core::{RateTableTarget, TableKey, TableQuery};

fn target(route: Option<usize>, plugin: usize) -> RateTableTarget {
    RateTableTarget {
        listener: "main".into(),
        route,
        plugin,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_rate_tables_survive_apply_and_reset_independently_on_h2() -> Result<()> {
    let backend = http_server("rate").await;
    let mut f = Fixture::new(Protocol::Http, backend.address);
    f.config.runtime.observe_plugins = true;
    f.config.listeners[0].plugins = vec![builtin("headers", serde_json::json!({}))?];
    let mut global = builtin(
        "rate_limit",
        serde_json::json!({"requests_per_second":1,"burst":1}),
    )?;
    global.hooks = Some(vec!["request_headers".into()]);
    f.config.listeners[0].routes = vec![serde_json::from_value(serde_json::json!({
        "path_prefix":"/limited","upstream":"main","plugins":[global,
          builtin("rate_limit",serde_json::json!({"requests_per_second":1,"burst":1,"key":"$http_x_tenant"}))?]}))?];
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let listed = p.rate_table_list(None, 1).await?;
    assert_eq!(listed["total_tables"], 2);
    assert_eq!(listed["truncated"], true);
    assert_eq!(listed["tables"][0]["target"]["route"], 0);
    assert_eq!(listed["tables"][0]["target"]["plugin"], 0);
    let (mut client, connection) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(TcpStream::connect(f.address()).await?),
    )
    .await?;
    let driver = tokio::spawn(connection);
    let request = || {
        Request::builder()
            .uri("http://test/limited")
            .header("x-tenant", "alice")
            .body(Full::new(Bytes::new()))
            .unwrap()
    };
    assert_eq!(client.send_request(request()).await?.status(), 200);
    assert_eq!(client.send_request(request()).await?.status(), 429);
    let query = TableQuery {
        key: Some(TableKey::text("alice")),
        ..Default::default()
    };
    let report = p.rate_table_show(target(Some(0), 1), query.clone()).await?;
    assert_eq!(report["table"]["entries"][0]["key"]["value"], "616c696365");
    let next = p.snapshot().config.encode()?;
    p.apply(next, 0).await?;
    let persisted = std::fs::read(&f.path)?;
    assert_eq!(
        p.rate_table_show(target(Some(0), 1), query.clone()).await?["generation"],
        1
    );
    assert!(p
        .rate_table_clear(target(Some(0), 0), None, true, 0)
        .await
        .unwrap_err()
        .to_string()
        .contains("generation_conflict"));
    p.rate_table_clear(target(Some(0), 0), Some(TableKey::text("global")), false, 1)
        .await?;
    assert_eq!(
        p.rate_table_show(target(Some(0), 1), query.clone()).await?["table"]["retained_entries"],
        1,
        "global clear must not remove the header table"
    );
    p.rate_table_clear(target(Some(0), 0), None, true, 1)
        .await?;
    p.rate_table_clear(target(Some(0), 1), query.key.clone(), false, 1)
        .await?;
    assert_eq!(client.send_request(request()).await?.status(), 200);
    assert_eq!(std::fs::read(&f.path)?, persisted);
    assert_eq!(p.snapshot().config.generation, 1);
    for invalid in [target(None, 0), target(Some(9), 0), target(Some(0), 9)] {
        assert!(p
            .rate_table_show(invalid, TableQuery::default())
            .await
            .is_err());
    }
    let mut next = p.snapshot().config.clone();
    next.listeners[0].routes[0].plugins[1] = builtin(
        "rate_limit",
        serde_json::json!({"requests_per_second":1,"burst":1,"key":"$http_x_tenant","max_keys":128}),
    )?;
    p.apply(next.encode()?, 1).await?;
    assert_eq!(
        p.rate_table_show(target(Some(0), 1), query).await?["table"]["retained_entries"],
        0
    );
    drop(client);
    driver.abort();
    p.shutdown().await;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clearing_connection_window_keeps_live_tcp_and_old_generation() -> Result<()> {
    let backend = echo().await;
    let mut f = Fixture::new(Protocol::Tcp, backend.address);
    f.config.runtime.io_workers = 2;
    f.config.listeners[0].plugins = vec![builtin(
        "connection_window",
        serde_json::json!({"max_connections":1,"window_ms":60000}),
    )?];
    std::fs::write(&f.path, f.config.encode()?)?;
    let p = f.start().await;
    let mut first = TcpStream::connect(f.address()).await?;
    roundtrip(&mut first, b"before clear").await?;
    p.apply(p.snapshot().config.encode()?, 0).await?;
    let mut denied = TcpStream::connect(f.address()).await?;
    assert!(timeout(Duration::from_secs(2), denied.read_u8())
        .await?
        .is_err());
    assert_eq!(
        p.rate_table_show(target(None, 0), TableQuery::default())
            .await?["table"]["entries"][0]["estimated_attempts"],
        2
    );
    p.rate_table_clear(
        target(None, 0),
        Some(TableKey::text("::ffff:127.0.0.1")),
        false,
        1,
    )
    .await?;
    let mut second = TcpStream::connect(f.address()).await?;
    roundtrip(&mut second, b"after clear").await?;
    roundtrip(&mut first, b"old session remains live").await?;
    assert_eq!(p.status()["live_generations"], 2);
    p.rate_table_clear(target(None, 0), None, true, 1).await?;
    roundtrip(&mut first, b"clear-all also preserves live sessions").await?;
    drop(first);
    drop(second);
    p.shutdown().await;
    Ok(())
}
