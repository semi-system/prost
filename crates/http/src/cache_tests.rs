use crate::cache_control::{cache_key, encode_key, CacheKeyInput, CacheQuery};
use crate::{
    body::{Completion, Guard},
    cache::Cache,
    Body,
};
use bytes::Bytes;
use http::{Request, Response};
use http_body_util::BodyExt;
fn response(bytes: usize) -> Response<Body> {
    Response::builder()
        .header("cache-control", "public,max-age=60")
        .body(Body::buffered(
            Bytes::from(vec![b'x'; bytes]),
            Guard {
                permit: None,
                keepalive: None,
                completion: Completion(None),
            },
        ))
        .unwrap()
}
fn key(path: &str) -> [u8; 32] {
    Cache::key(&Request::builder().uri(path).body(()).unwrap()).unwrap()
}
#[test]
fn local_socket_exhaustion_is_distinct_from_failed_backend_connects() {
    for kind in [
        std::io::ErrorKind::AddrNotAvailable,
        std::io::ErrorKind::OutOfMemory,
    ] {
        let error = anyhow::Error::new(std::io::Error::from(kind)).context("wrapped connect error");
        assert!(crate::local_capacity_error(&error));
    }
    assert!(!crate::local_capacity_error(
        &std::io::Error::from(std::io::ErrorKind::ConnectionRefused).into()
    ));
    #[cfg(unix)]
    for code in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS] {
        assert!(crate::local_capacity_error(
            &std::io::Error::from_raw_os_error(code).into()
        ));
    }
}
#[tokio::test]
async fn vary_variants_and_stale_refresh_respect_singleflight_and_purge_epoch() {
    let cache = Cache::new(prost_config::Cache {
        vary_headers: vec!["accept-language".into()],
        stale_while_revalidate_seconds: 5,
        max_background_refreshes: 1,
        ..Default::default()
    });
    let en = Request::builder()
        .uri("/vary")
        .header("accept-language", "en")
        .body(())
        .unwrap();
    let vi = Request::builder()
        .uri("/vary")
        .header("accept-language", "vi")
        .body(())
        .unwrap();
    let a = cache.variant_key(&en).unwrap();
    let b = cache.variant_key(&vi).unwrap();
    assert_ne!(a, b);
    for k in [a, b] {
        let mut r = response(3);
        r.headers_mut()
            .insert("vary", http::HeaderValue::from_static("Accept-Language"));
        r.headers_mut().insert(
            "cache-control",
            http::HeaderValue::from_static("public,max-age=1,stale-while-revalidate=5"),
        );
        cache.capture(k, &mut r);
        r.into_body().collect().await.unwrap();
    }
    assert!(cache.get(&a).is_some() && cache.get(&b).is_some());
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let (_, epoch) = cache.lookup(&a);
    let candidate = cache.candidate(&a, epoch).unwrap();
    let (_, first) = cache.swr(&candidate).unwrap();
    assert!(first.is_some());
    assert!(cache.swr(&candidate).unwrap().1.is_none());
    let (_, epoch_b) = cache.lookup(&b);
    let cb = cache.candidate(&b, epoch_b).unwrap();
    assert!(cache.swr(&cb).unwrap().1.is_none());
    cache.purge(None).unwrap();
    assert!(cache.swr(&candidate).is_none());
    drop(first);
    assert_eq!(cache.status().entries, 0);
    let mut unsafe_response = response(3);
    unsafe_response
        .headers_mut()
        .insert("vary", http::HeaderValue::from_static("*"));
    cache.capture(a, &mut unsafe_response);
    unsafe_response.into_body().collect().await.unwrap();
    assert!(cache.get(&a).is_none());
}
#[tokio::test]
async fn purge_exact_and_all_keep_leased_bytes_reserved_and_inspection_read_only() {
    let cache = Cache::new(prost_config::Cache {
        max_bytes: 4096,
        max_entries: 4,
        max_object_bytes: 1024,
        ..Default::default()
    });
    let a = key("/a");
    let b = key("/b");
    for k in [a, b] {
        let mut r = response(700);
        cache.capture(k, &mut r);
        r.into_body().collect().await.unwrap();
    }
    let held = cache.get(&a).unwrap();
    let slice = held.data.slice(..1);
    drop(held);
    let before = cache.stats();
    let view = cache
        .inspect(&CacheQuery {
            limit: 1,
            ..Default::default()
        })
        .unwrap();
    assert!(view.truncated);
    assert_eq!(view.entries.len(), 1);
    assert_eq!(cache.stats(), before);
    let purged = cache.purge(Some(&encode_key(&a).to_uppercase())).unwrap();
    assert_eq!(purged.removed_entries, 1);
    assert!(purged.used_bytes_after > 700);
    assert!(cache.get(&a).is_none());
    assert!(cache.get(&b).is_some());
    let epoch = cache.status().epoch;
    assert!(cache.purge(Some("invalid")).is_err());
    assert_eq!(cache.status().epoch, epoch);
    cache.purge(None).unwrap();
    assert_eq!(cache.status().entries, 0);
    assert!(cache.status().used_bytes > 0);
    drop(slice);
    assert_eq!(cache.status().used_bytes, 0);
    assert_eq!(cache.status().purged_entries, 2);
}
fn streamed(rx: tokio::sync::mpsc::UnboundedReceiver<Bytes>) -> Response<Body> {
    let stream = futures_util::stream::unfold(rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|data| (Ok::<_, crate::Error>(hyper::body::Frame::data(data)), rx))
    });
    Response::builder()
        .header("cache-control", "public,max-age=60")
        .body(Body::custom(Body::text(""), |_| {
            http_body_util::StreamBody::new(stream)
        }))
        .unwrap()
}
#[tokio::test]
async fn purge_blocks_fills_started_before_response_headers_or_during_data() {
    let cache = Cache::new(prost_config::Cache::default());
    let k = key("/race");
    let (_, old) = cache.lookup(&k);
    cache.purge(Some(&encode_key(&k))).unwrap();
    let mut late = response(3);
    cache.capture_at(k, old, &mut late);
    assert_eq!(
        late.into_body().collect().await.unwrap().to_bytes().len(),
        3
    );
    assert!(cache.get(&k).is_none());
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut active = streamed(rx);
    cache.capture(k, &mut active);
    let mut active = active.map(Box::pin);
    tx.send(Bytes::from_static(b"before")).unwrap();
    assert_eq!(
        active
            .body_mut()
            .frame()
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap(),
        "before"
    );
    cache.purge(None).unwrap();
    let mut fresh = Response::builder()
        .header("cache-control", "public,max-age=60")
        .body(Body::text("fresh"))
        .unwrap();
    cache.capture(k, &mut fresh);
    fresh.into_body().collect().await.unwrap();
    tx.send(Bytes::from_static(b"after")).unwrap();
    drop(tx);
    assert_eq!(
        active.into_body().collect().await.unwrap().to_bytes(),
        "after"
    );
    assert_eq!(cache.get(&k).unwrap().data, "fresh");
    assert_eq!(cache.status().active_captures, 0);
    assert_eq!(cache.status().discarded_fills, 2);
}
#[tokio::test]
async fn purge_blocks_old_finish_after_last_data_without_removing_new_entry() {
    let cache = Cache::new(prost_config::Cache::default());
    let k = key("/finish");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut active = streamed(rx);
    cache.capture(k, &mut active);
    let mut active = active.map(Box::pin);
    tx.send(Bytes::from_static(b"old")).unwrap();
    active.body_mut().frame().await.unwrap().unwrap();
    cache.purge(None).unwrap();
    let mut fresh = response(5);
    cache.capture(k, &mut fresh);
    fresh.into_body().collect().await.unwrap();
    drop(tx);
    active.into_body().collect().await.unwrap();
    assert_eq!(cache.get(&k).unwrap().data.len(), 5);
    assert_eq!(cache.status().active_captures, 0);
    assert_eq!(cache.status().discarded_fills, 1);
}
#[test]
fn administrative_key_matches_request_fingerprint_and_preserves_variants() {
    let input = CacheKeyInput {
        uri: "/object?q=1".into(),
        host: Some("Cache.Test".into()),
        accept_encoding: vec!["gzip".into(), "br".into()],
        ..Default::default()
    };
    let request = Request::builder()
        .uri("/object?q=1")
        .header("host", "Cache.Test")
        .header("accept-encoding", "gzip")
        .header("accept-encoding", "br")
        .body(())
        .unwrap();
    assert_eq!(
        cache_key(&input).unwrap(),
        encode_key(&Cache::key(&request).unwrap())
    );
    let mut other = input.clone();
    other.accept_encoding.reverse();
    assert_ne!(cache_key(&input).unwrap(), cache_key(&other).unwrap());
    other = input.clone();
    other.uri = "https://cache.test/object?q=1".into();
    assert_ne!(cache_key(&input).unwrap(), cache_key(&other).unwrap());
    for uri in ["*", "cache.test:443", "ftp://cache.test/a"] {
        other = input.clone();
        other.uri = uri.into();
        assert!(cache_key(&other).is_err());
    }
    other = input.clone();
    other.host = Some("bad\r\nvalue".into());
    assert!(cache_key(&other).is_err());
}
#[tokio::test]
async fn cache_budget_counts_evicted_payload_held_by_slow_hit() {
    let cache = Cache::new(prost_config::Cache {
        max_bytes: 2048,
        max_entries: 1,
        max_object_bytes: 1024,
        ..Default::default()
    });
    let first = key("/first");
    let second = key("/second");
    let mut r = response(700);
    cache.capture(first, &mut r);
    r.into_body().collect().await.unwrap();
    let held = cache.get(&first).unwrap();
    let mut r = response(700);
    cache.capture(second, &mut r);
    r.into_body().collect().await.unwrap();
    assert!(
        cache.get(&second).is_none(),
        "evicted bytes remain reserved while a hit is alive"
    );
    assert!(cache.stats().3 > 0);
    let retained_slice = held.data.slice(..1);
    drop(held);
    assert!(
        cache.stats().3 > 0,
        "a queued data slice retains the allocation budget"
    );
    drop(retained_slice);
    assert_eq!(cache.stats().3, 0);
    let mut r = response(700);
    cache.capture(second, &mut r);
    r.into_body().collect().await.unwrap();
    assert!(cache.get(&second).is_some());
}
#[tokio::test]
async fn cache_releases_abandoned_capture_and_rejects_oversize() {
    let cache = Cache::new(prost_config::Cache {
        max_bytes: 4096,
        max_entries: 2,
        max_object_bytes: 1024,
        ..Default::default()
    });
    let mut r = response(700);
    cache.capture(key("/cancel"), &mut r);
    assert!(cache.stats().3 > 0);
    drop(r);
    assert_eq!(cache.stats().3, 0);
    let mut r = response(2000);
    cache.capture(key("/large"), &mut r);
    assert_eq!(
        r.into_body().collect().await.unwrap().to_bytes().len(),
        2000
    );
    assert_eq!(cache.stats().3, 0);
}
#[test]
fn personalized_and_conditional_requests_never_look_up_cache() {
    for name in [
        "authorization",
        "cookie",
        "range",
        "if-none-match",
        "if-modified-since",
        "pragma",
        "upgrade",
    ] {
        let r = Request::builder()
            .uri("/")
            .header(name, "secret")
            .body(())
            .unwrap();
        assert!(Cache::key(&r).is_none());
    }
}

#[tokio::test]
async fn expired_public_object_is_not_served_and_releases_budget() {
    let cache = Cache::new(prost_config::Cache::default());
    let k = key("/ttl");
    let mut r = response(3);
    r.headers_mut().insert(
        "cache-control",
        http::HeaderValue::from_static("public,max-age=1"),
    );
    cache.capture(k, &mut r);
    r.into_body().collect().await.unwrap();
    assert!(cache.get(&k).is_some());
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    assert!(cache.get(&k).is_none());
    assert_eq!(cache.stats().3, 0);
}
