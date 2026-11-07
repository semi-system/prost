use super::*;

#[test]
fn strips_all_connection_nominated_fields_and_keeps_grpc_te() {
    let mut headers = HeaderMap::new();
    headers.append(
        header::CONNECTION,
        HeaderValue::from_static("keep-alive, x-first"),
    );
    headers.append(header::CONNECTION, HeaderValue::from_static("x-second"));
    headers.insert("x-first", HeaderValue::from_static("one"));
    headers.insert("x-second", HeaderValue::from_static("two"));
    headers.insert("keep-alive", HeaderValue::from_static("timeout=60"));
    headers.insert(header::TE, HeaderValue::from_static("trailers"));
    sanitize(&mut headers, false, true);
    assert!(!headers.contains_key("x-first") && !headers.contains_key("x-second"));
    assert!(!headers.contains_key(header::CONNECTION) && !headers.contains_key("keep-alive"));
    assert_eq!(headers[header::TE], "trailers");
}

#[test]
fn websocket_upgrade_headers_are_preserved() {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONNECTION,
        HeaderValue::from_static("upgrade, keep-alive"),
    );
    headers.insert(header::UPGRADE, HeaderValue::from_static("websocket"));
    sanitize(&mut headers, true, true);
    assert_eq!(headers[header::CONNECTION], "upgrade");
    assert_eq!(headers[header::UPGRADE], "websocket");
}
