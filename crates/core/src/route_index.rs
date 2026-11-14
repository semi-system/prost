use prost_config::Listener;
use std::collections::HashMap;

/// Prefix hashes are only an index: compare the configured text on every hit.
/// Memory scales with route count, including very long configured prefixes.
pub(crate) struct RouteIndex {
    buckets: HashMap<(usize, u64), Vec<(usize, usize)>>,
    max_len: usize,
}
impl RouteIndex {
    pub fn new(listener: &Listener, order: &[usize]) -> Self {
        let mut buckets: HashMap<_, Vec<_>> = HashMap::new();
        let mut max_len = 0;
        for (rank, &index) in order.iter().enumerate() {
            let prefix = listener.routes[index].path_prefix.as_bytes();
            let hash = prefix.iter().fold(OFFSET, |hash, &byte| step(hash, byte));
            buckets
                .entry((prefix.len(), hash))
                .or_default()
                .push((rank, index));
            max_len = max_len.max(prefix.len());
        }
        Self { buckets, max_len }
    }
    #[inline]
    pub fn select(
        &self,
        listener: &Listener,
        host: &str,
        path: &str,
        matches: impl Fn(usize) -> bool,
    ) -> Option<usize> {
        let mut best = (usize::MAX, None);
        let mut hash = OFFSET;
        for (offset, &byte) in path.as_bytes().iter().take(self.max_len).enumerate() {
            hash = step(hash, byte);
            if let Some(routes) = self.buckets.get(&(offset + 1, hash)) {
                for &(rank, index) in routes {
                    if rank >= best.0 {
                        break;
                    }
                    let route = &listener.routes[index];
                    if path.starts_with(&route.path_prefix)
                        && route
                            .host
                            .as_ref()
                            .is_none_or(|h| h.eq_ignore_ascii_case(host))
                        && matches(index)
                    {
                        best = (rank, Some(index));
                        break;
                    }
                }
            }
        }
        best.1
    }
}
const OFFSET: u64 = 0xcbf29ce484222325;
#[inline]
fn step(hash: u64, byte: u8) -> u64 {
    (hash ^ u64::from(byte)).wrapping_mul(0x100000001b3)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn indexed_routes_equal_linear_priority_host_and_order() {
        let routes:Vec<_>=(0..512).map(|i|serde_json::json!({"path_prefix":if i%5==0{"/".into()}else{format!("/service/{}/",i%32)},"host":if i%3==0{Some("EXAMPLE.test")}else{None},"priority":(i%7)-3,"upstream":"main"})).collect();
        let listener:Listener=serde_json::from_value(serde_json::json!({"id":"test","address":"127.0.0.1:8080","protocol":"http","routes":routes})).unwrap();
        let mut order: Vec<_> = (0..listener.routes.len()).collect();
        order.sort_by_key(|&i| {
            (
                std::cmp::Reverse(listener.routes[i].priority),
                std::cmp::Reverse(listener.routes[i].path_prefix.len()),
                i,
            )
        });
        let index = RouteIndex::new(&listener, &order);
        for host in ["example.test", "OTHER.test", ""] {
            for number in 0..65 {
                let path = format!("/service/{number}/nested");
                let expected = order.iter().copied().find(|&i| {
                    let r = &listener.routes[i];
                    path.starts_with(&r.path_prefix)
                        && r.host.as_ref().is_none_or(|h| h.eq_ignore_ascii_case(host))
                });
                assert_eq!(index.select(&listener, host, &path, |_| true), expected);
            }
        }
    }
    #[test]
    fn indexed_routes_continue_after_higher_priority_predicate_mismatch() {
        let routes:Vec<_>=(0..128).map(|i|serde_json::json!({"path_prefix":if i%3==0 {"/".into()} else {format!("/api/{}/",i%8)},
            "priority":i%5,"upstream":"main","methods":if i%2==0 {vec!["POST"]} else {vec![]},
            "headers":if i%4==0 {serde_json::json!([{"name":"x-region","equals":"eu"}])} else {serde_json::json!([])}})).collect();
        let listener:Listener=serde_json::from_value(serde_json::json!({"id":"test","address":"127.0.0.1:8080","protocol":"http","routes":routes})).unwrap();
        let mut order: Vec<_> = (0..listener.routes.len()).collect();
        order.sort_by_key(|&i| {
            (
                std::cmp::Reverse(listener.routes[i].priority),
                std::cmp::Reverse(listener.routes[i].path_prefix.len()),
                i,
            )
        });
        let predicates: Vec<_> = listener
            .routes
            .iter()
            .map(|r| crate::route_match::RouteMatch::compile(r).unwrap())
            .collect();
        let index = RouteIndex::new(&listener, &order);
        for method in [http::Method::GET, http::Method::POST, http::Method::DELETE] {
            for region in [None, Some("eu"), Some("us")] {
                let mut headers = http::HeaderMap::new();
                if let Some(region) = region {
                    headers.insert("x-region", region.parse().unwrap());
                }
                for i in 0..16 {
                    let path = format!("/api/{i}/item");
                    let matched = |index: usize| predicates[index].matches(&method, &headers);
                    let expected = order
                        .iter()
                        .copied()
                        .find(|&i| path.starts_with(&listener.routes[i].path_prefix) && matched(i));
                    assert_eq!(index.select(&listener, "", &path, matched), expected);
                }
            }
        }
    }
}
