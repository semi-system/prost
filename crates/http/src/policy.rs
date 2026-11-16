use crate::{
    cache::Cache, compression::Compressor, Body, ForwardOptions, HttpClient, KeepAlive, Outcome,
    Target,
};
use anyhow::Result;
use http::{Request, Response};
use hyper::body::Body as _;
use prost_config::{HttpPolicy, Upstream};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::OwnedSemaphorePermit;

#[derive(Clone)]
pub(crate) struct CacheResponse;
#[derive(Clone, Copy)]
pub(crate) struct OriginResult(pub Option<u16>);

pub struct HttpResources {
    config: HttpPolicy,
    cache: Option<Arc<Cache>>,
    compression: Option<Compressor>,
    retries: AtomicU64,
}
#[derive(Clone, Copy, Default)]
pub struct PolicyStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub cache_bytes: usize,
    pub cache_entries: usize,
    pub retries: u64,
}
impl HttpResources {
    pub fn cache_status(&self) -> Option<crate::cache_control::CacheStatus> {
        self.cache.as_ref().map(|cache| cache.status())
    }
    pub fn cache_entries(
        &self,
        query: &crate::cache_control::CacheQuery,
    ) -> Result<crate::cache_control::CacheView> {
        self.cache
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("cache is disabled"))?
            .inspect(query)
    }
    pub fn purge_cache(&self, key: Option<&str>) -> Result<crate::cache_control::CachePurge> {
        self.cache
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("cache is disabled"))?
            .purge(key)
    }
    pub fn new(config: HttpPolicy) -> Arc<Self> {
        Arc::new(Self {
            cache: config.cache.clone().map(Cache::new),
            compression: config.compression.clone().map(Compressor::new),
            config,
            retries: AtomicU64::new(0),
        })
    }
    pub fn stats(&self) -> PolicyStats {
        let (cache_hits, cache_misses, cache_evictions, cache_bytes, cache_entries) =
            self.cache.as_ref().map_or((0, 0, 0, 0, 0), |c| c.stats());
        PolicyStats {
            cache_hits,
            cache_misses,
            cache_evictions,
            cache_bytes,
            cache_entries,
            retries: self.retries.load(Ordering::Relaxed),
        }
    }
    pub fn compression_enabled(&self) -> bool {
        self.compression.is_some()
    }
    pub async fn prepare_request(
        &self,
        request: &mut Request<crate::IncomingBody>,
        upstream: &Upstream,
        options: ForwardOptions,
    ) -> Result<()> {
        if self.cache.is_some()
            && request.method() == http::Method::GET
            && request.body().size_hint().exact().is_none_or(|n| n == 0)
        {
            let timeout = options
                .request_policy
                .idle_timeout_ms
                .unwrap_or(upstream.idle_timeout_ms);
            request.body_mut().prepare_empty(timeout).await?;
        }
        Ok(())
    }
    pub fn compress(&self, method: &http::Method, accepted: &str, response: &mut Response<Body>) {
        if let Some(c) = &self.compression {
            c.apply(method, accepted, response);
        }
    }
    /// Called only when endpoints are unavailable, never for capacity rejection.
    pub fn unavailable<B: hyper::body::Body>(
        &self,
        request: &Request<B>,
    ) -> Option<CachedResponse> {
        if !request.body().is_end_stream() {
            return None;
        }
        let cache = self.cache.as_ref()?;
        let key = cache.variant_key(request)?;
        let (fresh, epoch) = cache.lookup(&key);
        fresh
            .or_else(|| cache.candidate(&key, epoch).and_then(|c| cache.stale(&c)))
            .map(CachedResponse)
    }
    #[allow(clippy::too_many_arguments)]
    pub async fn forward<B: Into<crate::IncomingBody>>(
        &self,
        request: Request<B>,
        target: &Target,
        upstream: &Upstream,
        client: &HttpClient,
        permit: OwnedSemaphorePermit,
        keepalive: KeepAlive,
        options: ForwardOptions,
    ) -> Result<Outcome> {
        let mut request = request.map(Into::into);
        self.prepare_request(&mut request, upstream, options)
            .await?;
        let keepalive = keepalive.with_admission(permit);
        let empty = request.body().is_end_stream();
        let key = self
            .cache
            .as_ref()
            .and_then(|cache| empty.then(|| cache.variant_key(&request)).flatten());
        let (fresh, epoch) = match (&self.cache, &key) {
            (Some(cache), Some(key)) => cache.lookup(key),
            _ => (None, 0),
        };
        if let Some(hit) = fresh {
            return Ok(cached(hit, keepalive, options));
        }
        if self.cache.is_some()
            && request
                .headers()
                .get_all(http::header::CACHE_CONTROL)
                .iter()
                .any(|h| {
                    h.to_str().is_ok_and(|s| {
                        s.split(',')
                            .any(|s| s.trim().eq_ignore_ascii_case("only-if-cached"))
                    })
                })
        {
            let mut outcome = cached(
                crate::cache::Hit {
                    headers: http::HeaderMap::new(),
                    version: request.version(),
                    data: bytes::Bytes::new(),
                },
                keepalive,
                options,
            );
            *outcome.response.status_mut() = http::StatusCode::GATEWAY_TIMEOUT;
            return Ok(outcome);
        }
        let candidate = match (&self.cache, &key) {
            (Some(cache), Some(key)) => cache.candidate(key, epoch),
            _ => None,
        };
        if let (Some(cache), Some(candidate), Some(key)) = (&self.cache, &candidate, key) {
            if let Some((hit, refresh)) = cache.swr(candidate) {
                if let Some(refresh) = refresh {
                    let cache = cache.clone();
                    let target = target.clone();
                    let upstream = upstream.clone();
                    let client = client.clone();
                    let mut next = Request::new(None);
                    *next.method_mut() = request.method().clone();
                    *next.uri_mut() = request.uri().clone();
                    *next.version_mut() = request.version();
                    *next.headers_mut() = request.headers().clone();
                    let keepalive = keepalive.clone();
                    let candidate = candidate.clone();
                    tokio::spawn(async move {
                        let _refresh = refresh;
                        let _ = tokio::time::timeout(cache.refresh_timeout(), async {
                            let mut outcome = crate::forward_inner(
                                next,
                                &target,
                                &upstream,
                                &client,
                                None,
                                keepalive,
                                ForwardOptions::default(),
                            )
                            .await?;
                            if outcome.response.status().as_u16() < 500 {
                                cache.invalidate(&candidate);
                            }
                            cache.capture_at(key, epoch, &mut outcome.response);
                            use http_body_util::BodyExt;
                            let mut body = Box::pin(outcome.response.into_body());
                            let mut bytes = 0;
                            while let Some(frame) = body.frame().await {
                                let frame = frame.map_err(anyhow::Error::from_boxed)?;
                                if let Some(data) = frame.data_ref() {
                                    bytes += data.len();
                                    if bytes > cache.object_limit() {
                                        break;
                                    }
                                }
                            }
                            Ok::<_, anyhow::Error>(())
                        })
                        .await;
                    });
                }
                return Ok(cached(hit, keepalive, options));
            }
        }
        let conditional = match (&self.cache, &candidate) {
            (Some(cache), Some(candidate)) if cache.can_revalidate(candidate) => {
                candidate.condition(request.headers_mut());
                true
            }
            _ => false,
        };
        let retry = self.config.retry.as_ref().filter(|_| {
            empty
                && matches!(*request.method(), http::Method::GET | http::Method::HEAD)
                && !request.headers().contains_key(http::header::UPGRADE)
        });
        let result = if retry.is_some() || conditional {
            let (parts, _) = request.into_parts();
            let attempts = retry.map_or(1, |r| r.max_attempts);
            let work = async {
                for attempt in 0..attempts {
                    if attempt > 0 {
                        self.retries.fetch_add(1, Ordering::Relaxed);
                    }
                    let mut next = Request::new(None);
                    *next.method_mut() = parts.method.clone();
                    *next.uri_mut() = parts.uri.clone();
                    *next.version_mut() = parts.version;
                    *next.headers_mut() = parts.headers.clone();
                    *next.extensions_mut() = parts.extensions.clone();
                    let result = crate::forward_inner(
                        next,
                        target,
                        upstream,
                        client,
                        None,
                        keepalive.clone(),
                        options,
                    )
                    .await;
                    if attempt + 1 < attempts {
                        match &result {
                            Ok(outcome)
                                if retry.is_some_and(|r| {
                                    r.statuses.contains(&outcome.response.status().as_u16())
                                }) =>
                            {
                                continue
                            }
                            Err(e) if origin_error(e) => continue,
                            _ => {}
                        }
                    }
                    return result;
                }
                unreachable!("validated retry count")
            };
            if let Some(retry) = retry {
                match tokio::time::timeout(std::time::Duration::from_millis(retry.timeout_ms), work)
                    .await
                {
                    Ok(result) => result,
                    Err(_) => Err(crate::HttpFailure::ResponseHeaderTimeout.into()),
                }
            } else {
                work.await
            }
        } else {
            crate::forward_inner(
                request.map(Some),
                target,
                upstream,
                client,
                None,
                keepalive.clone(),
                options,
            )
            .await
        };
        let failed = match &result {
            Err(error) => origin_error(error),
            Ok(outcome) => matches!(outcome.response.status().as_u16(), 500 | 502 | 503 | 504),
        };
        if failed {
            if let (Some(cache), Some(candidate)) = (&self.cache, &candidate) {
                if let Some(hit) = cache.stale(candidate) {
                    let status = result
                        .as_ref()
                        .ok()
                        .map(|outcome| outcome.response.status().as_u16());
                    let mut outcome = cached(hit, keepalive, options);
                    outcome
                        .response
                        .extensions_mut()
                        .insert(OriginResult(status));
                    return Ok(outcome);
                }
            }
        }
        let mut outcome = result?;
        if conditional && outcome.response.status() == http::StatusCode::NOT_MODIFIED {
            let cache = self.cache.as_ref().unwrap();
            let hit = cache.revalidated(candidate.as_ref().unwrap(), outcome.response.headers())?;
            outcome = cached(hit, keepalive, options);
            outcome
                .response
                .extensions_mut()
                .insert(OriginResult(Some(304)));
        } else if !failed {
            if let (Some(cache), Some(candidate)) = (&self.cache, &candidate) {
                cache.invalidate(candidate);
            }
        }
        if let (Some(cache), Some(key)) = (&self.cache, key) {
            cache.capture_at(key, epoch, &mut outcome.response);
        }
        Ok(outcome)
    }
}
fn origin_error(error: &anyhow::Error) -> bool {
    crate::client_request_failure(error).is_none() && !crate::local_capacity_error(error)
}
pub struct CachedResponse(crate::cache::Hit);
impl CachedResponse {
    pub fn respond(
        self,
        permit: OwnedSemaphorePermit,
        keepalive: KeepAlive,
        options: ForwardOptions,
    ) -> Outcome {
        cached(self.0, keepalive.with_admission(permit), options)
    }
}
fn cached(hit: crate::cache::Hit, keepalive: KeepAlive, options: ForwardOptions) -> Outcome {
    let (done, completion) = if options.notify_completion {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };
    let mut response = Response::new(Body::buffered(
        hit.data,
        crate::body::Guard {
            permit: None,
            keepalive: Some(keepalive),
            completion: crate::body::Completion(done),
        },
    ));
    *response.headers_mut() = hit.headers;
    *response.version_mut() = hit.version;
    response.extensions_mut().insert(CacheResponse);
    Outcome {
        response,
        tunnel: None,
        completion,
    }
}
