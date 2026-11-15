use crate::{
    config::Config,
    store::{now, Bundle},
};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Clone, Serialize)]
pub struct CertificateStatus {
    pub not_after: i64,
    pub bootstrap: bool,
    pub published: bool,
    pub published_at: i64,
    pub delivery_pending: usize,
    pub last_error: Option<&'static str>,
}
pub fn initial(config: &Config, bundles: &[Bundle]) -> BTreeMap<String, CertificateStatus> {
    config
        .certificates
        .iter()
        .zip(bundles)
        .map(|(spec, bundle)| {
            (
                spec.id.clone(),
                CertificateStatus {
                    not_after: bundle.not_after,
                    bootstrap: bundle.bootstrap,
                    published: false,
                    published_at: 0,
                    delivery_pending: spec.delivery.len(),
                    last_error: None,
                },
            )
        })
        .collect()
}
pub fn ready(values: &BTreeMap<String, CertificateStatus>, margin: u64) -> bool {
    !values.is_empty()
        && values.values().all(|value| {
            !value.bootstrap
                && value.published
                && value.not_after > now() + margin as i64
                && value.delivery_pending == 0
        })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn readiness_distinguishes_bootstrap_expired_unpublished_and_delivery() {
        let mut values = BTreeMap::from([(
            "edge".into(),
            CertificateStatus {
                not_after: now() + 300,
                bootstrap: true,
                published: false,
                published_at: 0,
                delivery_pending: 1,
                last_error: None,
            },
        )]);
        assert!(!ready(&values, 60));
        let value = values.get_mut("edge").unwrap();
        value.bootstrap = false;
        value.published = true;
        assert!(!ready(&values, 60));
        values.get_mut("edge").unwrap().delivery_pending = 0;
        assert!(ready(&values, 60));
        values.get_mut("edge").unwrap().not_after = now() + 30;
        assert!(!ready(&values, 60));
    }
}
