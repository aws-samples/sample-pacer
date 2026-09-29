//! The CRD manifest the operator chart installs is generated from the Rust types; this
//! keeps the two from drifting. After changing `crd.rs`, regenerate it with
//!
//! ```text
//! PACER_OPERATOR_BLESS_CRD=1 cargo test -p pacer-operator --test crd
//! ```
//!
//! (or `pacer-operator crd`, plus the header below).

use std::path::PathBuf;

use kube::CustomResourceExt;
use pacer_operator::crd::CacheRing;

/// Set to rewrite the checked-in manifest instead of comparing against it.
const BLESS_ENV: &str = "PACER_OPERATOR_BLESS_CRD";

const HEADER: &str = "# Generated from crates/pacer-operator/src/crd.rs. Do not edit: \
                      crates/pacer-operator/tests/crd.rs\n\
                      # fails when this file and the Rust types disagree, and says how to regenerate it.\n";

fn manifest() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../deploy/helm/pacer-operator/crds/cacherings.pacer.io.yaml")
}

#[test]
fn chart_crd_matches_the_rust_types() {
    let generated = format!(
        "{HEADER}{}",
        serde_yaml_ng::to_string(&CacheRing::crd()).unwrap()
    );
    if std::env::var_os(BLESS_ENV).is_some() {
        std::fs::write(manifest(), &generated).unwrap();
        return;
    }
    let on_disk = std::fs::read_to_string(manifest()).unwrap_or_default();
    assert!(
        on_disk == generated,
        "{} is stale; regenerate it with {BLESS_ENV}=1 cargo test -p pacer-operator --test crd",
        manifest().display()
    );
}
