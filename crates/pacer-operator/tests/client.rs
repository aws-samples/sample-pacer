//! The operator's kube client can build a TLS connector at all.
//!
//! kube 4's `rustls-tls` feature does not choose a rustls crypto backend — `aws-lc-rs` and
//! `ring` are separate kube features — and nothing else in this crate's dependency tree
//! chose one either. rustls then panics ("Could not automatically determine the
//! process-level CryptoProvider") the first time a TLS config is built, which for the
//! operator is the in-cluster client on its first line of work. Every unit, render and
//! image check passed with that build; the first live run crash-looped. The fix is the
//! `aws-lc-rs` kube feature in Cargo.toml (the backend the daemon uses too), and this test
//! is what fails if it goes.

#[test]
fn an_https_kube_client_builds() {
    // Never dialled: constructing the client is where the connector, and so the rustls
    // config, is built.
    let config = kube::Config::new("https://127.0.0.1:1".parse().unwrap());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _guard = runtime.enter();
    kube::Client::try_from(config).expect("an HTTPS kube client builds");
}
