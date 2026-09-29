//! `pacer-operator` — run the CacheRing controller, or print its CRD (ADR-0047).
//!
//! ```text
//! pacer-operator          # run the controller (in-cluster or from KUBECONFIG)
//! pacer-operator crd      # print the CustomResourceDefinition as YAML
//! ```

use anyhow::Context as _;
use kube::CustomResourceExt;
use pacer_operator::config::Config;
use pacer_operator::crd::CacheRing;
use pacer_operator::reconcile;
use pacer_operator::render::HelmRenderer;
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("crd") {
        print!("{}", serde_yaml_ng::to_string(&CacheRing::crd())?);
        return Ok(());
    }
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let config = Config::from_env();
    info!(?config, "starting");
    let client = kube::Client::try_default()
        .await
        .context("building the kube client")?;
    let renderer = HelmRenderer {
        helm: config.helm,
        chart: config.chart_dir,
    };
    reconcile::run(client, renderer, config.watch_namespace.as_deref()).await;
    Ok(())
}
