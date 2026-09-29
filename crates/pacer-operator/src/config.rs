//! Operator configuration, read from the environment. Every variable name is a `const`
//! here and referenced, never retyped (the same contract as the daemon's `config.rs`).

use std::path::PathBuf;

/// Directory of the `pacer` chart the operator renders rings with.
pub const ENV_CHART_DIR: &str = "PACER_OPERATOR_CHART_DIR";
/// Path to the `helm` executable.
pub const ENV_HELM: &str = "PACER_OPERATOR_HELM";
/// Namespace to watch for `CacheRing`s. Unset or empty: every namespace, which needs the
/// cluster-wide RBAC variant of the operator's install.
pub const ENV_WATCH_NAMESPACE: &str = "PACER_OPERATOR_WATCH_NAMESPACE";

/// Where the operator image puts the chart it was released with.
const DEFAULT_CHART_DIR: &str = "/opt/pacer/chart";
/// `helm` resolved through `PATH`, which is where the operator image installs it.
const DEFAULT_HELM: &str = "helm";

/// Resolved operator configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// See [`ENV_CHART_DIR`].
    pub chart_dir: PathBuf,
    /// See [`ENV_HELM`].
    pub helm: PathBuf,
    /// See [`ENV_WATCH_NAMESPACE`]; `None` watches every namespace.
    pub watch_namespace: Option<String>,
}

impl Config {
    /// Read the configuration from the process environment.
    #[must_use]
    pub fn from_env() -> Self {
        Self::from_lookup(|k| std::env::var(k).ok())
    }

    /// Read the configuration through `lookup`, so tests need not mutate the process
    /// environment.
    pub fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Self {
        let set = |k: &str| lookup(k).filter(|v| !v.is_empty());
        Self {
            chart_dir: set(ENV_CHART_DIR)
                .unwrap_or_else(|| DEFAULT_CHART_DIR.to_owned())
                .into(),
            helm: set(ENV_HELM)
                .unwrap_or_else(|| DEFAULT_HELM.to_owned())
                .into(),
            watch_namespace: set(ENV_WATCH_NAMESPACE),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_values_fall_back_to_defaults() {
        let c = Config::from_lookup(|k| (k == ENV_WATCH_NAMESPACE).then(String::new));
        assert_eq!(c.chart_dir, PathBuf::from(DEFAULT_CHART_DIR));
        assert_eq!(c.helm, PathBuf::from(DEFAULT_HELM));
        assert_eq!(c.watch_namespace, None);
    }

    #[test]
    fn set_values_are_taken() {
        let c = Config::from_lookup(|k| Some(format!("/x/{k}")));
        assert_eq!(
            c.watch_namespace.as_deref(),
            Some("/x/PACER_OPERATOR_WATCH_NAMESPACE")
        );
        assert_eq!(c.chart_dir, PathBuf::from("/x/PACER_OPERATOR_CHART_DIR"));
    }
}
