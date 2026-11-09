//! Shared tracing init: text by default, JSON when `RUST_LOG_FORMAT=json`.

use tracing::Level;
use tracing_subscriber::EnvFilter;

fn env_filter() -> EnvFilter {
    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"))
}

fn wants_json() -> bool {
    std::env::var("RUST_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
}

/// Install the global subscriber. Call once at process start.
pub fn init() {
    let filter = env_filter();
    if wants_json() {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_current_span(true)
            .with_span_list(false)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_max_level(Level::INFO)
            .init();
    }
}
