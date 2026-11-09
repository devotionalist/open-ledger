//! HTTP admin for the AutoRecovery sidecar.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;
use tracing::info;

use crate::metrics::ArMetrics;

#[derive(Clone)]
struct AppState {
    bookie_id: String,
    metrics: Arc<ArMetrics>,
}

#[derive(Serialize)]
struct StatusJson {
    bookie_id: String,
    auditor_leader: bool,
}

/// Serve `/`, `/api/v1/status`, `/metrics`, `/ready`, `/live` on a tokio runtime.
pub fn spawn(addr: SocketAddr, bookie_id: String, metrics: Arc<ArMetrics>) {
    std::thread::Builder::new()
        .name("ar-http".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("ar http runtime");
            if let Err(err) = rt.block_on(serve(addr, bookie_id, metrics)) {
                tracing::error!(%err, "AR HTTP server exited");
            }
        })
        .expect("spawn ar http");
}

async fn serve(
    addr: SocketAddr,
    bookie_id: String,
    metrics: Arc<ArMetrics>,
) -> Result<(), std::io::Error> {
    let state = AppState { bookie_id, metrics };
    let app = Router::new()
        .route("/", get(page_home))
        .route("/api/v1/status", get(api_status))
        .route("/metrics", get(metrics_handler))
        .route("/ready", get(|| async { (StatusCode::OK, "ready\n") }))
        .route("/live", get(|| async { (StatusCode::OK, "ok\n") }))
        .with_state(state);
    info!(%addr, "serving AutoRecovery HTTP admin");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}

async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    Json(StatusJson {
        bookie_id: state.bookie_id.clone(),
        auditor_leader: state.metrics.is_leader(),
    })
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    match state.metrics.encode() {
        Ok(body) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("metrics encode: {err}\n"),
        )
            .into_response(),
    }
}

async fn page_home(State(state): State<AppState>) -> Html<String> {
    let leader = state.metrics.is_leader();
    Html(format!(
        r##"<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><title>open-ledger AR · {id}</title>
<style>
  body {{ margin:0; font-family: "IBM Plex Sans", sans-serif; background:#101820; color:#e8eef4; }}
  main {{ max-width:720px; margin:0 auto; padding:2rem 1.25rem; }}
  a {{ color:#5eb1ff; margin-right:1rem; }}
  .card {{ background:#1b2630; border:1px solid #2c3a47; border-radius:10px; padding:1rem 1.1rem; margin-top:1rem; }}
  .ok {{ color:#3ecf8e; }}
</style></head>
<body><main>
  <p style="color:#5eb1ff;font-weight:600">open-ledger AutoRecovery</p>
  <h1>{id}</h1>
  <p>Auditor leader: <strong class="ok">{leader}</strong></p>
  <nav><a href="/api/v1/status">JSON</a><a href="/metrics">Prometheus</a><a href="/ready">/ready</a><a href="/live">/live</a></nav>
  <div class="card">Colocated sidecar for bookie <code>{id}</code>. Metrics cover election, registry heartbeats, under-replicated enqueue, and local worker copy progress.</div>
</main></body></html>
"##,
        id = state.bookie_id,
        leader = if leader { "yes" } else { "no" },
    ))
}
