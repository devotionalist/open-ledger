//! HTTP admin surface: status web UI, JSON API, Prometheus metrics, probes.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use open_ledger::LocalBookie;
use serde::Serialize;
use tracing::info;

use crate::metrics::Metrics;

const MAX_LEDGERS: usize = 200;

#[derive(Clone)]
struct AppState {
    bookie: Arc<LocalBookie>,
    metrics: Arc<Metrics>,
}

#[derive(Serialize)]
struct StatusJson {
    bookie_id: String,
    data_dir: String,
    journal_bytes: u64,
    ledger_count: usize,
    fenced_count: usize,
    ledgers: Vec<LedgerJson>,
}

#[derive(Serialize)]
struct LedgerJson {
    ledger_id: u64,
    fenced: bool,
    entry_count: usize,
    last_entry_id: i64,
    lac: i64,
}

/// Bind the admin HTTP server (`/`, `/api/v1/status`, `/metrics`, `/ready`, `/live`).
pub async fn serve(
    addr: std::net::SocketAddr,
    bookie: Arc<LocalBookie>,
    metrics: Arc<Metrics>,
) -> Result<(), std::io::Error> {
    let state = AppState { bookie, metrics };
    let app = Router::new()
        .route("/", get(page_home))
        .route("/api/v1/status", get(api_status))
        .route("/metrics", get(metrics_handler))
        .route("/ready", get(ready))
        .route("/live", get(live))
        .with_state(state);

    info!(%addr, "serving bookie HTTP admin");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await
}

async fn live() -> impl IntoResponse {
    (StatusCode::OK, "ok\n")
}

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    // Cookie already validated at open; presence of bookie_id is enough.
    if state.bookie.bookie_id().is_empty() {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    } else {
        (StatusCode::OK, "ready\n")
    }
}

async fn metrics_handler(State(state): State<AppState>) -> Response {
    let snap = state.bookie.admin_snapshot(0);
    state.metrics.refresh_gauges(&snap);
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

async fn api_status(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.bookie.admin_snapshot(MAX_LEDGERS);
    state.metrics.refresh_gauges(&snap);
    Json(StatusJson {
        bookie_id: snap.bookie_id,
        data_dir: state.bookie.data_dir().display().to_string(),
        journal_bytes: snap.journal_len,
        ledger_count: snap.ledger_count,
        fenced_count: snap.fenced_count,
        ledgers: snap
            .ledgers
            .into_iter()
            .map(|l| LedgerJson {
                ledger_id: l.ledger_id,
                fenced: l.fenced,
                entry_count: l.entry_count,
                last_entry_id: l.last_entry_id,
                lac: l.lac,
            })
            .collect(),
    })
}

async fn page_home(State(state): State<AppState>) -> Html<String> {
    let snap = state.bookie.admin_snapshot(MAX_LEDGERS);
    state.metrics.refresh_gauges(&snap);
    let rows: String = snap
        .ledgers
        .iter()
        .map(|l| {
            format!(
                "<tr><td>{id}</td><td>{fenced}</td><td>{entries}</td><td>{last}</td><td>{lac}</td></tr>",
                id = l.ledger_id,
                fenced = if l.fenced { "yes" } else { "no" },
                entries = l.entry_count,
                last = l.last_entry_id,
                lac = l.lac,
            )
        })
        .collect();
    let truncated = if snap.ledger_count > snap.ledgers.len() {
        format!(
            "<p class=\"muted\">Showing {} of {} ledgers.</p>",
            snap.ledgers.len(),
            snap.ledger_count
        )
    } else {
        String::new()
    };
    Html(format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>open-ledger · {bookie}</title>
  <style>
    :root {{
      --bg: #0f1419;
      --panel: #1a222c;
      --text: #e7ecf1;
      --muted: #8b9aab;
      --accent: #3d9cf0;
      --line: #2a3542;
      --ok: #3ecf8e;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(1200px 600px at 10% -10%, #1c2a3a 0%, var(--bg) 55%);
      color: var(--text); min-height: 100vh;
    }}
    main {{ max-width: 960px; margin: 0 auto; padding: 2.5rem 1.25rem 4rem; }}
    h1 {{ font-size: 1.75rem; letter-spacing: -0.02em; margin: 0 0 0.35rem; }}
    .brand {{ color: var(--accent); font-weight: 600; }}
    .muted {{ color: var(--muted); }}
    .grid {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
      gap: 0.75rem; margin: 1.5rem 0 2rem;
    }}
    .stat {{
      background: var(--panel); border: 1px solid var(--line); border-radius: 10px;
      padding: 0.9rem 1rem;
    }}
    .stat .label {{ font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.04em; }}
    .stat .value {{ font-size: 1.35rem; margin-top: 0.25rem; font-variant-numeric: tabular-nums; }}
    nav a {{
      color: var(--accent); text-decoration: none; margin-right: 1rem; font-size: 0.95rem;
    }}
    nav a:hover {{ text-decoration: underline; }}
    table {{
      width: 100%; border-collapse: collapse; background: var(--panel);
      border: 1px solid var(--line); border-radius: 10px; overflow: hidden;
    }}
    th, td {{
      text-align: left; padding: 0.65rem 0.85rem; border-bottom: 1px solid var(--line);
      font-variant-numeric: tabular-nums;
    }}
    th {{ color: var(--muted); font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.04em; }}
    tr:last-child td {{ border-bottom: none; }}
    .dot {{ color: var(--ok); }}
  </style>
</head>
<body>
  <main>
    <p class="brand">open-ledger</p>
    <h1>Bookie <span class="dot">●</span> {bookie}</h1>
    <p class="muted">{data_dir}</p>
    <nav>
      <a href="/api/v1/status">JSON status</a>
      <a href="/metrics">Prometheus metrics</a>
      <a href="/ready">/ready</a>
      <a href="/live">/live</a>
    </nav>
    <div class="grid">
      <div class="stat"><div class="label">Journal</div><div class="value">{journal} B</div></div>
      <div class="stat"><div class="label">Ledgers</div><div class="value">{ledgers}</div></div>
      <div class="stat"><div class="label">Fenced</div><div class="value">{fenced}</div></div>
    </div>
    <h2>Ledgers</h2>
    {truncated}
    <table>
      <thead><tr><th>ID</th><th>Fenced</th><th>Entries</th><th>Last entry</th><th>LAC</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </main>
</body>
</html>
"##,
        bookie = html_escape(snap.bookie_id.as_str()),
        data_dir = html_escape(&state.bookie.data_dir().display().to_string()),
        journal = snap.journal_len,
        ledgers = snap.ledger_count,
        fenced = snap.fenced_count,
        truncated = truncated,
        rows = if rows.is_empty() {
            "<tr><td colspan=\"5\" class=\"muted\">No ledgers yet.</td></tr>".into()
        } else {
            rows
        },
    ))
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
