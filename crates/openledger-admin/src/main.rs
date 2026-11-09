//! Cluster admin: Oxia coordination-plane metrics + multi-bookie overview UI.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use clap::Parser;
use open_ledger::keys;
use open_ledger_oxia::{
    connect_coord, AnyCoord, BookieRegistry, CoordBackend, CoordStore,
};
use prometheus::{Encoder, IntGauge, Registry, TextEncoder};
use serde::Serialize;
use tracing::{info, warn};

#[derive(Debug, Parser)]
#[command(name = "openledger-admin")]
struct Args {
    /// Coordination backend: `oxia` (default) or `openkv`.
    #[arg(long, env = "COORD_BACKEND", default_value = "oxia")]
    coord_backend: String,

    /// Oxia service address (when `--coord-backend=oxia`).
    #[arg(long, env = "OXIA_SERVICE", default_value = "oxia:6648")]
    oxia: String,

    /// OpenKV Gateway address (when `--coord-backend=openkv`).
    #[arg(long, env = "OPENKV_ADDR", default_value = "openkv:19000")]
    openkv: String,

    /// OpenKV ephemeral TTL; unused for Oxia.
    #[arg(long, env = "OPENKV_EPHEMERAL_TTL_SECS", default_value = "15")]
    openkv_ephemeral_ttl_secs: u64,

    /// HTTP listen address.
    #[arg(long, default_value = "0.0.0.0:8080")]
    listen: SocketAddr,

    /// How often to refresh coordination gauges.
    #[arg(long, default_value = "5")]
    refresh_secs: u64,
}

#[derive(Clone)]
struct AppState {
    metrics: Arc<ClusterMetrics>,
    cache: Arc<Mutex<ClusterSnapshot>>,
}

#[derive(Clone, Serialize, Default)]
struct ClusterSnapshot {
    bookies: Vec<BookieRow>,
    ledgers: usize,
    underreplicated: Vec<TaskRow>,
    refreshed_unix: i64,
}

#[derive(Clone, Serialize)]
struct BookieRow {
    bookie_id: String,
    address: String,
}

#[derive(Clone, Serialize)]
struct TaskRow {
    ledger_id: u64,
    missing_bookie: String,
    not_before_unix: i64,
}

struct ClusterMetrics {
    registry: Registry,
    alive_bookies: IntGauge,
    ledger_metas: IntGauge,
    underreplicated: IntGauge,
    waiting_not_before: IntGauge,
}

impl ClusterMetrics {
    fn new() -> prometheus::Result<Arc<Self>> {
        let registry = Registry::new();
        let alive_bookies = IntGauge::new(
            "openledger_cluster_alive_bookies",
            "Bookies present in the Oxia registry",
        )?;
        let ledger_metas = IntGauge::new(
            "openledger_cluster_ledger_metas",
            "Ledger metadata records in Oxia",
        )?;
        let underreplicated = IntGauge::new(
            "openledger_cluster_underreplicated_tasks",
            "Under-replicated task keys in Oxia",
        )?;
        let waiting_not_before = IntGauge::new(
            "openledger_cluster_underreplicated_waiting",
            "Under-replicated tasks still waiting for lostBookieRecoveryDelay",
        )?;
        for c in [
            Box::new(alive_bookies.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(ledger_metas.clone()),
            Box::new(underreplicated.clone()),
            Box::new(waiting_not_before.clone()),
        ] {
            registry.register(c)?;
        }
        Ok(Arc::new(Self {
            registry,
            alive_bookies,
            ledger_metas,
            underreplicated,
            waiting_not_before,
        }))
    }

    fn apply_snapshot(&self, snap: &ClusterSnapshot) {
        self.alive_bookies.set(snap.bookies.len() as i64);
        self.ledger_metas.set(snap.ledgers as i64);
        self.underreplicated.set(snap.underreplicated.len() as i64);
        let now = now_unix();
        let waiting = snap
            .underreplicated
            .iter()
            .filter(|t| t.not_before_unix > now)
            .count();
        self.waiting_not_before.set(waiting as i64);
    }

    fn encode(&self) -> Result<String, prometheus::Error> {
        let mut buf = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut buf)?;
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    if std::env::var("RUST_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false)
    {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(filter)
            .with_current_span(true)
            .with_span_list(false)
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

fn main() {
    init_logging();
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("runtime");
    if let Err(err) = rt.block_on(run(args)) {
        panic!("admin failed: {err}");
    }
}

async fn run(args: Args) -> Result<(), Box<dyn std::error::Error>> {
    let backend = CoordBackend::parse(&args.coord_backend)?;
    info!(
        coord_backend = ?backend,
        oxia = %args.oxia,
        openkv = %args.openkv,
        listen = %args.listen,
        "starting openledger-admin"
    );
    let coord = connect_coord(
        backend,
        &args.oxia,
        &args.openkv,
        Duration::from_secs(args.openkv_ephemeral_ttl_secs.max(1)),
    )?;
    let metrics = ClusterMetrics::new()?;
    let cache = Arc::new(Mutex::new(ClusterSnapshot::default()));
    let state = AppState {
        metrics: Arc::clone(&metrics),
        cache: Arc::clone(&cache),
    };

    // Background refresh of coordination-plane gauges / snapshot.
    let refresh_coord = Arc::clone(&coord);
    let refresh_metrics = Arc::clone(&metrics);
    let refresh_cache = Arc::clone(&cache);
    let every = Duration::from_secs(args.refresh_secs.max(1));
    std::thread::Builder::new()
        .name("cluster-refresh".into())
        .spawn(move || loop {
            match refresh_snapshot(&refresh_coord) {
                Ok(snap) => {
                    refresh_metrics.apply_snapshot(&snap);
                    *refresh_cache.lock().expect("cache") = snap;
                }
                Err(err) => warn!(%err, "cluster refresh failed"),
            }
            std::thread::sleep(every);
        })?;

    let app = Router::new()
        .route("/", get(page_home))
        .route("/api/v1/cluster", get(api_cluster))
        .route("/metrics", get(metrics_handler))
        .route("/ready", get(|| async { (StatusCode::OK, "ready\n") }))
        .route("/live", get(|| async { (StatusCode::OK, "ok\n") }))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn refresh_snapshot(coord: &Arc<AnyCoord>) -> open_ledger::Result<ClusterSnapshot> {
    let bookies = BookieRegistry::new(Arc::clone(coord)).list_alive()?;
    let ledgers = coord
        .range_scan(keys::ledgers_prefix(), keys::ledgers_prefix_end())?
        .len();
    let tasks = open_ledger_oxia::list_tasks(coord.as_ref())?;
    Ok(ClusterSnapshot {
        bookies: bookies
            .into_iter()
            .map(|b| BookieRow {
                bookie_id: b.bookie_id,
                address: b.address,
            })
            .collect(),
        ledgers,
        underreplicated: tasks
            .into_iter()
            .map(|t| TaskRow {
                ledger_id: t.ledger_id,
                missing_bookie: t.missing_bookie,
                not_before_unix: t.not_before_unix,
            })
            .collect(),
        refreshed_unix: now_unix(),
    })
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

async fn api_cluster(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.cache.lock().expect("cache").clone();
    Json(snap)
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
    let snap = state.cache.lock().expect("cache").clone();
    let bookie_rows: String = snap
        .bookies
        .iter()
        .map(|b| {
            format!(
                "<tr><td>{id}</td><td><code>{addr}</code></td></tr>",
                id = html_escape(&b.bookie_id),
                addr = html_escape(&b.address),
            )
        })
        .collect();
    let task_rows: String = snap
        .underreplicated
        .iter()
        .map(|t| {
            format!(
                "<tr><td>{lid}</td><td>{missing}</td><td>{nb}</td></tr>",
                lid = t.ledger_id,
                missing = html_escape(&t.missing_bookie),
                nb = t.not_before_unix,
            )
        })
        .collect();
    Html(format!(
        r##"<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"/><title>open-ledger cluster</title>
<style>
  :root {{ --bg:#0f1419; --panel:#1a222c; --text:#e7ecf1; --muted:#8b9aab; --accent:#3d9cf0; --line:#2a3542; }}
  body {{ margin:0; font-family:"IBM Plex Sans",sans-serif; background:radial-gradient(900px 500px at 0% 0%,#1a2a3a,var(--bg)); color:var(--text); }}
  main {{ max-width:980px; margin:0 auto; padding:2.25rem 1.25rem 4rem; }}
  a {{ color:var(--accent); margin-right:1rem; text-decoration:none; }}
  .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:.75rem; margin:1.25rem 0 1.75rem; }}
  .stat {{ background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:.9rem 1rem; }}
  .label {{ color:var(--muted); font-size:.75rem; text-transform:uppercase; letter-spacing:.04em; }}
  .value {{ font-size:1.35rem; margin-top:.2rem; font-variant-numeric:tabular-nums; }}
  table {{ width:100%; border-collapse:collapse; background:var(--panel); border:1px solid var(--line); border-radius:10px; overflow:hidden; margin-bottom:1.5rem; }}
  th,td {{ text-align:left; padding:.65rem .85rem; border-bottom:1px solid var(--line); }}
  th {{ color:var(--muted); font-size:.75rem; text-transform:uppercase; }}
  .muted {{ color:var(--muted); }}
</style></head>
<body><main>
  <p style="color:var(--accent);font-weight:600;margin:0">open-ledger</p>
  <h1 style="margin:.2rem 0 .4rem">Cluster</h1>
  <p class="muted">Refreshed unix={ts}</p>
  <nav><a href="/api/v1/cluster">JSON</a><a href="/metrics">Prometheus</a><a href="/ready">/ready</a></nav>
  <div class="grid">
    <div class="stat"><div class="label">Alive bookies</div><div class="value">{alive}</div></div>
    <div class="stat"><div class="label">Ledger metas</div><div class="value">{ledgers}</div></div>
    <div class="stat"><div class="label">Under-replicated</div><div class="value">{ur}</div></div>
  </div>
  <h2>Bookies</h2>
  <table><thead><tr><th>ID</th><th>Address</th></tr></thead><tbody>{bookie_rows}</tbody></table>
  <h2>Under-replicated</h2>
  <table><thead><tr><th>Ledger</th><th>Missing</th><th>not_before</th></tr></thead><tbody>{task_rows}</tbody></table>
</main></body></html>
"##,
        ts = snap.refreshed_unix,
        alive = snap.bookies.len(),
        ledgers = snap.ledgers,
        ur = snap.underreplicated.len(),
        bookie_rows = if bookie_rows.is_empty() {
            "<tr><td colspan=\"2\" class=\"muted\">None registered</td></tr>".into()
        } else {
            bookie_rows
        },
        task_rows = if task_rows.is_empty() {
            "<tr><td colspan=\"3\" class=\"muted\">Queue empty</td></tr>".into()
        } else {
            task_rows
        },
    ))
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
