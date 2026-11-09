//! Live OpenKV CAS smoke (create-only, version CAS, conflict).
//!
//! ```bash
//! OPENKV_ADDR=127.0.0.1:19000 cargo run -p open-ledger-oxia --example openkv_cas_smoke
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use open_ledger_oxia::{CoordStore, OpenKvCoord, PutExpect};

fn main() {
    let addr = std::env::var("OPENKV_ADDR").unwrap_or_else(|_| "127.0.0.1:19000".into());
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let key = format!("/open-ledger/smoke/cas-{suffix}");
    println!("gateway={addr} key={key}");

    let coord = OpenKvCoord::connect(&addr).unwrap_or_else(|e| panic!("connect: {e}"));

    // create-only
    let v1 = coord
        .put(&key, b"one", PutExpect::NotExists, false)
        .unwrap_or_else(|e| panic!("create-only: {e}"));
    println!("create-only ok revision={v1}");

    // second create-only must fail
    match coord.put(&key, b"dup", PutExpect::NotExists, false) {
        Err(open_ledger::Error::AlreadyExists(_)) => println!("create-only conflict ok"),
        other => panic!("expected AlreadyExists, got {other:?}"),
    }

    // version CAS
    let v2 = coord
        .put(&key, b"two", PutExpect::Version(v1), false)
        .unwrap_or_else(|e| panic!("cas@{v1}: {e}"));
    assert_ne!(v2, v1);
    println!("cas ok {v1} -> {v2}");

    // stale CAS must fail
    match coord.put(&key, b"stale", PutExpect::Version(v1), false) {
        Err(open_ledger::Error::VersionConflict(_)) => println!("stale cas conflict ok"),
        other => panic!("expected VersionConflict, got {other:?}"),
    }

    let got = coord.get(&key).unwrap_or_else(|e| panic!("get: {e}"));
    assert_eq!(got.value, b"two");
    assert_eq!(got.version_id, v2);
    println!("get ok value=two revision={v2}");

    // index + range for ledger-shaped key
    let lid = 900_000 + (suffix % 1000) as u64;
    let lkey = open_ledger::keys::ledger_key(lid);
    let _ = coord
        .put(&lkey, br#"{"smoke":true}"#, PutExpect::Any, false)
        .unwrap_or_else(|e| panic!("ledger put: {e}"));
    let listed = coord
        .range_scan(
            open_ledger::keys::ledgers_prefix(),
            open_ledger::keys::ledgers_prefix_end(),
        )
        .unwrap_or_else(|e| panic!("range_scan: {e}"));
    assert!(
        listed.iter().any(|r| r.key == lkey),
        "ledger key missing from index scan: {listed:?}"
    );
    println!("index range_scan ok ({} keys, includes {lkey})", listed.len());

    let _ = coord.delete(&key, Some(v2));
    let _ = coord.delete(&lkey, None);
    println!("PASS");
}
