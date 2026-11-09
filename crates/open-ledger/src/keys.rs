//! Coordination key layout for ledger metadata and AutoRecovery.
//!
//! Shared by Oxia and OpenKV backends. OpenKV has no Gateway range scan, so
//! listing uses DS Set indexes under [`index_ledgers`] / [`index_bookies`] /
//! [`index_underreplicated`].

use crate::types::{BookieId, LedgerId};

/// Namespace prefix for all open-ledger keys in the coordination store.
pub const ROOT: &str = "/open-ledger";

/// Ledger metadata key: `/open-ledger/ledgers/{id}`.
pub fn ledger_key(ledger_id: LedgerId) -> String {
    format!("{ROOT}/ledgers/{ledger_id}")
}

/// Inclusive start of the ledger key range for scans.
pub fn ledgers_prefix() -> &'static str {
    "/open-ledger/ledgers/"
}

/// Exclusive end of the ledger key range (`~` sentinel for hierarchical order).
pub fn ledgers_prefix_end() -> &'static str {
    "/open-ledger/ledgers/~"
}

/// Bookie registry key (ephemeral): `/open-ledger/bookies/{id}`.
pub fn bookie_key(bookie_id: &str) -> String {
    format!("{ROOT}/bookies/{bookie_id}")
}

/// Inclusive start of the bookie key range.
pub fn bookies_prefix() -> &'static str {
    "/open-ledger/bookies/"
}

/// Exclusive end of the bookie key range.
pub fn bookies_prefix_end() -> &'static str {
    "/open-ledger/bookies/~"
}

/// Under-replicated task key: `/open-ledger/underreplicated/{ledgerId}`.
pub fn underreplicated_key(ledger_id: LedgerId) -> String {
    format!("{ROOT}/underreplicated/{ledger_id}")
}

/// Inclusive start of under-replicated tasks.
pub fn underreplicated_prefix() -> &'static str {
    "/open-ledger/underreplicated/"
}

/// Exclusive end of under-replicated tasks.
pub fn underreplicated_prefix_end() -> &'static str {
    "/open-ledger/underreplicated/~"
}

/// Worker lock for a task (ephemeral): `/open-ledger/locks/underreplicated/{ledgerId}`.
pub fn underreplicated_lock_key(ledger_id: LedgerId) -> String {
    format!("{ROOT}/locks/underreplicated/{ledger_id}")
}

/// Auditor leader election key (ephemeral).
pub fn auditor_election_key() -> &'static str {
    "/open-ledger/election/auditor"
}

/// OpenKV DS Set of ledger metadata keys (members are full ledger keys).
pub fn index_ledgers() -> &'static str {
    "/open-ledger/idx/ledgers"
}

/// OpenKV DS Set of bookie registry keys.
pub fn index_bookies() -> &'static str {
    "/open-ledger/idx/bookies"
}

/// OpenKV DS Set of under-replicated task keys.
pub fn index_underreplicated() -> &'static str {
    "/open-ledger/idx/underreplicated"
}

/// Which OpenKV index Set (if any) tracks `key` for listing.
pub fn index_set_for_key(key: &str) -> Option<&'static str> {
    if key.starts_with(ledgers_prefix()) {
        Some(index_ledgers())
    } else if key.starts_with(bookies_prefix()) {
        Some(index_bookies())
    } else if key.starts_with(underreplicated_prefix()) {
        Some(index_underreplicated())
    } else {
        None
    }
}

/// OpenKV index Set for a CoordStore range `[min, max)`.
pub fn index_set_for_range(min_inclusive: &str, max_exclusive: &str) -> Option<&'static str> {
    if min_inclusive == ledgers_prefix() && max_exclusive == ledgers_prefix_end() {
        Some(index_ledgers())
    } else if min_inclusive == bookies_prefix() && max_exclusive == bookies_prefix_end() {
        Some(index_bookies())
    } else if min_inclusive == underreplicated_prefix()
        && max_exclusive == underreplicated_prefix_end()
    {
        Some(index_underreplicated())
    } else {
        None
    }
}

/// Parse ledger id from a ledger or underreplicated key suffix.
pub fn parse_ledger_id_suffix(key: &str, prefix: &str) -> Option<LedgerId> {
    key.strip_prefix(prefix)?.parse().ok()
}

/// Parse bookie id from a bookie registry key.
pub fn parse_bookie_id(key: &str) -> Option<BookieId> {
    key.strip_prefix(bookies_prefix()).map(str::to_string)
}
