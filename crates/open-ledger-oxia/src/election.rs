//! Auditor leader election via an Oxia ephemeral key.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use open_ledger::keys;
use open_ledger::{Error, Result};
use tracing::{info, warn};

use crate::coord::{CoordStore, PutExpect};

/// Tries to hold `/open-ledger/election/auditor` as an ephemeral record.
pub struct AuditorElection<C: CoordStore> {
    coord: Arc<C>,
    identity: String,
    is_leader: Arc<AtomicBool>,
}

impl<C: CoordStore + 'static> AuditorElection<C> {
    /// Create an election helper. `identity` should identify this autorecovery process.
    pub fn new(coord: Arc<C>, identity: impl Into<String>) -> Self {
        Self {
            coord,
            identity: identity.into(),
            is_leader: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Whether this process currently believes it is the auditor leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }

    /// Shared leader flag for other threads.
    pub fn leader_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.is_leader)
    }

    /// Attempt once to become / remain leader.
    ///
    /// Create-only when absent; refresh with version CAS when we already own
    /// the ephemeral key. Another holder's record means we are not leader.
    pub fn tick(&self) -> Result<()> {
        let key = keys::auditor_election_key();
        match self.coord.get(key) {
            Ok(rec) if rec.value == self.identity.as_bytes() => {
                self.apply_claim(
                    self.coord.put(
                        key,
                        self.identity.as_bytes(),
                        PutExpect::Version(rec.version_id),
                        true,
                    ),
                    /*announce=*/ false,
                )
            }
            Ok(_) => {
                self.set_leader(false);
                Ok(())
            }
            Err(Error::NotFound(_)) => self.apply_claim(
                self.coord.put(
                    key,
                    self.identity.as_bytes(),
                    PutExpect::NotExists,
                    true,
                ),
                /*announce=*/ true,
            ),
            Err(err) => Err(err),
        }
    }

    /// Background election loop.
    pub fn spawn(self: &Arc<Self>, every: Duration) {
        let this = Arc::clone(self);
        thread::Builder::new()
            .name("auditor-election".into())
            .spawn(move || loop {
                if let Err(err) = this.tick() {
                    warn!(%err, "auditor election tick failed");
                    this.set_leader(false);
                }
                thread::sleep(every);
            })
            .expect("spawn auditor election");
    }

    fn set_leader(&self, leader: bool) {
        self.is_leader.store(leader, Ordering::SeqCst);
    }

    /// Interpret a put result as leadership claim success / contended / hard error.
    fn apply_claim(&self, result: Result<i64>, announce: bool) -> Result<()> {
        match result {
            Ok(_) => {
                if announce && !self.is_leader.swap(true, Ordering::SeqCst) {
                    info!(identity = %self.identity, "became auditor leader");
                } else {
                    self.set_leader(true);
                }
                Ok(())
            }
            // Contended create / lost version: another holder or session expiry.
            Err(Error::AlreadyExists(_))
            | Err(Error::VersionConflict(_))
            | Err(Error::NotFound(_)) => {
                self.set_leader(false);
                Ok(())
            }
            Err(err) => {
                self.set_leader(false);
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coord::MemoryCoordStore;

    #[test]
    fn only_one_leader_with_create_only() {
        let coord = Arc::new(MemoryCoordStore::new());
        let a = AuditorElection::new(Arc::clone(&coord), "a");
        let b = AuditorElection::new(Arc::clone(&coord), "b");
        a.tick().unwrap();
        assert!(a.is_leader());
        b.tick().unwrap();
        assert!(!b.is_leader());
        a.tick().unwrap();
        assert!(a.is_leader());
    }
}
