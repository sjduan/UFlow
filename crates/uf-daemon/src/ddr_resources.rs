//! Shared physical DDR admission for legacy, external and managed resources.
//!
//! Lifecycle witnesses may retain per-backing state, but never add a second
//! capacity charge. Reservations cover provider creation outside Catalog locks.
use std::collections::HashMap;

#[derive(Debug)]
pub struct DdrResources {
    pub limit: u64,
    reservations: HashMap<String, u64>,
    resources: HashMap<u64, (u64, u64, u64)>, // object -> dev, inode, bytes
}

impl Default for DdrResources {
    fn default() -> Self {
        Self {
            limit: std::env::var("UF_DDR_TOTAL_MAX_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(u64::MAX),
            reservations: HashMap::new(),
            resources: HashMap::new(),
        }
    }
}

impl DdrResources {
    pub fn reserve_unique(&mut self, operation: &str, nbytes: u64) -> Result<(), String> {
        if self.reservations.contains_key(operation) {
            return Err("matching DDR creation in progress; retry after outcome".into());
        }
        self.reserve(operation, nbytes)
    }
    pub fn charged_bytes(&self) -> u64 {
        self.resources
            .values()
            .fold(0u64, |sum, (_, _, n)| sum.saturating_add(*n))
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reservations
            .values()
            .fold(0u64, |sum, n| sum.saturating_add(*n))
    }

    pub fn reserve(&mut self, operation: &str, nbytes: u64) -> Result<(), String> {
        if let Some(old) = self.reservations.get(operation) {
            return if *old == nbytes {
                Ok(())
            } else {
                Err("reservation identity mismatch".into())
            };
        }
        if nbytes == 0
            || self
                .charged_bytes()
                .checked_add(self.reserved_bytes())
                .and_then(|n| n.checked_add(nbytes))
                .is_none_or(|n| n > self.limit)
        {
            return Err("shared DDR capacity exceeded".into());
        }
        self.reservations.insert(operation.to_owned(), nbytes);
        Ok(())
    }

    pub fn cancel_uncreated(&mut self, operation: &str) {
        self.reservations.remove(operation);
    }

    pub fn commit(
        &mut self,
        operation: &str,
        object: u64,
        dev: u64,
        ino: u64,
        nbytes: u64,
    ) -> Result<(), String> {
        if self.reservations.get(operation) != Some(&nbytes) {
            return Err("no matching creation reservation".into());
        }
        if self.resources.contains_key(&object)
            || self
                .resources
                .values()
                .any(|(d, i, _)| (*d, *i) == (dev, ino))
        {
            return Err("physical DDR resource already charged".into());
        }
        self.resources.insert(object, (dev, ino, nbytes));
        self.reservations.remove(operation);
        Ok(())
    }

    pub fn admit_existing(
        &mut self,
        object: u64,
        dev: u64,
        ino: u64,
        nbytes: u64,
    ) -> Result<(), String> {
        let operation = format!("existing:{object}");
        self.reserve(&operation, nbytes)?;
        match self.commit(&operation, object, dev, ino, nbytes) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.cancel_uncreated(&operation);
                Err(error)
            }
        }
    }

    /// Only call after confirmed real provider/FD release and durable witnesses.
    pub fn release_confirmed(&mut self, object: u64) {
        self.resources.remove(&object);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_external_legacy_share_one_capacity_and_pending_reservation() {
        let mut ledger = DdrResources {
            limit: 256,
            ..Default::default()
        };
        ledger.reserve("pool", 128).unwrap();
        ledger.admit_existing(2, 1, 20, 64).unwrap();
        assert!(ledger.admit_existing(3, 1, 30, 65).is_err());
        ledger.commit("pool", 1, 1, 10, 128).unwrap();
        ledger.admit_existing(3, 1, 30, 64).unwrap();
        assert_eq!(ledger.charged_bytes(), 256);
        assert_eq!(ledger.reserved_bytes(), 0);
        assert!(ledger.admit_existing(4, 1, 10, 1).is_err());
        ledger.release_confirmed(2);
        assert_eq!(ledger.charged_bytes(), 192);
    }

    #[test]
    fn duplicate_identity_overflow_and_unconfirmed_creation_keep_capacity() {
        let mut ledger = DdrResources {
            limit: u64::MAX,
            ..Default::default()
        };
        ledger.reserve("unknown", u64::MAX).unwrap();
        assert!(ledger.reserve("other", 1).is_err());
        assert!(ledger.reserve("unknown", 1).is_err());
        assert_eq!(ledger.reserved_bytes(), u64::MAX);
        ledger.cancel_uncreated("unknown");
        ledger.admit_existing(1, 3, 4, 128).unwrap();
        assert!(ledger.admit_existing(2, 3, 4, 128).is_err());
        assert_eq!(ledger.charged_bytes(), 128);
        assert_eq!(ledger.reserved_bytes(), 0);
    }
}
