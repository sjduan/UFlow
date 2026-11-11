//! Shared physical DDR admission for legacy, external and managed resources.
//!
//! Lifecycle witnesses may retain per-backing state, but never add a second
//! capacity charge. Reservations cover provider creation outside Catalog locks.
use std::collections::{HashMap,HashSet};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackingId {
    Posix { dev: u64, inode: u64 },
    Provider { realm: String, provider: String, resource: String, generation: u64 },
}

/// An attempted creation with no valid absence/release proof. This is not a
/// backing and cannot be converted or cancelled by ordinary creation APIs.
#[derive(Debug)]
pub struct RetainedUncertainReservation {
    pub object_id: u64,
    pub witness: serde_json::Value,
}

#[derive(Debug)]
pub struct DdrResources {
    pub limit: u64,
    reservations: HashMap<String, u64>,
    resources: HashMap<u64, (BackingId, u64)>, // one charge per real backing, not per view
    retained: HashMap<u64, serde_json::Value>, // lifecycle witness; capacity remains in resources
    retained_reservations: HashMap<String, RetainedUncertainReservation>,
    retired_unstarted: HashSet<u64>, // fixed c039 IDs108/109: no backing, never reused
    terminal_domains: HashMap<u64, (BackingId, u64, serde_json::Value)>,
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
            retained: HashMap::new(),
            retained_reservations: HashMap::new(),
            retired_unstarted: HashSet::new(),
            terminal_domains: HashMap::new(),
        }
    }
}

impl DdrResources {
    pub(crate) fn active_reserved_bytes_for_terminal(&self)->u64{self.reserved_bytes()-self.retained_reserved_bytes()}
    pub(crate) fn protect_c039_unstarted_ids(&mut self)->Result<(),String>{
        if self.reservations.get("canary:nodeA")!=Some(&(2<<20)) || self.reservations.get("canary:nodeB")!=Some(&(2<<20)) ||
            !self.retired_unstarted.is_empty() || [108,109].iter().any(|id|self.resources.contains_key(id)) {
            return Err("fixed two reserved c039 IDs only; no reentry".into());
        }
        self.retired_unstarted.extend([108,109]);Ok(())
    }
    pub fn retain_uncertain_reservation(&mut self, operation:&str, object_id:u64,
                                      witness:serde_json::Value)->Result<(),String> {
        let nbytes=witness["nbytes"].as_u64().filter(|n|*n>0).ok_or("positive reserved bytes required")?;
        if self.reservations.get(operation).copied()!=Some(nbytes) ||
            witness["key"]!=operation || witness["state"]!="CreationUnknown" || object_id==0 ||
            self.resources.contains_key(&object_id) || self.retired_unstarted.contains(&object_id) || self.retained_reservations.contains_key(operation) ||
            self.retained_reservations.values().any(|r|r.object_id==object_id) {
            return Err("complete unique uncertain creation reservation required".into());
        }
        self.retained_reservations.insert(operation.into(),RetainedUncertainReservation{object_id,witness});Ok(())
    }
    pub fn retained_reserved_bytes(&self)->u64 {
        self.retained_reservations.keys().filter_map(|k|self.reservations.get(k)).copied().sum()
    }
    pub fn retain_provider(&mut self, object:u64, witness:serde_json::Value) -> Result<(),String> {
        if !matches!(self.resources.get(&object),Some((BackingId::Provider{..},_))) || self.retained.contains_key(&object) {
            return Err("unique typed provider backing required for retained obligation".into());
        }
        self.retained.insert(object,witness);Ok(())
    }
    pub fn retained_bytes(&self) -> u64 {
        self.retained.keys().filter_map(|id|self.charged_object_bytes(*id)).sum()
    }
    pub fn snapshot(&self) -> serde_json::Value {
        use serde_json::json;
        let mut value=json!({"charged_bytes":self.charged_bytes(),"reserved_bytes":self.reserved_bytes(),
            "retained_reserved_bytes":self.retained_reserved_bytes(),
            "active_reserved_bytes":self.reserved_bytes()-self.retained_reserved_bytes(),
            "retained_uncertain_reservations":self.retained_reservations.iter().map(|(key,r)|
                json!({"key":key,"object_id":r.object_id,"witness":r.witness})).collect::<Vec<_>>(),
            "retained_bytes":self.retained_bytes(),"active_bytes":self.charged_bytes()-self.retained_bytes(),
            "reservations":self.reservations,"retired_unstarted_object_ids":self.retired_unstarted.iter().copied().collect::<std::collections::BTreeSet<_>>(),
            "resources":self.resources.iter().map(|(id,(backing,n))| {
                let identity=match backing {
                    BackingId::Posix{dev,inode}=>json!({"kind":"posix","dev":dev,"inode":inode}),
                    BackingId::Provider{realm,provider,resource,generation}=>json!({"kind":"provider","realm":realm,
                        "provider":provider,"resource":resource,"generation":generation})};
                json!({"object_id":id,"nbytes":n,"backing":identity,"retained":self.retained.get(id)})
            }).collect::<Vec<_>>()});
        if !self.terminal_domains.is_empty() {
            value["terminal_domain_loss_tombstones"]=json!(self.terminal_domains.iter().map(|(id,(_,n,w))|
                (*id,json!({"object_id":id,"nbytes":n,"outcome":"TerminalDomainDataLost","native_SDK_ACK":false,"witness":w})))
                .collect::<std::collections::BTreeMap<_,_>>());
        }
        value
    }
    pub fn charged_object_bytes(&self, object: u64) -> Option<u64> {
        self.resources.get(&object).map(|(_, n)| *n)
    }
    pub fn reserve_unique(&mut self, operation: &str, nbytes: u64) -> Result<(), String> {
        if self.reservations.contains_key(operation) {
            return Err("matching DDR creation in progress; retry after outcome".into());
        }
        self.reserve(operation, nbytes)
    }
    pub fn charged_bytes(&self) -> u64 {
        self.resources
            .values()
            .fold(0u64, |sum, (_, n)| sum.saturating_add(*n))
    }

    pub fn reserved_bytes(&self) -> u64 {
        self.reservations
            .values()
            .fold(0u64, |sum, n| sum.saturating_add(*n))
    }

    pub fn reserve(&mut self, operation: &str, nbytes: u64) -> Result<(), String> {
        if self.retained_reservations.contains_key(operation){return Err("uncertain historical reservation cannot be replayed".into());}
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
        if self.retained_reservations.contains_key(operation){return;}
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
        self.commit_backing(operation, object, BackingId::Posix { dev, inode: ino }, nbytes)
    }

    pub fn commit_backing(&mut self, operation: &str, object: u64,
                          backing: BackingId, nbytes: u64) -> Result<(), String> {
        if self.retired_unstarted.contains(&object) || (!self.terminal_domains.is_empty() && object<113) || self.terminal_domains.contains_key(&object) ||
            self.terminal_domains.values().any(|(old,_,_)|old==&backing) || self.retained_reservations.contains_key(operation) ||
            self.retained_reservations.values().any(|r|r.object_id==object) {
            return Err("uncertain historical reservation has no confirmed backing/release".into());
        }
        if let BackingId::Provider { realm, provider, resource, generation } = &backing {
            if realm.is_empty() || provider.is_empty() || resource.is_empty() || *generation == 0 {
                return Err("complete provider backing identity required".into());
            }
        }
        if self.reservations.get(operation) != Some(&nbytes) {
            return Err("no matching creation reservation".into());
        }
        if self.resources.contains_key(&object)
            || self
                .resources
                .values()
                .any(|(identity, _)| identity == &backing)
        {
            return Err("physical DDR resource already charged".into());
        }
        self.resources.insert(object, (backing, nbytes));
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
        if self.retained.contains_key(&object) {return;} // No caller may infer old checked-close.
        self.resources.remove(&object);
    }

    /// Private opt-in CPU terminal-domain path. The permit cannot be minted
    /// from caller booleans or normal SDK release receipts.
    #[cfg(any(test, feature = "experimental-reconciliation"))]
    pub(crate) fn inherit_terminal_source(p:&crate::pool_terminal_domain::VerifiedCandidate)->Result<Self,String>{
        let source=p.source_ledger();let mut ledger=Self::default();ledger.limit=128<<20;
        for row in source["resources"].as_array().ok_or("source resources")? {
            let id=row["object_id"].as_u64().ok_or("object id")?;let n=row["nbytes"].as_u64().ok_or("bytes")?;
            let b=&row["backing"];if b["kind"]!="provider"{return Err("terminal source must be fixed provider resources".into());}
            let identity=BackingId::Provider{realm:b["realm"].as_str().ok_or("realm")?.into(),provider:b["provider"].as_str().ok_or("provider")?.into(),
                resource:b["resource"].as_str().ok_or("resource")?.into(),generation:b["generation"].as_u64().ok_or("generation")?};
            let key=format!("terminal-inherit:{id}");ledger.reserve_unique(&key,n)?;ledger.commit_backing(&key,id,identity,n)?;
            if !row["retained"].is_null(){ledger.retain_provider(id,row["retained"].clone())?;}
        }
        for (key,n) in source["reservations"].as_object().ok_or("reservations")?{ledger.reserve_unique(key,n.as_u64().ok_or("reserved bytes")?)?;}
        for row in source["retained_uncertain_reservations"].as_array().ok_or("uncertain reservations")? {
            ledger.retain_uncertain_reservation(row["key"].as_str().ok_or("reservation key")?,row["object_id"].as_u64().ok_or("reserved object")?,row["witness"].clone())?;
        }
        ledger.retired_unstarted.extend([108,109]);
        for (key,row) in p.prior_backings().as_object().ok_or("prior terminal backing map")? {
            let id=key.parse::<u64>().map_err(|_|"prior terminal object")?;
            let tomb=&source["terminal_domain_loss_tombstones"][key];let b=&row["backing"];
            if tomb["object_id"]!=id || row["object_id"]!=id || tomb["nbytes"]!=row["nbytes"] ||
                tomb["outcome"]!="TerminalDomainDataLost" || tomb["native_SDK_ACK"]!=false || b["kind"]!="provider" || ledger.resources.contains_key(&id) {
                return Err("exact previous tombstone and original backing required".into());
            }
            let backing=BackingId::Provider{realm:b["realm"].as_str().ok_or("prior realm")?.into(),provider:b["provider"].as_str().ok_or("prior provider")?.into(),
                resource:b["resource"].as_str().ok_or("prior resource")?.into(),generation:b["generation"].as_u64().ok_or("prior generation")?};
            if ledger.resources.values().any(|(live,_)|live==&backing) || ledger.terminal_domains.values().any(|(old,_,_)|old==&backing) {return Err("prior terminal backing overlaps another identity".into());}
            ledger.terminal_domains.insert(id,(backing,row["nbytes"].as_u64().ok_or("prior size")?,tomb["witness"].clone()));
        }
        p.matches_ledger(&ledger.snapshot())?;Ok(ledger)
    }
    #[cfg(any(test, feature = "experimental-reconciliation"))]
    pub(crate) fn apply_terminal_loss(&mut self,p:&crate::pool_terminal_domain::VerifiedCandidate)->Result<(),String>{
        if self.limit!=128<<20{return Err("terminal budget is fixed128, not configurable".into());}
        p.matches_ledger(&self.snapshot())?;
        if p.terminal_ids().iter().any(|id|self.terminal_domains.contains_key(id)) {return Err("terminal transaction cannot replay".into());}
        let total=p.terminal_ids().iter().try_fold(0u64,|sum,id|self.resources.get(id).and_then(|(_,n)|sum.checked_add(*n))).ok_or("eligible terminal size")?;
        if total!=52<<20{return Err("exact eligible52 required".into());}
        for &id in p.terminal_ids() {
            let (backing,n)=self.resources.remove(&id).ok_or("terminal resource missing")?;
            self.terminal_domains.insert(id,(backing,n,p.tombstone()));
        }
        if self.charged_bytes()!=12<<20 || self.retained_bytes()!=12<<20 || self.reserved_bytes()!=64<<20 || self.retained_reserved_bytes()!=64<<20 {
            return Err("terminal loss must preserve exact historical76".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_backing_has_no_fake_inode_and_views_do_not_recharge() {
        let mut ledger = DdrResources { limit:256, ..Default::default() };
        let backing=BackingId::Provider {realm:"sim:r".into(),provider:"mem_service_obmm".into(),
            resource:"nodeA:inc:allocation".into(),generation:7};
        ledger.reserve("one",128).unwrap();
        ledger.commit_backing("one",1,backing.clone(),128).unwrap();
        ledger.admit_existing(2,0,0,64).unwrap(); // A real POSIX identity is a distinct namespace.
        ledger.reserve("duplicate",64).unwrap();
        assert!(ledger.commit_backing("duplicate",3,backing,64).is_err());
        assert_eq!(ledger.charged_bytes(),192);
        assert_eq!(ledger.reserved_bytes(),64); // Unknown/duplicate creation not silently freed.
        assert!(ledger.reserve("another",1).is_err());
        ledger.release_confirmed(1);
        assert_eq!(ledger.charged_bytes(),64);
    }

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
