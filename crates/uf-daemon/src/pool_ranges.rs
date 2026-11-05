//! Half-open suballocation locks and content versions; not another capacity ledger.
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Segment {
    pub start: u64,
    pub end: u64,
    pub version: u64,
    pub content: String,
    pub state: &'static str,
}
#[derive(Clone, Debug)]
pub struct Allocation {
    pub id: u64,
    pub generation: u64,
    pub offset: u64,
    pub nbytes: u64,
    pub segments: Vec<Segment>,
}
impl Allocation {
    pub fn json(&self) -> Value {
        json!({"block_id":self.id,"generation":self.generation,"offset":self.offset,"nbytes":self.nbytes})
    }
}
#[derive(Clone, Debug)]
pub struct RangeLease {
    pub token: String,
    pub catalog_id: u64,
    pub client: String,
    pub block: u64,
    pub generation: u64,
    pub offset: u64,
    pub nbytes: u64,
    pub mode: String,
    pub state: &'static str,
    pub version: u64,
    pub content: String,
}
impl RangeLease {
    pub fn identity(&self) -> Value {
        json!({"lease_id":self.token,"lease_generation":self.catalog_id,"client_session":self.client,
               "block_id":self.block,"generation":self.generation,"offset":self.offset,
               "nbytes":self.nbytes,"mode":self.mode,"content_version":self.version,"content_id":self.content})
    }
}

#[derive(Default)]
pub struct Ranges {
    pub blocks: BTreeMap<u64, Allocation>,
    pub leases: BTreeMap<String, RangeLease>,
    next_version: u64,
}

fn overlap(a: u64, n: u64, b: u64, m: u64) -> bool {
    a < b + m && b < a + n
}

impl Ranges {
    pub fn allocate(
        &mut self,
        id: u64,
        size: u64,
        alignment: u64,
        capacity: u64,
    ) -> Result<Allocation, String> {
        if size == 0
            || alignment < 65536
            || !alignment.is_power_of_two()
            || size % 65536 != 0
            || self.blocks.len() >= 64
        {
            return Err("invalid bounded aligned allocation".into());
        }
        let mut occupied = self.blocks.values().collect::<Vec<_>>();
        occupied.sort_by_key(|b| b.offset);
        let mut offset = 0u64;
        for block in occupied {
            offset = offset
                .checked_add(alignment - 1)
                .ok_or("allocation overflow")?
                & !(alignment - 1);
            if offset
                .checked_add(size)
                .is_some_and(|end| end <= block.offset)
            {
                break;
            }
            offset = offset.max(block.offset + block.nbytes);
        }
        offset = offset
            .checked_add(alignment - 1)
            .ok_or("allocation overflow")?
            & !(alignment - 1);
        if offset.checked_add(size).is_none_or(|end| end > capacity) {
            return Err("pool allocation capacity".into());
        }
        let block = Allocation {
            id,
            generation: 1,
            offset,
            nbytes: size,
            segments: vec![Segment {
                start: offset,
                end: offset + size,
                version: 0,
                content: String::new(),
                state: "Uncommitted",
            }],
        };
        if self.blocks.insert(id, block.clone()).is_some() {
            return Err("allocation ID reused".into());
        }
        Ok(block)
    }

    pub fn acquire(
        &mut self,
        token: String,
        catalog_id: u64,
        client: String,
        block: u64,
        generation: u64,
        byte_offset: u64,
        nbytes: u64,
        mode: &str,
    ) -> Result<RangeLease, String> {
        let allocation = self.blocks.get(&block).ok_or("block not found")?;
        if generation != allocation.generation
            || nbytes == 0
            || byte_offset % 65536 != 0
            || nbytes % 65536 != 0
            || byte_offset
                .checked_add(nbytes)
                .is_none_or(|end| end > allocation.nbytes)
            || !matches!(mode, "r" | "w")
            || self.leases.len() >= 4096
            || self.leases.values().filter(|l| l.state != "Closed").count() >= 64
            || mode == "w"
                && self
                    .blocks
                    .values()
                    .map(|b| b.segments.len())
                    .sum::<usize>()
                    >= 126
        {
            return Err("stale block, access, or subrange bounds/alignment".into());
        }
        let offset = allocation.offset + byte_offset;
        if self.leases.values().any(|lease| {
            lease.state != "Closed"
                && overlap(offset, nbytes, lease.offset, lease.nbytes)
                && (lease.state == "Unknown" || mode == "w" || lease.mode == "w")
        }) {
            return Err("overlapping range lease conflict".into());
        }
        let (mut version, mut content) = (0, String::new());
        if mode == "r" {
            for part in allocation
                .segments
                .iter()
                .filter(|part| part.start < offset + nbytes && offset < part.end)
            {
                if part.state != "Committed"
                    || part.version == 0
                    || version != 0 && version != part.version
                {
                    return Err("read requires one completely Committed content version".into());
                }
                version = part.version;
                content = part.content.clone();
            }
            if version == 0 {
                return Err("uncommitted read".into());
            }
        }
        let lease = RangeLease {
            token: token.clone(),
            catalog_id,
            client,
            block,
            generation,
            offset,
            nbytes,
            mode: mode.to_owned(),
            state: "Active",
            version,
            content,
        };
        self.leases.insert(token, lease.clone());
        Ok(lease)
    }

    fn replace_segment(
        &mut self,
        block: u64,
        offset: u64,
        nbytes: u64,
        version: u64,
        content: &str,
        state: &'static str,
    ) {
        let allocation = self.blocks.get_mut(&block).unwrap();
        let end = offset + nbytes;
        let mut parts = Vec::new();
        for part in &allocation.segments {
            if part.end <= offset || part.start >= end {
                parts.push(part.clone());
                continue;
            }
            if part.start < offset {
                let mut left = part.clone();
                left.end = offset;
                parts.push(left);
            }
            if part.end > end {
                let mut right = part.clone();
                right.start = end;
                parts.push(right);
            }
        }
        parts.push(Segment {
            start: offset,
            end,
            version,
            content: content.to_owned(),
            state,
        });
        parts.sort_by_key(|part| part.start);
        allocation.segments = parts;
    }

    pub fn validate(&self, identity: &Value, client: &str) -> Result<RangeLease, String> {
        let token = identity
            .get("lease_id")
            .and_then(Value::as_str)
            .ok_or("lease token required")?;
        let lease = self.leases.get(token).ok_or("unknown lease")?;
        if lease.client != client || &lease.identity() != identity {
            return Err("stale full lease identity".into());
        }
        Ok(lease.clone())
    }

    pub fn commit(
        &mut self,
        lease: &RangeLease,
        content: &str,
        nbytes: u64,
    ) -> Result<u64, String> {
        if self.leases[&lease.token].state != "Active"
            || lease.mode != "w"
            || lease.nbytes != nbytes
        {
            return Err("active exact write lease required".into());
        }
        self.next_version = self
            .next_version
            .checked_add(1)
            .ok_or("content version exhausted")?;
        self.replace_segment(
            lease.block,
            lease.offset,
            lease.nbytes,
            self.next_version,
            content,
            "Committed",
        );
        self.leases.get_mut(&lease.token).unwrap().state = "Closed";
        Ok(self.next_version)
    }

    pub fn release(&mut self, lease: &RangeLease) -> Result<(), String> {
        let current = self.leases.get_mut(&lease.token).unwrap();
        if current.state == "Unknown" {
            return Err("unknown native obligation cannot be normally released".into());
        }
        current.state = "Closed"; // For a W lease the caller explicitly asserts it never started a write.
        Ok(())
    }

    pub fn unknown(&mut self, lease: &RangeLease) {
        if self.leases[&lease.token].state == "Closed" {
            return;
        }
        self.leases.get_mut(&lease.token).unwrap().state = "Unknown";
        if lease.mode == "w" {
            self.replace_segment(lease.block, lease.offset, lease.nbytes, 0, "", "Unknown");
        }
    }

    pub fn reconcile_unmapped(&mut self, client: &str) {
        // Invoked only after explicit native-drain attestation plus confirmed
        // parent unmap/FD close and drained exited children; never on timeout/death.
        let unknown = self
            .leases
            .values()
            .filter(|lease| lease.client == client && lease.state == "Unknown")
            .cloned()
            .collect::<Vec<_>>();
        for lease in unknown {
            if lease.mode == "w" {
                self.replace_segment(lease.block, lease.offset, lease.nbytes, 0, "", "Invalid");
            }
            self.leases.get_mut(&lease.token).unwrap().state = "Closed";
        }
    }

    pub fn snapshot(&self) -> Value {
        json!({"allocations":self.blocks.values().map(|block| {
            let mut value=block.json();
            value["segments"]=json!(block.segments.iter().map(|part| json!({"offset":part.start,"nbytes":part.end-part.start,
                "content_version":part.version,"content_id":part.content,"state":part.state})).collect::<Vec<_>>());
            value
        }).collect::<Vec<_>>(),"leases":self.leases.values().filter(|lease| lease.state!="Closed").map(|lease| {
            let mut value=lease.identity(); value["state"]=json!(if lease.state=="Unknown" {"Unknown"} else if lease.mode=="w" {"Writing"} else {"Reading"}); value
        }).collect::<Vec<_>>()})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const U: u64 = 65536;
    fn take(
        r: &mut Ranges,
        id: u64,
        offset: u64,
        n: u64,
        mode: &str,
    ) -> Result<RangeLease, String> {
        r.acquire(
            format!("lease-{id}"),
            id,
            format!("client-{id}"),
            1,
            1,
            offset,
            n,
            mode,
        )
    }
    #[test]
    fn same_allocation_held_ab_allow_disjoint_c_adjacent_and_rr() {
        let mut ranges = Ranges::default();
        ranges.allocate(1, 8 * U, U, 8 * U).unwrap();
        let a = take(&mut ranges, 1, 0, 2 * U, "w").unwrap();
        let b = take(&mut ranges, 2, 2 * U, 2 * U, "w").unwrap();
        let c = take(&mut ranges, 3, 4 * U, U, "w").unwrap();
        assert!(take(&mut ranges, 4, U, 2 * U, "r").is_err());
        assert!(take(&mut ranges, 5, U, U, "w").is_err());
        ranges.commit(&c, "c", U).unwrap();
        let c1 = take(&mut ranges, 6, 4 * U, U, "r").unwrap();
        let c2 = take(&mut ranges, 7, 4 * U, U, "r").unwrap();
        assert_eq!(ranges.leases[&a.token].state, "Active");
        assert_eq!(ranges.leases[&b.token].state, "Active");
        ranges.commit(&a, "a", 2 * U).unwrap();
        assert_eq!(
            ranges.validate(&c1.identity(), &c1.client).unwrap().version,
            c1.version
        );
        ranges.release(&c1).unwrap();
        ranges.release(&c2).unwrap();
    }
    #[test]
    fn unknown_is_not_timeout_release_and_partial_content_versions_are_local() {
        let mut ranges = Ranges::default();
        ranges.allocate(1, 4 * U, U, 4 * U).unwrap();
        let write = take(&mut ranges, 1, 0, 4 * U, "w").unwrap();
        ranges.commit(&write, "all", 4 * U).unwrap();
        let read = take(&mut ranges, 2, 0, U, "r").unwrap();
        let unknown = take(&mut ranges, 3, 2 * U, U, "w").unwrap();
        ranges.unknown(&unknown);
        assert!(ranges.release(&unknown).is_err());
        assert!(take(&mut ranges, 4, 2 * U, U, "r").is_err());
        assert_eq!(
            ranges
                .validate(&read.identity(), &read.client)
                .unwrap()
                .version,
            read.version
        );
        ranges.reconcile_unmapped(&unknown.client);
        assert!(take(&mut ranges, 5, 2 * U, U, "r").is_err());
        let replace = take(&mut ranges, 6, 2 * U, U, "w").unwrap();
        ranges.commit(&replace, "new", U).unwrap();
        assert!(take(&mut ranges, 7, U, 3 * U, "r").is_err());
        assert!(take(&mut ranges, 8, 2 * U, U, "r").is_ok());
    }
}
