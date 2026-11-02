//! DDR transfer cost arithmetic. This module does not grant a capability or
//! select a path: callers must first filter by semantics and verified capability.
//! All internal quantities are seconds, bytes and bytes/second.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Path {
    ReadOnlyMap,
    IndependentCopy,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Workload {
    pub transfer_bytes: u64,
    /// Bytes consumed on each read, not a page count or the mapping length.
    pub read_bytes: u64,
    /// Number of reads after the first read, on either candidate path.
    pub reuse_reads: u32,
}

/// A profile is meaningful only for the environment in which it was sampled.
/// Missing parameters are represented as None, not as a zero-cost operation.
#[derive(Clone, Debug)]
pub(crate) struct CostProfile {
    pub version: String,
    pub provenance: String,
    pub source_commit: String,
    pub loaded_libraries: String,
    pub environment: String,
    pub numa_condition: String,
    pub page_condition: String,
    pub pte_condition: String,
    pub cache_condition: String,
    pub concurrency: u32,
    pub sampled_at_ns: u128,
    pub expires_at_ns: u128,
    /// True only after the held-out validation threshold was met.
    pub ranking_validated: bool,
    pub map_setup_seconds: Option<f64>,
    pub map_first_read_bps: Option<f64>,
    pub map_reuse_read_bps: Option<f64>,
    pub copy_setup_seconds: Option<f64>,
    pub copy_bps: Option<f64>,
    pub copy_queue_seconds: Option<f64>,
    pub copy_first_read_bps: Option<f64>,
    pub copy_reuse_read_bps: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CostError {
    InvalidProfile,
    ExpiredProfile,
    EnvironmentMismatch,
    UnknownWorkload,
    MissingParameter,
    InvalidParameter,
    EstimateOverflow,
    RankingDisabled,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Estimate {
    pub setup_seconds: f64,
    pub movement_seconds: f64,
    pub queue_seconds: f64,
    pub first_read_seconds: f64,
    pub reuse_read_seconds: f64,
    pub total_seconds: f64,
}

impl CostProfile {
    fn validate(&self, now_ns: u128, concurrency: u32, numa: &str) -> Result<(), CostError> {
        if self.version.trim().is_empty()
            || self.provenance.trim().is_empty()
            || self.source_commit.trim().is_empty()
            || self.loaded_libraries.trim().is_empty()
            || self.environment.trim().is_empty()
            || self.numa_condition.trim().is_empty()
            || self.page_condition.trim().is_empty()
            || self.pte_condition.trim().is_empty()
            || self.cache_condition.trim().is_empty()
            || self.concurrency == 0
            || self.sampled_at_ns == 0
            || self.expires_at_ns <= self.sampled_at_ns
        {
            return Err(CostError::InvalidProfile);
        }
        if now_ns < self.sampled_at_ns || now_ns >= self.expires_at_ns {
            return Err(CostError::ExpiredProfile);
        }
        if concurrency != self.concurrency || numa != self.numa_condition {
            return Err(CostError::EnvironmentMismatch);
        }
        Ok(())
    }

    /// Calculate one already-admissible path. Merely obtaining an estimate does
    /// not make the path available and does not enable automatic ranking.
    pub(crate) fn estimate(
        &self,
        path: Path,
        work: Workload,
        now_ns: u128,
        concurrency: u32,
        numa: &str,
    ) -> Result<Estimate, CostError> {
        self.validate(now_ns, concurrency, numa)?;
        if work.transfer_bytes == 0 || work.read_bytes == 0 || work.read_bytes > work.transfer_bytes
        {
            return Err(CostError::UnknownWorkload);
        }
        let (setup, movement, queue, first_bps, reuse_bps) = match path {
            Path::ReadOnlyMap => (
                seconds(self.map_setup_seconds)?,
                0.0,
                0.0,
                bandwidth(self.map_first_read_bps)?,
                if work.reuse_reads == 0 {
                    None
                } else {
                    Some(bandwidth(self.map_reuse_read_bps)?)
                },
            ),
            Path::IndependentCopy => (
                seconds(self.copy_setup_seconds)?,
                (work.transfer_bytes as f64) / bandwidth(self.copy_bps)?,
                seconds(self.copy_queue_seconds)?,
                bandwidth(self.copy_first_read_bps)?,
                if work.reuse_reads == 0 {
                    None
                } else {
                    Some(bandwidth(self.copy_reuse_read_bps)?)
                },
            ),
        };
        let first = (work.read_bytes as f64) / first_bps;
        let reuse = reuse_bps.map_or(0.0, |bps| {
            (work.read_bytes as f64) * f64::from(work.reuse_reads) / bps
        });
        let total = setup + movement + queue + first + reuse;
        if !total.is_finite() {
            return Err(CostError::EstimateOverflow);
        }
        Ok(Estimate {
            setup_seconds: setup,
            movement_seconds: movement,
            queue_seconds: queue,
            first_read_seconds: first,
            reuse_read_seconds: reuse,
            total_seconds: total,
        })
    }

    pub(crate) fn ensure_ranking_allowed(
        &self,
        now_ns: u128,
        concurrency: u32,
        numa: &str,
    ) -> Result<(), CostError> {
        self.validate(now_ns, concurrency, numa)?;
        if self.ranking_validated {
            Ok(())
        } else {
            Err(CostError::RankingDisabled)
        }
    }
}

fn seconds(value: Option<f64>) -> Result<f64, CostError> {
    let value = value.ok_or(CostError::MissingParameter)?;
    if !value.is_finite() || value < 0.0 {
        return Err(CostError::InvalidParameter);
    }
    Ok(value)
}

fn bandwidth(value: Option<f64>) -> Result<f64, CostError> {
    let value = value.ok_or(CostError::MissingParameter)?;
    if !value.is_finite() || value <= 0.0 {
        return Err(CostError::InvalidParameter);
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Only arithmetic fixtures; these numbers are not calibration evidence.
    fn profile() -> CostProfile {
        CostProfile {
            version: "fixture-v1".into(),
            provenance: "unit-test-fixture".into(),
            source_commit: "fixture".into(),
            loaded_libraries: "fixture".into(),
            environment: "fixture".into(),
            numa_condition: "unknown".into(),
            page_condition: "pretouched".into(),
            pte_condition: "new-mapping".into(),
            cache_condition: "unknown".into(),
            concurrency: 1,
            sampled_at_ns: 1,
            expires_at_ns: 100,
            ranking_validated: false,
            map_setup_seconds: Some(1.0),
            map_first_read_bps: Some(10.0),
            map_reuse_read_bps: Some(20.0),
            copy_setup_seconds: Some(2.0),
            copy_bps: Some(10.0),
            copy_queue_seconds: Some(3.0),
            copy_first_read_bps: Some(20.0),
            copy_reuse_read_bps: Some(20.0),
        }
    }

    fn work() -> Workload {
        Workload {
            transfer_bytes: 100,
            read_bytes: 40,
            reuse_reads: 2,
        }
    }

    #[test]
    fn compares_identical_reads_and_separates_copy_phases() {
        let p = profile();
        let map = p
            .estimate(Path::ReadOnlyMap, work(), 2, 1, "unknown")
            .unwrap();
        let copy = p
            .estimate(Path::IndependentCopy, work(), 2, 1, "unknown")
            .unwrap();
        assert_eq!(
            (
                map.setup_seconds,
                map.movement_seconds,
                map.queue_seconds,
                map.first_read_seconds,
                map.reuse_read_seconds,
                map.total_seconds
            ),
            (1.0, 0.0, 0.0, 4.0, 4.0, 9.0)
        );
        assert_eq!(
            (
                copy.setup_seconds,
                copy.movement_seconds,
                copy.queue_seconds,
                copy.first_read_seconds,
                copy.reuse_read_seconds,
                copy.total_seconds
            ),
            (2.0, 10.0, 3.0, 2.0, 4.0, 21.0)
        );
    }

    #[test]
    fn missing_expired_mismatched_or_invalid_inputs_do_not_become_zero() {
        let mut p = profile();
        assert_eq!(
            p.estimate(Path::IndependentCopy, work(), 100, 1, "unknown")
                .unwrap_err(),
            CostError::ExpiredProfile
        );
        assert_eq!(
            p.estimate(Path::IndependentCopy, work(), 2, 2, "unknown")
                .unwrap_err(),
            CostError::EnvironmentMismatch
        );
        assert_eq!(
            p.estimate(Path::IndependentCopy, work(), 2, 1, "node0")
                .unwrap_err(),
            CostError::EnvironmentMismatch
        );
        p.copy_bps = None;
        assert_eq!(
            p.estimate(Path::IndependentCopy, work(), 2, 1, "unknown")
                .unwrap_err(),
            CostError::MissingParameter
        );
        p.copy_bps = Some(0.0);
        assert_eq!(
            p.estimate(Path::IndependentCopy, work(), 2, 1, "unknown")
                .unwrap_err(),
            CostError::InvalidParameter
        );
        p.copy_bps = Some(f64::NAN);
        assert_eq!(
            p.estimate(Path::IndependentCopy, work(), 2, 1, "unknown")
                .unwrap_err(),
            CostError::InvalidParameter
        );
        let unknown = Workload {
            read_bytes: 0,
            ..work()
        };
        assert_eq!(
            p.estimate(Path::ReadOnlyMap, unknown, 2, 1, "unknown")
                .unwrap_err(),
            CostError::UnknownWorkload
        );
    }

    #[test]
    fn ranking_is_disabled_without_held_out_validation() {
        let mut p = profile();
        assert_eq!(
            p.ensure_ranking_allowed(2, 1, "unknown"),
            Err(CostError::RankingDisabled)
        );
        p.ranking_validated = true;
        assert_eq!(p.ensure_ranking_allowed(2, 1, "unknown"), Ok(()));
        assert_eq!(
            p.ensure_ranking_allowed(100, 1, "unknown"),
            Err(CostError::ExpiredProfile)
        );
    }
}
