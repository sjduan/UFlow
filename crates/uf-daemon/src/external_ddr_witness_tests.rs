//! Cross-language, real-process witness fixture. This module is cfg(test) only.
//! Its journal remains dirty; it is not a recovery/coverage authorization.
use super::*;
use crate::ddr_process_identity::ProcessBirth;
use crate::ddr_recovery_audit::{Domain, EventKind};
use crate::ddr_recovery_codec;
use crate::ddr_recovery_durable::inspect;
use crate::ddr_recovery_journal::Mode;

#[path = "ddr_coverage_fixture_matrix.rs"]
mod coverage_matrix;

fn line() -> String {
    let mut text = String::new();
    assert_ne!(std::io::stdin().read_line(&mut text).unwrap(), 0);
    text.trim_end().to_owned()
}

fn marker(name: &str, value: &str) {
    println!("UF_WITNESS_{name}={value}");
    std::io::stdout().flush().unwrap();
}

fn domain(boot: &str) -> Domain {
    Domain {
        host: "fixture-host".into(),
        boot: boot.into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    }
}

fn revoke(socket: &str, mut value: Kv) {
    value.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(socket, &value, None, false).0, "status"),
        "ok"
    );
}

/// This entry point is compiled only into the unit-test executable. Each role
/// is an actual direct child launched and held by the pinned Python witness.
#[test]
fn child_holder() {
    let Ok(role) = std::env::var("UF_DDR_WITNESS_HOLDER_ROLE") else {
        return;
    };
    let root = PathBuf::from(std::env::var("UF_DDR_WITNESS_ROOT").unwrap());
    let endpoint = std::env::var("UF_DDR_WITNESS_ENDPOINT").unwrap();
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
    let boot = boot.trim();
    let live_drain = std::env::var_os("UF_DDR_WITNESS_LIVE_DRAIN").is_some();
    match role.as_str() {
        "broker" => {
            let authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, boot).unwrap();
            let verify_active = std::env::var_os("UF_DDR_WITNESS_VERIFY_ACTIVE").is_some();
            if verify_active {
                crate::ddr_witness_active::verify_fixture_active_parent(
                    &authority,
                    "fixture-broker",
                    None,
                )
                .unwrap();
                assert!(crate::ddr_witness_active::verify_fixture_active_parent(
                    &authority,
                    "not-the-bound-broker",
                    None,
                )
                .is_err());
            }
            // The first persisted record is this real broker's kernel-observed
            // birth. Leave the fixture dirty even when everyone exits normally:
            // a roster is not yet a full coverage/recovery proof.
            let mut catalog = Catalog::new(0, Vec::new());
            catalog.external_ddr = ExternalDdrState {
                host_id: "fixture-host".into(),
                boot_id: boot.into(),
                broker_instance: "fixture-broker".into(),
                next_generation: 1,
                owner_limit: 8192,
                total_limit: 8192,
                fixture_clean_checkpoint: false,
                ..Default::default()
            };
            catalog.external_ddr.attach_lifecycle(authority).unwrap();
            catalog.external_ddr.before_admission().unwrap();
            let shared: SharedCatalog = Arc::new((Mutex::new(catalog), Condvar::new()));
            let broker = start(&endpoint, Arc::clone(&shared)).unwrap();
            marker("BROKER_READY", "1");
            assert_eq!(line(), "copy");
            let source = decode(&line());
            let target = decode(&line());
            let epoch = shared.0.lock().unwrap().external_ddr.epoch;
            let ticket = admit_copy(
                &shared,
                &source,
                &target,
                epoch,
                17,
                37,
                19,
                BackingPurpose::ProbeOnly,
            )
            .unwrap();
            let worker = std::thread::spawn(move || ticket.execute());
            // WorkDrained is emitted only after the executor ticket's FDs are
            // closed and this actual thread has been joined.
            let completion = finish_copy(&shared, worker.join().unwrap()).unwrap();
            assert!(completion.probe_completed && completion.error.is_none());
            marker("COPY_DRAINED", "1");
            assert_eq!(line(), "stop");
            if verify_active {
                let catalog = shared.0.lock().unwrap();
                let external = &catalog.external_ddr;
                let authority = external.authority.as_ref().unwrap();
                assert_eq!(external.sessions.len(), 3);
                for session in external.sessions.values() {
                    crate::ddr_witness_active::verify_fixture_active_parent(
                        authority,
                        "fixture-broker",
                        Some(&session.birth),
                    )
                    .unwrap();
                    let mut wrong = session.birth.clone();
                    wrong.starttime_ticks += 1;
                    assert!(crate::ddr_witness_active::verify_fixture_active_parent(
                        authority,
                        "fixture-broker",
                        Some(&wrong),
                    )
                    .is_err());
                }
                // A genuine process that is a child of this broker, not the
                // witness, must never be accepted as a direct witness child.
                let mut outsider = Command::new("/bin/sleep").arg("1").spawn().unwrap();
                let outsider_birth = crate::ddr_process_identity::observe(
                    outsider.id() as i32,
                    unsafe { libc::geteuid() },
                    unsafe { libc::getegid() },
                )
                .unwrap();
                assert!(crate::ddr_witness_active::verify_fixture_active_parent(
                    authority,
                    "fixture-broker",
                    Some(&outsider_birth),
                )
                .is_err());
                assert!(outsider.wait().unwrap().success());
                marker("ACTIVE_VERIFIED", "1");
            }
            stop(&shared, broker);
            let external = &shared.0.lock().unwrap().external_ddr;
            if live_drain {
                assert_eq!(external.committed_bytes(), 8192);
            } else if std::env::var_os("UF_DDR_WITNESS_FAULT").is_none() {
                assert_eq!(external.committed_bytes(), 0);
            }
            assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
        }
        "owner" => {
            let source = memfd();
            let source_handle = register(&endpoint, &source);
            let source_meta = source.metadata().unwrap();
            let target = memfd();
            let target_handle = register_pending(&endpoint, &target, "RegisterExternalProbe", 4096);
            let target_meta = target.metadata().unwrap();
            // The broker's copy path needs sole writable access to the target.
            let target_ro = File::open(format!("/proc/self/fd/{}", target.as_raw_fd())).unwrap();
            drop(target);
            marker("SOURCE", encode(&source_handle).trim_end());
            marker(
                "SOURCE_ID",
                &format!("{}:{}", source_meta.dev(), source_meta.ino()),
            );
            marker("TARGET", encode(&target_handle).trim_end());
            marker(
                "TARGET_ID",
                &format!("{}:{}", target_meta.dev(), target_meta.ino()),
            );
            assert_eq!(line(), "stop");
            if !live_drain {
                revoke(&endpoint, target_handle);
                revoke(&endpoint, source_handle);
            }
            drop((source, target_ro));
        }
        "accessor" | "worker" => {
            let mut acquire = decode(&line());
            acquire.insert("op".into(), "AcquireExternalRead".into());
            acquire.insert("access_mode".into(), "read".into());
            acquire.insert("offset_bytes".into(), "17".into());
            acquire.insert("nbytes".into(), "19".into());
            let (view, files) = exchange(&endpoint, &acquire, None, true);
            assert_eq!(get(&view, "status"), "ok");
            assert_eq!(files.len(), 1);
            let mapping = unsafe {
                libc::mmap(
                    std::ptr::null_mut(),
                    4096,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    files[0].as_raw_fd(),
                    0,
                )
            };
            assert_ne!(mapping, libc::MAP_FAILED);
            let data = unsafe { std::slice::from_raw_parts((mapping as *const u8).add(17), 19) };
            assert_eq!(data, b"existing-owner-data");
            marker("HOLDING", &role);
            let action = line();
            if action == "signal" {
                unsafe { libc::raise(libc::SIGTERM) };
                panic!("signal unexpectedly returned");
            }
            if action == "nonzero" {
                unsafe { libc::_exit(7) };
            }
            assert_eq!(action, "stop");
            assert_eq!(unsafe { libc::munmap(mapping, 4096) }, 0);
            drop(files);
            if !live_drain {
                let mut release = view;
                release.insert("op".into(), "ReleaseExternalView".into());
                assert_eq!(
                    get(&exchange(&endpoint, &release, None, false).0, "status"),
                    "ok"
                );
            }
        }
        _ => panic!("unknown fixture role"),
    }
}

fn roster_line<'a>(roster: &'a str, role: &str) -> &'a str {
    let role_token = format!("\"role\":\"{role}\"");
    let mut found = roster
        .lines()
        .filter(|line| line.contains("\"type\":\"child_birth\"") && line.contains(&role_token));
    let item = found.next().expect("missing pinned witness birth");
    assert!(found.next().is_none(), "duplicate fixture role");
    assert!(item.contains("\"pidfd_opened\":true"));
    item
}

fn canonical_birth(b: &ProcessBirth) -> String {
    format!("\"birth\":{{\"gid\":{},\"pid\":{},\"pidns_dev\":{},\"pidns_ino\":{},\"starttime_ticks\":{},\"uid\":{}}}", b.gid, b.pid, b.pid_namespace_dev, b.pid_namespace_ino, b.starttime_ticks, b.uid)
}

fn observed_backing(
    roster: &str,
    role: &str,
    birth: &ProcessBirth,
    dev: u64,
    ino: u64,
    mapped: bool,
) -> bool {
    roster.lines().any(|line| {
        line.contains("\"type\":\"child_resource_observed\"")
            && line.contains(&format!("\"role\":\"{role}\""))
            && line.contains(&canonical_birth(birth))
            && line.contains(&format!("\"dev\":{dev},\"fd_observed\":true,\"ino\":{ino}"))
            && (!mapped || line.contains("\"mapping_observed\":true"))
    })
}

fn live_recovery_fixture() -> (PathBuf, String, String) {
    live_recovery_fixture_with_active_check(false)
}

fn live_recovery_fixture_with_active_check(verify_active: bool) -> (PathBuf, String, String) {
    let base = fs::canonicalize(std::env::var_os("CARGO_TARGET_DIR").unwrap()).unwrap();
    assert!(base.starts_with("/workspace/uflow/runs/"));
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = base.join(format!("rf-{}-{nonce:x}", std::process::id()));
    let endpoint = base
        .join(format!("rs-{}-{nonce:x}", std::process::id()))
        .to_str()
        .unwrap()
        .to_owned();
    assert!(endpoint.len() + ".external-ddr".len() < 108);
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .unwrap()
        .trim()
        .to_owned();
    ManagedAuthority::initialize_boot_fixture(&root, &endpoint, &boot).unwrap();
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/ddr_v0/t3_witness_pin_wip.py");
    let mut command = Command::new("python3");
    command
        .arg(script)
        .arg("--rust-holders-fixture")
        .arg(&root)
        .arg("--child-exe")
        .arg(std::env::current_exe().unwrap())
        .arg("--endpoint")
        .arg(&endpoint)
        .arg("--live-drain");
    if verify_active {
        command.arg("--verify-active");
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    (root, endpoint, boot)
}

#[test]
fn active_pinned_witness_checks_real_parent_and_each_live_peer() {
    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let (root, endpoint, boot) = live_recovery_fixture_with_active_check(true);
    let authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
    let coverage = authority.verify_fixture_coverage().unwrap();
    assert_eq!(coverage.subjects().len(), 4);
    assert_eq!(coverage.work_drains().len(), 1);
    assert_eq!(fs::read(root.join("state")).unwrap(), b"dirty\n");
    println!("live pinned witness: real direct broker and peers accepted; wrong birth/broker and outsider rejected; sealed evidence separately reverified");
}

fn sealed_transaction_tamper_is_rejected(
    root: &Path,
    endpoint: &str,
    boot: &str,
    result: &crate::ddr_recovery_fixture::RecoveryResult,
) {
    use crate::ddr_recovery_journal::Frame;
    let journal_path = root.join("recovery.journal");
    let head_path = root.join("recovery.head");
    let saved_journal = fs::read(&journal_path).unwrap();
    let saved_head = fs::read(&head_path).unwrap();
    let saved_state = fs::read(root.join("state")).unwrap();
    for target in 0..6 {
        let authority = ManagedAuthority::open_boot_fixture(root, endpoint, boot).unwrap();
        let mut journal = authority.fixture_pinned_journal().unwrap();
        let frames = journal.verify().unwrap();
        let suffix = &frames[frames.len() - 3..];
        let record = match target {
            0 | 3 | 5 => 0,
            1 | 4 => 1,
            2 => 2,
            _ => unreachable!(),
        };
        let mut value: serde_json::Value = serde_json::from_slice(&suffix[record].payload).unwrap();
        match target {
            0 => value["resolutions"][0]["resolution"] = serde_json::json!("UNKNOWN"),
            1 => value["audit_sha256"] = serde_json::json!("0".repeat(64)),
            2 => value["commit_sha256"] = serde_json::json!("0".repeat(64)),
            3 => value["after"]["retained_backings"] = serde_json::json!(1),
            4 => {
                value["input_head"]["sequence"] = serde_json::json!(result.input_head.sequence + 1)
            }
            5 => {
                value["resolutions"].as_array_mut().unwrap().remove(0);
            }
            _ => unreachable!(),
        }
        let mut changed = serde_json::to_vec(&value).unwrap();
        changed.push(b'\n');
        let mut bytes = saved_journal[..result.input_head.offset as usize].to_vec();
        let mut previous = result.input_head.checksum;
        for (index, frame) in suffix.iter().enumerate() {
            let payload = if index == record {
                &changed
            } else {
                &frame.payload
            };
            let encoded =
                Frame::encode_fixture(frame.kind, frame.sequence, previous, payload).unwrap();
            previous = u64::from_be_bytes(
                encoded[encoded.len() - 12..encoded.len() - 4]
                    .try_into()
                    .unwrap(),
            );
            bytes.extend_from_slice(&encoded);
        }
        drop(journal);
        fs::write(&journal_path, &bytes).unwrap();
        fs::write(&head_path, format!("uflow-ddr-recovery-head-v1\nmode=clean\nbroker={}\nsequence={}\noffset={}\nchecksum={previous:016x}\nclean_sequence={}\n", result.clean_head.broker, result.clean_head.sequence, bytes.len(), result.clean_head.sequence)).unwrap();
        // Frame CRCs and the durable head were deliberately reanchored: only
        // semantic transaction/evidence validation can reject this forgery.
        assert!(authority.fixture_pinned_journal().unwrap().verify().is_ok());
        let error = crate::ddr_recovery_fixture::verify_sealed(&authority, None).unwrap_err();
        assert!(error.to_string().contains("do not match evidence"));
        let mut next_domain = domain(boot);
        next_domain.broker = "next-fixture-broker".into();
        assert!(
            crate::ddr_recovery_durable::DurableAudit::start_after_fixture_recovery(
                &authority,
                next_domain,
            )
            .is_err()
        );
        drop(authority);
        let changed_head = fs::read(&head_path).unwrap();
        assert!(ManagedAuthority::recover_quarantined_fixture(
            root,
            endpoint,
            boot,
            &result.recovery_id
        )
        .is_err());
        assert_eq!(fs::read(&journal_path).unwrap(), bytes);
        assert_eq!(fs::read(&head_path).unwrap(), changed_head);
        assert_eq!(fs::read(root.join("state")).unwrap(), saved_state);
        fs::write(&journal_path, &saved_journal).unwrap();
        fs::write(&head_path, &saved_head).unwrap();
        assert_eq!(
            ManagedAuthority::recover_quarantined_fixture(
                root,
                endpoint,
                boot,
                &result.recovery_id
            )
            .unwrap(),
            *result
        );
        println!("reanchored recovery suffix tamper={target}: rejected without changes");
    }
}

#[test]
fn recovery_rejects_corrupt_or_missing_journal_without_side_effects() {
    use crate::ddr_recovery_audit::Event;
    use crate::ddr_recovery_journal::{Frame, Kind};
    use std::io::Write;

    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let cases = [
        "missing-state",
        "missing-head",
        "missing-journal",
        "missing-trust",
        "truncated",
        "complete-tail-lost",
        "synced-tail-with-old-head",
        "checksum",
        "unknown-version",
        "unknown-event",
        "sequence-gap",
        "duplicate-sequence",
        "out-of-order",
        "illegal-transition",
        "oversized-state",
        "oversized-head",
    ];
    for case in cases {
        let (root, endpoint, boot) = live_recovery_fixture();
        let authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
        let mut journal = authority.fixture_pinned_journal().unwrap();
        let head = journal.head().clone();
        let frames = journal.verify().unwrap();
        let mut bytes = fs::read(root.join("recovery.journal")).unwrap();
        drop(journal);
        drop(authority);
        let path = root.join("recovery.journal");
        match case {
            "missing-state" => fs::rename(root.join("state"), root.join("held-state")).unwrap(),
            "missing-head" => {
                fs::rename(root.join("recovery.head"), root.join("held-head")).unwrap()
            }
            "missing-journal" => fs::rename(&path, root.join("held-journal")).unwrap(),
            "missing-trust" => {
                fs::rename(root.join("witness.trust"), root.join("held-trust")).unwrap()
            }
            "truncated" => fs::write(&path, &bytes[..bytes.len() - 1]).unwrap(),
            "complete-tail-lost" => {
                let len = bytes.len() - frames.last().unwrap().encoded_len() as usize;
                fs::write(&path, &bytes[..len]).unwrap();
            }
            "synced-tail-with-old-head" => {
                let extra = Frame::encode_fixture(
                    Kind::ViewCommit,
                    head.sequence + 1,
                    head.checksum,
                    b"extra",
                )
                .unwrap();
                let mut file = fs::OpenOptions::new().append(true).open(&path).unwrap();
                file.write_all(&extra).unwrap();
                file.sync_all().unwrap();
            }
            "checksum" => {
                bytes[30] ^= 1;
                fs::write(&path, &bytes).unwrap();
            }
            "unknown-version" => {
                bytes[4..6].copy_from_slice(&2u16.to_be_bytes());
                fs::write(&path, &bytes).unwrap();
            }
            "unknown-event" => {
                bytes[6..8].copy_from_slice(&65535u16.to_be_bytes());
                fs::write(&path, &bytes).unwrap();
            }
            "sequence-gap" => {
                bytes[8..16].copy_from_slice(&2u64.to_be_bytes());
                fs::write(&path, &bytes).unwrap();
            }
            "duplicate-sequence" => {
                let second = frames[0].encoded_len() as usize;
                bytes[second + 8..second + 16].copy_from_slice(&1u64.to_be_bytes());
                fs::write(&path, &bytes).unwrap();
            }
            "out-of-order" => {
                let first = frames[0].encoded_len() as usize;
                let second = first + frames[1].encoded_len() as usize;
                let mut reordered = bytes[first..second].to_vec();
                reordered.extend_from_slice(&bytes[..first]);
                reordered.extend_from_slice(&bytes[second..]);
                fs::write(&path, reordered).unwrap();
            }
            "illegal-transition" => {
                let original_events: Vec<Event> = frames
                    .iter()
                    .map(|frame| ddr_recovery_codec::decode(frame).unwrap())
                    .collect();
                let view = original_events
                    .iter()
                    .find_map(|event| match &event.kind {
                        EventKind::ViewIntent(view) => Some(view),
                        _ => None,
                    })
                    .unwrap();
                let mut bad = original_events.last().unwrap().clone();
                bad.kind = EventKind::ViewCloseDone {
                    view_id: view.view_id,
                    lease_id: view.lease_id,
                };
                let (kind, payload) = ddr_recovery_codec::encode(&bad).unwrap();
                let previous = frames[frames.len() - 2].checksum;
                let encoded =
                    Frame::encode_fixture(kind, head.sequence, previous, &payload).unwrap();
                let checksum = u64::from_be_bytes(
                    encoded[encoded.len() - 12..encoded.len() - 4]
                        .try_into()
                        .unwrap(),
                );
                bytes.truncate(bytes.len() - frames.last().unwrap().encoded_len() as usize);
                bytes.extend_from_slice(&encoded);
                fs::write(&path, &bytes).unwrap();
                fs::write(root.join("recovery.head"), format!("uflow-ddr-recovery-head-v1\nmode=dirty\nbroker={}\nsequence={}\noffset={}\nchecksum={checksum:016x}\nclean_sequence=0\n", head.broker, head.sequence, bytes.len())).unwrap();
            }
            "oversized-state" => fs::write(root.join("state"), vec![b'x'; 4097]).unwrap(),
            "oversized-head" => fs::write(root.join("recovery.head"), vec![b'x'; 4097]).unwrap(),
            _ => unreachable!(),
        }
        let snapshot = || {
            [
                "state",
                "recovery.head",
                "recovery.journal",
                "witness.trust",
            ]
            .iter()
            .map(|name| fs::read(root.join(name)).ok())
            .collect::<Vec<_>>()
        };
        let before = snapshot();
        assert!(
            ManagedAuthority::recover_quarantined_fixture(
                &root,
                &endpoint,
                &boot,
                "corrupt-recovery"
            )
            .is_err(),
            "{case} accepted"
        );
        assert_eq!(snapshot(), before, "{case} changed rejected evidence");
        assert_ne!(
            fs::read(root.join("state")).ok().as_deref(),
            Some(b"clean\n".as_slice()),
            "{case} wrote false clean"
        );
        println!("recovery corruption={case}: rejected without changes");
    }
}

#[test]
fn recovery_transaction_fault_windows_are_fail_closed_or_same_commit() {
    use crate::ddr_managed_authority::FixtureRecoveryFault as Fault;
    use crate::ddr_recovery_journal::{Kind, WriteStage};

    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let mut faults = vec![
        Fault::PendingBeforeTruncate,
        Fault::PendingAfterTruncate,
        Fault::PendingBeforeSync,
        Fault::PendingAfterSync,
        Fault::PendingAfterDirectorySync,
        Fault::AfterPending,
    ];
    for kind in [
        Kind::RecoveryAudit,
        Kind::RecoveryCommit,
        Kind::CleanCheckpoint,
    ] {
        for stage in [
            WriteStage::BeforeJournalWrite,
            WriteStage::AfterPartialJournalWrite,
            WriteStage::BeforeJournalSync,
            WriteStage::AfterJournalSync,
            WriteStage::AfterHeadTruncate,
            WriteStage::BeforeHeadSync,
            WriteStage::AfterHeadSync,
        ] {
            faults.push(Fault::Journal(kind, stage));
        }
    }
    faults.extend([
        Fault::FinalBeforeTruncate,
        Fault::FinalAfterTruncate,
        Fault::FinalBeforeSync,
        Fault::FinalAfterSync,
        Fault::FinalAfterDirectorySync,
        Fault::FinalReadback,
    ]);
    for fault in faults {
        let (root, endpoint, boot) = live_recovery_fixture();
        let mut authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
        let original = authority
            .verify_fixture_coverage()
            .unwrap()
            .input_head()
            .clone();
        assert!(
            authority
                .recover_fixture_fault_locked("fault-recovery", fault)
                .is_err(),
            "fault unexpectedly returned success: {fault:?}"
        );
        assert!(!authority.admits_new_work(), "poison lost: {fault:?}");
        assert!(authority.recover_fixture_locked("fault-recovery").is_err());
        let state = fs::read(root.join("state")).unwrap();
        let head = fs::read(root.join("recovery.head")).unwrap();
        let journal = fs::read(root.join("recovery.journal")).unwrap();
        drop(authority);

        let can_finish = matches!(
            fault,
            Fault::PendingBeforeTruncate
                | Fault::Journal(Kind::CleanCheckpoint, WriteStage::BeforeHeadSync)
                | Fault::Journal(Kind::CleanCheckpoint, WriteStage::AfterHeadSync)
                | Fault::FinalBeforeTruncate
                | Fault::FinalBeforeSync
                | Fault::FinalAfterSync
                | Fault::FinalAfterDirectorySync
                | Fault::FinalReadback
        );
        let retry = ManagedAuthority::recover_quarantined_fixture(
            &root,
            &endpoint,
            &boot,
            "fault-recovery",
        );
        if can_finish {
            let result = retry.unwrap_or_else(|e| panic!("{fault:?}: {e}"));
            assert_eq!(result.input_head, original);
            assert_eq!(result.clean_head.mode, Mode::Clean);
            assert_eq!(fs::read(root.join("state")).unwrap(), b"clean\n");
        } else {
            assert!(
                retry.is_err(),
                "incomplete transaction recovered: {fault:?}"
            );
            assert_ne!(state, b"clean\n", "false clean: {fault:?}");
            assert_eq!(fs::read(root.join("state")).unwrap(), state);
            assert_eq!(fs::read(root.join("recovery.head")).unwrap(), head);
            assert_eq!(fs::read(root.join("recovery.journal")).unwrap(), journal);
        }
        println!("recovery fault={fault:?}: failed; same-commit completion={can_finish}");
    }
}

#[test]
fn witness_direct_children_really_hold_backing_and_journal_drains() {
    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let base = fs::canonicalize(std::env::var_os("CARGO_TARGET_DIR").unwrap()).unwrap();
    assert!(base.starts_with("/workspace/uflow/runs/"));
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = base.join(format!("wh-{}-{nonce:x}", std::process::id()));
    let endpoint = base.join(format!("ws-{}-{nonce:x}", std::process::id()));
    let endpoint = endpoint.to_str().unwrap();
    assert!(endpoint.len() + ".external-ddr".len() < 108);
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
    let boot = boot.trim();
    ManagedAuthority::initialize_boot_fixture(&root, endpoint, boot).unwrap();
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/ddr_v0/t3_witness_pin_wip.py");
    let status = Command::new("python3")
        .arg(script)
        .arg("--rust-holders-fixture")
        .arg(&root)
        .arg("--child-exe")
        .arg(std::env::current_exe().unwrap())
        .arg("--endpoint")
        .arg(endpoint)
        .arg("--live-drain")
        .status()
        .unwrap();
    assert!(status.success(), "pinned witness fixture failed: {status}");

    // Obtain the evidence directory only from the bootstrap-pinned trust slot;
    // the Python witness has independently validated its pin and sealed roster.
    let authority = ManagedAuthority::open_boot_fixture(&root, endpoint, boot).unwrap();
    let trust = fs::read_to_string(root.join("witness.trust")).unwrap();
    let evidence = trust
        .split_once("\"evidence_dir\":\"")
        .unwrap()
        .1
        .split_once('"')
        .unwrap()
        .0;
    assert!(Path::new(evidence).starts_with(&base));
    let roster = fs::read_to_string(Path::new(evidence).join("witness.journal")).unwrap();
    let mut journal = authority.fixture_pinned_journal().unwrap();
    assert_eq!(journal.head().mode, Mode::Dirty);
    let events: Vec<_> = journal
        .verify()
        .unwrap()
        .iter()
        .map(|frame| ddr_recovery_codec::decode(frame).unwrap())
        .collect();
    let mut broker_seen = false;
    let mut owner_seen = false;
    let mut accessor_seen = false;
    let mut worker_seen = false;
    let mut source_id = None;
    let mut backing_inodes = Vec::new();
    let mut broker_birth = None;
    let mut owner_birth = None;
    let mut accessor_birth = None;
    let mut worker_birth = None;
    let mut work_started = false;
    let mut work_drained = false;
    let mut broker_drained = false;
    for event in &events {
        assert_eq!(event.domain, domain(boot));
        match &event.kind {
            EventKind::BrokerBirth { birth } => {
                assert!(roster_line(&roster, "broker").contains(&canonical_birth(birth)));
                broker_seen = true;
                broker_birth = Some(birth.clone());
            }
            EventKind::RegisterIntent(backing) => {
                assert!(roster_line(&roster, "owner").contains(&canonical_birth(&backing.owner)));
                assert_eq!(backing.bytes, 4096);
                owner_seen = true;
                owner_birth = Some(backing.owner.clone());
                backing_inodes.push((backing.memfd_dev, backing.memfd_ino));
                if matches!(
                    backing.purpose,
                    crate::ddr_recovery_audit::Purpose::Immutable
                ) {
                    source_id = Some(backing.object_id);
                }
            }
            EventKind::ViewIntent(view) => {
                assert_eq!(Some(view.object_id), source_id);
                assert_eq!((view.offset, view.bytes), (17, 19));
                let identity = canonical_birth(&view.accessor);
                if roster_line(&roster, "accessor").contains(&identity) {
                    accessor_seen = true;
                    accessor_birth = Some(view.accessor.clone());
                }
                if roster_line(&roster, "worker").contains(&identity) {
                    worker_seen = true;
                    worker_birth = Some(view.accessor.clone());
                }
            }
            EventKind::WorkIntent(work) => {
                assert_eq!(Some(work.source_id), source_id);
                work_started = true;
            }
            EventKind::WorkDrained { .. } => {
                assert!(work_started);
                work_drained = true;
            }
            EventKind::BrokerDrained(snapshot) => {
                assert!(work_drained);
                assert_eq!(
                    (
                        snapshot.handlers,
                        snapshot.in_flight,
                        snapshot.pending_handoffs
                    ),
                    (0, 0, 0)
                );
                assert_eq!(snapshot.retained_backings, 2);
                assert_eq!(snapshot.active_views, 2);
                assert_eq!(snapshot.potential_holders, 3);
                broker_drained = true;
            }
            EventKind::CleanCheckpoint(_) => panic!("fixture must never authorize clean"),
            _ => {}
        }
    }
    assert!(
        broker_seen
            && owner_seen
            && accessor_seen
            && worker_seen
            && work_started
            && work_drained
            && broker_drained
    );
    assert_eq!(backing_inodes.len(), 2);
    let (source_dev, source_ino) = backing_inodes[0];
    for (role, birth, mapped) in [
        ("broker", broker_birth.as_ref().unwrap(), false),
        ("owner", owner_birth.as_ref().unwrap(), false),
        ("accessor", accessor_birth.as_ref().unwrap(), true),
        ("worker", worker_birth.as_ref().unwrap(), true),
    ] {
        assert!(observed_backing(
            &roster, role, birth, source_dev, source_ino, mapped
        ));
    }
    let (target_dev, target_ino) = backing_inodes[1];
    assert!(observed_backing(
        &roster,
        "broker",
        broker_birth.as_ref().unwrap(),
        target_dev,
        target_ino,
        false
    ));
    assert!(observed_backing(
        &roster,
        "owner",
        owner_birth.as_ref().unwrap(),
        target_dev,
        target_ino,
        false
    ));
    for (dev, ino) in backing_inodes {
        assert!(roster
            .lines()
            .any(|line| line.contains("\"type\":\"witness_no_backing\"")
                && line.contains(&format!("\"dev\":{dev},\"ino\":{ino}"))));
    }
    assert!(
        !inspect(authority.fixture_pinned_journal().unwrap(), domain(boot))
            .unwrap()
            .is_checkpointed()
    );
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
    let coverage = authority
        .verify_fixture_coverage()
        .expect("pinned full coverage");
    assert_eq!(coverage.input_head().mode, Mode::Dirty);
    assert_eq!(coverage.subjects().len(), 4);
    assert_eq!(coverage.work_drains().len(), 1);
    assert_eq!(
        coverage.events().len() as u64,
        coverage.input_head().sequence
    );
    // The real broker left live backing/view obligations, not a fabricated
    // zero snapshot. Resolve only a replay copy using the actual pinned exit
    // evidence and prove a changed full birth cannot use that evidence.
    let mut audit = crate::ddr_recovery_audit::AuditReplay::new(domain(boot)).unwrap();
    for event in coverage.events() {
        audit.apply(event.clone()).unwrap();
    }
    let before = audit.observed_drain(0, 0);
    assert!(before.retained_backings > 0);
    assert!(before.active_views > 0);
    assert!(before.potential_holders > 0);
    let after = audit.resolved_fixture_drain(&coverage).unwrap();
    assert_eq!(after.retained_backings, 0);
    assert_eq!(after.active_views, 0);
    assert_eq!(after.potential_holders, 0);
    // Resolution is a projection; it must not mutate the input replay.
    assert_eq!(audit.observed_drain(0, 0), before);
    let mut changed_birth = crate::ddr_recovery_audit::AuditReplay::new(domain(boot)).unwrap();
    let mut changed = false;
    for event in coverage.events() {
        let mut event = event.clone();
        if !changed {
            if let EventKind::ViewIntent(view) = &mut event.kind {
                view.accessor.starttime_ticks += 1;
                changed = true;
            }
        }
        changed_birth.apply(event).unwrap();
    }
    assert!(changed);
    assert!(changed_birth.resolved_fixture_drain(&coverage).is_err());
    println!("coverage={coverage:?}");
    drop(journal);
    drop(authority);
    coverage_matrix::run(&root, endpoint, boot, Path::new(evidence));
    let mut authority = ManagedAuthority::open_boot_fixture(&root, endpoint, boot).unwrap();
    let input = coverage.input_head().clone();
    let resolution_count = coverage
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event.kind,
                EventKind::RegisterIntent(_) | EventKind::ViewIntent(_) | EventKind::WorkIntent(_)
            )
        })
        .count();
    let original_journal = fs::read(root.join("recovery.journal")).unwrap();
    let original_head = fs::read(root.join("recovery.head")).unwrap();
    let original_state = fs::read(root.join("state")).unwrap();
    assert!(authority
        .recover_fixture_fault_locked(
            "capacity-recovery",
            crate::ddr_managed_authority::FixtureRecoveryFault::Capacity,
        )
        .is_err());
    assert!(!authority.admits_new_work());
    assert!(authority
        .recover_fixture_locked("capacity-recovery")
        .is_err());
    assert_eq!(
        fs::read(root.join("recovery.journal")).unwrap(),
        original_journal
    );
    assert_eq!(fs::read(root.join("recovery.head")).unwrap(), original_head);
    assert_eq!(fs::read(root.join("state")).unwrap(), original_state);
    let locked = ManagedAuthority::recover_quarantined_fixture(
        &root,
        endpoint,
        boot,
        "real-holders-recovery",
    );
    assert_eq!(locked.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(
        fs::read(root.join("recovery.journal")).unwrap(),
        original_journal
    );
    assert_eq!(fs::read(root.join("recovery.head")).unwrap(), original_head);
    assert_eq!(fs::read(root.join("state")).unwrap(), original_state);
    drop(coverage);
    drop(authority);
    let recovered = ManagedAuthority::recover_quarantined_fixture(
        &root,
        endpoint,
        boot,
        "real-holders-recovery",
    )
    .expect("complete real witness evidence should recover the isolated fixture");
    assert_eq!(recovered.input_head, input);
    assert_eq!(recovered.resolutions.len(), resolution_count);
    assert!(recovered
        .resolutions
        .iter()
        .all(|r| r["resolution"] != "UNKNOWN"));
    assert_eq!(recovered.clean_head.mode, Mode::Clean);
    assert_eq!(fs::read(root.join("state")).unwrap(), b"clean\n");
    assert!(fs::read(root.join("recovery.journal"))
        .unwrap()
        .starts_with(&original_journal));
    // This is a real recovery suffix from the isolated witness process, not a
    // synthetic frame marker. Only the evidence-verified fixture seam may
    // start a new replay; the unmodified production route remains closed.
    let authority = ManagedAuthority::open_boot_fixture(&root, endpoint, boot).unwrap();
    let rejected =
        crate::ddr_recovery_durable::fixture_replay_rejects_forged_semantics(&authority).unwrap();
    assert_eq!(rejected, 11);
    println!("fixture semantic replay rejected {rejected} in-memory forged-proof cases");
    let mut next_domain = domain(boot);
    next_domain.broker = "next-fixture-broker".into();
    assert!(crate::ddr_recovery_durable::DurableAudit::start(
        authority.fixture_pinned_journal().unwrap(),
        next_domain.clone(),
    )
    .is_err());
    assert!(
        crate::ddr_recovery_durable::DurableAudit::start_after_fixture_recovery(
            &authority,
            next_domain.clone(),
        )
        .is_ok()
    );
    assert!(
        crate::ddr_recovery_durable::DurableAudit::start_after_fixture_recovery(
            &authority,
            domain(boot),
        )
        .is_err()
    );
    for change in 0..4 {
        let mut wrong = next_domain.clone();
        match change {
            0 => wrong.cap_revision += 1,
            1 => wrong.owner_limit -= 1,
            2 => wrong.total_limit += 1,
            3 => wrong.boot = "wrong-boot".into(),
            _ => unreachable!(),
        }
        assert!(
            crate::ddr_recovery_durable::DurableAudit::start_after_fixture_recovery(
                &authority, wrong,
            )
            .is_err()
        );
    }
    drop(authority);
    let sealed_journal = fs::read(root.join("recovery.journal")).unwrap();
    let sealed_head = fs::read(root.join("recovery.head")).unwrap();
    let repeated = ManagedAuthority::recover_quarantined_fixture(
        &root,
        endpoint,
        boot,
        "real-holders-recovery",
    )
    .unwrap();
    assert_eq!(recovered, repeated);
    sealed_transaction_tamper_is_rejected(&root, endpoint, boot, &recovered);
    assert!(
        ManagedAuthority::recover_quarantined_fixture(&root, endpoint, boot, "wrong-id").is_err()
    );
    assert_eq!(
        fs::read(root.join("recovery.journal")).unwrap(),
        sealed_journal
    );
    assert_eq!(fs::read(root.join("recovery.head")).unwrap(), sealed_head);
    // Even if a new generation dirties state before its first journal frame,
    // the prior recovery marker is absent and old evidence cannot clear it.
    let mut next = ManagedAuthority::open_boot_fixture(&root, endpoint, boot).unwrap();
    next.before_admission().unwrap();
    drop(next);
    assert!(ManagedAuthority::recover_quarantined_fixture(
        &root,
        endpoint,
        boot,
        "real-holders-recovery"
    )
    .is_err());
    assert_eq!(fs::read(root.join("state")).unwrap(), b"dirty\n");
    assert_eq!(
        fs::read(root.join("recovery.journal")).unwrap(),
        sealed_journal
    );
    assert_eq!(fs::read(root.join("recovery.head")).unwrap(), sealed_head);
    println!(
        "pinned holder fixture root={} evidence={evidence}; recovery verified, new dirty generation retained",
        root.display()
    );
}

/// The first recovery here is produced by the real direct-parent witness and
/// live holders above. The later generation uses the isolated Rust broker and
/// actual socket/FD/handler drain, not a second trusted witness generation.
#[test]
fn recovered_history_replays_a_later_real_broker_and_rejects_reanchored_tampering() {
    use crate::ddr_recovery_journal::{AnchoredJournal, Frame, Kind};

    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let (root, endpoint, boot) = live_recovery_fixture();
    let recovered =
        ManagedAuthority::recover_quarantined_fixture(&root, &endpoint, &boot, "history-recovery")
            .unwrap();
    let recovery_journal = fs::read(root.join("recovery.journal")).unwrap();
    let authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
    let mut next_domain = domain(&boot);
    next_domain.broker = "history-next-broker".into();
    let audit = crate::ddr_recovery_durable::DurableAudit::start_after_fixture_recovery(
        &authority,
        next_domain.clone(),
    )
    .unwrap();
    let mut catalog = Catalog::new(0, Vec::new());
    catalog.external_ddr = ExternalDdrState {
        host_id: "fixture-host".into(),
        boot_id: boot.clone(),
        broker_instance: next_domain.broker.clone(),
        next_generation: 1,
        owner_limit: 8192,
        total_limit: 8192,
        authority: Some(authority),
        fixture_audit: Some(audit),
        broker_birth_pending: true,
        fixture_clean_checkpoint: true,
        ..Default::default()
    };
    let shared: SharedCatalog = Arc::new((Mutex::new(catalog), Condvar::new()));
    let broker = start(&endpoint, Arc::clone(&shared)).unwrap();
    let source = memfd();
    let handle = register(&endpoint, &source);
    revoke(&endpoint, handle);
    drop(source);
    stop(&shared, broker);
    drop(shared);
    assert_eq!(fs::read(root.join("state")).unwrap(), b"clean\n");
    let saved_journal = fs::read(root.join("recovery.journal")).unwrap();
    let saved_head = fs::read(root.join("recovery.head")).unwrap();
    assert!(saved_journal.starts_with(&recovery_journal));

    let authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
    let mut journal = authority.fixture_pinned_journal().unwrap();
    let head = journal.head().clone();
    let frames = journal.verify().unwrap();
    assert!(head.sequence > recovered.clean_head.sequence);
    assert_eq!(
        crate::ddr_recovery_fixture::verify_sealed(&authority, Some("history-recovery")).unwrap(),
        recovered
    );
    let mut later = domain(&boot);
    later.broker = "history-third-broker".into();
    assert!(
        crate::ddr_recovery_durable::DurableAudit::start_after_fixture_recovery(
            &authority,
            later.clone(),
        )
        .is_ok()
    );
    assert!(
        crate::ddr_recovery_durable::DurableAudit::start_after_fixture_recovery(
            &authority,
            next_domain.clone(),
        )
        .is_err()
    );
    drop(journal);
    drop(authority);
    assert_eq!(
        ManagedAuthority::recover_quarantined_fixture(&root, &endpoint, &boot, "history-recovery")
            .unwrap(),
        recovered
    );

    // Deliberately recompute every frame checksum and the durable head in this
    // isolated fixture: frame order/checksums alone must not hide a bad audit,
    // unapproved budget revision, reused broker, or false zero checkpoint.
    let recovery_index = recovered.input_head.sequence as usize;
    let later_start = recovered.clean_head.sequence as usize;
    let journal_path = root.join("recovery.journal");
    let head_path = root.join("recovery.head");
    for case in [
        "resolution",
        "revision",
        "broker-reuse",
        "nonzero",
        "extra-recovery",
        "missing-birth",
    ] {
        let mut bytes = Vec::new();
        let mut previous = 0u64;
        for (index, frame) in frames.iter().enumerate() {
            let mut kind = frame.kind;
            let mut payload = frame.payload.clone();
            if case == "resolution" && index == recovery_index {
                let mut value: serde_json::Value = serde_json::from_slice(&payload).unwrap();
                value["resolutions"][0]["resolution"] = serde_json::json!("UNKNOWN");
                payload = serde_json::to_vec(&value).unwrap();
                payload.push(b'\n');
            } else if index >= later_start && case == "broker-reuse" {
                let mut event = ddr_recovery_codec::decode(frame).unwrap();
                event.domain.broker = "fixture-broker".into();
                (kind, payload) = ddr_recovery_codec::encode(&event).unwrap();
            } else if index == later_start && case == "revision" {
                let mut event = ddr_recovery_codec::decode(frame).unwrap();
                event.domain.cap_revision += 1;
                (kind, payload) = ddr_recovery_codec::encode(&event).unwrap();
            } else if index == frames.len() - 1 && case == "nonzero" {
                let mut event = ddr_recovery_codec::decode(frame).unwrap();
                if let EventKind::CleanCheckpoint(ref mut snapshot) = event.kind {
                    snapshot.retained_backings = 1;
                } else {
                    panic!("later generation lacks a clean checkpoint");
                }
                (kind, payload) = ddr_recovery_codec::encode(&event).unwrap();
            } else if index == later_start && case == "extra-recovery" {
                kind = Kind::RecoveryAudit;
            } else if index == later_start && case == "missing-birth" {
                // Put a valid next-generation intent first instead of its
                // broker birth, preserving a correctly framed clean history.
                kind = frames[later_start + 1].kind;
                payload = frames[later_start + 1].payload.clone();
            }
            let encoded = Frame::encode_fixture(kind, frame.sequence, previous, &payload).unwrap();
            previous = u64::from_be_bytes(
                encoded[encoded.len() - 12..encoded.len() - 4]
                    .try_into()
                    .unwrap(),
            );
            bytes.extend_from_slice(&encoded);
        }
        let head_broker = if case == "broker-reuse" {
            "fixture-broker"
        } else {
            head.broker.as_str()
        };
        let changed_head = format!("uflow-ddr-recovery-head-v1\nmode=clean\nbroker={head_broker}\nsequence={}\noffset={}\nchecksum={previous:016x}\nclean_sequence={}\n", head.sequence, bytes.len(), head.clean_sequence);
        fs::write(&journal_path, &bytes).unwrap();
        fs::write(&head_path, changed_head.as_bytes()).unwrap();
        let jm = fs::metadata(&journal_path).unwrap();
        let hm = fs::metadata(&head_path).unwrap();
        assert!(AnchoredJournal::open(
            &journal_path,
            &head_path,
            (jm.dev(), jm.ino()),
            (hm.dev(), hm.ino()),
        )
        .unwrap()
        .verify()
        .is_ok());
        let error = match ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot) {
            Ok(_) => panic!("accepted reanchored {case}"),
            Err(error) => error,
        };
        if case == "missing-birth" {
            assert!(error.to_string().contains("lacks broker birth"));
        }
        assert_eq!(fs::read(&journal_path).unwrap(), bytes);
        assert_eq!(fs::read(&head_path).unwrap(), changed_head.as_bytes());
        assert_eq!(fs::read(root.join("state")).unwrap(), b"clean\n");
        fs::write(&journal_path, &saved_journal).unwrap();
        fs::write(&head_path, &saved_head).unwrap();
        println!("history tamper={case}: framing valid, semantic replay rejected");
    }
    assert!(ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).is_ok());
}

#[test]
fn recovery_namespace_failure_poison_survives_restoration_of_pinned_file() {
    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let (root, endpoint, boot) = live_recovery_fixture();
    let mut authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
    let trust = root.join("witness.trust");
    let held = root.join("held-witness.trust");
    let original_state = fs::read(root.join("state")).unwrap();
    let original_head = fs::read(root.join("recovery.head")).unwrap();
    let original_journal = fs::read(root.join("recovery.journal")).unwrap();
    fs::rename(&trust, &held).unwrap();
    assert!(authority
        .recover_fixture_locked("namespace-recovery")
        .is_err());
    fs::rename(&held, &trust).unwrap();
    assert!(!authority.admits_new_work());
    assert!(authority
        .recover_fixture_locked("namespace-recovery")
        .is_err());
    assert_eq!(fs::read(root.join("state")).unwrap(), original_state);
    assert_eq!(fs::read(root.join("recovery.head")).unwrap(), original_head);
    assert_eq!(
        fs::read(root.join("recovery.journal")).unwrap(),
        original_journal
    );
    drop(authority);
    assert!(ManagedAuthority::recover_quarantined_fixture(
        &root,
        &endpoint,
        &boot,
        "namespace-recovery"
    )
    .is_ok());
}

#[test]
fn recovery_evidence_failure_poison_survives_restoration_in_dirty_and_sealed_instances() {
    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    for initially_clean in [false, true] {
        for name in ["witness.head", "witness.journal"] {
            for missing in [false, true] {
                let (root, endpoint, boot) = live_recovery_fixture();
                let mut authority =
                    ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
                let id = "evidence-poison-recovery";
                if initially_clean {
                    let sealed = authority.recover_fixture_locked(id).unwrap();
                    assert!(authority.admits_new_work());
                    assert!(authority.recover_fixture_locked("wrong-id").is_err());
                    assert!(authority.recover_fixture_locked("").is_err());
                    assert!(authority.admits_new_work(), "wrong id poisoned valid seal");
                    assert_eq!(authority.recover_fixture_locked(id).unwrap(), sealed);
                } else {
                    assert!(!authority.admits_new_work());
                }
                let trust: serde_json::Value =
                    serde_json::from_slice(&fs::read(root.join("witness.trust")).unwrap()).unwrap();
                let path = Path::new(trust["evidence_dir"].as_str().unwrap()).join(name);
                let held = path.with_extension("held-original");
                let original = fs::read(&path).unwrap();
                let snapshot = || {
                    [
                        "state",
                        "recovery.head",
                        "recovery.journal",
                        "witness.trust",
                    ]
                    .iter()
                    .map(|file| fs::read(root.join(file)).unwrap())
                    .collect::<Vec<_>>()
                };
                let before = snapshot();
                if missing {
                    fs::rename(&path, &held).unwrap();
                } else {
                    let mut corrupt = original.clone();
                    corrupt[0] ^= 1;
                    fs::write(&path, corrupt).unwrap();
                }
                let corrupted = fs::read(&path).ok();
                // A wrong requested ID must not hide invalid sealed evidence.
                let attempted_id = if missing && name == "witness.head" {
                    ""
                } else if initially_clean && missing {
                    "wrong-id"
                } else {
                    id
                };
                assert!(authority.recover_fixture_locked(attempted_id).is_err());
                assert_eq!(snapshot(), before, "rejection changed durable authority");
                assert_eq!(
                    fs::read(&path).ok(),
                    corrupted,
                    "rejection changed evidence"
                );
                assert!(
                    !authority.admits_new_work(),
                    "failed verification allowed admission"
                );
                assert!(authority.before_admission().is_err());
                assert!(authority
                    .clean_after_drain(&DrainedCommitments {
                        accepting_stopped: true,
                        ..Default::default()
                    })
                    .is_err());
                assert_eq!(
                    ManagedAuthority::recover_quarantined_fixture(&root, &endpoint, &boot, id)
                        .unwrap_err()
                        .kind(),
                    std::io::ErrorKind::WouldBlock,
                    "poisoned authority released its lock"
                );
                if missing {
                    fs::rename(&held, &path).unwrap();
                } else {
                    fs::write(&path, &original).unwrap();
                }
                assert!(!authority.admits_new_work(), "restoration cleared poison");
                assert!(authority.before_admission().is_err());
                assert!(authority.recover_fixture_locked(id).is_err());
                assert!(authority
                    .clean_after_drain(&DrainedCommitments {
                        accepting_stopped: true,
                        ..Default::default()
                    })
                    .is_err());
                assert_eq!(
                    snapshot(),
                    before,
                    "poisoned retry changed durable authority"
                );
                assert_eq!(
                    ManagedAuthority::recover_quarantined_fixture(&root, &endpoint, &boot, id)
                        .unwrap_err()
                        .kind(),
                    std::io::ErrorKind::WouldBlock
                );
                drop(authority);
                let mut reopened =
                    ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
                if !initially_clean {
                    assert!(!reopened.admits_new_work(), "dirty restart lost quarantine");
                }
                assert!(reopened.recover_fixture_locked(id).is_ok());
                assert!(reopened.admits_new_work());
                println!("evidence poison clean={initially_clean} file={name} missing={missing}: rejected, restored and explicitly reverified");
            }
        }
    }
}

#[test]
fn wrong_id_does_not_poison_a_valid_interrupted_sealed_recovery() {
    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    use crate::ddr_managed_authority::FixtureRecoveryFault as Fault;
    use crate::ddr_recovery_journal::{Kind, WriteStage};

    let (root, endpoint, boot) = live_recovery_fixture();
    let mut authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
    let id = "interrupted-recovery";
    assert!(authority.recover_fixture_locked("").is_err());
    assert!(authority
        .recover_fixture_fault_locked(
            id,
            Fault::Journal(Kind::CleanCheckpoint, WriteStage::AfterHeadSync)
        )
        .is_err());
    drop(authority);
    let mut reopened = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
    assert!(!reopened.admits_new_work());
    let state = fs::read(root.join("state")).unwrap();
    let head = fs::read(root.join("recovery.head")).unwrap();
    let journal = fs::read(root.join("recovery.journal")).unwrap();
    assert!(reopened.recover_fixture_locked("wrong-id").is_err());
    assert_eq!(fs::read(root.join("state")).unwrap(), state);
    assert_eq!(fs::read(root.join("recovery.head")).unwrap(), head);
    assert_eq!(fs::read(root.join("recovery.journal")).unwrap(), journal);
    assert!(!reopened.admits_new_work());
    let result = reopened.recover_fixture_locked(id).unwrap();
    assert!(reopened.admits_new_work());
    assert_eq!(reopened.recover_fixture_locked(id).unwrap(), result);
}

#[test]
fn sealed_recovery_readback_failure_poisons_the_locked_instance() {
    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let (root, endpoint, boot) = live_recovery_fixture();
    let mut authority = ManagedAuthority::open_boot_fixture(&root, &endpoint, &boot).unwrap();
    let id = "readback-recovery";
    let result = authority.recover_fixture_locked(id).unwrap();
    let state = fs::read(root.join("state")).unwrap();
    let head = fs::read(root.join("recovery.head")).unwrap();
    let journal = fs::read(root.join("recovery.journal")).unwrap();
    assert!(authority
        .recover_fixture_fault_locked(
            id,
            crate::ddr_managed_authority::FixtureRecoveryFault::SealedReadback,
        )
        .is_err());
    assert!(!authority.admits_new_work());
    assert!(authority.recover_fixture_locked(id).is_err());
    assert!(authority.before_admission().is_err());
    assert!(authority
        .clean_after_drain(&DrainedCommitments {
            accepting_stopped: true,
            ..Default::default()
        })
        .is_err());
    assert_eq!(fs::read(root.join("state")).unwrap(), state);
    assert_eq!(fs::read(root.join("recovery.head")).unwrap(), head);
    assert_eq!(fs::read(root.join("recovery.journal")).unwrap(), journal);
    assert_eq!(
        ManagedAuthority::recover_quarantined_fixture(&root, &endpoint, &boot, id)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    drop(authority);
    assert_eq!(
        ManagedAuthority::recover_quarantined_fixture(&root, &endpoint, &boot, id).unwrap(),
        result
    );
}

#[test]
fn failed_witness_never_cleans_or_reopens_the_pinned_fixture() {
    if std::env::var_os("UFLOW_WITNESS_RUST_FIXTURE").as_deref() != Some("1".as_ref()) {
        return;
    }
    let base = fs::canonicalize(std::env::var_os("CARGO_TARGET_DIR").unwrap()).unwrap();
    assert!(base.starts_with("/workspace/uflow/runs/"));
    let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id").unwrap();
    let boot = boot.trim();
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/ddr_v0/t3_witness_pin_wip.py");
    for fault in ["signal", "nonzero", "timeout", "echild", "unstable"] {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = base.join(format!("wf-{}-{nonce:x}", std::process::id()));
        let endpoint = base.join(format!("wn-{}-{nonce:x}", std::process::id()));
        let endpoint = endpoint.to_str().unwrap();
        assert!(endpoint.len() + ".external-ddr".len() < 108);
        ManagedAuthority::initialize_boot_fixture(&root, endpoint, boot).unwrap();
        let status = Command::new("python3")
            .arg(&script)
            .arg("--rust-holders-fixture")
            .arg(&root)
            .arg("--child-exe")
            .arg(std::env::current_exe().unwrap())
            .arg("--endpoint")
            .arg(endpoint)
            .arg("--fault")
            .arg(fault)
            .status()
            .unwrap();
        assert!(
            status.success(),
            "{fault} negative fixture failed: {status}"
        );
        let mut authority = ManagedAuthority::open_boot_fixture(&root, endpoint, boot).unwrap();
        let mut journal = authority.fixture_pinned_journal().unwrap();
        assert_eq!(journal.head().mode, Mode::Dirty, "{fault}");
        assert!(journal.verify().unwrap().iter().all(|frame| !matches!(
            ddr_recovery_codec::decode(frame).unwrap().kind,
            EventKind::CleanCheckpoint(_)
        )));
        assert!(
            authority.before_admission().is_err(),
            "{fault} admitted after restart"
        );
        assert!(
            authority.verify_fixture_coverage().is_err(),
            "{fault} accepted by coverage"
        );
        if matches!(fault, "signal" | "nonzero") {
            coverage_matrix::forged_wait_rejected(&mut authority, &root);
        }
        assert!(
            authority
                .clean_after_drain(&DrainedCommitments {
                    accepting_stopped: true,
                    ..Default::default()
                })
                .is_err(),
            "{fault} cleared quarantine"
        );
        assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
        println!("failure={fault} remains quarantined at {}", root.display());
    }
}
