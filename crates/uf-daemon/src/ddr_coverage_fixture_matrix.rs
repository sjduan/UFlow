//! Adversarial mutations of a run-owned fixture, never a production root.
use super::*;
use crate::ddr_recovery_journal::{Frame, Kind};
use std::os::unix::fs::PermissionsExt;

// Rebuild the witness hash chain so a valid-looking seal cannot substitute for
// bidirectional coverage. These are synthetic inputs, not actual wait data.
fn rewrite_roster(root: &Path, evidence: &Path, events: &[serde_json::Value], anchor: bool) {
    let mut previous = "0".repeat(64);
    let mut data = Vec::new();
    for (index, event) in events.iter().enumerate() {
        let mut frame = serde_json::json!({"schema":"uflow-ddr-witness-fixture-v1", "seq":index + 1,
            "previous":previous, "event":event});
        let hash = crate::ddr_recovery_coverage::digest(&serde_json::to_vec(&frame).unwrap());
        frame["checksum"] = serde_json::Value::String(hash.clone());
        data.extend(serde_json::to_vec(&frame).unwrap());
        data.push(b'\n');
        previous = hash;
    }
    let head = serde_json::json!({"schema":"uflow-ddr-witness-fixture-v1", "seq":events.len(),
        "offset":data.len(), "checksum":previous});
    let mut head_bytes = serde_json::to_vec(&head).unwrap();
    head_bytes.push(b'\n');
    if anchor {
        let trust_path = root.join("witness.trust");
        let mut trust: serde_json::Value =
            serde_json::from_slice(&fs::read(&trust_path).unwrap()).unwrap();
        trust["sealed_head_sha256"] =
            serde_json::json!(crate::ddr_recovery_coverage::digest(&head_bytes));
        let mut encoded = serde_json::to_vec(&trust).unwrap();
        encoded.push(b'\n');
        fs::write(trust_path, encoded).unwrap();
    }
    fs::write(evidence.join("witness.journal"), data).unwrap();
    fs::write(evidence.join("witness.head"), head_bytes).unwrap();
}

fn reject_using(authority: &mut ManagedAuthority, root: &Path, case: &str) {
    let before_state = fs::read(root.join("state")).unwrap();
    let before_head = fs::read(root.join("recovery.head")).unwrap();
    let before_journal = fs::read(root.join("recovery.journal")).unwrap();
    // Check the specific mutation before recovery poisons this instance;
    // otherwise a previous failure could make later cases pass vacuously.
    assert!(
        authority.verify_fixture_coverage().is_err(),
        "{case} accepted by coverage"
    );
    assert!(
        authority.recover_fixture_locked("matrix-recovery").is_err(),
        "{case} authorized recovery"
    );
    assert_eq!(
        fs::read(root.join("state")).unwrap(),
        before_state,
        "{case}"
    );
    assert_eq!(
        fs::read(root.join("recovery.head")).unwrap(),
        before_head,
        "{case}"
    );
    assert_eq!(
        fs::read(root.join("recovery.journal")).unwrap(),
        before_journal,
        "{case}"
    );
    assert!(
        authority.verify_fixture_coverage().is_err(),
        "{case} accepted"
    );
    assert!(
        authority.before_admission().is_err(),
        "{case} admitted new work"
    );
    assert!(
        authority
            .clean_after_drain(&DrainedCommitments {
                accepting_stopped: true,
                ..Default::default()
            })
            .is_err(),
        "{case} cleared quarantine"
    );
    assert_eq!(
        fs::read(root.join("state")).unwrap(),
        b"dirty\n",
        "{case} wrote clean"
    );
    assert!(fs::read_to_string(root.join("recovery.head"))
        .unwrap()
        .contains("mode=dirty\n"));
    println!("coverage negative={case}: rejected, dirty preserved");
}

struct MatrixContext<'a> {
    endpoint: &'a str,
    boot: &'a str,
}

fn rejected(context: &MatrixContext<'_>, root: &Path, case: &str) {
    // Open a fresh locked instance for each independently mutated fixture.
    let mut authority =
        ManagedAuthority::open_boot_fixture(root, context.endpoint, context.boot).unwrap();
    reject_using(&mut authority, root, case);
}

pub(super) fn forged_wait_rejected(authority: &mut ManagedAuthority, root: &Path) {
    let trust: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("witness.trust")).unwrap()).unwrap();
    let evidence = Path::new(trust["evidence_dir"].as_str().unwrap());
    let roster_path = evidence.join("witness.journal");
    let wh_path = evidence.join("witness.head");
    let roster = fs::read(&roster_path).unwrap();
    let wh = fs::read(&wh_path).unwrap();
    let mut events: Vec<serde_json::Value> = roster
        .split_inclusive(|b| *b == b'\n')
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap()["event"].clone())
        .collect();
    let wait = events
        .iter_mut()
        .find(|v| v["type"] == "child_wait" && v["role"] == "worker")
        .unwrap();
    assert_ne!(wait["raw_wait_status"], serde_json::json!(0));
    wait["raw_wait_status"] = serde_json::json!(0);
    wait["normal_exit_zero"] = serde_json::json!(true);
    events.last_mut().unwrap()["all_normal_zero"] = serde_json::json!(true);
    rewrite_roster(root, evidence, &events, false);
    reject_using(authority, root, "forged-normal-wait-after-real-failure");
    fs::write(roster_path, roster).unwrap();
    fs::write(wh_path, wh).unwrap();
}

pub(super) fn run(root: &Path, endpoint: &str, boot: &str, evidence: &Path) {
    let authority = &MatrixContext { endpoint, boot };
    let roster_path = evidence.join("witness.journal");
    let wh_path = evidence.join("witness.head");
    let roster = fs::read(&roster_path).unwrap();
    let wh = fs::read(&wh_path).unwrap();
    let trust_path = root.join("witness.trust");
    let trust = fs::read(&trust_path).unwrap();
    let original: Vec<serde_json::Value> = roster
        .split_inclusive(|b| *b == b'\n')
        .map(|line| serde_json::from_slice::<serde_json::Value>(line).unwrap()["event"].clone())
        .collect();
    let restore = || {
        fs::write(&roster_path, &roster).unwrap();
        fs::write(&wh_path, &wh).unwrap();
        fs::write(&trust_path, &trust).unwrap();
    };

    let mut missing: Vec<_> = original
        .iter()
        .filter(|event| event["role"] != "worker")
        .cloned()
        .collect();
    missing.last_mut().unwrap()["children"] = serde_json::json!(3);
    rewrite_roster(root, evidence, &missing, true);
    rejected(authority, root, "missing-holder-resealed");
    restore();

    let mut extra = original.clone();
    let mut extra_birth = original
        .iter()
        .find(|v| v["type"] == "child_birth" && v["role"] == "worker")
        .unwrap()
        .clone();
    let mut extra_wait = original
        .iter()
        .find(|v| v["type"] == "child_wait" && v["role"] == "worker")
        .unwrap()
        .clone();
    for event in [&mut extra_birth, &mut extra_wait] {
        event["birth"]["pid"] =
            serde_json::json!(event["birth"]["pid"].as_u64().unwrap() + 10_000_000);
        event["launch_id"] = serde_json::json!("holders-run-999");
    }
    extra.insert(1, extra_birth);
    let end = extra.len() - 1;
    extra.insert(end, extra_wait);
    extra.last_mut().unwrap()["children"] = serde_json::json!(5);
    rewrite_roster(root, evidence, &extra, true);
    rejected(authority, root, "extra-forged-child-resealed");
    restore();

    for field in ["starttime_ticks", "pidns_ino", "pidns_dev"] {
        let mut reused = original.clone();
        for event in &mut reused {
            if event["role"] == "worker" {
                let value = event["birth"][field].as_u64().unwrap();
                event["birth"][field] = serde_json::json!(value + 1);
            }
        }
        rewrite_roster(root, evidence, &reused, true);
        rejected(authority, root, field);
        restore();
    }
    let mut wrong_boot = original.clone();
    for event in &mut wrong_boot {
        if event["role"] == "worker" {
            event["boot_id"] = serde_json::json!("other-boot");
        }
    }
    rewrite_roster(root, evidence, &wrong_boot, true);
    rejected(authority, root, "pid-reused-on-other-boot");
    restore();

    let mut code = original.clone();
    code[0]["witness_code_sha256"] = serde_json::json!("0".repeat(64));
    rewrite_roster(root, evidence, &code, true);
    rejected(authority, root, "witness-version-mismatch");
    restore();
    let mut modified = roster.clone();
    modified[10] ^= 1;
    fs::write(&roster_path, modified).unwrap();
    rejected(authority, root, "in-place-content-tamper");
    restore();
    fs::write(&roster_path, &roster[..roster.len() - 10]).unwrap();
    rejected(authority, root, "truncated-roster");
    restore();
    let old = evidence.join("held-original.head");
    fs::rename(&wh_path, &old).unwrap();
    fs::write(&wh_path, &wh).unwrap();
    fs::set_permissions(&wh_path, fs::Permissions::from_mode(0o600)).unwrap();
    rejected(authority, root, "evidence-inode-replacement");
    fs::remove_file(&wh_path).unwrap();
    fs::rename(&old, &wh_path).unwrap();

    // A recomputed witness chain/head without the independent anchor fails.
    let mut resealed = original.clone();
    resealed[1]["argv_sha256"] = serde_json::json!("1".repeat(64));
    rewrite_roster(root, evidence, &resealed, false);
    rejected(authority, root, "content-resealed-without-trust-anchor");
    restore();
    for (field, value, label) in [
        (
            "witness_code_sha256",
            serde_json::json!("0".repeat(64)),
            "pinned-witness-version",
        ),
        (
            "pin_code_sha256",
            serde_json::json!("0".repeat(64)),
            "pinned-launcher-version",
        ),
        (
            "interpreter_sha256",
            serde_json::json!("0".repeat(64)),
            "pinned-interpreter-version",
        ),
        (
            "child_executable_sha256",
            serde_json::json!("0".repeat(64)),
            "pinned-child-version",
        ),
        ("host", serde_json::json!("other-host"), "foreign-host"),
        (
            "domain",
            serde_json::json!("other-domain"),
            "foreign-domain",
        ),
        ("boot", serde_json::json!("other-boot"), "foreign-boot"),
        ("cap_revision", serde_json::json!(2), "foreign-cap-revision"),
        (
            "broker_instance",
            serde_json::json!("other-broker"),
            "foreign-broker",
        ),
        (
            "lifecycle_epoch",
            serde_json::json!(1),
            "non-genesis-binding",
        ),
    ] {
        let mut forged: serde_json::Value = serde_json::from_slice(&trust).unwrap();
        forged[field] = value;
        let mut bytes = serde_json::to_vec(&forged).unwrap();
        bytes.push(b'\n');
        fs::write(&trust_path, bytes).unwrap();
        rejected(authority, root, label);
        fs::write(&trust_path, &trust).unwrap();
    }

    // Same real roster, but remove the persisted work-drain and verification
    // frames while re-anchoring the synthetic journal to an exact dirty head.
    let journal_path = root.join("recovery.journal");
    let head_path = root.join("recovery.head");
    let saved_journal = fs::read(&journal_path).unwrap();
    let saved_head = fs::read(&head_path).unwrap();
    let frames = {
        let locked = ManagedAuthority::open_boot_fixture(root, endpoint, boot).unwrap();
        let mut journal = locked.fixture_pinned_journal().unwrap();
        journal.verify().unwrap()
    };
    let mut data = Vec::new();
    let mut previous = 0u64;
    let mut sequence = 0u64;
    for frame in frames
        .iter()
        .filter(|f| !matches!(f.kind, Kind::WorkDrained | Kind::WorkVerified))
    {
        sequence += 1;
        let encoded =
            Frame::encode_fixture(frame.kind, sequence, previous, &frame.payload).unwrap();
        previous = u64::from_be_bytes(
            encoded[encoded.len() - 12..encoded.len() - 4]
                .try_into()
                .unwrap(),
        );
        data.extend(encoded);
    }
    fs::write(&journal_path, &data).unwrap();
    fs::write(&head_path, format!("uflow-ddr-recovery-head-v1\nmode=dirty\nbroker=fixture-broker\nsequence={sequence}\noffset={}\nchecksum={previous:016x}\nclean_sequence=0\n", data.len())).unwrap();
    rejected(authority, root, "matching-roster-missing-work-drain");
    fs::write(&journal_path, &saved_journal).unwrap();
    fs::write(&head_path, &saved_head).unwrap();

    // A synthetically anchored prior checkpoint exercises the explicit
    // non-genesis gate before event decoding; it is not a real restart proof.
    let first =
        Frame::encode_fixture(Kind::CleanCheckpoint, 1, 0, b"synthetic-checkpoint").unwrap();
    let mut previous =
        u64::from_be_bytes(first[first.len() - 12..first.len() - 4].try_into().unwrap());
    let mut data = first;
    let mut sequence = 1u64;
    for frame in &frames {
        sequence += 1;
        let encoded =
            Frame::encode_fixture(frame.kind, sequence, previous, &frame.payload).unwrap();
        previous = u64::from_be_bytes(
            encoded[encoded.len() - 12..encoded.len() - 4]
                .try_into()
                .unwrap(),
        );
        data.extend(encoded);
    }
    fs::write(&journal_path, &data).unwrap();
    fs::write(&head_path, format!("uflow-ddr-recovery-head-v1\nmode=dirty\nbroker=fixture-broker\nsequence={sequence}\noffset={}\nchecksum={previous:016x}\nclean_sequence=1\n", data.len())).unwrap();
    {
        let locked = ManagedAuthority::open_boot_fixture(root, endpoint, boot).unwrap();
        assert!(locked
            .verify_fixture_coverage()
            .unwrap_err()
            .to_string()
            .contains("dirty genesis generation"));
    }
    rejected(authority, root, "synthetic-prior-clean-checkpoint");
    fs::write(&journal_path, saved_journal).unwrap();
    fs::write(&head_path, saved_head).unwrap();
    let locked = ManagedAuthority::open_boot_fixture(root, endpoint, boot).unwrap();
    assert!(
        locked.verify_fixture_coverage().is_ok(),
        "fixture evidence not restored"
    );
}
