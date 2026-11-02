//! Private production-seam tests, not public Plan/SDK acceptance.
use super::super::capability::{self, CopyRequest};
use super::*;
use uf_core::topology::ddr::{self, DdrDiscovery, DiscoveryConfig, Support};

fn cap_fixture() -> (PathBuf, String, SharedCatalog, Broker) {
    let f = fixture();
    {
        let mut st = f.2 .0.lock().unwrap();
        let authority = st.external_ddr.authority.take().unwrap();
        st.external_ddr.attach_lifecycle(authority).unwrap();
        st.external_ddr.fixture_clean_checkpoint = true;
    }
    f
}

#[test]
fn production_quota_rollback_and_mutable_source_never_mint_evidence() {
    let (root, socket, shared, broker) = cap_fixture();
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let (target, _target_file) = staged(&shared, 4096);
    let req = CopyRequest {
        source: source.clone(),
        target: target.clone(),
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let mut observer = discovery(&socket, 8192);
    let snapshot = observer.snapshot().unwrap();
    assert!(capability::probe(&shared, &mut observer, &snapshot, peer(), &req).is_err());
    assert_eq!(
        unsafe {
            libc::fcntl(
                source_file.as_raw_fd(),
                libc::F_ADD_SEALS,
                libc::F_SEAL_WRITE,
            )
        },
        0
    );
    // Both T1 + staging already consume the domain; probe cannot oversell or
    // prefill bytes before a successful reservation in that same ledger.
    assert!(capability::probe(&shared, &mut observer, &snapshot, peer(), &req).is_err());
    let fields = uf_core::kv(&[
        ("name", "overflow".into()),
        ("owner_name", "overflow".into()),
        ("size_bytes", "1024".into()),
    ]);
    assert!(capability::stage(&shared, peer(), &fields, memfd_sized(1024)).is_err());
    {
        let st = shared.0.lock().unwrap();
        assert_eq!(st.external_ddr.committed_bytes(), 8192);
        assert_eq!(st.external_ddr.backings.len(), 2);
        assert!(
            st.external_ddr.copy_work.is_empty() && st.external_ddr.capability_receipts.is_empty()
        );
        assert!(st.external_ddr.backings.values().all(|b| b.in_flight == 0));
    }
    revoke(&socket, target);
    revoke(&socket, source);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

fn peer() -> Peer {
    Peer {
        pid: std::process::id() as i32,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    }
}

fn discovery(socket: &str, cap: u64) -> DdrDiscovery {
    DdrDiscovery::new(DiscoveryConfig {
        realm: "capability-test".into(),
        control_socket: socket.into(),
        max_age: Duration::from_secs(20),
        host_budget_limit_bytes: cap,
    })
    .unwrap()
}

fn staged(shared: &SharedCatalog, size: u64) -> (Kv, File) {
    let file = memfd_sized(size);
    file.write_all_at(&vec![0x42; size as usize], 0).unwrap();
    let fields = uf_core::kv(&[
        ("name", "private-stage".into()),
        ("owner_name", "target-owner".into()),
        ("size_bytes", size.to_string()),
    ]);
    let descriptor = capability::stage(shared, peer(), &fields, file.try_clone().unwrap()).unwrap();
    (descriptor, file)
}

#[test]
fn child_private_capability_owner() {
    let Ok(role) = std::env::var("UF_T33_OWNER_ROLE") else {
        return;
    };
    let socket = std::env::var("UF_T33_SOCKET").unwrap();
    let anchor_file = memfd_sized(1024);
    assert_eq!(
        unsafe {
            libc::fcntl(
                anchor_file.as_raw_fd(),
                libc::F_ADD_SEALS,
                libc::F_SEAL_WRITE,
            )
        },
        0
    );
    let fields = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "owner-anchor".into()),
        ("owner_name", role.clone()),
        ("size_bytes", "1024".into()),
    ]);
    let anchor = exchange(&socket, &fields, Some(&anchor_file), true).0;
    assert_eq!(get(&anchor, "status"), "ok");
    if role == "source" {
        println!("T33_HANDLE={}", encode(&anchor).trim_end());
        std::io::stdout().flush().unwrap();
        let mut command = String::new();
        std::io::stdin().read_line(&mut command).unwrap();
        assert_eq!(command.trim(), "stop");
    } else {
        let target_file = memfd_sized(1024);
        target_file.write_all_at(&vec![0x42; 1024], 0).unwrap();
        let target = register_pending(&socket, &target_file, "RegisterExternalStaging", 1024);
        let readonly = File::open(format!("/proc/self/fd/{}", target_file.as_raw_fd())).unwrap();
        drop(target_file); // cooperative exclusive writer handoff
        println!("T33_HANDLE={}", encode(&target).trim_end());
        std::io::stdout().flush().unwrap();
        let mut command = String::new();
        std::io::stdin().read_line(&mut command).unwrap();
        assert_eq!(command.trim(), "verify");
        let mut actual = vec![0; 1024];
        readonly.read_exact_at(&mut actual, 0).unwrap();
        let mut expected = vec![0x42; 1024];
        expected[37..56].copy_from_slice(b"existing-owner-data");
        assert_eq!(actual, expected);
        println!("T33_VERIFIED=full-target");
        std::io::stdout().flush().unwrap();
        command.clear();
        std::io::stdin().read_line(&mut command).unwrap();
        assert_eq!(command.trim(), "stop");
        drop(readonly);
        revoke(&socket, target);
    }
    revoke(&socket, anchor);
}

#[test]
fn production_two_real_owners_c_e_receipt_and_business_copy() {
    let (root, socket, shared, broker) = cap_fixture();
    let spawn = |role: &str| {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "external_ddr::managed_tests::capability_tests::child_private_capability_owner",
                "--nocapture",
            ])
            .env("UF_T33_OWNER_ROLE", role)
            .env("UF_T33_SOCKET", &socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut first = spawn("source");
    let mut second = spawn("target");
    let pids = [first.id(), second.id()];
    println!("T33_OWNER_PIDS={pids:?}");
    assert_ne!(pids[0], pids[1]);
    let mut source_output = BufReader::new(first.stdout.take().unwrap());
    let mut target_output = BufReader::new(second.stdout.take().unwrap());
    let marker = |reader: &mut dyn BufRead, key: &str| loop {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
        if let Some((_, value)) = line.split_once(key) {
            break value.trim().to_owned();
        }
    };
    let source = decode(&marker(&mut source_output, "T33_HANDLE="));
    let target = decode(&marker(&mut target_output, "T33_HANDLE="));
    assert_ne!(
        get(&source, "owner_incarnation"),
        get(&target, "owner_incarnation")
    );
    let request = CopyRequest {
        source,
        target,
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let target_peer = Peer {
        pid: second.id() as i32,
        ..peer()
    };
    let mut observer = discovery(&socket, 8192);
    let previous = observer.snapshot().unwrap();
    let evidence =
        capability::probe(&shared, &mut observer, &previous, target_peer, &request).unwrap();
    let current = observer.snapshot().unwrap();
    assert!(capability::admit(
        &shared,
        &mut observer,
        &current,
        peer(),
        &request,
        &evidence
    )
    .is_err());
    let id = capability::admit(
        &shared,
        &mut observer,
        &current,
        target_peer,
        &request,
        &evidence,
    )
    .unwrap();
    let result = capability::join(&shared, id).unwrap();
    assert!(result.published && result.error.is_none());
    second
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"verify\n")
        .unwrap();
    assert_eq!(marker(&mut target_output, "T33_VERIFIED="), "full-target");
    for child in [&mut first, &mut second] {
        child.stdin.as_mut().unwrap().write_all(b"stop\n").unwrap();
    }
    let first_result = first.wait_with_output().unwrap();
    let second_result = second.wait_with_output().unwrap();
    assert!(
        first_result.status.success(),
        "{}",
        String::from_utf8_lossy(&first_result.stderr)
    );
    assert!(
        second_result.status.success(),
        "{}",
        String::from_utf8_lossy(&second_result.stderr)
    );
    for pid in pids {
        assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
    }
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

#[test]
fn production_failed_worker_never_ready_and_rolls_back_after_join() {
    let (root, socket, shared, broker) = cap_fixture();
    let (source, _source_file) = sealed_source(&socket);
    let (target, target_file) = staged(&shared, 1024);
    let req = CopyRequest {
        source: source.clone(),
        target: target.clone(),
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let mut observer = discovery(&socket, 8192);
    let previous = observer.snapshot().unwrap();
    let evidence = capability::probe(&shared, &mut observer, &previous, peer(), &req).unwrap();
    let snapshot = observer.snapshot().unwrap();
    let changed = target_file.try_clone().unwrap();
    let id = capability::admit_with_hook(
        &shared,
        &mut observer,
        &snapshot,
        peer(),
        &req,
        &evidence,
        move || {
            assert_eq!(
                unsafe {
                    libc::fcntl(
                        changed.as_raw_fd(),
                        libc::F_ADD_SEALS,
                        libc::F_SEAL_WRITE | libc::F_SEAL_SEAL,
                    )
                },
                0
            );
        },
    )
    .unwrap();
    let done = capability::join(&shared, id).unwrap();
    assert!(!done.published && !done.probe_completed && done.error.is_some());
    assert_eq!(done.copied_bytes, 0);
    {
        let st = shared.0.lock().unwrap();
        assert!(st
            .objects
            .get(&number(&target, "object_id").unwrap())
            .is_none_or(|object| object.state != "Ready"));
        assert!(st.external_ddr.copy_work.is_empty());
        assert_eq!(st.external_ddr.committed_bytes(), 4096);
    }
    let mut actual = vec![0; 1024];
    target_file.read_exact_at(&mut actual, 0).unwrap();
    assert_eq!(actual, vec![0x42; 1024]);
    revoke(&socket, target);
    revoke(&socket, source);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

fn sealed_source(socket: &str) -> (Kv, File) {
    let file = memfd();
    assert_eq!(
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_WRITE) },
        0
    );
    (register(socket, &file), file)
}

fn revoke(socket: &str, mut descriptor: Kv) {
    descriptor.insert("op".into(), "RevokeExternalBacking".into());
    let result = exchange(socket, &descriptor, None, false).0;
    assert_eq!(get(&result, "status"), "ok", "{result:?}");
}

#[test]
fn production_receipt_binds_c_e_target_range_versions_and_real_completion() {
    let (root, socket, shared, broker) = cap_fixture();
    let (source, _file) = sealed_source(&socket);
    let (target, file) = staged(&shared, 1024);
    let req = CopyRequest {
        source: source.clone(),
        target: target.clone(),
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let mut observer = discovery(&socket, 8192);
    let before = observer.snapshot().unwrap();
    let evidence = capability::probe(&shared, &mut observer, &before, peer(), &req).unwrap();
    let snapshot = observer.snapshot().unwrap();
    {
        let st = shared.0.lock().unwrap();
        assert_eq!(
            ddr::verify_capability_evidence(&snapshot, &evidence, &st.external_ddr),
            Ok(Support::Supported)
        );
        assert_eq!(st.external_ddr.committed_bytes(), 5120);
        assert!(st
            .external_ddr
            .backings
            .values()
            .filter(|b| b.purpose == BackingPurpose::ProbeOnly)
            .all(|b| !b.charged && b.state != "active"));
    }
    let mut bad_cases = Vec::new();
    let mut bad = evidence.clone();
    bad.proof[0] ^= 1;
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.expires_at_unix_ns = 1;
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.realm.push_str("-foreign");
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.source.broker_instance.push_str("-foreign");
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.topology_generation += 1;
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.capability_generation += 1;
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.destination_pool_generation += 1;
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.provenance = "other-executor".into();
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.offset += 1;
    bad_cases.push(bad);
    let mut bad = evidence.clone();
    bad.bytes += 1;
    bad_cases.push(bad);
    for bad in bad_cases {
        assert!(capability::admit(&shared, &mut observer, &snapshot, peer(), &req, &bad).is_err());
    }
    for key in [
        "object_id",
        "placement_id",
        "binding_id",
        "owner_lease_id",
        "size_bytes",
        "pool_generation",
        "resource_generation",
        "owner_incarnation",
        "purpose",
    ] {
        let mut changed = req.clone();
        changed.target.insert(key.into(), "changed".into());
        assert!(
            capability::admit(
                &shared,
                &mut observer,
                &snapshot,
                peer(),
                &changed,
                &evidence
            )
            .is_err(),
            "{key}"
        );
    }
    let mut changed = req.clone();
    changed.target_offset += 1;
    assert!(capability::admit(
        &shared,
        &mut observer,
        &snapshot,
        peer(),
        &changed,
        &evidence
    )
    .is_err());
    let foreign = Peer {
        pid: peer().pid + 100000,
        ..peer()
    };
    assert!(
        capability::admit(&shared, &mut observer, &snapshot, foreign, &req, &evidence).is_err()
    );
    let mut original = vec![0; 1024];
    file.read_exact_at(&mut original, 0).unwrap();
    assert_eq!(original, vec![0x42; 1024]);
    assert!(shared.0.lock().unwrap().external_ddr.copy_work.is_empty());
    let id = capability::admit(&shared, &mut observer, &snapshot, peer(), &req, &evidence).unwrap();
    assert!(capability::admit(&shared, &mut observer, &snapshot, peer(), &req, &evidence).is_err());
    let done = capability::join(&shared, id).unwrap();
    assert!(done.published && !done.probe_completed && done.error.is_none());
    assert_eq!((done.copied_bytes, done.verified_bytes), (19, 0));
    file.read_exact_at(&mut original, 0).unwrap();
    let mut expected = vec![0x42; 1024];
    expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(original, expected);
    assert!(file.write_all_at(&[0], 0).is_err());
    revoke(&socket, target);
    revoke(&socket, source);
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

#[test]
fn production_cancel_retains_charge_and_writer_until_actual_join() {
    let (root, socket, shared, broker) = cap_fixture();
    let (source, _file) = sealed_source(&socket);
    let (target, _target_file) = staged(&shared, 1024);
    let req = CopyRequest {
        source: source.clone(),
        target: target.clone(),
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let mut observer = discovery(&socket, 8192);
    let before = observer.snapshot().unwrap();
    let evidence = capability::probe(&shared, &mut observer, &before, peer(), &req).unwrap();
    let snapshot = observer.snapshot().unwrap();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let id = capability::admit_with_hook(
        &shared,
        &mut observer,
        &snapshot,
        peer(),
        &req,
        &evidence,
        move || {
            entered_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        },
    )
    .unwrap();
    entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    shared
        .0
        .lock()
        .unwrap()
        .external_ddr
        .cancel_copy_wip(id)
        .unwrap();
    revoke(&socket, target.clone());
    revoke(&socket, source);
    {
        let st = shared.0.lock().unwrap();
        assert_eq!(st.external_ddr.committed_bytes(), 5120);
        assert_eq!(st.external_ddr.copy_work.len(), 1);
        assert!(st.objects[&number(&target, "object_id").unwrap()].state != "Ready");
    }
    release_tx.send(()).unwrap();
    let done = capability::join(&shared, id).unwrap();
    assert!(!done.published && done.error.is_some());
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

#[test]
fn production_expired_and_epoch_changed_receipts_fail_closed() {
    let (root, socket, shared, broker) = cap_fixture();
    let (source, _file) = sealed_source(&socket);
    let (target, _target_file) = staged(&shared, 1024);
    let req = CopyRequest {
        source: source.clone(),
        target: target.clone(),
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let mut observer = discovery(&socket, 8192);
    let before = observer.snapshot().unwrap();
    let evidence = capability::probe(&shared, &mut observer, &before, peer(), &req).unwrap();
    let snapshot = observer.snapshot().unwrap();
    std::thread::sleep(Duration::from_millis(5100));
    assert!(capability::admit(&shared, &mut observer, &snapshot, peer(), &req, &evidence).is_err());
    let current = observer.snapshot().unwrap();
    let evidence = capability::probe(&shared, &mut observer, &current, peer(), &req).unwrap();
    let old = observer.snapshot().unwrap();
    let (other, _other_file) = staged(&shared, 1024);
    assert!(capability::admit(&shared, &mut observer, &old, peer(), &req, &evidence).is_err());
    let current = observer.snapshot().unwrap();
    assert!(capability::admit(&shared, &mut observer, &current, peer(), &req, &evidence).is_err());
    revoke(&socket, other);
    revoke(&socket, target);
    revoke(&socket, source);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

#[test]
fn bounded_probe_exactly_checks_full_neighbours_and_ordinary_exceeds_old_limit() {
    let src = memfd();
    let source = File::open(format!("/proc/self/fd/{}", src.as_raw_fd())).unwrap();
    let target = memfd_sized(17 * 1024 * 1024);
    crate::ddr_copy::fill_probe(&target).unwrap();
    let completion =
        crate::ddr_copy::copy_range(&source, &target, 17, 37, 19, &AtomicBool::new(false)).unwrap();
    assert_eq!(
        (completion.bytes_copied, completion.bytes_verified),
        (19, 0)
    );
    assert_eq!(
        crate::ddr_copy::verify_probe(&source, &target, 17, 37, 19).unwrap(),
        17 * 1024 * 1024
    );
    for position in [0, 36, 37, 55, 56, 65537, 17 * 1024 * 1024 - 1] {
        let mut byte = [0];
        target.read_exact_at(&mut byte, position).unwrap();
        target.write_all_at(&[byte[0] ^ 1], position).unwrap();
        assert!(
            crate::ddr_copy::verify_probe(&source, &target, 17, 37, 19).is_err(),
            "{position}"
        );
        target.write_all_at(&byte, position).unwrap();
    }
}

#[test]
fn production_two_process_t1_contention_with_staging_probe_and_business_same_ledger() {
    let (root, socket, shared, broker) = cap_fixture();
    let source_file = memfd_sized(1024);
    assert_eq!(
        unsafe {
            libc::fcntl(
                source_file.as_raw_fd(),
                libc::F_ADD_SEALS,
                libc::F_SEAL_WRITE,
            )
        },
        0
    );
    let fields = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "small-source".into()),
        ("owner_name", "source".into()),
        ("size_bytes", "1024".into()),
    ]);
    let source = exchange(&socket, &fields, Some(&source_file), true).0;
    assert_eq!(get(&source, "status"), "ok");
    let (business_target, business_file) = staged(&shared, 1024);
    let (next_target, _next_file) = staged(&shared, 1024);
    let business = CopyRequest {
        source: source.clone(),
        target: business_target.clone(),
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let mut observer = discovery(&socket, 8192);
    let before = observer.snapshot().unwrap();
    let evidence = capability::probe(&shared, &mut observer, &before, peer(), &business).unwrap();
    let snapshot = observer.snapshot().unwrap();
    let (business_tx, business_rx) = std::sync::mpsc::channel();
    let (business_enter_tx, business_enter_rx) = std::sync::mpsc::channel();
    let business_id = capability::admit_with_hook(
        &shared,
        &mut observer,
        &snapshot,
        peer(),
        &business,
        &evidence,
        move || {
            business_enter_tx.send(()).unwrap();
            business_rx.recv_timeout(Duration::from_secs(20)).unwrap();
        },
    )
    .unwrap();
    business_enter_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap();
    let next = CopyRequest {
        target: next_target.clone(),
        ..business.clone()
    };
    let next_shared = Arc::clone(&shared);
    let next_socket = socket.clone();
    let caller = peer();
    let (probe_tx, probe_rx) = std::sync::mpsc::channel();
    let (probe_enter_tx, probe_enter_rx) = std::sync::mpsc::channel();
    let probing = std::thread::spawn(move || {
        let mut observer = discovery(&next_socket, 8192);
        let snapshot = observer.snapshot().unwrap();
        capability::probe_with_hook(
            &next_shared,
            &mut observer,
            &snapshot,
            caller,
            &next,
            move || {
                probe_enter_tx.send(()).unwrap();
                probe_rx.recv_timeout(Duration::from_secs(20)).unwrap();
            },
        )
    });
    probe_enter_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let spawn = || {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "external_ddr::managed_tests::child_competing_registration",
                "--nocapture",
            ])
            .env("UF_DDR_FIXTURE_SOCKET", &socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut first = spawn();
    let mut second = spawn();
    let pids = [first.id(), second.id()];
    println!("T33_COMPETITOR_PIDS={pids:?}");
    assert_ne!(pids[0], pids[1]);
    first.stdin.as_mut().unwrap().write_all(b"go\n").unwrap();
    second.stdin.as_mut().unwrap().write_all(b"go\n").unwrap();
    let outcome = |reader: &mut dyn BufRead| loop {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
        if let Some((_, v)) = line.split_once("FIXTURE_OUTCOME=") {
            break v.trim().to_owned();
        }
    };
    // Keep both pipes open until the harness prints its final test result.
    let mut first_output = BufReader::new(first.stdout.take().unwrap());
    let mut second_output = BufReader::new(second.stdout.take().unwrap());
    let outcomes = [outcome(&mut first_output), outcome(&mut second_output)];
    assert_eq!(
        outcomes.iter().filter(|v| *v == "ok").count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(
        outcomes.iter().filter(|v| *v == "error").count(),
        1,
        "{outcomes:?}"
    );
    {
        let st = shared.0.lock().unwrap();
        assert_eq!(st.external_ddr.committed_bytes(), 8192);
        assert_eq!(st.external_ddr.copy_work.len(), 2);
        assert_eq!(
            st.external_ddr
                .fixture_audit
                .as_ref()
                .unwrap()
                .replay()
                .charged_bytes(),
            8192
        );
    }
    for child in [&mut first, &mut second] {
        child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"release\n")
            .unwrap();
    }
    let first_result = first.wait_with_output().unwrap();
    let second_result = second.wait_with_output().unwrap();
    assert!(
        first_result.status.success(),
        "{}",
        String::from_utf8_lossy(&first_result.stderr)
    );
    assert!(
        second_result.status.success(),
        "{}",
        String::from_utf8_lossy(&second_result.stderr)
    );
    for pid in pids {
        assert!(!PathBuf::from(format!("/proc/{pid}")).exists());
    }
    // Release probe first: it validates but cannot publish its private target.
    probe_tx.send(()).unwrap();
    probing.join().unwrap().unwrap();
    business_tx.send(()).unwrap();
    let done = capability::join(&shared, business_id).unwrap();
    assert!(done.published && done.error.is_none());
    let mut actual = vec![0; 1024];
    business_file.read_exact_at(&mut actual, 0).unwrap();
    let mut expected = vec![0x42; 1024];
    expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(actual, expected);
    for descriptor in [next_target, business_target, source] {
        revoke(&socket, descriptor);
    }
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}
