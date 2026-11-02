//! WIP isolated broker/authority wiring tests. Not a two-owner T3 acceptance.
use super::*;
use std::ffi::CString;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Condvar, Mutex};

#[path = "external_ddr_witness_tests.rs"]
mod witness_fixture;

#[path = "external_ddr_capability_tests.rs"]
mod capability_tests;

#[path = "external_ddr_public_tests.rs"]
mod public_tests;

const FIXTURE_ENDPOINT: &str = "/workspace/uflow/runs/authority-fixture/control.sock";

fn fixture() -> (PathBuf, String, SharedCatalog, Broker) {
    let base = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"));
    let base = fs::canonicalize(base).unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;
    // Kernel clock resolution may give parallel tests the same timestamp.
    // Preserve the short socket path while making each fixture name unique
    // within this test process; never delete an existing fixture to retry.
    static LAST_NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let previous = LAST_NONCE
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |last| {
            now.max(last).checked_add(1)
        })
        .expect("fixture nonce exhausted");
    let nonce = now.max(previous) + 1;
    let root = base.join(format!("broker-authority-{}-{}", std::process::id(), nonce));
    let socket = base.join(format!("br-{}-{}.sock", std::process::id(), nonce));
    let socket = socket.to_str().unwrap().to_owned();
    assert!(
        socket.len() + ".external-ddr".len() < 108,
        "test socket path is too long"
    );
    ManagedAuthority::initialize_fixture(&root, FIXTURE_ENDPOINT).unwrap();
    let authority = ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    let mut catalog = Catalog::new(0, Vec::new());
    catalog.external_ddr = ExternalDdrState {
        host_id: "fixture-host".into(),
        boot_id: "fixture-boot".into(),
        broker_instance: "fixture-broker".into(),
        next_generation: 1,
        owner_limit: 8192,
        total_limit: 8192,
        authority: Some(authority),
        ..Default::default()
    };
    let shared: SharedCatalog = Arc::new((Mutex::new(catalog), Condvar::new()));
    let broker = start(&socket, Arc::clone(&shared)).unwrap();
    (root, socket, shared, broker)
}

fn memfd_sized(size: u64) -> File {
    let name = CString::new("uflow-authority-fixture").unwrap();
    let fd =
        unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING) };
    assert!(fd >= 0);
    let file = unsafe { File::from_raw_fd(fd) };
    file.set_len(size).unwrap();
    file.write_all_at(b"existing-owner-data", 17).unwrap();
    assert_eq!(
        unsafe {
            libc::fcntl(
                fd,
                libc::F_ADD_SEALS,
                libc::F_SEAL_GROW | libc::F_SEAL_SHRINK,
            )
        },
        0
    );
    file
}

fn memfd() -> File {
    memfd_sized(4096)
}

fn live_fds_for(file: &File) -> usize {
    let expected = file.metadata().unwrap();
    fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| fs::metadata(entry.path()).ok())
        .filter(|meta| meta.dev() == expected.dev() && meta.ino() == expected.ino())
        .count()
}

fn exchange(socket: &str, request: &Kv, fd: Option<&File>, provisional: bool) -> (Kv, Vec<File>) {
    let mut stream = UnixStream::connect(format!("{}.external-ddr", socket)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_reply(&mut stream, request, fd).unwrap();
    let (line, files) = receive(&mut stream).unwrap();
    let reply = decode(&line);
    if provisional && get(&reply, "status") == "ok" {
        stream.write_all(b"OK\n").unwrap();
        let mut confirm = [0u8; 10];
        stream.read_exact(&mut confirm).unwrap();
        assert_eq!(&confirm, b"committed\n");
    }
    (reply, files)
}

fn register(socket: &str, backing: &File) -> Kv {
    let request = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "fixture".into()),
        ("owner_name", "fixture-owner".into()),
        ("size_bytes", "4096".into()),
    ]);
    let (reply, files) = exchange(socket, &request, Some(backing), true);
    assert_eq!(get(&reply, "status"), "ok", "{reply:?}");
    assert!(files.is_empty());
    reply
}

fn register_pending(socket: &str, backing: &File, purpose_op: &str, size: u64) -> Kv {
    let request = uf_core::kv(&[
        ("op", purpose_op.to_string()),
        ("name", "copy-target".into()),
        ("owner_name", "fixture-owner".into()),
        ("size_bytes", size.to_string()),
    ]);
    let (reply, files) = exchange(socket, &request, Some(backing), true);
    assert_eq!(get(&reply, "status"), "ok", "{reply:?}");
    assert!(files.is_empty());
    reply
}

fn admit_copy(
    shared: &SharedCatalog,
    source: &Kv,
    target: &Kv,
    epoch: u64,
    source_offset: u64,
    target_offset: u64,
    bytes: u64,
    purpose: BackingPurpose,
) -> Result<copy_admission::Ticket, String> {
    let mut st = shared.0.lock().unwrap();
    let mut external = mem::take(&mut st.external_ddr);
    let result = external.admit_copy_wip(
        &mut st,
        source,
        target,
        epoch,
        source_offset,
        target_offset,
        bytes,
        purpose,
    );
    st.external_ddr = external;
    result
}

fn finish_copy(
    shared: &SharedCatalog,
    drained: copy_admission::DrainedTicket,
) -> Result<copy_admission::Completion, String> {
    let mut st = shared.0.lock().unwrap();
    let mut external = mem::take(&mut st.external_ddr);
    let result = external.finish_copy_wip(&mut st, drained);
    st.external_ddr = external;
    result
}

fn stop(shared: &SharedCatalog, broker: Broker) {
    shared.0.lock().unwrap().shutdown_requested = true;
    let _ = broker.stop(true);
}

#[test]
fn startup_rejects_mismatch_and_holds_dirty_without_repairing_files() {
    use crate::ddr_recovery_journal::{Kind, Mode};
    let (root, _socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let external = &mut st.external_ddr;
        let authority = external.authority.take().unwrap();
        external.host_id = "wrong-host".into();
        assert!(external.attach_lifecycle(authority).is_err());
        assert!(!external.has_authority());
        external.host_id = "fixture-host".into();
        let authority =
            ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
        external.attach_lifecycle(authority).unwrap();
        let journal = external
            .authority
            .as_ref()
            .unwrap()
            .fixture_pinned_journal()
            .unwrap();
        assert_eq!(journal.head().sequence, 0);
        external.before_admission().unwrap();
        let mut journal = external
            .authority
            .as_ref()
            .unwrap()
            .fixture_pinned_journal()
            .unwrap();
        assert_eq!(journal.head().mode, Mode::Dirty);
        assert_eq!(journal.verify().unwrap()[0].kind, Kind::BrokerBirth);
    }
    stop(&shared, broker);
    drop(shared);
    let before: Vec<_> = ["state", "recovery.head", "recovery.journal"]
        .iter()
        .map(|name| fs::read(root.join(name)).unwrap())
        .collect();
    let authority = ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    let mut external = ExternalDdrState {
        host_id: "fixture-host".into(),
        boot_id: "fixture-boot".into(),
        broker_instance: "next-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
        ..Default::default()
    };
    external.attach_lifecycle(authority).unwrap();
    assert!(external.has_authority());
    assert!(!external.configured());
    assert!(ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).is_err());
    let after: Vec<_> = ["state", "recovery.head", "recovery.journal"]
        .iter()
        .map(|name| fs::read(root.join(name)).unwrap())
        .collect();
    assert_eq!(before, after);
}

#[test]
fn stats_reports_unknown_without_authority_and_numeric_when_cleanly_attached() {
    let unattached: SharedCatalog =
        Arc::new((Mutex::new(Catalog::new(0, Vec::new())), Condvar::new()));
    let stats = crate::stats_service::get_stats(&Kv::new(), &unattached);
    assert_eq!(get(&stats, "external_ddr_committed_bytes"), "unknown");

    let (_root, _socket, shared, broker) = fixture();
    let stats = crate::stats_service::get_stats(&Kv::new(), &shared);
    assert_eq!(get(&stats, "external_ddr_committed_bytes"), "0");
    {
        let mut catalog = shared.0.lock().unwrap();
        let external = &mut catalog.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
        external.fixture_audit.as_mut().unwrap().poison();
    }
    let stats = crate::stats_service::get_stats(&Kv::new(), &shared);
    assert_eq!(get(&stats, "external_ddr_committed_bytes"), "unknown");
    stop(&shared, broker);
}

#[test]
fn unused_attached_authority_stops_without_creating_dirty_history() {
    let (root, _socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let external = &mut st.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
    }
    stop(&shared, broker);
    assert_eq!(fs::read(root.join("state")).unwrap(), b"clean\n");
    assert_eq!(
        fs::metadata(root.join("recovery.journal")).unwrap().len(),
        0
    );
}

#[test]
fn nonzero_catalog_reservation_is_recorded_and_cannot_be_checkpointed() {
    use crate::ddr_recovery_audit::EventKind;
    use crate::ddr_recovery_journal::{Kind, Mode};
    let (root, _socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let external = &mut st.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
        external.before_admission().unwrap();
        // Injected catalog obligation, not a production reservation API.
        external.independent_reservations.insert(17, 4096);
        external.fixture_clean_checkpoint = true;
    }
    stop(&shared, broker);
    let st = shared.0.lock().unwrap();
    let mut journal = st
        .external_ddr
        .authority
        .as_ref()
        .unwrap()
        .fixture_pinned_journal()
        .unwrap();
    assert_eq!(journal.head().mode, Mode::Dirty);
    let frames = journal.verify().unwrap();
    let drain = frames
        .iter()
        .find(|frame| frame.kind == Kind::BrokerDrained)
        .unwrap();
    match crate::ddr_recovery_codec::decode(drain).unwrap().kind {
        EventKind::BrokerDrained(snapshot) => assert_eq!(snapshot.reservations, 1),
        _ => panic!("expected broker drain"),
    }
    assert!(!frames
        .iter()
        .any(|frame| frame.kind == Kind::CleanCheckpoint));
    assert_eq!(fs::read(root.join("state")).unwrap(), b"dirty\n");
}

#[test]
fn shutdown_waits_for_owned_copy_worker_before_recording_nonzero_drain() {
    use crate::ddr_recovery_journal::{Kind, Mode};
    use std::sync::mpsc;
    let (root, socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let external = &mut st.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
    }
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd();
    let target = register_pending(&socket, &target_file, "RegisterExternalProbe", 4096);
    let (started_tx, started_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    {
        let mut st = shared.0.lock().unwrap();
        let mut external = mem::take(&mut st.external_ddr);
        external
            .admit_and_spawn_test_hook(
                &mut st,
                &source,
                &target,
                external.epoch,
                17,
                37,
                19,
                BackingPurpose::ProbeOnly,
                move || {
                    started_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                },
            )
            .unwrap();
        st.external_ddr = external;
    }
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    shared.0.lock().unwrap().shutdown_requested = true;
    let (done_tx, done_rx) = mpsc::channel();
    let stopper = thread::spawn(move || {
        done_tx.send(broker.stop(true)).unwrap();
    });
    assert!(done_rx.recv_timeout(Duration::from_millis(150)).is_err());
    resume_tx.send(()).unwrap();
    assert!(done_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .is_err());
    stopper.join().unwrap();
    let st = shared.0.lock().unwrap();
    assert!(st.external_ddr.copy_workers.is_empty());
    assert!(st.external_ddr.copy_work.is_empty());
    assert_eq!(st.external_ddr.committed_bytes(), 8192);
    let mut journal = st
        .external_ddr
        .authority
        .as_ref()
        .unwrap()
        .fixture_pinned_journal()
        .unwrap();
    assert_eq!(journal.head().mode, Mode::Dirty);
    let kinds: Vec<_> = journal
        .verify()
        .unwrap()
        .iter()
        .map(|frame| frame.kind)
        .collect();
    let work = kinds
        .iter()
        .position(|kind| *kind == Kind::WorkDrained)
        .unwrap();
    let drain = kinds
        .iter()
        .position(|kind| *kind == Kind::BrokerDrained)
        .unwrap();
    assert!(work < drain);
    assert!(!kinds.contains(&Kind::CleanCheckpoint));
    assert_eq!(fs::read(root.join("state")).unwrap(), b"dirty\n");
}

#[test]
fn private_admit_and_spawn_owns_worker_and_rejects_after_shutdown() {
    use crate::ddr_recovery_journal::Kind;
    let (_root, socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let external = &mut st.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
    }
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd();
    let target = register_pending(&socket, &target_file, "RegisterExternalProbe", 4096);
    let id = {
        let mut st = shared.0.lock().unwrap();
        let mut external = mem::take(&mut st.external_ddr);
        let epoch = external.epoch;
        let id = external
            .admit_and_spawn_wip(
                &mut st,
                &source,
                &target,
                epoch,
                17,
                37,
                19,
                BackingPurpose::ProbeOnly,
            )
            .unwrap();
        assert!(external.copy_workers.contains_key(&id));
        st.shutdown_requested = true;
        assert!(external
            .admit_and_spawn_wip(
                &mut st,
                &source,
                &target,
                epoch,
                17,
                37,
                19,
                BackingPurpose::ProbeOnly,
            )
            .is_err());
        st.external_ddr = external;
        id
    };
    assert!(broker.stop(true).is_err());
    let st = shared.0.lock().unwrap();
    assert!(st.external_ddr.copy_workers.is_empty());
    assert!(st.external_ddr.copy_work.is_empty());
    assert!(st.external_ddr.copy_results[&id].probe_completed);
    let mut journal = st
        .external_ddr
        .authority
        .as_ref()
        .unwrap()
        .fixture_pinned_journal()
        .unwrap();
    let kinds: Vec<_> = journal.verify().unwrap().iter().map(|f| f.kind).collect();
    assert!(kinds.contains(&Kind::WorkDrained));
    assert!(kinds.contains(&Kind::BrokerDrained));
}

#[test]
fn panicking_owned_copy_worker_never_produces_a_broker_drain() {
    use crate::ddr_recovery_journal::{Kind, Mode};
    let (root, socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let external = &mut st.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
    }
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd();
    let target = register_pending(&socket, &target_file, "RegisterExternalProbe", 4096);
    {
        let mut st = shared.0.lock().unwrap();
        let mut external = mem::take(&mut st.external_ddr);
        external
            .admit_and_spawn_test_hook(
                &mut st,
                &source,
                &target,
                external.epoch,
                17,
                37,
                19,
                BackingPurpose::ProbeOnly,
                || panic!("worker fault"),
            )
            .unwrap();
        st.external_ddr = external;
    }
    stop(&shared, broker);
    let st = shared.0.lock().unwrap();
    assert_eq!(st.external_ddr.copy_work.len(), 1);
    assert!(st
        .external_ddr
        .fixture_audit
        .as_ref()
        .unwrap()
        .is_poisoned());
    let mut journal = st
        .external_ddr
        .authority
        .as_ref()
        .unwrap()
        .fixture_pinned_journal()
        .unwrap();
    assert_eq!(journal.head().mode, Mode::Dirty);
    assert!(!journal.verify().unwrap().iter().any(|f| matches!(
        f.kind,
        Kind::WorkDrained | Kind::BrokerDrained | Kind::CleanCheckpoint
    )));
    assert_eq!(fs::read(root.join("state")).unwrap(), b"dirty\n");
}

#[test]
fn failed_control_drain_cannot_clean_even_a_zero_obligation_fixture() {
    let (root, _socket, shared, broker) = fixture();
    {
        let mut catalog = shared.0.lock().unwrap();
        catalog
            .external_ddr
            .authority
            .as_mut()
            .unwrap()
            .before_admission()
            .unwrap();
        catalog.shutdown_requested = true;
    }
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
    assert!(broker.stop(false).is_err());
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
    drop(shared);
    let reopened = ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    assert!(!reopened.admits_new_work());
}

#[test]
fn pinned_fixture_normal_clean_restart_accepts_t1_and_rejects_old_identity() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::inspect;
    use crate::ddr_recovery_journal::Mode;

    let (root, socket, shared, broker) = fixture();
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    {
        let mut catalog = shared.0.lock().unwrap();
        let external = &mut catalog.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
        external.fixture_clean_checkpoint = true;
    }

    let owner_file = memfd();
    let old = register(&socket, &owner_file);
    let mut acquire = old.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let (view, files) = exchange(&socket, &acquire, None, true);
    assert_eq!(get(&view, "status"), "ok");
    assert_eq!(files.len(), 1);
    let mut data = [0; 19];
    files[0].read_exact_at(&mut data, 17).unwrap();
    assert_eq!(&data, b"existing-owner-data");
    drop(files); // close the received view before the legal release
    let mut release = view;
    release.insert("op".into(), "ReleaseExternalView".into());
    assert_eq!(
        get(&exchange(&socket, &release, None, false).0, "status"),
        "ok"
    );
    let mut revoke = old.clone();
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker); // joins real listener and handlers before checkpoint
    {
        let catalog = shared.0.lock().unwrap();
        let journal = catalog
            .external_ddr
            .authority
            .as_ref()
            .unwrap()
            .fixture_pinned_journal()
            .unwrap();
        assert_eq!(journal.head().mode, Mode::Clean);
        assert!(inspect(journal, domain.clone()).unwrap().is_checkpointed());
    }
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
    drop(shared);

    // Reopen the very same fixture files/lock, and deliberately collide IDs in
    // a fresh catalog. This is not the fixed production root or old SDK test.
    let authority = ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    let mut next_domain = domain;
    next_domain.broker = "fixture-broker-next".into();
    let mut catalog = Catalog::new(0, Vec::new());
    catalog.external_ddr = ExternalDdrState {
        host_id: "fixture-host".into(),
        boot_id: "fixture-boot".into(),
        broker_instance: next_domain.broker.clone(),
        next_generation: 1,
        owner_limit: 8192,
        total_limit: 8192,
        fixture_clean_checkpoint: true,
        ..Default::default()
    };
    catalog.external_ddr.attach_lifecycle(authority).unwrap();
    let shared2: SharedCatalog = Arc::new((Mutex::new(catalog), Condvar::new()));
    let broker2 = start(&socket, Arc::clone(&shared2)).unwrap();
    let new_file = memfd();
    let new = register(&socket, &new_file);
    assert_eq!(get(&old, "object_id"), get(&new, "object_id"));
    assert_ne!(get(&old, "broker_instance"), get(&new, "broker_instance"));
    let before = shared2.0.lock().unwrap().external_ddr.committed_bytes();
    let (stale, stale_files) = exchange(&socket, &acquire, None, false);
    assert_eq!(get(&stale, "status"), "error");
    assert!(stale_files.is_empty());
    assert_eq!(
        shared2.0.lock().unwrap().external_ddr.committed_bytes(),
        before
    );
    let mut revoke_new = new;
    revoke_new.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke_new, None, false).0, "status"),
        "ok"
    );
    stop(&shared2, broker2);
    let catalog = shared2.0.lock().unwrap();
    let journal = catalog
        .external_ddr
        .authority
        .as_ref()
        .unwrap()
        .fixture_pinned_journal()
        .unwrap();
    assert_eq!(journal.head().mode, Mode::Clean);
    assert!(inspect(journal, next_domain).unwrap().is_checkpointed());
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

#[test]
fn pinned_fixture_live_mapping_forces_dirty_restart_and_remains_readable() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::{inspect, DurableAudit};
    use crate::ddr_recovery_journal::Mode;

    let (root, socket, shared, broker) = fixture();
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    {
        let mut catalog = shared.0.lock().unwrap();
        let external = &mut catalog.external_ddr;
        let journal = external
            .authority
            .as_ref()
            .unwrap()
            .fixture_pinned_journal()
            .unwrap();
        external.fixture_audit = Some(DurableAudit::start(journal, domain.clone()).unwrap());
        external.fixture_clean_checkpoint = true;
    }
    let owner_file = memfd();
    let backing = register(&socket, &owner_file);
    let mut acquire = backing;
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let (view, files) = exchange(&socket, &acquire, None, true);
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
    drop(files); // the mapping itself is deliberately still live
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
    {
        let catalog = shared.0.lock().unwrap();
        let journal = catalog
            .external_ddr
            .authority
            .as_ref()
            .unwrap()
            .fixture_pinned_journal()
            .unwrap();
        assert_eq!(journal.head().mode, Mode::Dirty);
        let replay = inspect(journal, domain).unwrap();
        assert_eq!(replay.charged_bytes(), 4096);
        assert_eq!(replay.observed_drain(0, 0).active_views, 1);
        assert!(!replay.unresolved_holders().is_empty());
    }
    drop(shared);
    let mut restarted =
        ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    assert!(restarted.before_admission().is_err());
    assert!(restarted
        .clean_after_drain(&DrainedCommitments {
            accepting_stopped: true,
            ..Default::default()
        })
        .is_err());

    // The diagnostic broker must hold the inherited lock and reject both
    // exact old descriptors and new work, including a zero-looking snapshot.
    let names = ["state", "recovery.head", "recovery.journal"];
    let before: Vec<_> = names
        .iter()
        .map(|name| fs::read(root.join(name)).unwrap())
        .collect();
    let mut catalog = Catalog::new(0, Vec::new());
    catalog.external_ddr = ExternalDdrState {
        host_id: "fixture-host".into(),
        boot_id: "fixture-boot".into(),
        broker_instance: "quarantine-diagnostic".into(),
        next_generation: 1,
        owner_limit: 8192,
        total_limit: 8192,
        ..Default::default()
    };
    catalog.external_ddr.attach_lifecycle(restarted).unwrap();
    assert!(!catalog.external_ddr.configured());
    let diagnostic: SharedCatalog = Arc::new((Mutex::new(catalog), Condvar::new()));
    let diagnostic_broker = start(&socket, Arc::clone(&diagnostic)).unwrap();
    assert!(ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).is_err());
    let (stale, received) = exchange(&socket, &acquire, None, false);
    assert_eq!(get(&stale, "status"), "error");
    assert!(get(&stale, "detail").contains("quarantined"));
    assert!(received.is_empty());
    let snapshot = uf_core::kv(&[("op", "GetExternalDdrSnapshot".into())]);
    let (unknown, received) = exchange(&socket, &snapshot, None, false);
    assert_eq!(get(&unknown, "status"), "error");
    assert!(get(&unknown, "detail").contains("quarantined"));
    assert!(!unknown.contains_key("committed_bytes"));
    assert!(!unknown.contains_key("projection_json"));
    assert!(received.is_empty());
    let stats = crate::stats_service::get_stats(&Kv::new(), &diagnostic);
    assert_eq!(get(&stats, "status"), "ok");
    assert_eq!(get(&stats, "external_ddr_committed_bytes"), "unknown");
    let registration = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "new".into()),
        ("owner_name", "new-owner".into()),
        ("size_bytes", "4096".into()),
    ]);
    let (denied, received) = exchange(&socket, &registration, Some(&memfd()), false);
    assert_eq!(get(&denied, "status"), "error");
    assert!(get(&denied, "detail").contains("quarantined"));
    assert!(received.is_empty());
    let payload = unsafe { std::slice::from_raw_parts((mapping as *const u8).add(17), 19) };
    assert_eq!(payload, b"existing-owner-data");
    stop(&diagnostic, diagnostic_broker);
    drop(diagnostic);
    let after: Vec<_> = names
        .iter()
        .map(|name| fs::read(root.join(name)).unwrap())
        .collect();
    assert_eq!(before, after);
    let mut released =
        ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    assert!(released.before_admission().is_err());
    assert_eq!(unsafe { libc::munmap(mapping, 4096) }, 0);
    // No recovery is attempted: this fixture intentionally remains dirty.
}

#[test]
fn registration_fixture_persists_intent_and_commit_before_reply_and_rolls_back_on_log_failure() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::{inspect, DurableAudit};
    use crate::ddr_recovery_journal::AnchoredJournal;

    let (root, socket, shared, broker) = fixture();
    let journal_path = root.join("audit-fixture-journal");
    let head_path = root.join("audit-fixture-head");
    let journal = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&journal_path)
        .unwrap();
    let mut head = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&head_path)
        .unwrap();
    head.write_all(AnchoredJournal::genesis_state().as_bytes())
        .unwrap();
    journal.sync_all().unwrap();
    head.sync_all().unwrap();
    File::open(&root).unwrap().sync_all().unwrap();
    let j = journal.metadata().unwrap();
    let h = head.metadata().unwrap();
    let jid = (j.dev(), j.ino());
    let hid = (h.dev(), h.ino());
    drop(journal);
    drop(head);
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    let open = || AnchoredJournal::open(&journal_path, &head_path, jid, hid).unwrap();
    let audit = DurableAudit::start(open(), domain.clone()).unwrap();
    shared.0.lock().unwrap().external_ddr.fixture_audit = Some(audit);

    let owner_file = memfd();
    let descriptor = register(&socket, &owner_file);
    let replay = inspect(open(), domain.clone()).unwrap();
    assert_eq!(replay.charged_bytes(), 4096);
    let before = {
        let st = shared.0.lock().unwrap();
        (
            st.objects.len(),
            st.data_objects.len(),
            st.placements.len(),
            st.resource_bindings.len(),
            st.leases.len(),
        )
    };
    fs::rename(&journal_path, root.join("audit-fixture-old-journal")).unwrap();
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&journal_path)
        .unwrap();
    // The failed pre-send intent must not leak an FD or a provisional view.
    let mut acquire = descriptor;
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let (reply, fds) = exchange(&socket, &acquire, None, false);
    assert_eq!(get(&reply, "status"), "error");
    assert!(fds.is_empty());
    assert!(shared.0.lock().unwrap().external_ddr.views.is_empty());
    let rejected = memfd();
    let request = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "rejected".into()),
        ("owner_name", "fixture-owner".into()),
        ("size_bytes", "4096".into()),
    ]);
    assert_eq!(
        get(
            &exchange(&socket, &request, Some(&rejected), true).0,
            "status"
        ),
        "error"
    );
    assert_eq!(live_fds_for(&rejected), 1);
    {
        let mut st = shared.0.lock().unwrap();
        assert_eq!(
            (
                st.objects.len(),
                st.data_objects.len(),
                st.placements.len(),
                st.resource_bindings.len(),
                st.leases.len()
            ),
            before
        );
        assert_eq!(st.external_ddr.committed_bytes(), 4096);
        assert!(st.external_ddr.before_admission().is_err());
    }
    fs::remove_file(&journal_path).unwrap();
    fs::rename(root.join("audit-fixture-old-journal"), &journal_path).unwrap();
    assert_eq!(inspect(open(), domain).unwrap().charged_bytes(), 4096);
    // A poisoned writer is not reusable even after its original inode returns.
    assert_eq!(
        get(
            &exchange(&socket, &request, Some(&rejected), true).0,
            "status"
        ),
        "error"
    );
    stop(&shared, broker);
}

#[test]
fn close_done_inode_failure_keeps_charge_after_fd_close_and_rejects_success() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::{inspect, DurableAudit};
    use crate::ddr_recovery_journal::AnchoredJournal;

    let (root, socket, shared, broker) = fixture();
    let journal_path = root.join("close-journal");
    let head_path = root.join("close-head");
    let journal = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&journal_path)
        .unwrap();
    let mut head = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&head_path)
        .unwrap();
    head.write_all(AnchoredJournal::genesis_state().as_bytes())
        .unwrap();
    journal.sync_all().unwrap();
    head.sync_all().unwrap();
    File::open(&root).unwrap().sync_all().unwrap();
    let j = journal.metadata().unwrap();
    let h = head.metadata().unwrap();
    let jid = (j.dev(), j.ino());
    let hid = (h.dev(), h.ino());
    drop((journal, head));
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    let open = || AnchoredJournal::open(&journal_path, &head_path, jid, hid).unwrap();
    shared.0.lock().unwrap().external_ddr.fixture_audit =
        Some(DurableAudit::start(open(), domain.clone()).unwrap());
    let owner_file = memfd();
    let mut descriptor = register(&socket, &owner_file);
    let object_id = number(&descriptor, "object_id").unwrap();
    let displaced = root.join("close-old-journal");
    let path = journal_path.clone();
    let old = displaced.clone();
    shared
        .0
        .lock()
        .unwrap()
        .external_ddr
        .after_close_before_done = Some(Box::new(move || {
        fs::rename(&path, &old).unwrap();
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
    }));
    descriptor.insert("op".into(), "RevokeExternalBacking".into());
    let (reply, _) = exchange(&socket, &descriptor, None, false);
    assert_eq!(get(&reply, "status"), "error");
    {
        let mut st = shared.0.lock().unwrap();
        let backing = &st.external_ddr.backings[&object_id];
        assert!(backing.file.is_none()); // actual close happened before the injected fault
        assert!(backing.charged);
        assert_eq!(st.external_ddr.committed_bytes(), 4096);
        assert!(st.external_ddr.before_admission().is_err());
    }
    fs::remove_file(&journal_path).unwrap();
    fs::rename(&displaced, &journal_path).unwrap();
    assert_eq!(inspect(open(), domain).unwrap().charged_bytes(), 4096);
    assert_eq!(
        get(&exchange(&socket, &descriptor, None, false).0, "status"),
        "error"
    );
    stop(&shared, broker);
}

#[test]
fn fixture_view_handoffs_are_journaled_before_fd_and_unknown_outcomes_stay_exposed() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::{inspect, DurableAudit};
    use crate::ddr_recovery_journal::AnchoredJournal;

    let (root, socket, shared, broker) = fixture();
    let journal_path = root.join("view-audit-journal");
    let head_path = root.join("view-audit-head");
    let journal = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&journal_path)
        .unwrap();
    let mut head = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&head_path)
        .unwrap();
    head.write_all(AnchoredJournal::genesis_state().as_bytes())
        .unwrap();
    journal.sync_all().unwrap();
    head.sync_all().unwrap();
    File::open(&root).unwrap().sync_all().unwrap();
    let j = journal.metadata().unwrap();
    let h = head.metadata().unwrap();
    let jid = (j.dev(), j.ino());
    let hid = (h.dev(), h.ino());
    drop(journal);
    drop(head);
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    let open = || AnchoredJournal::open(&journal_path, &head_path, jid, hid).unwrap();
    shared.0.lock().unwrap().external_ddr.fixture_audit =
        Some(DurableAudit::start(open(), domain.clone()).unwrap());

    let owner_file = memfd();
    let descriptor = register(&socket, &owner_file);
    // Another fully closed backing may release its own charge; it must not
    // erase or duplicate the original object's accounting.
    let other_file = memfd();
    let mut other = register(&socket, &other_file);
    assert_eq!(
        inspect(open(), domain.clone()).unwrap().charged_bytes(),
        8192
    );
    other.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &other, None, false).0, "status"),
        "ok"
    );
    assert_eq!(
        inspect(open(), domain.clone()).unwrap().charged_bytes(),
        4096
    );
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        4096
    );
    let mut acquire = descriptor.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());

    let (sent_tx, sent_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    shared.0.lock().unwrap().external_ddr.after_handoff_send = Some(Box::new(move || {
        sent_tx.send(()).unwrap();
        resume_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }));
    // Receive the actual SCM_RIGHTS FD, but inspect the synced log before ACK.
    let mut stream = UnixStream::connect(format!("{}.external-ddr", socket)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_reply(&mut stream, &acquire, None).unwrap();
    let (line, parent_fds) = receive(&mut stream).unwrap();
    let parent = decode(&line);
    assert_eq!(get(&parent, "status"), "ok");
    assert_eq!(parent_fds.len(), 1);
    sent_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    // Owner FD, catalog FD, and received FD only: the handler duplicate has
    // closed even while the handler is deliberately held before ACK.
    assert_eq!(live_fds_for(&owner_file), 3);
    resume_tx.send(()).unwrap();
    let parent_id = number(&parent, "view_id").unwrap();
    let before_ack = inspect(open(), domain.clone()).unwrap();
    assert_eq!(before_ack.observed_drain(0, 0).pending_handoffs, 1);
    stream.write_all(b"OK\n").unwrap();
    let mut confirmed = [0u8; 10];
    stream.read_exact(&mut confirmed).unwrap();
    assert_eq!(&confirmed, b"committed\n");
    assert_eq!(
        inspect(open(), domain.clone())
            .unwrap()
            .observed_drain(0, 0)
            .active_views,
        1
    );

    let mut retain = parent.clone();
    retain.insert("op".into(), "RetainExternalView".into());
    retain.insert("offset_bytes".into(), "1".into());
    retain.insert("nbytes".into(), "5".into());
    let (child, child_fds) = exchange(&socket, &retain, None, true);
    assert_eq!(get(&child, "status"), "ok", "{child:?}");
    assert_eq!(child_fds.len(), 1);
    assert_eq!(
        inspect(open(), domain.clone())
            .unwrap()
            .observed_drain(0, 0)
            .active_views,
        2
    );

    // Start a second real FD handoff before creating an uncertain outcome.
    // Once either handoff is unknown, no later admission is permitted.
    let mut no_ack = UnixStream::connect(format!("{}.external-ddr", socket)).unwrap();
    no_ack
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_reply(&mut no_ack, &acquire, None).unwrap();
    let (line, abandoned_fds) = receive(&mut no_ack).unwrap();
    let abandoned = decode(&line);
    let abandoned_id = number(&abandoned, "view_id").unwrap();
    assert_eq!(abandoned_fds.len(), 1);
    assert_eq!(
        inspect(open(), domain.clone())
            .unwrap()
            .observed_drain(0, 0)
            .pending_handoffs,
        1
    );

    // Simulate post-commit confirmation-loss undo. The confirmation above
    // actually succeeded; this explicitly exercises the same internal branch.
    {
        let mut st = shared.0.lock().unwrap();
        let mut external = mem::take(&mut st.external_ddr);
        external.undo(&mut st, Undo::View(parent_id));
        assert!(external.uncertain_exposure);
        assert!(!external.configured());
        assert!(external.before_admission().is_err());
        assert!(external.commitments_unknown());
        st.external_ddr = external;
    }
    let mut bytes = [0u8; 19];
    parent_fds[0].read_exact_at(&mut bytes, 17).unwrap();
    assert_eq!(&bytes, b"existing-owner-data");
    assert!(inspect(open(), domain.clone())
        .unwrap()
        .view_is_possibly_exposed(parent_id));
    drop(child_fds); // the cooperative client closes its FD before release
    let mut release_child = child;
    release_child.insert("op".into(), "ReleaseExternalView".into());
    assert_eq!(
        get(&exchange(&socket, &release_child, None, false).0, "status"),
        "ok"
    );
    assert_eq!(
        get(&exchange(&socket, &release_child, None, false).0, "status"),
        "ok"
    );
    assert_eq!(
        inspect(open(), domain.clone())
            .unwrap()
            .observed_drain(0, 0)
            .active_views,
        0
    );

    // Actual missing-ACK path: close the already-received FD handoff.
    drop(no_ack);
    let mut revoke = descriptor;
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    stop(&shared, broker); // joins the handler, without polling or force
    let replay = inspect(open(), domain).unwrap();
    assert!(replay.view_is_possibly_exposed(abandoned_id));
    assert!(replay.view_is_possibly_exposed(parent_id));
    assert_eq!(replay.charged_bytes(), 4096);
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        4096
    );
    assert_eq!(replay.observed_drain(0, 0).active_views, 0);
    assert_eq!(replay.observed_drain(0, 0).pending_handoffs, 0);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
    drop((parent_fds, abandoned_fds));
}

#[test]
fn unacked_fd_handoff_stops_admission_and_cannot_clean() {
    let (root, socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let external = &mut st.external_ddr;
        let authority = external.authority.take().unwrap();
        external.attach_lifecycle(authority).unwrap();
        external.fixture_clean_checkpoint = true;
    }
    let owner = memfd();
    let descriptor = register(&socket, &owner);
    let mut acquire = descriptor.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let mut handoff = UnixStream::connect(format!("{}.external-ddr", socket)).unwrap();
    handoff
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    send_reply(&mut handoff, &acquire, None).unwrap();
    let (line, received) = receive(&mut handoff).unwrap();
    assert_eq!(get(&decode(&line), "status"), "ok");
    assert_eq!(received.len(), 1);
    drop(handoff); // peer may keep the FD; no ACK was sent
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if shared.0.lock().unwrap().external_ddr.uncertain_exposure {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "missing-ACK handler did not settle"
        );
        thread::sleep(Duration::from_millis(5));
    }
    let extra = memfd();
    let request = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "later".into()),
        ("owner_name", "later".into()),
        ("size_bytes", "4096".into()),
    ]);
    let (rejected, fds) = exchange(&socket, &request, Some(&extra), false);
    assert_eq!(get(&rejected, "status"), "error");
    assert!(fds.is_empty());
    assert!(shared.0.lock().unwrap().external_ddr.commitments_unknown());
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
    drop(shared);
    let mut reopened =
        ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    assert!(reopened.is_inherited_quarantine());
    assert!(reopened.before_admission().is_err());
    drop(received);
}

#[test]
fn fixture_probe_cancel_keeps_references_until_join_and_durable_drain() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::{inspect, DurableAudit};
    use crate::ddr_recovery_journal::AnchoredJournal;

    let (root, socket, shared, broker) = fixture();
    let journal_path = root.join("work-audit-journal");
    let head_path = root.join("work-audit-head");
    let journal = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&journal_path)
        .unwrap();
    let mut head = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&head_path)
        .unwrap();
    head.write_all(AnchoredJournal::genesis_state().as_bytes())
        .unwrap();
    journal.sync_all().unwrap();
    head.sync_all().unwrap();
    File::open(&root).unwrap().sync_all().unwrap();
    let j = journal.metadata().unwrap();
    let h = head.metadata().unwrap();
    let jid = (j.dev(), j.ino());
    let hid = (h.dev(), h.ino());
    drop(journal);
    drop(head);
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    let open = || AnchoredJournal::open(&journal_path, &head_path, jid, hid).unwrap();
    shared.0.lock().unwrap().external_ddr.fixture_audit =
        Some(DurableAudit::start(open(), domain.clone()).unwrap());

    let source_file = memfd();
    let source = register(&socket, &source_file);
    let probe_file = memfd_sized(1024);
    let before = vec![0x6e; 1024];
    probe_file.write_all_at(&before, 0).unwrap();
    let probe = register_pending(&socket, &probe_file, "RegisterExternalProbe", 1024);
    let epoch = shared.0.lock().unwrap().external_ddr.epoch;
    // A probe-only backing cannot be reused as a staging target.
    assert!(admit_copy(
        &shared,
        &source,
        &probe,
        epoch,
        17,
        37,
        19,
        BackingPurpose::Staging
    )
    .is_err());
    assert_eq!(inspect(open(), domain.clone()).unwrap().active_work(), 0);
    let ticket = admit_copy(
        &shared,
        &source,
        &probe,
        epoch,
        17,
        37,
        19,
        BackingPurpose::ProbeOnly,
    )
    .unwrap();
    let id = ticket.id();
    let recorded = inspect(open(), domain.clone()).unwrap();
    assert_eq!(recorded.active_work(), 1);
    assert_eq!(recorded.charged_bytes(), 5120);
    let (ready_send, ready_recv) = std::sync::mpsc::channel();
    let (go_send, go_recv) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        ready_send.send(()).unwrap();
        go_recv.recv().unwrap();
        ticket.execute()
    });
    ready_recv.recv_timeout(Duration::from_secs(5)).unwrap();
    {
        let mut st = shared.0.lock().unwrap();
        st.external_ddr.cancel_copy_wip(id).unwrap();
        st.external_ddr.cancel_copy_wip(id).unwrap();
    }
    let mut revoke = source;
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    let before_join = inspect(open(), domain.clone()).unwrap();
    assert_eq!(before_join.active_work(), 1);
    assert_eq!(before_join.charged_bytes(), 5120);
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        5120
    );
    go_send.send(()).unwrap();
    let drained = worker.join().unwrap();
    let completion = finish_copy(&shared, drained).unwrap();
    assert!(!completion.published && !completion.probe_completed && completion.error.is_some());
    assert_eq!(completion.copied_bytes, 0);
    let mut actual = vec![0u8; 1024];
    probe_file.read_exact_at(&mut actual, 0).unwrap();
    assert_eq!(actual, before);
    let after = inspect(open(), domain.clone()).unwrap();
    assert_eq!(after.active_work(), 0);
    assert_eq!(after.charged_bytes(), 0);
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);

    // Also run the successful test-only probe. It remains a pending backing,
    // not an externally publishable business copy or a capability receipt.
    let source_file2 = memfd();
    let source2 = register(&socket, &source_file2);
    let probe_file2 = memfd_sized(1024);
    probe_file2.write_all_at(&before, 0).unwrap();
    let probe2 = register_pending(&socket, &probe_file2, "RegisterExternalProbe", 1024);
    let epoch2 = shared.0.lock().unwrap().external_ddr.epoch;
    let ticket2 = admit_copy(
        &shared,
        &source2,
        &probe2,
        epoch2,
        17,
        37,
        19,
        BackingPurpose::ProbeOnly,
    )
    .unwrap();
    assert_eq!(inspect(open(), domain.clone()).unwrap().active_work(), 1);
    let completed = finish_copy(&shared, ticket2.execute()).unwrap();
    assert!(!completed.published && completed.probe_completed && completed.error.is_none());
    assert_eq!((completed.copied_bytes, completed.verified_bytes), (19, 19));
    probe_file2.read_exact_at(&mut actual, 0).unwrap();
    let mut expected = before;
    expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(actual, expected);
    assert_eq!(inspect(open(), domain.clone()).unwrap().active_work(), 0);
    let mut import_probe = probe2.clone();
    import_probe.insert("op".into(), "AcquireExternalRead".into());
    import_probe.insert("access_mode".into(), "read".into());
    import_probe.insert("offset_bytes".into(), "37".into());
    import_probe.insert("nbytes".into(), "19".into());
    let (denied, files) = exchange(&socket, &import_probe, None, false);
    assert_eq!(get(&denied, "status"), "error");
    assert!(files.is_empty());
    for mut handle in [probe2, source2] {
        handle.insert("op".into(), "RevokeExternalBacking".into());
        assert_eq!(
            get(&exchange(&socket, &handle, None, false).0, "status"),
            "ok"
        );
    }
    assert_eq!(inspect(open(), domain.clone()).unwrap().charged_bytes(), 0);

    // Test-only staged copy: drain, verify, persist publication intent, seal,
    // then persist completion before Ready. Sentinel versions are not proof.
    let source_file3 = memfd();
    let source3 = register(&socket, &source_file3);
    let stage_file = memfd_sized(1024);
    let stage_before = vec![0x42; 1024];
    stage_file.write_all_at(&stage_before, 0).unwrap();
    let stage = register_pending(&socket, &stage_file, "RegisterExternalStaging", 1024);
    let epoch3 = shared.0.lock().unwrap().external_ddr.epoch;
    assert!(admit_copy(
        &shared,
        &source3,
        &stage,
        epoch3 - 1,
        17,
        37,
        19,
        BackingPurpose::Staging
    )
    .is_err());
    assert_eq!(inspect(open(), domain.clone()).unwrap().active_work(), 0);
    let ticket3 = admit_copy(
        &shared,
        &source3,
        &stage,
        epoch3,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let work3 = ticket3.id();
    let drained = ticket3.execute();
    assert_eq!(inspect(open(), domain.clone()).unwrap().active_work(), 1);
    let complete3 = finish_copy(&shared, drained).unwrap();
    assert!(complete3.published && complete3.error.is_none());
    assert_eq!((complete3.copied_bytes, complete3.verified_bytes), (19, 19));
    stage_file.read_exact_at(&mut actual, 0).unwrap();
    let mut stage_expected = stage_before;
    stage_expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(actual, stage_expected);
    assert!(stage_file.write_all_at(b"x", 37).is_err());
    let events: Vec<_> = open()
        .verify()
        .unwrap()
        .iter()
        .map(|frame| crate::ddr_recovery_codec::decode(frame).unwrap().kind)
        .collect();
    assert!(events.iter().any(|event| matches!(event, crate::ddr_recovery_audit::EventKind::PublishDone { work_id, seals } if *work_id == work3 && *seals & 15 == 15)));
    let mut acquire_stage = stage.clone();
    acquire_stage.insert("op".into(), "AcquireExternalRead".into());
    acquire_stage.insert("access_mode".into(), "read".into());
    acquire_stage.insert("offset_bytes".into(), "37".into());
    acquire_stage.insert("nbytes".into(), "19".into());
    let (view, fds) = exchange(&socket, &acquire_stage, None, true);
    assert_eq!(get(&view, "status"), "ok");
    assert_eq!(fds.len(), 1);
    assert_eq!(
        unsafe { libc::fcntl(fds[0].as_raw_fd(), libc::F_GETFL) } & libc::O_ACCMODE,
        libc::O_RDONLY
    );
    drop(fds);
    let mut release = view;
    release.insert("op".into(), "ReleaseExternalView".into());
    assert_eq!(
        get(&exchange(&socket, &release, None, false).0, "status"),
        "ok"
    );
    for mut handle in [stage, source3] {
        handle.insert("op".into(), "RevokeExternalBacking".into());
        assert_eq!(
            get(&exchange(&socket, &handle, None, false).0, "status"),
            "ok"
        );
    }
    assert_eq!(inspect(open(), domain.clone()).unwrap().charged_bytes(), 0);

    // A live writable mapping makes Linux reject sealing: no Ready and no
    // publish-done record; the target is retired only after actual drain.
    let source_file4 = memfd();
    let source4 = register(&socket, &source_file4);
    let target4_file = memfd_sized(1024);
    let mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            1024,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            target4_file.as_raw_fd(),
            0,
        )
    };
    assert_ne!(mapping, libc::MAP_FAILED);
    let target4 = register_pending(&socket, &target4_file, "RegisterExternalStaging", 1024);
    let epoch4 = shared.0.lock().unwrap().external_ddr.epoch;
    let ticket4 = admit_copy(
        &shared,
        &source4,
        &target4,
        epoch4,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let work4 = ticket4.id();
    let failed = finish_copy(&shared, ticket4.execute()).unwrap();
    assert!(
        !failed.published
            && failed
                .error
                .as_deref()
                .is_some_and(|error| error.contains("seal failed"))
    );
    assert_eq!(unsafe { libc::munmap(mapping, 1024) }, 0);
    let events: Vec<_> = open()
        .verify()
        .unwrap()
        .iter()
        .map(|frame| crate::ddr_recovery_codec::decode(frame).unwrap().kind)
        .collect();
    assert!(events.iter().any(|event| matches!(event, crate::ddr_recovery_audit::EventKind::PublishAbort { work_id } if *work_id == work4)));
    assert!(!events.iter().any(|event| matches!(event, crate::ddr_recovery_audit::EventKind::PublishDone { work_id, .. } if *work_id == work4)));
    let target4_id = number(&target4, "object_id").unwrap();
    assert_ne!(
        shared.0.lock().unwrap().data_objects[&target4_id].state,
        "Ready"
    );
    let mut revoke_source = source4;
    revoke_source.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke_source, None, false).0, "status"),
        "ok"
    );
    assert_eq!(inspect(open(), domain.clone()).unwrap().charged_bytes(), 0);

    // Inject a real pinned-inode replacement after successful seal but before
    // publish-done sync, in this isolated fixture only. No Ready or new
    // admission is allowed, even if the original journal inode is restored.
    let source_file5 = memfd();
    let source5 = register(&socket, &source_file5);
    let target5_file = memfd_sized(1024);
    let target5 = register_pending(&socket, &target5_file, "RegisterExternalStaging", 1024);
    let epoch5 = shared.0.lock().unwrap().external_ddr.epoch;
    let ticket5 = admit_copy(
        &shared,
        &source5,
        &target5,
        epoch5,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let work5 = ticket5.id();
    let drained5 = ticket5.execute();
    let displaced = root.join("work-audit-displaced-journal");
    let result = {
        let mut st = shared.0.lock().unwrap();
        let mut external = mem::take(&mut st.external_ddr);
        let result = external.finish_copy_wip_with_hook(&mut st, drained5, || {
            fs::rename(&journal_path, &displaced).unwrap();
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(&journal_path)
                .unwrap();
        });
        st.external_ddr = external;
        result
    };
    assert!(result.is_err());
    assert_ne!(
        unsafe { libc::fcntl(target5_file.as_raw_fd(), libc::F_GET_SEALS) } & libc::F_SEAL_WRITE,
        0
    );
    fs::remove_file(&journal_path).unwrap();
    fs::rename(&displaced, &journal_path).unwrap();
    let events: Vec<_> = open()
        .verify()
        .unwrap()
        .iter()
        .map(|frame| crate::ddr_recovery_codec::decode(frame).unwrap().kind)
        .collect();
    assert!(events.iter().any(|event| matches!(event, crate::ddr_recovery_audit::EventKind::PublishIntent { work_id } if *work_id == work5)));
    assert!(!events.iter().any(|event| matches!(event, crate::ddr_recovery_audit::EventKind::PublishDone { work_id, .. } if *work_id == work5)));
    {
        let mut st = shared.0.lock().unwrap();
        assert_ne!(
            st.data_objects[&number(&target5, "object_id").unwrap()].state,
            "Ready"
        );
        assert_eq!(st.external_ddr.copy_work.len(), 1);
        assert_eq!(st.external_ddr.committed_bytes(), 5120);
        assert!(st.external_ddr.before_admission().is_err());
    }
    assert_eq!(inspect(open(), domain).unwrap().charged_bytes(), 5120);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
}

#[test]
fn fixture_rejects_neighbor_mutation_between_worker_and_publish() {
    let (root, socket, shared, broker) = fixture();
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd_sized(1024);
    target_file.write_all_at(&vec![0x5a; 1024], 0).unwrap();
    let target = register_pending(&socket, &target_file, "RegisterExternalStaging", 1024);
    let epoch = shared.0.lock().unwrap().external_ddr.epoch;
    let ticket = admit_copy(
        &shared,
        &source,
        &target,
        epoch,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let drained = ticket.execute();
    // Simulate an owner violating the cooperative handoff after the worker's
    // check but before seal; a stale executor result must not publish Ready.
    target_file.write_all_at(&[0x11], 2).unwrap();
    let done = finish_copy(&shared, drained).unwrap();
    assert!(!done.published && done.error.as_deref() == Some("target neighbours changed"));
    let mut acquire = target.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "37".into());
    acquire.insert("nbytes".into(), "19".into());
    assert_eq!(
        get(&exchange(&socket, &acquire, None, false).0, "status"),
        "error"
    );
    assert_eq!(shared.0.lock().unwrap().external_ddr.copy_work.len(), 0);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
}

#[test]
fn child_cross_owner_fixture() {
    let Ok(role) = std::env::var("UF_DDR_CROSS_FIXTURE_ROLE") else {
        return;
    };
    let socket = std::env::var("UF_DDR_CROSS_FIXTURE_SOCKET").unwrap();
    let register_small = |file: &File, name: &str| {
        let request = uf_core::kv(&[
            ("op", "RegisterExternalBacking".into()),
            ("name", name.into()),
            ("owner_name", role.clone()),
            ("size_bytes", "1024".into()),
        ]);
        let (reply, files) = exchange(&socket, &request, Some(file), true);
        assert_eq!(get(&reply, "status"), "ok", "{reply:?}");
        assert!(files.is_empty());
        reply
    };
    let readonly = |file: &File| File::open(format!("/proc/self/fd/{}", file.as_raw_fd())).unwrap();
    let revoke = |mut handle: Kv| {
        handle.insert("op".into(), "RevokeExternalBacking".into());
        assert_eq!(
            get(&exchange(&socket, &handle, None, false).0, "status"),
            "ok"
        );
    };
    if role == "source" {
        let file = memfd_sized(1024);
        let handle = register_small(&file, "cross-source");
        let _read_only = readonly(&file);
        drop(file);
        println!("CROSS_SOURCE={}", encode(&handle).trim_end());
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "stop");
        revoke(handle);
    } else {
        assert_eq!(role, "destination");
        let anchor_file = memfd_sized(1024);
        let anchor = register_small(&anchor_file, "cross-anchor");
        let stage_file = memfd_sized(1024);
        let probe_file = memfd_sized(1024);
        let cancel_file = memfd_sized(1024);
        stage_file.write_all_at(&vec![0x42; 1024], 0).unwrap();
        probe_file.write_all_at(&vec![0x7b; 1024], 0).unwrap();
        cancel_file.write_all_at(&vec![0x31; 1024], 0).unwrap();
        let stage = register_pending(&socket, &stage_file, "RegisterExternalStaging", 1024);
        let probe = register_pending(&socket, &probe_file, "RegisterExternalProbe", 1024);
        let cancel = register_pending(&socket, &cancel_file, "RegisterExternalStaging", 1024);
        let stage_ro = readonly(&stage_file);
        let probe_ro = readonly(&probe_file);
        let cancel_ro = readonly(&cancel_file);
        drop((stage_file, probe_file, cancel_file)); // cooperative write handoff
        println!("CROSS_ANCHOR={}", encode(&anchor).trim_end());
        println!("CROSS_STAGE={}", encode(&stage).trim_end());
        println!("CROSS_PROBE={}", encode(&probe).trim_end());
        println!("CROSS_CANCEL={}", encode(&cancel).trim_end());
        std::io::stdout().flush().unwrap();
        let mut line = String::new();
        std::io::stdin().read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "verify");
        for (file, fill, copied) in [
            (&stage_ro, 0x42, true),
            (&probe_ro, 0x7b, true),
            (&cancel_ro, 0x31, false),
        ] {
            let mut actual = vec![0; 1024];
            file.read_exact_at(&mut actual, 0).unwrap();
            let mut expected = vec![fill; 1024];
            if copied {
                expected[37..56].copy_from_slice(b"existing-owner-data");
            }
            assert_eq!(actual, expected);
        }
        println!("CROSS_VERIFIED");
        std::io::stdout().flush().unwrap();
        line.clear();
        std::io::stdin().read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "stop");
        for handle in [stage, probe, cancel, anchor] {
            revoke(handle);
        }
    }
}

#[test]
fn fixture_two_real_owners_guarded_copy_and_cancel() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::{inspect, DurableAudit};
    use crate::ddr_recovery_journal::AnchoredJournal;
    use uf_core::topology::ddr::{DdrDiscovery, DiscoveryConfig, Operation, Support};

    let (root, socket, shared, broker) = fixture();
    let journal_path = root.join("owners-audit-journal");
    let head_path = root.join("owners-audit-head");
    let journal = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&journal_path)
        .unwrap();
    let mut head = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&head_path)
        .unwrap();
    head.write_all(AnchoredJournal::genesis_state().as_bytes())
        .unwrap();
    journal.sync_all().unwrap();
    head.sync_all().unwrap();
    File::open(&root).unwrap().sync_all().unwrap();
    let j = journal.metadata().unwrap();
    let h = head.metadata().unwrap();
    let jid = (j.dev(), j.ino());
    let hid = (h.dev(), h.ino());
    drop(journal);
    drop(head);
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    let open = || AnchoredJournal::open(&journal_path, &head_path, jid, hid).unwrap();
    shared.0.lock().unwrap().external_ddr.fixture_audit =
        Some(DurableAudit::start(open(), domain.clone()).unwrap());
    let spawn = |role: &str| {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "external_ddr::managed_tests::child_cross_owner_fixture",
                "--nocapture",
            ])
            .env("UF_DDR_CROSS_FIXTURE_ROLE", role)
            .env("UF_DDR_CROSS_FIXTURE_SOCKET", &socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap()
    };
    let mut src_owner = spawn("source");
    let mut dst_owner = spawn("destination");
    assert_ne!(src_owner.id(), dst_owner.id());
    assert_ne!(src_owner.id(), std::process::id());
    assert_ne!(dst_owner.id(), std::process::id());
    let mut src_output = BufReader::new(src_owner.stdout.take().unwrap());
    let mut dst_output = BufReader::new(dst_owner.stdout.take().unwrap());
    fn read_marker(reader: &mut impl BufRead, marker: &str) -> String {
        loop {
            let mut line = String::new();
            assert_ne!(
                reader.read_line(&mut line).unwrap(),
                0,
                "owner exited before {marker}"
            );
            if let Some((_, value)) = line.split_once(marker) {
                return value.trim().to_owned();
            }
        }
    }
    let source = decode(&read_marker(&mut src_output, "CROSS_SOURCE="));
    let _anchor = decode(&read_marker(&mut dst_output, "CROSS_ANCHOR="));
    let stage = decode(&read_marker(&mut dst_output, "CROSS_STAGE="));
    let probe = decode(&read_marker(&mut dst_output, "CROSS_PROBE="));
    let cancelled_target = decode(&read_marker(&mut dst_output, "CROSS_CANCEL="));
    assert_ne!(
        get(&source, "owner_incarnation"),
        get(&stage, "owner_incarnation")
    );
    assert_ne!(get(&source, "object_id"), get(&stage, "object_id"));
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        5120
    );
    assert_eq!(
        inspect(open(), domain.clone()).unwrap().charged_bytes(),
        5120
    );

    let mut discovery = DdrDiscovery::new(DiscoveryConfig {
        realm: "fixture".into(),
        control_socket: PathBuf::from(&socket),
        max_age: Duration::from_secs(5),
        host_budget_limit_bytes: 8192,
    })
    .unwrap();
    let snapshot = discovery.snapshot().unwrap();
    let source_backing = snapshot
        .backings
        .iter()
        .find(|b| b.identity.object_id.to_string() == get(&source, "object_id"))
        .unwrap();
    let stage_backing = snapshot
        .backings
        .iter()
        .find(|b| b.identity.object_id.to_string() == get(&stage, "object_id"))
        .unwrap();
    assert_eq!(source_backing.owner_pid, u64::from(src_owner.id()));
    assert_eq!(stage_backing.owner_pid, u64::from(dst_owner.id()));
    assert!(snapshot
        .capabilities
        .iter()
        .any(|cap| cap.operation == Operation::Copy
            && cap.source == source_backing.identity
            && cap.destination_address_domain == stage_backing.identity.owner_incarnation
            && cap.support == Support::Unsupported));
    assert!(snapshot
        .capabilities
        .iter()
        .filter(|cap| cap.operation == Operation::Copy)
        .all(|cap| cap.support == Support::Unsupported));
    let probe_ticket = copy_admission::guarded_admit_wip(
        &shared,
        &mut discovery,
        &snapshot,
        &source,
        &probe,
        17,
        37,
        19,
        BackingPurpose::ProbeOnly,
    )
    .unwrap();
    let probe_done = finish_copy(&shared, probe_ticket.execute()).unwrap();
    assert!(probe_done.probe_completed && !probe_done.published);
    // The old snapshot is stale after probe admission/drain; do not silently
    // replan or execute the business target from that snapshot.
    assert!(copy_admission::guarded_admit_wip(
        &shared,
        &mut discovery,
        &snapshot,
        &source,
        &stage,
        17,
        37,
        19,
        BackingPurpose::Staging
    )
    .is_err());
    assert_eq!(shared.0.lock().unwrap().external_ddr.copy_work.len(), 0);

    let current = discovery.snapshot().unwrap();
    let cancelled = copy_admission::guarded_admit_wip(
        &shared,
        &mut discovery,
        &current,
        &source,
        &cancelled_target,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let cancelled_id = cancelled.id();
    let (ready_send, ready_recv) = std::sync::mpsc::channel();
    let (go_send, go_recv) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        ready_send.send(()).unwrap();
        go_recv.recv().unwrap();
        cancelled.execute()
    });
    ready_recv.recv_timeout(Duration::from_secs(5)).unwrap();
    shared
        .0
        .lock()
        .unwrap()
        .external_ddr
        .cancel_copy_wip(cancelled_id)
        .unwrap();
    assert_eq!(inspect(open(), domain.clone()).unwrap().active_work(), 1);
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        5120
    );
    go_send.send(()).unwrap();
    let cancelled_result = finish_copy(&shared, worker.join().unwrap()).unwrap();
    assert!(!cancelled_result.published && cancelled_result.error.is_some());
    assert_eq!(cancelled_result.copied_bytes, 0);

    let current = discovery.snapshot().unwrap();
    let business = copy_admission::guarded_admit_wip(
        &shared,
        &mut discovery,
        &current,
        &source,
        &stage,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let business_done = finish_copy(&shared, business.execute()).unwrap();
    assert!(business_done.published && business_done.error.is_none());
    assert_eq!(
        (business_done.copied_bytes, business_done.verified_bytes),
        (19, 19)
    );
    let mut acquire = stage.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "0".into());
    acquire.insert("nbytes".into(), "1024".into());
    let (view, files) = exchange(&socket, &acquire, None, true);
    assert_eq!(get(&view, "status"), "ok");
    assert_eq!(files.len(), 1);
    let mut actual = vec![0; 1024];
    files[0].read_exact_at(&mut actual, 0).unwrap();
    let mut expected = vec![0x42; 1024];
    expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(actual, expected);
    drop(files);
    let mut release = view;
    release.insert("op".into(), "ReleaseExternalView".into());
    assert_eq!(
        get(&exchange(&socket, &release, None, false).0, "status"),
        "ok"
    );

    dst_owner
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"verify\n")
        .unwrap();
    read_marker(&mut dst_output, "CROSS_VERIFIED");
    dst_owner
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"stop\n")
        .unwrap();
    assert!(dst_owner.wait().unwrap().success());
    src_owner
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"stop\n")
        .unwrap();
    assert!(src_owner.wait().unwrap().success());
    let snapshot_req = uf_core::kv(&[("op", "GetExternalDdrSnapshot".into())]);
    let (final_snapshot, _) = exchange(&socket, &snapshot_req, None, false);
    assert_eq!(get(&final_snapshot, "committed_bytes"), "0");
    let replay = inspect(open(), domain).unwrap();
    assert_eq!(replay.charged_bytes(), 0);
    assert_eq!(replay.active_work(), 0);
    assert!(replay.unresolved_holders().is_empty());
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
}

#[test]
fn child_exits_without_revoking_fixture_backing() {
    let Ok(socket) = std::env::var("UF_DDR_PEER_EXIT_FIXTURE_SOCKET") else {
        return;
    };
    let owner_file = memfd();
    let _descriptor = register(&socket, &owner_file);
    println!("PEER_REGISTERED");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "exit");
    drop(owner_file); // natural process exit, deliberately no broker revoke
}

#[test]
fn fixture_peer_exit_does_not_become_a_normal_exit_witness() {
    use crate::ddr_recovery_audit::Domain;
    use crate::ddr_recovery_durable::{inspect, DurableAudit};
    use crate::ddr_recovery_journal::AnchoredJournal;

    let (root, socket, shared, broker) = fixture();
    let journal_path = root.join("exit-audit-journal");
    let head_path = root.join("exit-audit-head");
    let journal = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&journal_path)
        .unwrap();
    let mut head = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&head_path)
        .unwrap();
    head.write_all(AnchoredJournal::genesis_state().as_bytes())
        .unwrap();
    journal.sync_all().unwrap();
    head.sync_all().unwrap();
    File::open(&root).unwrap().sync_all().unwrap();
    let j = journal.metadata().unwrap();
    let h = head.metadata().unwrap();
    let jid = (j.dev(), j.ino());
    let hid = (h.dev(), h.ino());
    drop(journal);
    drop(head);
    let domain = Domain {
        host: "fixture-host".into(),
        boot: "fixture-boot".into(),
        domain: "external-ddr-v0".into(),
        cap_revision: 1,
        broker: "fixture-broker".into(),
        owner_limit: 8192,
        total_limit: 8192,
    };
    let open = || AnchoredJournal::open(&journal_path, &head_path, jid, hid).unwrap();
    shared.0.lock().unwrap().external_ddr.fixture_audit =
        Some(DurableAudit::start(open(), domain.clone()).unwrap());
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "external_ddr::managed_tests::child_exits_without_revoking_fixture_backing",
            "--nocapture",
        ])
        .env("UF_DDR_PEER_EXIT_FIXTURE_SOCKET", &socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id() as i32;
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    loop {
        let mut line = String::new();
        assert_ne!(
            stdout.read_line(&mut line).unwrap(),
            0,
            "child exited before registration"
        );
        if line.contains("PEER_REGISTERED") {
            break;
        }
    }
    let birth = {
        let st = shared.0.lock().unwrap();
        let client = st.external_ddr.current_pid[&pid];
        st.external_ddr.sessions[&client].birth.clone()
    };
    child.stdin.as_mut().unwrap().write_all(b"exit\n").unwrap();
    // This direct parent observed exit 0, but it is not a pinned trusted
    // witness: the broker's pidfd record must not turn it into proof.
    assert!(child.wait().unwrap().success());
    let snapshot = uf_core::kv(&[("op", "GetExternalDdrSnapshot".into())]);
    let (reply, _) = exchange(&socket, &snapshot, None, false);
    assert_eq!(get(&reply, "status"), "error");
    {
        let mut st = shared.0.lock().unwrap();
        assert_eq!(st.external_ddr.committed_bytes(), 0);
        assert!(st.external_ddr.commitments_unknown());
        assert!(st.external_ddr.before_admission().is_err());
    }
    let replay = inspect(open(), domain).unwrap();
    assert_eq!(replay.charged_bytes(), 0);
    assert_eq!(replay.unresolved_holders(), vec![birth]);
    assert_eq!(replay.observed_drain(0, 0).potential_holders, 1);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "dirty\n");
}

#[test]
fn t1_registration_and_clean_drain_use_the_fixture_authority() {
    let (root, socket, shared, broker) = fixture();
    let backing = memfd();
    let descriptor = register(&socket, &backing);
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        4096
    );
    assert!(!ManagedAuthority::open_fixture(&root, FIXTURE_ENDPOINT).is_ok());
    let mut revoke = descriptor;
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    let (reply, _) = exchange(&socket, &revoke, None, false);
    assert_eq!(get(&reply, "status"), "ok", "{reply:?}");
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
    drop(shared);
    let mut authority =
        ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    authority.before_admission().unwrap();
    authority
        .clean_after_drain(&DrainedCommitments {
            accepting_stopped: true,
            ..Default::default()
        })
        .unwrap();
    assert!(!Path::new(&format!("{}.external-ddr", socket)).exists());
}

#[test]
fn reused_pid_with_different_birth_cannot_inherit_a_session() {
    let (_root, socket, shared, broker) = fixture();
    let backing = memfd();
    let descriptor = register(&socket, &backing);
    let pid = std::process::id() as i32;
    let original = {
        let mut catalog = shared.0.lock().unwrap();
        let external = &mut catalog.external_ddr;
        let client_id = external.current_pid[&pid];
        let birth = &mut external.sessions.get_mut(&client_id).unwrap().birth;
        let original = birth.starttime_ticks;
        // Synthetic mismatch, not a real PID reuse or witness recovery proof.
        birth.starttime_ticks = original.checked_add(1).unwrap();
        original
    };
    let mut acquire = descriptor.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let (reply, files) = exchange(&socket, &acquire, None, true);
    assert_eq!(get(&reply, "status"), "error", "{reply:?}");
    assert!(files.is_empty());
    {
        let mut catalog = shared.0.lock().unwrap();
        let external = &mut catalog.external_ddr;
        assert_eq!(external.committed_bytes(), 4096);
        assert!(external.views.is_empty());
        let client_id = external.current_pid[&pid];
        external
            .sessions
            .get_mut(&client_id)
            .unwrap()
            .birth
            .starttime_ticks = original;
    }
    let mut revoke = descriptor;
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    stop(&shared, broker);
}

#[test]
fn reused_pid_cannot_release_the_original_view_or_owner_lease() {
    let (_root, socket, shared, broker) = fixture();
    let backing = memfd();
    let descriptor = register(&socket, &backing);
    let mut acquire = descriptor.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let (view, files) = exchange(&socket, &acquire, None, true);
    assert_eq!(get(&view, "status"), "ok");
    let mut release = view.clone();
    release.insert("op".into(), "ReleaseExternalView".into());
    let mut revoke = descriptor.clone();
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    let pid = std::process::id() as i32;
    for mismatch in 0..3 {
        let original = {
            let mut catalog = shared.0.lock().unwrap();
            let external = &mut catalog.external_ddr;
            let client_id = external.current_pid[&pid];
            let birth = &mut external.sessions.get_mut(&client_id).unwrap().birth;
            let original = birth.clone();
            match mismatch {
                0 => birth.starttime_ticks += 1,
                1 => birth.pid_namespace_ino += 1,
                _ => birth.pid_namespace_dev += 1,
            }
            original
        };
        assert_eq!(
            get(&exchange(&socket, &release, None, false).0, "status"),
            "error"
        );
        assert_eq!(
            get(&exchange(&socket, &revoke, None, false).0, "status"),
            "error"
        );
        let mut catalog = shared.0.lock().unwrap();
        let external = &mut catalog.external_ddr;
        assert_eq!(external.committed_bytes(), 4096);
        let view_id = number(&view, "view_id").unwrap();
        assert!(!external.views[&view_id].released);
        assert!(external.owns_lease(number(&view, "lease_id").unwrap()));
        let client_id = external.current_pid[&pid];
        external.sessions.get_mut(&client_id).unwrap().birth = original;
    }
    let mut wrong_boot = release.clone();
    wrong_boot.insert("boot_id".into(), "another-boot".into());
    assert_eq!(
        get(&exchange(&socket, &wrong_boot, None, false).0, "status"),
        "error"
    );
    {
        let catalog = shared.0.lock().unwrap();
        let external = &catalog.external_ddr;
        assert_eq!(external.committed_bytes(), 4096);
        assert!(!external.views[&number(&view, "view_id").unwrap()].released);
        assert!(external.owns_lease(number(&view, "lease_id").unwrap()));
    }
    drop(files);
    assert_eq!(
        get(&exchange(&socket, &release, None, false).0, "status"),
        "ok"
    );
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    stop(&shared, broker);
}

#[test]
fn outstanding_view_survives_shutdown_but_dirty_restart_refuses_admission() {
    let (root, socket, shared, broker) = fixture();
    let backing = memfd();
    let descriptor = register(&socket, &backing);
    let mut acquire = descriptor;
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let (reply, mut files) = exchange(&socket, &acquire, None, true);
    assert_eq!(get(&reply, "status"), "ok", "{reply:?}");
    assert_eq!(files.len(), 1);
    let view = files.pop().unwrap();
    assert_eq!(
        unsafe { libc::fcntl(view.as_raw_fd(), libc::F_GETFL) } & libc::O_ACCMODE,
        libc::O_RDONLY
    );
    let mapped = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            4096,
            libc::PROT_READ,
            libc::MAP_SHARED,
            view.as_raw_fd(),
            0,
        )
    };
    assert_ne!(mapped, libc::MAP_FAILED);
    stop(&shared, broker);
    drop(shared);
    let mut quarantined =
        ManagedAuthority::open_fixture_after_release(&root, FIXTURE_ENDPOINT).unwrap();
    assert!(quarantined.before_admission().is_err());
    assert!(quarantined
        .clean_after_drain(&DrainedCommitments {
            accepting_stopped: true,
            ..Default::default()
        })
        .is_err());
    let bytes = unsafe { std::slice::from_raw_parts((mapped as *const u8).add(17), 19) };
    assert_eq!(bytes, b"existing-owner-data");
    assert_eq!(unsafe { libc::munmap(mapped, 4096) }, 0);
    assert!(!Path::new(&format!("{}.external-ddr", socket)).exists());
}

#[test]
fn replaced_lock_stops_admission_but_preserves_release_and_existing_view() {
    let (root, socket, shared, broker) = fixture();
    let backing = memfd();
    let descriptor = register(&socket, &backing);
    let mut acquire = descriptor.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "17".into());
    acquire.insert("nbytes".into(), "19".into());
    let (view, mut files) = exchange(&socket, &acquire, None, true);
    assert_eq!(get(&view, "status"), "ok");
    fs::rename(root.join("authority.lock"), root.join("replaced.lock")).unwrap();
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(root.join("authority.lock"))
        .unwrap();
    let newcomer = memfd();
    let request = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "newcomer".into()),
        ("owner_name", "newcomer".into()),
        ("size_bytes", "4096".into()),
    ]);
    assert_eq!(
        get(
            &exchange(&socket, &request, Some(&newcomer), true).0,
            "status"
        ),
        "error"
    );
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        4096
    );
    let view_fd = files.pop().unwrap();
    let mut bytes = [0u8; 19];
    view_fd.read_exact_at(&mut bytes, 17).unwrap();
    assert_eq!(&bytes, b"existing-owner-data");
    drop(view_fd);
    let mut release = view;
    release.insert("op".into(), "ReleaseExternalView".into());
    assert_eq!(
        get(&exchange(&socket, &release, None, false).0, "status"),
        "ok"
    );
    let mut revoke = descriptor;
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
    drop(shared);
    assert!(ManagedAuthority::open_fixture(&root, FIXTURE_ENDPOINT).is_err());
}

#[test]
fn staged_targets_and_probe_remain_pending_and_share_the_t1_budget() {
    use uf_core::topology::ddr::{DdrDiscovery, DiscoveryConfig, Operation, Support};
    let (_root, socket, shared, broker) = fixture();
    let request = |op: &str, size: u64| {
        uf_core::kv(&[
            ("op", op.to_string()),
            ("name", "pending-target".into()),
            ("owner_name", "fixture-owner".into()),
            ("size_bytes", size.to_string()),
        ])
    };
    let target = memfd_sized(1024);
    let no_anchor = exchange(
        &socket,
        &request("RegisterExternalStaging", 1024),
        Some(&target),
        true,
    )
    .0;
    assert_eq!(get(&no_anchor, "status"), "error");
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    let anchor_file = memfd();
    let anchor = register(&socket, &anchor_file);
    let stage = exchange(
        &socket,
        &request("RegisterExternalStaging", 1024),
        Some(&target),
        true,
    )
    .0;
    assert_eq!(get(&stage, "status"), "ok", "{stage:?}");
    assert_eq!(get(&stage, "purpose"), "staging");
    let probe_file = memfd_sized(1024);
    let probe = exchange(
        &socket,
        &request("RegisterExternalProbe", 1024),
        Some(&probe_file),
        true,
    )
    .0;
    assert_eq!(get(&probe, "status"), "ok", "{probe:?}");
    assert_eq!(get(&probe, "purpose"), "probe_only");
    let extra = memfd();
    let over = exchange(
        &socket,
        &request("RegisterExternalStaging", 4096),
        Some(&extra),
        true,
    )
    .0;
    assert_eq!(get(&over, "status"), "error");
    let readonly = File::open(format!("/proc/self/fd/{}", extra.as_raw_fd())).unwrap();
    let bad_access = exchange(
        &socket,
        &request("RegisterExternalProbe", 4096),
        Some(&readonly),
        true,
    )
    .0;
    assert_eq!(get(&bad_access, "status"), "error");
    let mut acquire = stage.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "0".into());
    acquire.insert("nbytes".into(), "1".into());
    let (reply, files) = exchange(&socket, &acquire, None, true);
    assert_eq!(get(&reply, "status"), "error");
    assert!(files.is_empty());
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        6144
    );
    let mut discovery = DdrDiscovery::new(DiscoveryConfig {
        realm: "fixture".into(),
        control_socket: PathBuf::from(&socket),
        max_age: Duration::from_secs(3),
        host_budget_limit_bytes: 8192,
    })
    .unwrap();
    let snapshot = discovery.snapshot().unwrap();
    assert_eq!(snapshot.committed_bytes, 6144);
    assert_eq!(snapshot.pending_handoffs, 0);
    let stage_id: u64 = get(&stage, "object_id").parse().unwrap();
    let anchor_id: u64 = get(&anchor, "object_id").parse().unwrap();
    let staged = snapshot
        .backings
        .iter()
        .find(|b| b.identity.object_id == stage_id)
        .unwrap();
    assert_eq!(staged.state, "pending");
    assert!(discovery.guard(&snapshot, &staged.identity, 0, 1).is_err());
    let source = snapshot
        .backings
        .iter()
        .find(|b| b.identity.object_id == anchor_id)
        .unwrap();
    assert!(discovery.guard(&snapshot, &source.identity, 0, 1).is_ok());
    assert!(snapshot
        .capabilities
        .iter()
        .filter(|c| c.operation == Operation::Copy)
        .all(|c| c.support == Support::Unsupported));
    for mut handle in [stage, probe, anchor] {
        handle.insert("op".into(), "RevokeExternalBacking".into());
        assert_eq!(
            get(&exchange(&socket, &handle, None, false).0, "status"),
            "ok"
        );
    }
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
}

#[test]
fn private_c_guard_then_catalog_compare_rejects_stale_and_allows_real_copy() {
    use uf_core::topology::ddr::{DdrDiscovery, DiscoveryConfig};
    let (_root, socket, shared, broker) = fixture();
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd_sized(1024);
    let before = vec![0x3c; 1024];
    target_file.write_all_at(&before, 0).unwrap();
    let target = register_pending(&socket, &target_file, "RegisterExternalStaging", 1024);
    let mut discovery = DdrDiscovery::new(DiscoveryConfig {
        realm: "fixture".into(),
        control_socket: PathBuf::from(&socket),
        max_age: Duration::from_secs(3),
        host_budget_limit_bytes: 8192,
    })
    .unwrap();
    let old = discovery.snapshot().unwrap();
    let extra_file = memfd_sized(1024);
    let request = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "epoch-change".into()),
        ("owner_name", "fixture-owner".into()),
        ("size_bytes", "1024".into()),
    ]);
    let (extra, _) = exchange(&socket, &request, Some(&extra_file), true);
    assert_eq!(get(&extra, "status"), "ok");
    assert!(copy_admission::guarded_admit_wip(
        &shared,
        &mut discovery,
        &old,
        &source,
        &target,
        17,
        37,
        19,
        BackingPurpose::Staging
    )
    .is_err());
    assert_eq!(shared.0.lock().unwrap().external_ddr.copy_work.len(), 0);
    let mut bytes = vec![0; 1024];
    target_file.read_exact_at(&mut bytes, 0).unwrap();
    assert_eq!(bytes, before);
    let mut revoke_extra = extra;
    revoke_extra.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke_extra, None, false).0, "status"),
        "ok"
    );
    let current = discovery.snapshot().unwrap();
    let ticket = copy_admission::guarded_admit_wip(
        &shared,
        &mut discovery,
        &current,
        &source,
        &target,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let drained = ticket.execute();
    assert!(finish_copy(&shared, drained).unwrap().published);
    target_file.read_exact_at(&mut bytes, 0).unwrap();
    let mut expected = before;
    expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(bytes, expected);
    for mut handle in [target, source] {
        handle.insert("op".into(), "RevokeExternalBacking".into());
        assert_eq!(
            get(&exchange(&socket, &handle, None, false).0, "status"),
            "ok"
        );
    }
    stop(&shared, broker);
}

#[test]
fn private_copy_admission_rejects_stale_then_seals_real_nonzero_copy() {
    let (_root, socket, shared, broker) = fixture();
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd_sized(1024);
    let before = vec![0x5a; 1024];
    target_file.write_all_at(&before, 0).unwrap();
    let target = register_pending(&socket, &target_file, "RegisterExternalStaging", 1024);
    let epoch = shared.0.lock().unwrap().external_ddr.epoch;
    assert!(admit_copy(
        &shared,
        &source,
        &target,
        epoch - 1,
        17,
        37,
        19,
        BackingPurpose::Staging
    )
    .is_err());
    let mut forged = target.clone();
    forged.insert("binding_id".into(), "999999".into());
    assert!(admit_copy(
        &shared,
        &source,
        &forged,
        epoch,
        17,
        37,
        19,
        BackingPurpose::Staging
    )
    .is_err());
    assert_eq!(shared.0.lock().unwrap().external_ddr.copy_work.len(), 0);
    let mut readback = vec![0; 1024];
    target_file.read_exact_at(&mut readback, 0).unwrap();
    assert_eq!(readback, before);
    let ticket = admit_copy(
        &shared,
        &source,
        &target,
        epoch,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    assert_eq!(shared.0.lock().unwrap().external_ddr.copy_work.len(), 1);
    let work_id = ticket.id();
    let source_fds_with_ticket = live_fds_for(&source_file);
    let target_fds_with_ticket = live_fds_for(&target_file);
    let drained = ticket.execute();
    assert_eq!(live_fds_for(&source_file) + 1, source_fds_with_ticket);
    assert_eq!(live_fds_for(&target_file) + 1, target_fds_with_ticket);
    // Closing worker FD clones does not itself release catalog retain/counts;
    // finish is a distinct operation after the worker has actually drained.
    assert_eq!(shared.0.lock().unwrap().external_ddr.copy_work.len(), 1);
    let finished = finish_copy(&shared, drained).unwrap();
    assert!(finished.published && !finished.probe_completed && finished.error.is_none());
    assert_eq!((finished.copied_bytes, finished.verified_bytes), (19, 19));
    target_file.read_exact_at(&mut readback, 0).unwrap();
    let mut expected = before;
    expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(readback, expected);
    let seals = unsafe { libc::fcntl(target_file.as_raw_fd(), libc::F_GET_SEALS) };
    assert_ne!(seals & libc::F_SEAL_WRITE, 0);
    assert!(target_file.write_all_at(b"x", 37).is_err());
    let repeated = shared.0.lock().unwrap().external_ddr.copy_results[&work_id].clone();
    assert!(repeated.published && repeated.error.is_none());
    assert_eq!((repeated.copied_bytes, repeated.verified_bytes), (19, 19));
    for mut handle in [target, source] {
        handle.insert("op".into(), "RevokeExternalBacking".into());
        assert_eq!(
            get(&exchange(&socket, &handle, None, false).0, "status"),
            "ok"
        );
    }
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
}

#[test]
fn private_cancel_and_revoke_keep_backings_until_real_worker_drains() {
    let (_root, socket, shared, broker) = fixture();
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd();
    let target = register_pending(&socket, &target_file, "RegisterExternalStaging", 4096);
    let epoch = shared.0.lock().unwrap().external_ddr.epoch;
    let ticket = admit_copy(
        &shared,
        &source,
        &target,
        epoch,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let id = ticket.id();
    let (ready_send, ready_recv) = std::sync::mpsc::channel();
    let (go_send, go_recv) = std::sync::mpsc::channel();
    let worker = thread::spawn(move || {
        ready_send.send(()).unwrap();
        go_recv.recv().unwrap();
        ticket.execute()
    });
    ready_recv.recv_timeout(Duration::from_secs(5)).unwrap();
    {
        let mut st = shared.0.lock().unwrap();
        st.external_ddr.cancel_copy_wip(id).unwrap();
        st.external_ddr.cancel_copy_wip(id).unwrap();
    }
    let mut revoke_source = source;
    revoke_source.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke_source, None, false).0, "status"),
        "ok"
    );
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        8192
    );
    let extra = memfd();
    let request = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "extra".into()),
        ("owner_name", "extra".into()),
        ("size_bytes", "4096".into()),
    ]);
    assert_eq!(
        get(&exchange(&socket, &request, Some(&extra), true).0, "status"),
        "error"
    );
    assert_eq!(shared.0.lock().unwrap().external_ddr.copy_work.len(), 1);
    go_send.send(()).unwrap();
    let execution = worker.join().unwrap();
    let finished = finish_copy(&shared, execution).unwrap();
    assert!(!finished.published && finished.error.is_some());
    assert_eq!(finished.copied_bytes, 0);
    let mut st = shared.0.lock().unwrap();
    st.external_ddr.cancel_copy_wip(id).unwrap();
    assert_eq!(st.external_ddr.committed_bytes(), 0);
    assert_eq!(st.external_ddr.copy_work.len(), 0);
    drop(st);
    stop(&shared, broker);
}

#[test]
fn private_probe_copies_but_never_publishes_or_becomes_a_business_target() {
    let (_root, socket, shared, broker) = fixture();
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let probe_file = memfd_sized(1024);
    probe_file.write_all_at(&vec![0x7b; 1024], 0).unwrap();
    let probe = register_pending(&socket, &probe_file, "RegisterExternalProbe", 1024);
    let epoch = shared.0.lock().unwrap().external_ddr.epoch;
    assert!(admit_copy(
        &shared,
        &source,
        &probe,
        epoch,
        17,
        37,
        19,
        BackingPurpose::Staging
    )
    .is_err());
    let ticket = admit_copy(
        &shared,
        &source,
        &probe,
        epoch,
        17,
        37,
        19,
        BackingPurpose::ProbeOnly,
    )
    .unwrap();
    let drained = ticket.execute();
    let completed = finish_copy(&shared, drained).unwrap();
    assert!(!completed.published && completed.probe_completed && completed.error.is_none());
    let mut bytes = vec![0; 1024];
    probe_file.read_exact_at(&mut bytes, 0).unwrap();
    let mut expected = vec![0x7b; 1024];
    expected[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(bytes, expected);
    let new_epoch = shared.0.lock().unwrap().external_ddr.epoch;
    assert!(admit_copy(
        &shared,
        &source,
        &probe,
        new_epoch,
        17,
        37,
        19,
        BackingPurpose::ProbeOnly
    )
    .is_err());
    let mut acquire = probe.clone();
    acquire.insert("op".into(), "AcquireExternalRead".into());
    acquire.insert("access_mode".into(), "read".into());
    acquire.insert("offset_bytes".into(), "0".into());
    acquire.insert("nbytes".into(), "1".into());
    assert_eq!(
        get(&exchange(&socket, &acquire, None, true).0, "status"),
        "error"
    );
    for mut handle in [probe, source] {
        handle.insert("op".into(), "RevokeExternalBacking".into());
        assert_eq!(
            get(&exchange(&socket, &handle, None, false).0, "status"),
            "ok"
        );
    }
    stop(&shared, broker);
}

#[test]
fn private_seal_failure_never_publishes_target() {
    let (_root, socket, shared, broker) = fixture();
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd_sized(1024);
    let mapping = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            1024,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            target_file.as_raw_fd(),
            0,
        )
    };
    assert_ne!(mapping, libc::MAP_FAILED);
    let target = register_pending(&socket, &target_file, "RegisterExternalStaging", 1024);
    let epoch = shared.0.lock().unwrap().external_ddr.epoch;
    let ticket = admit_copy(
        &shared,
        &source,
        &target,
        epoch,
        17,
        37,
        19,
        BackingPurpose::Staging,
    )
    .unwrap();
    let drained = ticket.execute();
    let completed = finish_copy(&shared, drained).unwrap();
    assert!(
        !completed.published
            && completed
                .error
                .as_deref()
                .is_some_and(|e| e.contains("seal failed"))
    );
    assert_eq!(unsafe { libc::munmap(mapping, 1024) }, 0);
    let target_id: u64 = get(&target, "object_id").parse().unwrap();
    let st = shared.0.lock().unwrap();
    let row = &st.external_ddr.backings[&target_id];
    assert_eq!(row.state, "registration_rollback");
    assert!(row.file.is_none());
    assert_ne!(st.data_objects[&target_id].state, "Ready");
    drop(st);
    let mut revoke = source;
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    stop(&shared, broker);
}

#[test]
fn child_competing_registration() {
    let Ok(socket) = std::env::var("UF_DDR_FIXTURE_SOCKET") else {
        return;
    };
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "go");
    let backing = memfd();
    let request = uf_core::kv(&[
        ("op", "RegisterExternalBacking".into()),
        ("name", "competitor".into()),
        ("owner_name", "competitor".into()),
        ("size_bytes", "4096".into()),
    ]);
    let (reply, _) = exchange(&socket, &request, Some(&backing), true);
    println!("FIXTURE_OUTCOME={}", get(&reply, "status"));
    std::io::stdout().flush().unwrap();
    line.clear();
    std::io::stdin().read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "release");
    if get(&reply, "status") == "ok" {
        let mut revoke = reply;
        revoke.insert("op".into(), "RevokeExternalBacking".into());
        let (result, _) = exchange(&socket, &revoke, None, false);
        assert_eq!(get(&result, "status"), "ok", "{result:?}");
    }
}

#[test]
fn independent_owner_processes_compete_for_one_catalog_budget() {
    let (_root, socket, shared, broker) = fixture();
    let parent_backing = memfd();
    let parent_descriptor = register(&socket, &parent_backing);
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
    assert_ne!(first.id(), second.id());
    first.stdin.as_mut().unwrap().write_all(b"go\n").unwrap();
    second.stdin.as_mut().unwrap().write_all(b"go\n").unwrap();
    fn outcome(reader: &mut impl BufRead) -> String {
        loop {
            let mut line = String::new();
            assert_ne!(
                reader.read_line(&mut line).unwrap(),
                0,
                "child exited before outcome"
            );
            if let Some((_, value)) = line.split_once("FIXTURE_OUTCOME=") {
                return value.trim().to_owned();
            }
        }
    }
    let mut out1 = BufReader::new(first.stdout.take().unwrap());
    let mut out2 = BufReader::new(second.stdout.take().unwrap());
    let outcomes = [outcome(&mut out1), outcome(&mut out2)];
    assert_eq!(
        outcomes.iter().filter(|value| *value == "ok").count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(
        outcomes.iter().filter(|value| *value == "error").count(),
        1,
        "{outcomes:?}"
    );
    {
        let st = shared.0.lock().unwrap();
        assert_eq!(st.external_ddr.committed_bytes(), 8192);
        assert_eq!(
            st.external_ddr.sessions.len(),
            2,
            "failed registration retained a session"
        );
    }
    first
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"release\n")
        .unwrap();
    second
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"release\n")
        .unwrap();
    assert!(first.wait().unwrap().success());
    assert!(second.wait().unwrap().success());
    let mut revoke = parent_descriptor;
    revoke.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(
        get(&exchange(&socket, &revoke, None, false).0, "status"),
        "ok"
    );
    assert_eq!(shared.0.lock().unwrap().external_ddr.committed_bytes(), 0);
    stop(&shared, broker);
}
