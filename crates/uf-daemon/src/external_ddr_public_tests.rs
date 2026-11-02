//! Public authenticated wire regressions, with isolated authority fixtures.
use super::*;
use crate::catalog::TransferEventRecord;
use crate::external_ddr::{capability, transfer};

fn setup() -> (PathBuf, String, SharedCatalog, Broker, Kv, File, Kv, File) {
    let (root, socket, shared, broker) = fixture();
    {
        let mut st = shared.0.lock().unwrap();
        let authority = st.external_ddr.authority.take().unwrap();
        st.external_ddr.attach_lifecycle(authority).unwrap();
        st.external_ddr.fixture_clean_checkpoint = true;
    }
    let source_file = memfd();
    let source = register(&socket, &source_file);
    let target_file = memfd_sized(1024);
    let target = register_pending(&socket, &target_file, "RegisterExternalStaging", 1024);
    (
        root,
        socket,
        shared,
        broker,
        source,
        source_file,
        target,
        target_file,
    )
}

fn request(source: &Kv, target: &Kv, op: &str) -> Kv {
    uf_core::kv(&[
        ("op", op.into()),
        ("operation", "copy".into()),
        ("mode", "external_copy".into()),
        ("source_json", serde_json::to_string(source).unwrap()),
        ("target_json", serde_json::to_string(target).unwrap()),
        ("src_offset_bytes", "17".into()),
        ("dst_offset_bytes", "37".into()),
        ("nbytes", "19".into()),
    ])
}

fn wire(socket: &str, fields: &Kv) -> Kv {
    let (reply, files) = exchange(socket, fields, None, false);
    assert!(files.is_empty());
    reply
}

fn revoke(socket: &str, mut fields: Kv) {
    fields.insert("op".into(), "RevokeExternalBacking".into());
    assert_eq!(get(&wire(socket, &fields), "status"), "ok");
}

#[test]
fn public_plan_submit_freezes_tuple_tokens_and_rejects_stale_without_work() {
    let (root, socket, shared, broker, source, file, target, target_file) = setup();
    // Cooperative T1 registration alone does not grant immutable copy rights.
    assert_eq!(
        get(
            &wire(&socket, &request(&source, &target, "PlanTransfer")),
            "status"
        ),
        "error"
    );
    assert_eq!(
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_WRITE) },
        0
    );
    let estimate = wire(&socket, &request(&source, &target, "EstimateCost"));
    assert_eq!(get(&estimate, "cost_version"), "unknown");
    assert_eq!(get(&estimate, "automatic_ranking"), "disabled");
    let old = wire(&socket, &request(&source, &target, "PlanTransfer"));
    assert_eq!(get(&old, "status"), "ok", "{old:?}");
    // A version change after planning must never rebase the old receipt.
    shared.0.lock().unwrap().external_ddr.epoch += 1;
    let before = shared.0.lock().unwrap().external_ddr.next_copy_work_id;
    let mut submit = uf_core::kv(&[
        ("op", "SubmitTransfer".into()),
        ("plan_id", get(&old, "plan_id").into()),
        ("plan_token", get(&old, "plan_token").into()),
    ]);
    assert_eq!(get(&wire(&socket, &submit), "status"), "error");
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.next_copy_work_id,
        before
    );
    let mut unchanged = vec![0; 1024];
    target_file.read_exact_at(&mut unchanged, 0).unwrap();
    assert_eq!(&unchanged[17..36], b"existing-owner-data");
    assert_eq!(&unchanged[37..56], &[0; 19]);
    let plan = wire(&socket, &request(&source, &target, "PlanTransfer"));
    assert_eq!(get(&plan, "status"), "ok", "{plan:?}");
    submit.insert("plan_id".into(), get(&plan, "plan_id").into());
    // Same numeric ID, stale token, same authenticated peer: still refused.
    submit.insert("plan_token".into(), get(&old, "plan_token").into());
    assert_eq!(get(&wire(&socket, &submit), "status"), "error");
    submit.insert("plan_token".into(), get(&plan, "plan_token").into());
    // The legacy control router cannot bypass authenticated admission.
    assert_eq!(
        get(
            &crate::router::handle_request(submit.clone(), &shared),
            "status"
        ),
        "error"
    );
    let submitted = wire(&socket, &submit);
    assert_eq!(get(&submitted, "status"), "ok", "{submitted:?}");
    assert_eq!(get(&wire(&socket, &submit), "status"), "error");
    let event = uf_core::kv(&[
        ("op", "WaitEvent".into()),
        ("event_id", get(&submitted, "event_id").into()),
        ("event_token", get(&submitted, "event_token").into()),
        ("timeout_ms", "4000".into()),
    ]);
    let done = wire(&socket, &event);
    assert_eq!(get(&done, "event_status"), "complete", "{done:?}");
    assert_eq!(get(&done, "copied_bytes"), "19");
    assert_eq!(get(&done, "drained"), "true");
    let mut actual = vec![0; 1024];
    target_file.read_exact_at(&mut actual, 0).unwrap();
    unchanged[37..56].copy_from_slice(b"existing-owner-data");
    assert_eq!(actual, unchanged);
    revoke(&socket, target);
    revoke(&socket, source);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}

#[test]
fn public_cancel_and_wait_do_not_release_before_actual_worker_join() {
    let (root, socket, shared, broker, source, file, target, _target_file) = setup();
    assert_eq!(
        unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, libc::F_SEAL_WRITE) },
        0
    );
    let observer = shared
        .0
        .lock()
        .unwrap()
        .external_ddr
        .discovery
        .clone()
        .unwrap();
    let mut observer = observer.lock().unwrap();
    let snapshot = observer.snapshot().unwrap();
    let peer = Peer {
        pid: std::process::id() as i32,
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    };
    let req = capability::CopyRequest {
        source: source.clone(),
        target: target.clone(),
        source_offset: 17,
        target_offset: 37,
        bytes: 19,
    };
    let evidence = capability::probe(&shared, &mut observer, &snapshot, peer, &req).unwrap();
    let snapshot = observer.snapshot().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let work = capability::admit_with_hook(
        &shared,
        &mut observer,
        &snapshot,
        peer,
        &req,
        &evidence,
        move || recv.recv().unwrap(),
    )
    .unwrap();
    drop(observer);
    let id = {
        let mut st = shared.0.lock().unwrap();
        let id = st.take_next_transfer_event_id();
        let client_id = st.external_ddr.current_pid[&peer.pid];
        st.transfer_events.insert(
            id,
            TransferEventRecord {
                event_id: id,
                client_id,
                status: "running".into(),
                external: Some(transfer::ExternalEvent {
                    token: "current-event".into(),
                    work_id: Some(work),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        id
    };
    let mut event = uf_core::kv(&[
        ("op", "CancelEvent".into()),
        ("event_id", id.to_string()),
        ("event_token", "old-event".into()),
    ]);
    assert_eq!(get(&wire(&socket, &event), "status"), "error");
    assert_eq!(
        get(
            &crate::router::handle_request(event.clone(), &shared),
            "status"
        ),
        "error"
    );
    event.insert("event_token".into(), "current-event".into());
    let cancel = wire(&socket, &event);
    assert_eq!(get(&cancel, "cancel_requested"), "true");
    assert_eq!(get(&cancel, "drained"), "false");
    event.insert("op".into(), "WaitEvent".into());
    event.insert("timeout_ms".into(), "1".into());
    assert_eq!(get(&wire(&socket, &event), "status"), "error");
    {
        let st = shared.0.lock().unwrap();
        assert_eq!(st.external_ddr.committed_bytes(), 5120);
        assert_eq!(
            st.external_ddr.backings[&number(&source, "object_id").unwrap()].in_flight,
            1
        );
        assert_eq!(
            st.objects[&number(&target, "object_id").unwrap()].state,
            "Creating"
        );
    }
    send.send(()).unwrap();
    event.insert("timeout_ms".into(), "4000".into());
    let done = wire(&socket, &event);
    assert_eq!(get(&done, "event_status"), "cancelled", "{done:?}");
    assert_eq!(get(&done, "drained"), "true");
    assert_eq!(
        shared.0.lock().unwrap().external_ddr.committed_bytes(),
        4096
    );
    revoke(&socket, target);
    revoke(&socket, source);
    stop(&shared, broker);
    assert_eq!(fs::read_to_string(root.join("state")).unwrap(), "clean\n");
}
