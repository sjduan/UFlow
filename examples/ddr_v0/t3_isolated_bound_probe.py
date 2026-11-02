#!/usr/bin/env python3
"""Real daemon BIND gate probe in a private mount namespace, not acceptance.

By default the BIND installer is a cfg(test) helper. With --trusted the
installer and witness use their real fixed-root deployment paths. Both modes
remain in quarantine and prove neither admission, SEAL installation nor recovery.
"""
from __future__ import annotations

import argparse
import array
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import select
import shutil
import socket
import subprocess
import sys
import time

BASE = Path("/workspace/uflow/runs")
RUN = BASE / "isolated-bound"
ROOT = Path("/workspace/uflow/authority/external-ddr-v0")
FILES = ("bootstrap", "authority.lock", "state", "recovery.journal", "recovery.head", "witness.trust")


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")


def identity(path: Path) -> tuple[int, int]:
    meta = path.stat(follow_symlinks=False)
    return meta.st_dev, meta.st_ino


def inner(nsroot: Path, fd: int, parent_mnt: int, hashes: dict[str, str], trusted: bool) -> None:
    if nsroot.resolve(strict=True) != nsroot or not nsroot.is_relative_to(BASE):
        raise RuntimeError("source is not a canonical task-owned directory")
    expected = os.fstat(fd)
    if identity(nsroot) != (expected.st_dev, expected.st_ino):
        raise RuntimeError("source identity changed")
    actual_parent_mnt = os.stat(f"/proc/{os.getppid()}/ns/mnt").st_ino
    if actual_parent_mnt != parent_mnt or os.stat("/proc/self/ns/mnt").st_ino == parent_mnt:
        raise RuntimeError("mount namespace was not isolated")
    mountinfo = Path("/proc/self/mountinfo").read_text()
    if any(field.startswith("shared:") for line in mountinfo.splitlines()
           for field in line.split(" - ", 1)[0].split()[6:]):
        raise RuntimeError("mount propagation was not private")
    authority_parent = os.stat("authority", dir_fd=fd, follow_symlinks=False)
    subprocess.run(["mount", "--bind", str(nsroot), "/workspace/uflow"], check=True)
    if identity(Path("/workspace/uflow")) != (expected.st_dev, expected.st_ino) \
            or identity(ROOT.parent) != (authority_parent.st_dev, authority_parent.st_ino) or ROOT.exists():
        raise RuntimeError("fixed authority is not a fresh task-owned mount")
    binary = RUN / "uf-daemon"
    binder = RUN / "test-binder"
    witness = RUN / "t3_deployment_witness.py"
    for path in ((binary, witness) if trusted else (binary, binder, witness)):
        if digest(path) != hashes[path.name]:
            raise RuntimeError("probe artifact differs from tested source/build")
    python = os.path.realpath(sys.executable)
    endpoint = str(RUN / "a.sock")
    evidence = RUN / "evidence-g1"
    alias = RUN / "authority-alias"
    library = f"{RUN}/shim:/usr/local/Ascend/cann-9.0.0/lib64"
    supervisor_env = {"LD_LIBRARY_PATH": library}
    daemon_env = {"LD_LIBRARY_PATH": library, "UF_EXTERNAL_DDR_HOST_ID": "host",
                  "UF_EXTERNAL_DDR_OWNER_MAX_BYTES": "16", "UF_EXTERNAL_DDR_TOTAL_MAX_BYTES": "64"}
    if trusted:
        supervisor_env = daemon_env
    py_artifact = {"path": python, "sha256": digest(Path(python))}
    script_artifact = {"path": str(witness), "sha256": digest(witness)}
    binary_artifact = {"path": str(binary), "sha256": digest(binary)}
    supervisor = {"role": "supervisor", "executable": py_artifact, "entrypoint": script_artifact,
                  "argv": [python, str(witness), "_production_bindcase", str(alias),
                           "{uflow:evidence_dir}", str(binder), str(binary),
                           "{uflow:endpoint}", "{uflow:generation}"], "environment": supervisor_env}
    if trusted:
        supervisor["argv"] = [python, str(witness), "--trusted-generation", str(binary),
                              "{uflow:endpoint}", "{uflow:evidence_dir}",
                              "{uflow:generation}", "{uflow:broker}"]
    broker = {"role": "broker", "executable": binary_artifact, "entrypoint": binary_artifact,
              "argv": [str(binary), "--socket", "{uflow:endpoint}", "--execution-mode", "simulated",
                       "--startup-probe-bytes", "0", "--bound-external-ddr-gate"], "environment": daemon_env}
    other = [{"role": role, "executable": binary_artifact, "entrypoint": binary_artifact,
              "argv": [str(binary)], "environment": {}}
             for role in ("launcher", "interpreter", "owner", "accessor")]
    policy_path = RUN / "policy.json"
    policy_path.write_bytes(canonical({"schema": "uflow-ddr-deployment-policy-v1",
                                       "roles": [supervisor, broker, *other]}) + b"\n")
    bootstrap = subprocess.run([str(binary), "--socket", endpoint, "--initialize-external-ddr-authority",
                                "--external-ddr-witness-policy", str(policy_path)], env=daemon_env,
                               stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20)
    (RUN / "bootstrap.log").write_bytes(bootstrap.stdout)
    if bootstrap.returncode != 0:
        raise RuntimeError(f"isolated bootstrap failed: {bootstrap.returncode}")
    if trusted:
        # A real foreign parent creates a valid-looking witness stream and
        # launches a real gated child. Its argv is not the pre-pinned trusted
        # supervisor, so the production binder must reject before any write.
        before_untrusted = {name: digest(ROOT / name) for name in FILES}
        untrusted_dir = RUN / "evidence-untrusted"
        untrusted_dir.mkdir(mode=0o700)
        spec = importlib.util.spec_from_file_location("probe_witness", witness)
        if spec is None or spec.loader is None:
            raise RuntimeError("cannot load witness fixture")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        outsider = module.Witness(untrusted_dir, "untrusted-g", "ready-broker")
        deploy_parent, deploy_child = socket.socketpair()
        deploy_parent.settimeout(15)
        child_parent, child_socket = socket.socketpair()
        child_parent.settimeout(15)
        outsider_pid = None
        untrusted_log = RUN / "untrusted-binder.log"
        def exact(channel: socket.socket, expected_message: bytes) -> None:
            data = bytearray()
            while len(data) < len(expected_message):
                part = channel.recv(len(expected_message) - len(data))
                if not part:
                    raise RuntimeError("untrusted test channel closed early")
                data.extend(part)
            if bytes(data) != expected_message:
                raise RuntimeError("untrusted test channel sent unexpected bytes")
        try:
            with untrusted_log.open("xb") as log:
                rejected_binder = subprocess.Popen(
                    [str(binary), "--socket", endpoint, "--begin-external-ddr-generation"],
                    env=daemon_env, stdin=deploy_child.fileno(), stdout=log, stderr=log, close_fds=True)
                deploy_child.close()
                exact(deploy_parent, b"UFLOW-READY-1\n")
                exact(deploy_parent, b"UFLOW-BINDER-LOCKED\n")
                outsider_command = [str(binary), "--socket", endpoint, "--execution-mode", "simulated",
                                    "--startup-probe-bytes", "0", "--bound-external-ddr-gate"]
                outsider_pid = outsider.spawn_gated("broker", outsider_command, daemon_env,
                                                    control_fd=child_socket.fileno())
                child_socket.close()
                outsider.release_gate(outsider_pid)
                exact(child_parent, b"UFLOW-READY-1\n")
                proposal = {"generation": "untrusted-g", "broker": "ready-broker",
                            "evidence_dir": str(untrusted_dir)}
                deploy_parent.sendall(b"R" + canonical(proposal) + b"\n")
                if rejected_binder.wait(timeout=20) == 0:
                    raise AssertionError("untrusted supervisor installed a BIND")
            if b"live role argument vector differs from policy" not in untrusted_log.read_bytes():
                raise AssertionError("untrusted parent was not rejected by the pinned argv policy")
            child_parent.close()  # no release: the real daemon must refuse its gate
            if outsider.wait(outsider_pid, 15) == 0:
                raise AssertionError("untrusted gated daemon exited successfully")
            if before_untrusted != {name: digest(ROOT / name) for name in FILES}:
                raise AssertionError("untrusted binding attempt changed authority")
        finally:
            deploy_parent.close()
            deploy_child.close()
            child_parent.close()
            child_socket.close()
            outsider.close()
    if not trusted:
        alias.mkdir(mode=0o700)
        subprocess.run(["mount", "--bind", str(ROOT), str(alias)], check=True)
        if identity(alias) != identity(ROOT):
            raise RuntimeError("test alias does not name the original lock namespace")
    evidence.mkdir(mode=0o700)
    command = [python, str(witness), "_production_bindcase", str(alias), str(evidence), str(binder),
               str(binary), endpoint, "real-g1"]
    if trusted:
        command = [python, str(witness), "--trusted-generation", str(binary), endpoint,
                   str(evidence), "real-g1", "ready-broker"]
    actor = subprocess.Popen(command, env=supervisor_env, stdin=subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, close_fds=True)
    try:
        if trusted:
            assert actor.stdout is not None
            poller = getattr(select, "poll")()
            poller.register(actor.stdout.fileno(), getattr(select, "POLLIN") |
                            getattr(select, "POLLERR") | getattr(select, "POLLHUP"))
            if not poller.poll(60000):
                raise TimeoutError(f"trusted witness did not reach gate: pid={actor.pid}")
            ready = actor.stdout.readline()
            if ready != b"UFLOW-BOUND-QUARANTINED\n":
                raise RuntimeError(f"trusted witness did not bind: {ready!r}")
            deadline = time.monotonic() + 15
            while not Path(endpoint).exists() or not Path(endpoint + ".external-ddr").exists():
                if time.monotonic() >= deadline or actor.poll() is not None:
                    raise RuntimeError("trusted bound diagnostic listeners did not start")
                time.sleep(0.01)
            def request(path: str, data: bytes, fd_to_send: int | None = None) -> bytes:
                with socket.socket(getattr(socket, "AF_UNIX"), socket.SOCK_STREAM) as conn:
                    conn.settimeout(5)
                    conn.connect(path)
                    if fd_to_send is None:
                        conn.sendall(data)
                    else:
                        sent = getattr(conn, "sendmsg")([data], [(socket.SOL_SOCKET, getattr(socket, "SCM_RIGHTS"),
                                                                  array.array("i", [fd_to_send]))])
                        if sent <= 0:
                            raise RuntimeError("SCM_RIGHTS probe did not send")
                        conn.sendall(data[sent:])
                    reply = bytearray()
                    while b"\n" not in reply and len(reply) < 8192:
                        part = conn.recv(8192)
                        if not part:
                            break
                        reply.extend(part)
                    return bytes(reply)
            if b"external_ddr_committed_bytes=unknown" not in request(endpoint, b"op=GetStats\n"):
                raise AssertionError("bound generation reported known commitments")
            memfd = getattr(os, "memfd_create")("bound-gate-probe", getattr(os, "MFD_CLOEXEC") |
                                                getattr(os, "MFD_ALLOW_SEALING"))
            try:
                os.ftruncate(memfd, 4096)
                rejected = request(endpoint + ".external-ddr", b"op=RegisterExternalBacking\n", memfd)
            finally:
                os.close(memfd)
            if b"status=error" not in rejected:
                raise AssertionError("uncompleted production generation admitted registration")
            if b"status=ok" not in request(endpoint, b"op=ShutdownDaemon\n"):
                raise RuntimeError("trusted bound shutdown failed")
            remainder, _ = actor.communicate(timeout=75)
            output = ready + remainder
        else:
            output, _ = actor.communicate(timeout=75)
    except subprocess.TimeoutExpired:
        raise TimeoutError(f"isolated witness remains running: pid={actor.pid}") from None
    (RUN / "witness.log").write_bytes(output)
    if actor.returncode != 0:
        raise RuntimeError(f"isolated witness failed: {actor.returncode}; {output.decode(errors='replace')}")
    state = json.loads((ROOT / "state").read_bytes())
    trust = [json.loads(line) for line in (ROOT / "witness.trust").read_bytes().splitlines()]
    witness_frames = [json.loads(line)["event"] for line in (evidence / "witness.journal").read_bytes().splitlines()]
    if len(trust) != 2 or trust[-1].get("type") != "BIND" or state.get("closure") != "bound" \
            or state.get("pending") is not None or trust[-1]["payload"]["generation"] != "real-g1" \
            or witness_frames[-2].get("type") != "child_wait" \
            or witness_frames[-2].get("raw_wait_status") != 0 or witness_frames[-1].get("type") != "witness_seal":
        raise AssertionError("real BIND or real normal witness wait missing")
    if Path(endpoint).exists() or Path(endpoint + ".external-ddr").exists():
        raise AssertionError("bound quarantine left a socket after real shutdown")
    result = {"scope": ("real fixed-root BIND installer + daemon + witness; quarantine only" if trusted else
                        "real non-test daemon + real witness, BIND by test-only installer; quarantine only"),
              "namespace_inode": os.stat("/proc/self/ns/mnt").st_ino, "parent_namespace_inode": parent_mnt,
              "source_identity": [expected.st_dev, expected.st_ino], "authority_identity": identity(ROOT),
              "test_alias_same_inode": not trusted, "bootstrap_exit": bootstrap.returncode,
              "witness_exit": actor.returncode, "witness_output": output.decode(errors="replace"),
              "generation": state["generation"], "closure": state["closure"],
              "trust_types": [frame["type"] for frame in trust],
              "witness_event_types": [event["type"] for event in witness_frames],
              "untrusted_parent_rejected_read_only": trusted,
              "broker_raw_wait": witness_frames[-2]["raw_wait_status"],
              "authority_hashes": {name: digest(ROOT / name) for name in FILES},
              "not_proved": (["admission", "trust SEAL transaction", "second generation",
                              "recovery", "reconfigure", "full T3.2 acceptance"] if trusted else
                             ["production installer", "admission", "trust SEAL transaction", "second generation",
                              "recovery", "reconfigure", "full T3.2 acceptance"])}
    (RUN / "result.json").write_text(json.dumps(result, sort_keys=True, indent=2) + "\n")
    print(json.dumps(result, sort_keys=True), flush=True)


def outer(run_dir: Path, binary: Path, binder: Path | None, witness: Path, shim: Path, trusted: bool) -> None:
    parent = run_dir.parent.resolve(strict=True)
    if not parent.is_relative_to(BASE) or parent == BASE or run_dir.parent != parent:
        raise ValueError("probe must use a canonical task-owned run directory")
    binary, witness, shim = (item.resolve(strict=True) for item in (binary, witness, shim))
    binder = binder.resolve(strict=True) if binder is not None else None
    artifacts = [binary, witness, shim] + ([binder] if binder is not None else [])
    if not all(item.is_relative_to(parent) for item in artifacts):
        raise ValueError("probe artifacts must belong to this task run")
    run_dir.mkdir(mode=0o700)
    nsroot = run_dir / "nsroot"
    nsroot.mkdir(mode=0o700)
    (nsroot / "authority").mkdir(mode=0o700)
    (nsroot / "runs").mkdir(mode=0o700)
    local = nsroot / "runs" / "isolated-bound"
    local.mkdir(mode=0o700)
    (local / "shim").mkdir(mode=0o700)
    copies = [(binary, local / "uf-daemon"), (witness, local / "t3_deployment_witness.py")]
    if not trusted:
        if binder is None:
            raise ValueError("the fixture mode requires its test binder")
        copies.append((binder, local / "test-binder"))
    copies.append((shim, local / "shim" / "libuf_acl_shim.so"))
    for source, dest in copies:
        shutil.copyfile(source, dest)
        os.chmod(dest, 0o700 if dest.name in ("uf-daemon", "test-binder") else 0o600)
    hashes = {dest.name: digest(source) for source, dest in copies[:-1]}
    fd = os.open(nsroot, os.O_RDONLY | getattr(os, "O_DIRECTORY") | getattr(os, "O_CLOEXEC")
                 | getattr(os, "O_NOFOLLOW"))
    try:
        try:
            inner(nsroot, fd, os.stat(f"/proc/{os.getppid()}/ns/mnt").st_ino, hashes, trusted)
        except RuntimeError as error:
            if str(error) != "mount namespace was not isolated":
                raise
        else:
            raise AssertionError("outer namespace guard unexpectedly accepted")
        command = ["unshare", "--mount", "--propagation", "private", sys.executable,
                   str(Path(__file__).resolve(strict=True)), "--inside", str(nsroot), "--root-fd", str(fd),
                   "--parent-mnt", str(os.stat("/proc/self/ns/mnt").st_ino), "--hashes", json.dumps(hashes)]
        if trusted:
            command.append("--trusted")
        with (run_dir / "outer.log").open("xb") as log:
            child = subprocess.Popen(command, pass_fds=(fd,), stdin=subprocess.DEVNULL,
                                     stdout=log, stderr=subprocess.STDOUT)
            try:
                rc = child.wait(timeout=120)
            except subprocess.TimeoutExpired:
                raise TimeoutError(f"isolated probe remains running: pid={child.pid}") from None
    finally:
        os.close(fd)
    sys.stdout.buffer.write((run_dir / "outer.log").read_bytes())
    if rc != 0:
        raise RuntimeError(f"bound probe failed: exit={rc}; preserved at {run_dir}")
    print(f"isolated bound probe: exit=0; results={run_dir}/nsroot/runs/isolated-bound", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path)
    parser.add_argument("--daemon", type=Path)
    parser.add_argument("--binder", type=Path)
    parser.add_argument("--witness", type=Path)
    parser.add_argument("--shim", type=Path)
    parser.add_argument("--trusted", action="store_true")
    parser.add_argument("--inside", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--root-fd", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--parent-mnt", type=int, help=argparse.SUPPRESS)
    parser.add_argument("--hashes", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.inside is not None:
        if args.root_fd is None or args.parent_mnt is None or args.hashes is None:
            parser.error("incomplete isolation proof")
        inner(args.inside, args.root_fd, args.parent_mnt, json.loads(args.hashes), args.trusted)
    else:
        if any(value is None for value in (args.run_dir, args.daemon, args.witness, args.shim)) \
                or (not args.trusted and args.binder is None):
            parser.error("--run-dir, --daemon, --witness and --shim are required; fixture mode also needs --binder")
        outer(args.run_dir, args.daemon, args.binder, args.witness, args.shim, args.trusted)


if __name__ == "__main__":
    main()
