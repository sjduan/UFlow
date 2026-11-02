#!/usr/bin/env python3
"""Isolated-fixture witness deployment binding; NOT a recovery authority.

The fixture bootstrap pins an initially empty witness.trust inode.  The direct
parent fills it once under that fixture's lock before any child is released.
The witness remains the sole writer of the separate roster journal/head.
Reading this binding is only provenance checking: journal semantic replay,
complete holder/worker coverage, and any operation that clears quarantine are
NOT implemented here. Production bootstrap/admission stays disabled.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile
import unittest

import t3_supervisor_wip as supervisor

SCHEMA = "uflow-ddr-witness-binding-fixture-v2"
ROOT_PREFIX = Path("/workspace/uflow/runs")
BOOTSTRAP_KEYS = (
    "domain", "host", "boot", "endpoint", "owner_limit", "total_limit", "revision",
    "dir_dev", "dir_ino", "lock_dev", "lock_ino", "state_dev", "state_ino",
    "journal_dev", "journal_ino", "journal_head_dev", "journal_head_ino",
    "witness_trust_dev", "witness_trust_ino",
)
GENESIS_HEAD = (
    b"uflow-ddr-recovery-head-v1\nmode=clean\nbroker=none\nsequence=0\n"
    b"offset=0\nchecksum=0000000000000000\nclean_sequence=0\n"
)


def _private_open(path: Path, write: bool = False) -> int:
    fd = os.open(path, (os.O_RDWR if write else os.O_RDONLY) | os.O_CLOEXEC | os.O_NOFOLLOW)
    meta = os.fstat(fd)
    if not stat.S_ISREG(meta.st_mode) or meta.st_uid != os.geteuid() or meta.st_mode & 0o077 or meta.st_nlink != 1:
        os.close(fd)
        raise ValueError("fixture file is not private, owned and singly linked")
    return fd


def _read(fd: int, maximum: int) -> bytes:
    meta = os.fstat(fd)
    if meta.st_size > maximum:
        raise ValueError("fixture file exceeds limit")
    data = os.pread(fd, maximum + 1, 0)
    if len(data) != meta.st_size:
        raise ValueError("fixture file changed during read")
    return data


def _identity(fd: int) -> list[int]:
    value = os.fstat(fd)
    return [value.st_dev, value.st_ino]


def _directory(path: Path) -> Path:
    canonical = path.resolve(strict=True)
    if canonical != path or not canonical.is_relative_to(ROOT_PREFIX):
        raise ValueError("fixture must be a canonical task-owned run directory")
    meta = path.stat(follow_symlinks=False)
    if not stat.S_ISDIR(meta.st_mode) or meta.st_uid != os.geteuid() or meta.st_mode & 0o077:
        raise ValueError("fixture directory must be private and owned")
    return canonical


def _bootstrap(root: Path) -> dict[str, str]:
    fd = _private_open(root / "bootstrap")
    try:
        raw = _read(fd, 4096)
    finally:
        os.close(fd)
    lines = raw.decode("ascii").splitlines(keepends=True)
    if len(lines) != len(BOOTSTRAP_KEYS) + 1 or lines[0] != "uflow-ddr-authority-v3\n":
        raise ValueError("invalid fixture bootstrap")
    result = {}
    for key, line in zip(BOOTSTRAP_KEYS, lines[1:]):
        if not line.startswith(key + "=") or not line.endswith("\n"):
            raise ValueError("noncanonical fixture bootstrap")
        value = line[len(key) + 1:-1]
        if not value or "=" in value or "\n" in value:
            raise ValueError("invalid bootstrap field")
        result[key] = value
    if result["domain"] != "external-ddr-v0":
        raise ValueError("wrong fixture domain")
    for key in BOOTSTRAP_KEYS[4:]:
        value = result[key]
        if not value.isascii() or not value.isdigit() or str(int(value)) != value:
            raise ValueError("invalid bootstrap integer")
    if int(result["revision"]) == 0:
        raise ValueError("invalid cap revision")
    meta = root.stat(follow_symlinks=False)
    if [meta.st_dev, meta.st_ino] != [int(result["dir_dev"]), int(result["dir_ino"])]:
        raise ValueError("fixture bootstrap root mismatch")
    return result


def _pinned_file(root: Path, bootstrap: dict[str, str], name: str, prefix: str, write: bool = False) -> int:
    fd = _private_open(root / name, write)
    if _identity(fd) != [int(bootstrap[prefix + "_dev"]), int(bootstrap[prefix + "_ino"])]:
        os.close(fd)
        raise ValueError("fixture bootstrap file identity mismatch")
    return fd


def _pinned_files(root: Path, bootstrap: dict[str, str], specs: tuple[tuple[str, str, bool], ...]) -> list[int]:
    result = []
    try:
        for name, prefix, write in specs:
            result.append(_pinned_file(root, bootstrap, name, prefix, write))
        return result
    except BaseException:
        for fd in result:
            os.close(fd)
        raise


def _evidence_files(evidence: Path) -> tuple[int, int]:
    journal_fd = _private_open(evidence / "witness.journal")
    try:
        return journal_fd, _private_open(evidence / "witness.head")
    except BaseException:
        os.close(journal_fd)
        raise


def _is_token(value: str) -> bool:
    return bool(value) and len(value) <= 96 and value.isascii() and all(c.isalnum() or c in "._-" for c in value)


def pin_fixture(root: Path, witness: supervisor.Witness, broker_instance: str, lifecycle_epoch: int,
                child_executable: str | None = None) -> None:
    """Trusted fixture deployment only, before launching any managed child.

    The initial implementation only accepts genesis, not a clean restart. It
    never accepts production authority roots, never overwrites a trust slot and
    does not itself start the broker or authorize any resource side effect.
    """
    root = _directory(root)
    evidence = _directory(witness.directory)
    if not witness._require_fixture_binding or witness._fixture_binding_check is not None \
            or witness._children or witness._sealed or witness._poisoned:
        raise ValueError("fixture witness must be bound before child launch")
    # The current Rust catalog begins its lifecycle counter at zero.
    if not _is_token(broker_instance) or type(lifecycle_epoch) is not int or lifecycle_epoch < 0:
        raise ValueError("invalid fixture generation")
    lock_fd = _private_open(root / "authority.lock", True)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        bootstrap = _bootstrap(root)
        if _identity(lock_fd) != [int(bootstrap["lock_dev"]), int(bootstrap["lock_ino"])]:
            raise ValueError("fixture lock does not match bootstrap")
        if bootstrap["boot"] != witness.boot_id:
            raise ValueError("fixture and witness boots differ")
        state_fd, journal_fd, head_fd, trust_fd = _pinned_files(root, bootstrap, (
            ("state", "state", False), ("recovery.journal", "journal", False),
            ("recovery.head", "journal_head", False), ("witness.trust", "witness_trust", True)))
        try:
            if _read(state_fd, 4096) != b"clean\n" or _read(journal_fd, 1) != b"" \
                    or _read(head_fd, 4096) != GENESIS_HEAD or _read(trust_fd, 4096) != b"":
                raise ValueError("fixture must be unbound and at genesis")
            events = supervisor.verify_fixture_structure(evidence)
            if len(events) != 1 or events[0].get("type") != "witness_start":
                raise ValueError("witness roster already started")
            start = events[0]
            interpreter_path = os.path.realpath(supervisor.sys.executable)
            witness_code_path = os.path.realpath(supervisor.__file__)
            pin_code_path = os.path.realpath(__file__)
            child_path = os.path.realpath(child_executable or interpreter_path)
            if start.get("witness_birth") != supervisor._birth(os.getpid()) \
                    or start.get("run_id") != witness.run_id \
                    or start.get("boot_id") != bootstrap["boot"] \
                    or start.get("witness_code_sha256") != supervisor._digest_file(witness_code_path) \
                    or start.get("interpreter_sha256") != supervisor._digest_file(interpreter_path):
                raise ValueError("live witness identity or code mismatch")
            journal_meta, evidence_head = _evidence_files(evidence)
            try:
                if tuple(_identity(journal_meta)) != witness._journal_identity \
                        or tuple(_identity(evidence_head)) != witness._head_identity:
                    raise ValueError("witness evidence inode mismatch")
                binding = {"schema": SCHEMA, "run_id": witness.run_id,
                           "host": bootstrap["host"], "domain": bootstrap["domain"],
                           "boot": bootstrap["boot"], "cap_revision": int(bootstrap["revision"]),
                           "broker_instance": broker_instance, "lifecycle_epoch": lifecycle_epoch,
                           "broker_journal": _identity(journal_fd), "broker_head": _identity(head_fd),
                           "initial_head_sha256": hashlib.sha256(GENESIS_HEAD).hexdigest(),
                           "witness_birth": start["witness_birth"],
                           "witness_code_sha256": start["witness_code_sha256"],
                           "witness_code_path": witness_code_path,
                           "interpreter_path": interpreter_path,
                           "interpreter_sha256": start["interpreter_sha256"],
                           "pin_code_path": pin_code_path,
                           "pin_code_sha256": supervisor._digest_file(pin_code_path),
                           "child_executable_path": child_path,
                           "child_executable_sha256": supervisor._digest_file(child_path),
                           "sealed_head_sha256": "pending",
                           "evidence_dir": str(evidence), "evidence_journal": _identity(journal_meta),
                           "evidence_head": _identity(evidence_head)}
                data = supervisor._canonical(binding) + b"\n"
                if len(data) > 4096:
                    raise ValueError("fixture binding too large")
                supervisor._write_all(trust_fd, data)
                os.fsync(trust_fd)
                root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
                try:
                    os.fsync(root_fd)
                finally:
                    os.close(root_fd)
                if _read(trust_fd, 4096) != data:
                    raise ValueError("fixture binding readback mismatch")
                # In bound mode every child launch re-reads the pinned trust
                # and evidence identities before creating the child process.
                def check_before_launch() -> None:
                    current, _ = read_pinned_fixture(root)
                    if current != binding:
                        raise ValueError("fixture deployment binding changed")
                witness._fixture_binding_check = check_before_launch
                witness._fixture_child_sha256 = binding["child_executable_sha256"]
                def bind_seal() -> None:
                    seal_fixture(root, binding)
                witness._fixture_seal_callback = bind_seal
            finally:
                os.close(journal_meta)
                os.close(evidence_head)
        finally:
            for fd in (state_fd, journal_fd, head_fd, trust_fd):
                os.close(fd)
    finally:
        os.close(lock_fd)


def read_pinned_fixture(root: Path) -> tuple[dict, list[dict]]:
    """Read pinned witness provenance only; never authorizes recovery/clean.

    The caller cannot choose the evidence path: it is read from the bootstrap-
    pinned trust slot. This WIP does not check semantic broker journal replay,
    complete obligations, worker drain, or whether a sealed roster covers them.
    """
    root = _directory(root)
    bootstrap = _bootstrap(root)
    trust_fd, journal_fd, head_fd, lock_fd, state_fd = _pinned_files(root, bootstrap, (
        ("witness.trust", "witness_trust", False), ("recovery.journal", "journal", False),
        ("recovery.head", "journal_head", False), ("authority.lock", "lock", False),
        ("state", "state", False)))
    try:
        raw = _read(trust_fd, 4096)
        binding = json.loads(raw)
        keys = {"schema", "run_id", "host", "domain", "boot", "cap_revision", "broker_instance",
                "lifecycle_epoch", "broker_journal", "broker_head", "initial_head_sha256",
                "witness_birth", "witness_code_sha256", "witness_code_path", "interpreter_path",
                "interpreter_sha256", "pin_code_path", "pin_code_sha256", "child_executable_path",
                "child_executable_sha256", "sealed_head_sha256", "evidence_dir", "evidence_journal", "evidence_head"}
        if not isinstance(binding, dict) or set(binding) != keys or supervisor._canonical(binding) + b"\n" != raw:
            raise ValueError("invalid fixture witness binding")
        if binding["schema"] != SCHEMA or binding["host"] != bootstrap["host"] \
                or binding["domain"] != bootstrap["domain"] or binding["boot"] != bootstrap["boot"] \
                or binding["cap_revision"] != int(bootstrap["revision"]) \
                or binding["broker_journal"] != _identity(journal_fd) or binding["broker_head"] != _identity(head_fd) \
                or binding["initial_head_sha256"] != hashlib.sha256(GENESIS_HEAD).hexdigest() \
                or not isinstance(binding["broker_instance"], str) or not _is_token(binding["broker_instance"]) \
                or type(binding["lifecycle_epoch"]) is not int or binding["lifecycle_epoch"] < 0:
            raise ValueError("fixture generation binding mismatch")
        for path_key, hash_key in (("witness_code_path", "witness_code_sha256"),
                                   ("interpreter_path", "interpreter_sha256"),
                                   ("pin_code_path", "pin_code_sha256"),
                                   ("child_executable_path", "child_executable_sha256")):
            path = binding[path_key]
            if not isinstance(path, str) or not os.path.isabs(path) or os.path.realpath(path) != path \
                    or supervisor._digest_file(path) != binding[hash_key]:
                raise ValueError("fixture pinned code mismatch")
        evidence = _directory(Path(binding["evidence_dir"]))
        evidence_journal, evidence_head = _evidence_files(evidence)
        try:
            if _identity(evidence_journal) != binding["evidence_journal"] \
                    or _identity(evidence_head) != binding["evidence_head"]:
                raise ValueError("pinned evidence inode mismatch")
            # The structure helper reopens these names; this fixture operates
            # only inside trusted same-UID deployment, not against hostile races.
            events = supervisor.verify_fixture_structure(evidence)
            sealed = binding["sealed_head_sha256"]
            if sealed != "pending" and (not isinstance(sealed, str) or
                    sealed != hashlib.sha256(_read(evidence_head, 4096)).hexdigest() or
                    events[-1].get("type") != "witness_seal"):
                raise ValueError("fixture witness seal anchor mismatch")
            start = events[0]
            if start.get("run_id") != binding["run_id"] or start.get("boot_id") != binding["boot"] \
                    or start.get("witness_birth") != binding["witness_birth"] \
                    or start.get("witness_code_sha256") != binding["witness_code_sha256"] \
                    or start.get("interpreter_sha256") != binding["interpreter_sha256"]:
                raise ValueError("pinned witness identity mismatch")
            return binding, events
        finally:
            os.close(evidence_journal)
            os.close(evidence_head)
    finally:
        os.close(trust_fd)
        os.close(journal_fd)
        os.close(head_fd)
        os.close(lock_fd)
        os.close(state_fd)


def seal_fixture(root: Path, initial_binding: dict) -> None:
    """Anchor the sealed witness head, under the original fixture lock.

    This detects evidence changes but not a hostile same-UID writer able to
    rewrite both pinned trust and evidence. It never changes clean/dirty.
    """
    root = _directory(root)
    lock_fd = _private_open(root / "authority.lock", True)
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        bootstrap = _bootstrap(root)
        if _identity(lock_fd) != [int(bootstrap["lock_dev"]), int(bootstrap["lock_ino"])]:
            raise ValueError("fixture lock changed before seal")
        binding, events = read_pinned_fixture(root)
        if binding != initial_binding or binding["sealed_head_sha256"] != "pending" \
                or events[-1].get("type") != "witness_seal":
            raise ValueError("fixture is not at its original sealed roster")
        evidence = _directory(Path(binding["evidence_dir"]))
        journal_fd, head_fd = _evidence_files(evidence)
        trust_fd = _pinned_file(root, bootstrap, "witness.trust", "witness_trust", True)
        try:
            if _identity(journal_fd) != binding["evidence_journal"] \
                    or _identity(head_fd) != binding["evidence_head"] \
                    or _read(trust_fd, 4096) != supervisor._canonical(binding) + b"\n":
                raise ValueError("fixture evidence or trust changed before seal")
            sealed = dict(binding)
            sealed["sealed_head_sha256"] = hashlib.sha256(_read(head_fd, 4096)).hexdigest()
            encoded = supervisor._canonical(sealed) + b"\n"
            if len(encoded) > 4096:
                raise ValueError("sealed fixture binding too large")
            os.ftruncate(trust_fd, 0)
            os.lseek(trust_fd, 0, os.SEEK_SET)
            supervisor._write_all(trust_fd, encoded)
            os.fsync(trust_fd)
            if _read(trust_fd, 4096) != encoded:
                raise ValueError("sealed fixture binding readback mismatch")
        finally:
            os.close(journal_fd)
            os.close(head_fd)
            os.close(trust_fd)
    finally:
        os.close(lock_fd)


FIXTURE_ROOT: Path | None = None


def _make_authority(root: Path) -> None:
    """Make only a synthetic v3 genesis fixture, not a deployed authority."""
    root.mkdir(mode=0o700)
    names = ("authority.lock", "state", "recovery.journal", "recovery.head", "witness.trust")
    for name in names:
        fd = os.open(root / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
        try:
            if name == "state":
                supervisor._write_all(fd, b"clean\n")
            elif name == "recovery.head":
                supervisor._write_all(fd, GENESIS_HEAD)
            os.fsync(fd)
        finally:
            os.close(fd)
    def inode(name: str) -> tuple[int, int]:
        meta = (root / name).stat()
        return meta.st_dev, meta.st_ino
    root_meta = root.stat()
    data = {"domain": "external-ddr-v0", "host": "fixture-host",
            "boot": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "endpoint": "/workspace/uflow/runs/fixture/a.sock", "owner_limit": 8192,
            "total_limit": 8192, "revision": 1, "dir_dev": root_meta.st_dev, "dir_ino": root_meta.st_ino}
    for prefix, name in (("lock", "authority.lock"), ("state", "state"),
                         ("journal", "recovery.journal"), ("journal_head", "recovery.head"),
                         ("witness_trust", "witness.trust")):
        data[prefix + "_dev"], data[prefix + "_ino"] = inode(name)
    text = "uflow-ddr-authority-v3\n" + "".join(f"{key}={data[key]}\n" for key in BOOTSTRAP_KEYS)
    fd = os.open(root / "bootstrap", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC, 0o600)
    try:
        supervisor._write_all(fd, text.encode("ascii"))
        os.fsync(fd)
    finally:
        os.close(fd)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class PinTests(unittest.TestCase):
    def setUp(self) -> None:
        assert FIXTURE_ROOT is not None
        self.run_dir = Path(tempfile.mkdtemp(prefix="binding-", dir=FIXTURE_ROOT))
        self.root = self.run_dir / "authority-fixture"
        self.evidence = self.run_dir / "evidence"
        _make_authority(self.root)
        self.evidence.mkdir(mode=0o700)
        self.witness = supervisor.Witness(self.evidence, "pin-run", require_fixture_binding=True)
        self.addCleanup(self.witness.close)

    def test_pin_before_launch_and_read_only_bound_roster(self) -> None:
        pin_fixture(self.root, self.witness, "broker-fixture", 1)
        with self.assertRaises(ValueError):
            pin_fixture(self.root, self.witness, "broker-other", 1)
        owner = self.witness.spawn("owner", [os.path.realpath(supervisor.sys.executable), supervisor.__file__, "_exit", "0"])
        self.witness.wait(owner, timeout=5)
        self.witness.seal()
        binding, events = read_pinned_fixture(self.root)
        supervisor.verify_fixture_roster(events, binding["run_id"], binding["witness_code_sha256"],
                                         binding["witness_birth"], binding["boot"], [events[1]["birth"]])
        self.assertEqual(binding["broker_instance"], "broker-fixture")
        self.assertEqual(binding["lifecycle_epoch"], 1)
        self.assertEqual(events[-1]["type"], "witness_seal")
        self.assertEqual((self.root / "state").read_bytes(), b"clean\n")

    def test_replaced_trust_or_evidence_inode_is_rejected(self) -> None:
        pin_fixture(self.root, self.witness, "broker-fixture", 1)
        os.rename(self.evidence / "witness.head", self.evidence / "old.head")
        (self.evidence / "witness.head").write_bytes((self.evidence / "old.head").read_bytes())
        os.chmod(self.evidence / "witness.head", 0o600)
        with self.assertRaises(ValueError):
            read_pinned_fixture(self.root)
        with self.assertRaises(ValueError):
            self.witness.spawn("owner", [os.path.realpath(supervisor.sys.executable), supervisor.__file__, "_exit", "0"])
        self.assertEqual(self.witness._children, {})
        os.rename(self.root / "witness.trust", self.root / "old.trust")
        (self.root / "witness.trust").write_bytes((self.root / "old.trust").read_bytes())
        os.chmod(self.root / "witness.trust", 0o600)
        with self.assertRaises(ValueError):
            read_pinned_fixture(self.root)

    def test_unbound_child_is_not_launched(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "not been deployment-bound"):
            self.witness.spawn("owner", [os.path.realpath(supervisor.sys.executable), supervisor.__file__, "_exit", "0"])
        self.assertEqual(self.witness._children, {})
        self.assertEqual((self.root / "witness.trust").read_bytes(), b"")

    def test_zero_initial_lifecycle_epoch_is_valid(self) -> None:
        pin_fixture(self.root, self.witness, "broker-fixture", 0)
        binding, _ = read_pinned_fixture(self.root)
        self.assertEqual(binding["lifecycle_epoch"], 0)

    def test_wrong_boot_dirty_head_and_held_lock_cannot_bind(self) -> None:
        manifest_path = self.root / "bootstrap"
        manifest = manifest_path.read_bytes()
        boot = self.witness.boot_id.encode("ascii")
        manifest_path.write_bytes(manifest.replace(b"boot=" + boot + b"\n", b"boot=wrong-boot\n"))
        with self.assertRaises(ValueError):
            pin_fixture(self.root, self.witness, "broker-fixture", 1)
        self.assertEqual((self.root / "witness.trust").read_bytes(), b"")
        manifest_path.write_bytes(manifest)

        state = self.root / "state"
        state.write_bytes(b"dirty\n")
        with self.assertRaises(ValueError):
            pin_fixture(self.root, self.witness, "broker-fixture", 1)
        self.assertEqual((self.root / "witness.trust").read_bytes(), b"")
        state.write_bytes(b"clean\n")

        fd = _private_open(self.root / "authority.lock", True)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaises(BlockingIOError):
                pin_fixture(self.root, self.witness, "broker-fixture", 1)
        finally:
            os.close(fd)
        self.assertEqual((self.root / "witness.trust").read_bytes(), b"")


def exercise_rust_genesis_fixture(root: Path) -> None:
    """Cross-language fixture only; children do not acquire managed FDs."""
    root = _directory(root)
    evidence = Path(tempfile.mkdtemp(prefix="witness-rust-fixture-", dir=root.parent))
    witness = supervisor.Witness(evidence, "rust-pin-run", require_fixture_binding=True)
    try:
        pin_fixture(root, witness, "fixture-broker", 1)
        children = [witness.spawn(role, [os.path.realpath(supervisor.sys.executable),
                                         supervisor.__file__, "_exit", "0"])
                    for role in ("broker", "owner", "accessor", "worker")]
        for pid in children:
            if not supervisor._normal(witness.wait(pid, timeout=5)):
                raise RuntimeError("fixture child did not exit normally")
        witness.seal()
        binding, events = read_pinned_fixture(root)
        births = [event["birth"] for event in events if event["type"] == "child_birth"]
        supervisor.verify_fixture_roster(events, binding["run_id"], binding["witness_code_sha256"],
                                         binding["witness_birth"], binding["boot"], births)
        if len(births) != 4 or (root / "state").read_bytes() != b"clean\n":
            raise RuntimeError("fixture roster or authority state changed")
        print("rust-genesis-fixture: pinned 4 direct children, not resource coverage")
    finally:
        witness.close()


def exercise_rust_holders_fixture(root: Path, child_exe: Path, endpoint: str,
                                  fault: str | None = None, live_drain: bool = False,
                                  verify_active: bool = False) -> None:
    """Launch real test-binary children; control pipes never carry backing FDs.

    The pinned broker journal and witness roster, not these control messages,
    are the source for the Rust fixture's identity/resource/drain assertions.
    This is not a coverage verifier and never authorizes recovery.
    """
    root = _directory(root)
    if live_drain and fault is not None:
        raise ValueError("live drain cannot be combined with witness fault")
    if verify_active and (not live_drain or fault is not None):
        raise ValueError("active witness verification requires the normal live-drain fixture")
    executable = str(child_exe.resolve(strict=True))
    evidence = Path(tempfile.mkdtemp(prefix="real-witness-", dir=root.parent))
    witness = supervisor.Witness(evidence, "holders-run", require_fixture_binding=True)
    pids: list[int] = []

    def launch(role: str) -> int:
        env = dict(os.environ)
        env.update(UF_DDR_WITNESS_HOLDER_ROLE=role, UF_DDR_WITNESS_ROOT=str(root),
                   UF_DDR_WITNESS_ENDPOINT=endpoint)
        if fault is not None:
            env["UF_DDR_WITNESS_FAULT"] = fault
        if live_drain:
            env["UF_DDR_WITNESS_LIVE_DRAIN"] = "1"
        if verify_active:
            env["UF_DDR_WITNESS_VERIFY_ACTIVE"] = "1"
        pid = witness.spawn(role, [executable, "--exact",
                            "external_ddr::managed_tests::witness_fixture::child_holder",
                            "--nocapture"], env=env, control=True)
        pids.append(pid)
        return pid

    def send(pid: int, text: str) -> None:
        stream = witness._children[pid]["process"].stdin
        stream.write((text + "\n").encode("ascii"))
        stream.flush()

    def marker(pid: int, name: str) -> str:
        stream = witness._children[pid]["process"].stdout
        poller = supervisor.select.poll()
        poller.register(stream.fileno(), supervisor.select.POLLIN | supervisor.select.POLLHUP | supervisor.select.POLLERR)
        deadline = supervisor.time.monotonic() + 30
        data = bytearray()
        needle = ("UF_WITNESS_" + name + "=").encode("ascii")
        while supervisor.time.monotonic() < deadline:
            if not poller.poll(100):
                continue
            byte = os.read(stream.fileno(), 1)
            if not byte:
                raise RuntimeError(f"fixture child exited before {name}")
            data.extend(byte)
            if len(data) > 65536:
                raise RuntimeError("fixture control output limit exceeded")
            if byte == b"\n":
                if needle in data:
                    return bytes(data).split(needle, 1)[1].strip().decode("ascii")
                data.clear()
        raise TimeoutError(f"fixture child timed out before {name}")

    try:
        pin_fixture(root, witness, "fixture-broker", 0, executable)
        broker = launch("broker")
        assert marker(broker, "BROKER_READY") == "1"
        owner = launch("owner")
        source = marker(owner, "SOURCE")
        dev, ino = (int(value) for value in marker(owner, "SOURCE_ID").split(":"))
        target = marker(owner, "TARGET")
        target_dev, target_ino = (int(value) for value in marker(owner, "TARGET_ID").split(":"))
        send(broker, "copy")
        send(broker, source)
        send(broker, target)
        assert marker(broker, "COPY_DRAINED") == "1"
        accessor = launch("accessor")
        send(accessor, source)
        assert marker(accessor, "HOLDING") == "accessor"
        if fault == "unstable":
            original = supervisor._birth
            observations = 0

            def unstable(pid: int) -> dict:
                nonlocal observations
                result = original(pid)
                observations += 1
                if observations == 2:
                    result["starttime_ticks"] += 1
                return result

            with supervisor.mock.patch.object(supervisor, "_birth", side_effect=unstable):
                try:
                    launch("worker")
                except RuntimeError as error:
                    if "birth changed" not in str(error):
                        raise
                else:
                    raise RuntimeError("unstable birth incorrectly opened gate")
            worker = None
        else:
            worker = launch("worker")
            send(worker, source)
            assert marker(worker, "HOLDING") == "worker"
        if not witness._poisoned:
            witness.record_no_backing(dev, ino)
            witness.record_no_backing(target_dev, target_ino)
            for pid in (broker, owner, accessor, worker):
                assert pid is not None
                witness.record_child_backing(pid, dev, ino)
            for pid in (broker, owner):
                witness.record_child_backing(pid, target_dev, target_ino)

        if live_drain:
            # The broker exits after joining handlers/work while the two
            # views and backings remain retained. The actual holders then
            # close their FDs/mappings and exit under this trusted parent.
            assert worker is not None
            for pid in (broker, worker, accessor, owner):
                send(pid, "stop")
                if verify_active and pid == broker:
                    assert marker(pid, "ACTIVE_VERIFIED") == "1"
                if not supervisor._normal(witness.wait(pid, timeout=30)):
                    raise RuntimeError("live-drain child did not exit normally")
        elif fault in ("signal", "nonzero"):
            assert worker is not None
            send(worker, fault)
            status = witness.wait(worker, timeout=30)
            if (fault == "signal" and not os.WIFSIGNALED(status)) \
                    or (fault == "nonzero" and (not os.WIFEXITED(status) or os.WEXITSTATUS(status) != 7)):
                raise RuntimeError("unexpected failure wait status")
        elif fault == "timeout":
            assert worker is not None
            try:
                witness.wait(worker, timeout=0)
            except TimeoutError:
                pass
            else:
                raise RuntimeError("live worker unexpectedly passed timeout")
            send(worker, "stop")
            witness._children[worker]["process"].wait(timeout=30)
        elif fault == "echild":
            assert worker is not None
            send(worker, "stop")
            waited, status = os.waitpid(worker, 0)
            if waited != worker or not supervisor._normal(status):
                raise RuntimeError("unexpected setup wait result")
            witness._children[worker]["process"].returncode = 0
            try:
                witness.wait(worker, timeout=30)
            except RuntimeError as error:
                if "ECHILD" not in str(error):
                    raise
            else:
                raise RuntimeError("ECHILD incorrectly accepted")
        elif worker is not None:
            send(worker, "stop")
            if not supervisor._normal(witness.wait(worker, timeout=30)):
                raise RuntimeError("fixture worker did not exit normally")

        if not live_drain:
            for pid in (accessor, owner, broker):
                send(pid, "stop")
                if witness._poisoned:
                    witness._children[pid]["process"].wait(timeout=30)
                elif not supervisor._normal(witness.wait(pid, timeout=30)):
                    raise RuntimeError("fixture child did not exit normally")
        if witness._poisoned:
            try:
                witness.seal()
            except RuntimeError:
                pass
            else:
                raise RuntimeError("poisoned witness incorrectly sealed")
        else:
            witness.seal()
        binding, events = read_pinned_fixture(root)
        births = [item["birth"] for item in events if item["type"] == "child_birth"]
        if fault is None:
            supervisor.verify_fixture_roster(events, binding["run_id"], binding["witness_code_sha256"],
                                             binding["witness_birth"], binding["boot"], births)
        else:
            try:
                supervisor.verify_fixture_roster(events, binding["run_id"], binding["witness_code_sha256"],
                                                 binding["witness_birth"], binding["boot"], births)
            except ValueError:
                pass
            else:
                raise RuntimeError("failed witness incorrectly accepted")
        roles = [item["role"] for item in events if item["type"] == "child_birth"]
        expected_roles = ["broker", "owner", "accessor"] + ([] if fault == "unstable" else ["worker"])
        if roles != expected_roles or (root / "state").read_bytes() != b"dirty\n":
            raise RuntimeError("fixture roles or dirty quarantine state mismatch")
        print("rust-holders-fixture: actual direct children and raw wait status; no recovery authorized")
    finally:
        # On failure closing control streams lets run-owned children fail/exit;
        # do not kill, forge wait results, or write a clean checkpoint.
        for pid in pids:
            process = witness._children[pid]["process"]
            if process.stdin is not None:
                process.stdin.close()
            if process.stdout is not None:
                process.stdout.close()
            if process.returncode is None:
                try:
                    process.wait(timeout=10)
                except supervisor.subprocess.TimeoutExpired:
                    pass
        witness.close()


def main() -> None:
    global FIXTURE_ROOT
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--self-test-run-dir", type=Path)
    group.add_argument("--rust-genesis-fixture", type=Path)
    group.add_argument("--rust-holders-fixture", type=Path)
    parser.add_argument("--child-exe", type=Path)
    parser.add_argument("--endpoint")
    parser.add_argument("--fault", choices=("signal", "nonzero", "timeout", "echild", "unstable"))
    parser.add_argument("--live-drain", action="store_true")
    parser.add_argument("--verify-active", action="store_true")
    args = parser.parse_args()
    if args.rust_holders_fixture is not None:
        if args.child_exe is None or args.endpoint is None:
            parser.error("--rust-holders-fixture requires --child-exe and --endpoint")
        exercise_rust_holders_fixture(args.rust_holders_fixture, args.child_exe, args.endpoint,
                                      args.fault, args.live_drain, args.verify_active)
        return
    if args.rust_genesis_fixture is not None:
        exercise_rust_genesis_fixture(args.rust_genesis_fixture)
        return
    FIXTURE_ROOT = _directory(args.self_test_run_dir.resolve(strict=True))
    unittest.main(argv=[supervisor.sys.argv[0]], verbosity=2)


if __name__ == "__main__":
    main()
