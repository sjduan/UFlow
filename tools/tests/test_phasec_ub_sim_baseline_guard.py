import importlib.util
import tempfile
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "phasec" / "ub_sim_baseline_guard.py"
)
SPEC = importlib.util.spec_from_file_location("ub_sim_baseline_guard", MODULE_PATH)
guard = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(guard)


class BaselineGuardUnitTests(unittest.TestCase):
    def test_parse_lock_accepts_required_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "mem_service.lock"
            lock.write_text(
                "lock_version=1\nversion=0.1.0\nrevision=" + "a" * 40 + "\n",
                encoding="utf-8",
            )
            self.assertEqual(guard.parse_lock(lock)["revision"], "a" * 40)

    def test_parse_lock_rejects_missing_revision(self):
        with tempfile.TemporaryDirectory() as directory:
            lock = Path(directory) / "mem_service.lock"
            lock.write_text("lock_version=1\nversion=0.1.0\n", encoding="utf-8")
            with self.assertRaises(guard.GuardFailure):
                guard.parse_lock(lock)

    def test_save_and_load_state_round_trip(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            state = {"gates": {"U1": {"status": "pass"}}}
            guard.save_state(output, state)
            loaded = guard.load_state(output)
            self.assertEqual(loaded["schema_version"], guard.SCHEMA_VERSION)
            self.assertEqual(loaded["gates"]["U1"]["status"], "pass")

    def test_report_requires_u0_through_u3(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            state = {
                "gates": {
                    name: {"status": "pass"} for name in ("U0", "U1", "U2", "U3")
                }
            }
            guard.save_state(output, state)
            args = type(
                "Args",
                (),
                {
                    "output_dir": output,
                    "latest": True,
                    "repaired": True,
                    "require_u4": False,
                },
            )()
            self.assertEqual(guard.report(args), 0)
            loaded = guard.load_state(output)
            self.assertEqual(loaded["status"], "latest-repaired-ready")
            self.assertTrue((output / "summary.md").is_file())

    def test_report_can_require_u4(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            guard.save_state(
                output,
                {
                    "gates": {
                        name: {"status": "pass"}
                        for name in ("U0", "U1", "U2", "U3")
                    }
                },
            )
            args = type(
                "Args",
                (),
                {
                    "output_dir": output,
                    "latest": True,
                    "repaired": True,
                    "require_u4": True,
                },
            )()
            self.assertEqual(guard.report(args), 1)
            self.assertFalse(guard.load_state(output)["ready"])

    def test_offline_report_cannot_claim_latest(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            guard.save_state(
                output,
                {
                    "source_mode": "offline",
                    "gates": {
                        name: {"status": "pass"}
                        for name in ("U0", "U1", "U2", "U3")
                    },
                },
            )
            args = type(
                "Args",
                (),
                {
                    "output_dir": output,
                    "latest": True,
                    "repaired": True,
                    "require_u4": False,
                },
            )()
            self.assertEqual(guard.report(args), 0)
            loaded = guard.load_state(output)
            self.assertFalse(loaded["latest"])
            self.assertEqual(loaded["status"], "last-known-good")

    def test_smoke_entrypoints_are_explicit(self):
        self.assertEqual(set(guard.SMOKE_SCRIPTS), {"two", "four", "eight"})

    def test_record_requires_nonempty_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            evidence = Path(directory) / "cargo.log"
            evidence.write_text("test result: ok\n", encoding="utf-8")
            args = type(
                "Args",
                (),
                {
                    "output_dir": output,
                    "gate": "U2",
                    "status": "pass",
                    "evidence": [evidence],
                    "reason": None,
                },
            )()
            self.assertEqual(guard.record(args), 0)
            self.assertEqual(
                guard.load_state(output)["gates"]["U2"]["status"], "pass"
            )

    def test_record_preserves_existing_gate_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            evidence = Path(directory) / "smoke.log"
            evidence.write_text("Passed: 1 / 1\n", encoding="utf-8")
            guard.save_state(
                output, {"gates": {"U3": {"status": "pass", "tier": "two"}}}
            )
            args = type(
                "Args",
                (),
                {
                    "output_dir": output,
                    "gate": "U3",
                    "status": "pass",
                    "evidence": [evidence],
                    "reason": None,
                },
            )()
            self.assertEqual(guard.record(args), 0)
            self.assertEqual(
                guard.load_state(output)["gates"]["U3"]["tier"], "two"
            )

    def test_record_rejects_empty_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            evidence = Path(directory) / "empty.log"
            evidence.touch()
            args = type(
                "Args",
                (),
                {
                    "output_dir": output,
                    "gate": "U2",
                    "status": "pass",
                    "evidence": [evidence],
                    "reason": None,
                },
            )()
            with self.assertRaises(guard.GuardFailure):
                guard.record(args)


if __name__ == "__main__":
    unittest.main()
