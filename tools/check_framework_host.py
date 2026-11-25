"""Build and check the A5 branch's Linux host entry points and data contracts.

Requires Linux, GCC, Cargo and already cached locked Rust dependencies. No NPU,
Guest, Simpler worker, daemon service or historical recovery is started.
"""
from pathlib import Path
import argparse
import json
import os
import signal
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    shim = output / "fence"
    shim.mkdir()
    env = {**os.environ, "PATH": str(Path.home() / ".cargo/bin") + os.pathsep + os.environ.get("PATH", ""),
           "CARGO_TARGET_DIR": str(output / "target"), "CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0",
           "UF_ACL_SHIM_LIB_DIR": str(shim), "LD_LIBRARY_PATH": str(shim),
           "PYTHONPATH": str(source / "sdk/python"), "PYTHONDONTWRITEBYTECODE": "1"}
    for name in ("UF_TARGET_DEVICE", "UF_EXECUTION_MODE", "UF_HBM_STARTUP_PROBE_BYTES"):
        env.pop(name, None)
    steps = []

    def run(name, command, seconds=300, expected=0):
        start = time.monotonic()
        with (output / (name + ".log")).open("xb") as log:
            child = subprocess.Popen(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT,
                                     start_new_session=True)
            try:
                code = child.wait(timeout=seconds)
            except BaseException:
                # Only this fresh command's own dedicated process group.
                if child.poll() is None:
                    os.killpg(child.pid, signal.SIGTERM)
                    try:
                        child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(child.pid, signal.SIGKILL)
                        child.wait(timeout=5)
                raise
        steps.append({"name": name, "argv": [str(x) for x in command], "exit": code,
                      "seconds": round(time.monotonic() - start, 3)})
        (output / "steps.json").write_text(json.dumps(steps, indent=2) + "\n")
        print(name, code, flush=True)
        if code != expected:
            raise RuntimeError(f"{name} exited {code}; see its log")

    run("compile-acl-fence", ["gcc", "-shared", "-fPIC", str(source / "tools/fixtures/acl_api_fence.c"),
                              "-o", str(shim / "libuf_acl_shim.so")])
    run("default-daemon-build", ["cargo", "build", "--offline", "--locked", "-p", "uf-daemon", "-j4"], 900)
    binary = output / "target/debug/uf-daemon"
    run("native-help", [str(binary), "--help"], 5)
    if "UFlow A5 supernode development baseline" not in (output / "native-help.log").read_text():
        raise RuntimeError("native entry did not expose the A5 baseline")
    endpoint = output / "unused.sock"
    run("native-needs-assignment", [str(binary), "--socket", str(endpoint)], 5, 1)
    run("legacy-a3-owner-rejected", [str(binary), "--socket", str(endpoint),
                                    "--host-pool", "--pool-provider-python", sys.executable], 5, 101)
    run("simulator-needs-config", [str(binary), "--socket", str(endpoint), "--host-pool"], 5, 1)
    if endpoint.exists():
        raise RuntimeError("a rejected entry point created a control socket")
    for index, flag in enumerate(("--pool-terminal-domain-loss", "--pool-shutdown-report",
                                  "--pool-terminal-c101-manifest")):
        command = [str(binary), flag]
        if flag.endswith("manifest"):
            command.extend(["/missing-delivery-proof", "bad-hash"])
        name = f"default-private-command-{index}"
        run(name, command, 5, 1)
        if "no recovery IO performed" not in (output / (name + ".log")).read_text():
            raise RuntimeError("default recovery command did not fail at the feature boundary")
    run("core-agent-tests", ["cargo", "test", "--offline", "--locked", "-p", "uf-core", "-p", "uf-ub-sim-agent", "-j4"], 300)
    for group in ("control_shutdown_tests", "transfer_executor::monitor_tests", "ssd_direct::tests",
                  "providers::registry::tests", "ddr_resources::tests", "pool_ranges::tests",
                  "managed_pool::tests", "mem_service_pool::tests", "managed_read_cache::tests",
                  "managed_write::tests", "pool_experiments::tests"):
        run(group.split("::")[0], ["cargo", "test", "--offline", "--locked", "-p", "uf-daemon", "-j4", group], 300)
    run("experimental-compatibility-build", ["cargo", "check", "--offline", "--locked", "-p", "uf-daemon",
                                            "--features", "experimental-reconciliation", "-j4"], 300)
    sdk_env = output / "sdk-venv"
    run("sdk-venv", [sys.executable, "-B", "-m", "venv", "--without-pip", str(sdk_env)], 120)
    run("external-ddr-sdk", [str(sdk_env / "bin/python"), "-B", "examples/ddr_v0/test_t34_sdk.py"], 120)
    run("monitor-host-tests", [sys.executable, "-B", "-m", "unittest", "discover",
                               "-s", "tools/tests", "-p", "test_uflow_monitor_api.py"], 120)
    (output / "result.json").write_text(json.dumps({"pass": True, "CPU_only": True,
        "physical_device_or_Guest_started": False, "historical_recovery_executed": False,
        "steps": steps}, indent=2) + "\n")


if __name__ == "__main__":
    main()
