#!/usr/bin/env python3
"""Finite calibration wait, no automatic kill and no broker/authority control.

The scheduled task budget must already be effective. This helper neither grants
that budget nor changes it. The deployment driver owns normal broker shutdown
AFTER calibration cleanup, plus evidence/lock/socket audits.
"""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
import signal
import subprocess
import sys
import time


def supervise(command, run_seconds, cleanup_seconds, stop_file, log_path, result_path):
    assert not stop_file.exists() and not log_path.exists() and not result_path.exists()
    started = time.monotonic()
    stop_requested = False
    def request_stop(signum=None, frame=None):
        nonlocal stop_requested
        if not stop_requested:
            with stop_file.open('x') as file:
                file.write('cooperative stop: finish issued work, no new rounds\n')
            stop_requested = True
    handlers = {sig: signal.signal(sig, request_stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    proc = None
    result = {'status': 'failed', 'argv': command, 'run_seconds': run_seconds,
              'cleanup_seconds': cleanup_seconds, 'broker_stop_owned_by': 'deployment driver'}
    try:
        # Popen, not run(timeout=...) or a Popen context manager: neither an
        # elapsed deadline nor supervisor exit may kill a holder/worker.
        with log_path.open('x') as log:
            proc = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log,
                                    stderr=subprocess.STDOUT, start_new_session=True)
            result['pid'] = proc.pid
            while proc.poll() is None:
                elapsed = time.monotonic() - started
                if elapsed >= run_seconds:
                    request_stop()
                if elapsed >= run_seconds + cleanup_seconds:
                    result.update(error='cooperative cleanup deadline exceeded; process preserved, manual attention required',
                                  process_still_running=True)
                    return 2
                time.sleep(.1)
            result.update(exit=proc.returncode, process_still_running=False,
                          status='pass' if proc.returncode == 0 and not stop_requested else 'failed')
            return 0 if result['status'] == 'pass' else (proc.returncode or 1)
    finally:
        result.update(stop_requested=stop_requested, elapsed_seconds=time.monotonic() - started)
        with result_path.open('x') as file:
            json.dump(result, file, indent=2); file.write('\n')
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--task-budget-seconds', required=True, type=float)
    parser.add_argument('--run-seconds', required=True, type=float)
    parser.add_argument('--cleanup-seconds', type=float, default=900)
    parser.add_argument('--log', required=True, type=Path)
    parser.add_argument('--result', required=True, type=Path)
    parser.add_argument('--stop-file', required=True, type=Path)
    parser.add_argument('calibration_args', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    assert all(math.isfinite(n) and n > 0 for n in (
        args.task_budget_seconds, args.run_seconds, args.cleanup_seconds))
    assert args.cleanup_seconds >= 900
    # Caller accounts for already-spent setup/verification time separately.
    assert args.run_seconds + args.cleanup_seconds <= args.task_budget_seconds
    extra = args.calibration_args
    if extra and extra[0] == '--':
        extra = extra[1:]
    assert not any(option == flag or option.startswith(flag + '=') for option in extra
                   for flag in ('--soft-deadline-seconds', '--stop-file', '--worker', '--attempt-log'))
    command = [sys.executable, str(Path(__file__).with_name('t35_calibrate.py')),
               '--soft-deadline-seconds', str(args.run_seconds), '--stop-file', str(args.stop_file), *extra]
    return supervise(command, args.run_seconds, args.cleanup_seconds, args.stop_file, args.log, args.result)


if __name__ == '__main__':
    sys.exit(main())
