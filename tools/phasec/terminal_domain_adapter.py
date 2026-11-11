"""C consumer of independently accepted E source; no ledger operations here.

Rust embeds this source and the external manager pins. Each invocation uses a
fresh isolated interpreter; every E module is verified before the first exec.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import types

ROOT = Path('/workspace/uflow/runs/mem-service-shared-pool-20261108')
PIN_SHA = 'b793a03e31b41709ba7002a8305eb83b06efaa4f114997f67307760597d6fdb8'
ORDER = ('uflow_qemu_evidence', 'uflow_qemu_metadata', 'uflow_qemu_fixed_launch', 'uflow_qemu_domain')


def require(value, message):
    if not value:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode()


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON key')
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))


def read(path, maximum=4 << 20, mode=None):
    path = Path(path)
    require(path.is_absolute() and path.resolve(strict=True) == path and 'fm' not in path.parts,
            'normalized ordinary evidence path required')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC)
    with os.fdopen(fd, 'rb') as stream:
        first = os.fstat(stream.fileno())
        require(stat.S_ISREG(first.st_mode) and first.st_uid == os.geteuid() and first.st_nlink == 1
                and 0 < first.st_size <= maximum, 'bounded single-link owned evidence required')
        require(mode is None or stat.S_IMODE(first.st_mode) == mode, 'source mode drift')
        raw = stream.read(maximum + 1)
        last = os.fstat(stream.fileno())
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns, s.st_nlink)
    require(identity(first) == identity(last) == identity(path.lstat()) and len(raw) == first.st_size,
            'evidence changed while read')
    return raw


def reference(row):
    raw = read(row['path'])
    require(len(raw) == row['bytes'] and hashlib.sha256(raw).hexdigest() == row['sha256'], 'external evidence pin drift')
    return raw


def load_modules(pins):
    require(not any(name in sys.modules for name in ORDER), 'preexisting E module cache rejected')
    bodies = {}
    for name in ORDER:
        row = pins['modules'][name + '.py']
        require(Path(row['remote']) == ROOT / 'implementation/qemu-domain-v1/source-r009/scripts' / (name + '.py'),
                'accepted E source location required')
        raw = read(row['remote'], 128 << 10, 0o755)
        require(hashlib.sha256(raw).hexdigest() == row['sha256'], 'accepted E source hash drift: ' + name)
        bodies[name] = raw
    for name in ORDER:
        module = types.ModuleType(name)
        module.__file__ = pins['modules'][name + '.py']['remote']
        sys.modules[name] = module
        exec(compile(bodies[name], module.__file__, 'exec'), module.__dict__)
    return sys.modules['uflow_qemu_domain'], sys.modules['uflow_qemu_metadata']


def verify_current(pins):
    # Reference checks supplement, rather than replace, E's full fresh predicate.
    for key in ('producer_source_manifest', 'candidate', 'domain_plan', 'companion_plan'):
        reference(pins[key])
    domain, _ = load_modules(pins)
    row = pins['candidate']
    candidate = domain.validate_candidate(row['path'], row['sha256'], for_current_domain=True)
    require(candidate['candidate_id'] == pins['candidate_id'] and candidate['applies_to_current_domain'] is True
            and candidate['current_eligible_capacity_bytes'] == 52 << 20
            and candidate['only_eligible_resource_ids'] == [110, 111, 112]
            and candidate['state_sha256'] == pins['current_state_sha256'], 'exact current c044 eligibility required')
    return {'schema': 1, 'kind': 'EValidatedTerminalDomainProofV1', 'consumer_pins_sha256': PIN_SHA,
            'producer_commit': pins['producer_commit'], 'validated_candidate': candidate,
            'evidence_references': {key: pins[key] for key in
                                    ('candidate', 'domain_plan', 'producer_source_manifest', 'companion_plan')},
            'verified_module_sources': pins['modules']}


def execution_context(request, pins):
    """Explicit resolved plan plus actual E0 ancestry, before E validation/locks.

    This is an execution binding, not a ready flag or a human-approval oracle.
    The final plan and its external digest must be reviewed before invocation.
    """
    row = request['execution_manifest']
    path = Path(row['path'])
    require(path.name == 'execution.json' and path.parent.parent == ROOT / 'implementation'
            and re.fullmatch(r'config-terminal-c[0-9]{3}-v[1-9][0-9]{0,2}', path.parent.name),
            'fixed resolved execution manifest required in dedicated task namespace')
    plan = decode(reference(row))
    require(plan['schema'] == 1 and plan['kind'] == 'C044TerminalResolvedExecutionV1'
            and plan['consumer_pins_sha256'] == PIN_SHA and plan['state_sha256'] == pins['current_state_sha256']
            and plan['candidate'] == pins['candidate'] and plan['original_authority'] == str(ROOT / 'implementation/responsibility-c013')
            and plan['eligible_resource_ids'] == [110, 111, 112] and plan['terminal_bytes'] == 52 << 20
            and plan['protected_bytes'] == 76 << 20, 'exact resolved c044 transaction scope required')
    action = request['execution_action']
    require(action in ('terminal-only', 'successor'), 'explicit execution phase required')
    require(set(plan['phases']) == {'terminal-only','successor'}, 'exact terminal and successor phases required')
    terminal = plan['phases']['terminal-only']['job']
    successor = plan['phases']['successor']['job']
    require(re.fullmatch(r'c[0-9]{3}', terminal) and re.fullmatch(r'c[0-9]{3}', successor)
            and terminal != successor and path.parent.name.startswith('config-terminal-' + terminal + '-v'),
            'unique manifest-bound phase jobs required')
    for name, item in plan['phases'].items():
        require(item['config']['path'] == str(path.parent / (item['job'] + '.json')), 'phase config outside resolved namespace')
        configured = decode(reference(item['config']))
        require(configured['root'] == str(ROOT) and configured['job'] == item['job']
                and configured['kind'] == ('build' if name == 'terminal-only' else 'guest'), 'phase job/kind binding mismatch')
    phase = plan['phases'][action]
    job = phase['job']
    config = decode(reference(phase['config']))
    require(config['root'] == str(ROOT) and config['job'] == job and config['kind'] == ('build' if action == 'terminal-only' else 'guest'),
            'resolved E0 job identity mismatch')
    exe = Path(request['daemon_path'])
    require(str(exe) == plan['daemon']['path'] and hashlib.sha256(read(exe, 128 << 20)).hexdigest() == plan['daemon']['sha256'],
            'reviewed executable required')
    # The bridge child and daemon must be descendants of the actual session
    # leader owned by the independently pinned supervisor for this exact job.
    monitor = ROOT / 'runs' / job / 'monitor'
    launch = decode(read(monitor / 'launch-once.json'))
    require(launch['argv'][-2:] == ['--actor', phase['config']['path']], 'wrong E0 actor context')
    def proc(pid):
        text = Path('/proc/%d/stat' % pid).read_text()
        tail = text[text.rfind(')') + 2:].split()
        return int(tail[1]), int(tail[3]), int(tail[19])
    parent, session, _ = proc(os.getpid())
    require(proc(launch['supervisor_pid'])[2] == launch['supervisor_birth'], 'supervisor birth changed')
    leader_parent, leader_session, _ = proc(session)
    require(leader_parent == launch['supervisor_pid'] and leader_session == session, 'dedicated E0 session required')
    ancestors = []
    for _ in range(16):
        ancestors.append(parent)
        if parent == session:
            break
        parent, child_session, _ = proc(parent)
        require(child_session == session, 'execution escaped E0 session')
    require(session in ancestors and request['daemon_pid'] in ancestors, 'daemon outside supervised ancestry')
    require(os.readlink('/proc/%d/exe' % request['daemon_pid']) == str(exe), 'daemon executable identity changed')
    close = int(os.environ['UFLOW_ENV_CLOSE_FD'])
    require(stat.S_ISFIFO(os.fstat(close).st_mode), 'actual inherited supervisor close pipe required')
    return {'execution_manifest_sha256': row['sha256'], 'phase': action, 'job': job}


def main(request):
    raw = request['consumer_pins'].encode()
    require(hashlib.sha256(raw).hexdigest() == PIN_SHA, 'external consumer pins drift')
    pins = decode(raw)
    action = request['action']
    if action == 'execution-context':
        return execution_context(request, pins)
    require(action == 'verify-current', 'read-only adapter action required')
    return verify_current(pins)


if __name__ == '__main__':
    try:
        raw = sys.stdin.buffer.read((64 << 10) + 1)
        require(len(raw) <= 64 << 10, 'bounded adapter request required')
        print(canonical(main(decode(raw))).decode())
    except Exception as error:
        sys.stderr.write('REJECT_C_TERMINAL_ADAPTER: ' + str(error)[:1200] + '\n')
        raise SystemExit(2)
