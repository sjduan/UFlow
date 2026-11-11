"""Register exact post-run export metadata through the accepted E API.

All requests/registries/receipts are new C-owned data, never E source or an old
registry. Directory enumeration discovers candidates; only full E validation
admits a file. Unknown extras remain fatal to the exact-union companion audit.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat

import terminal_domain_adapter as adapter
from pool_journal_capture import capture


def accepted():
    path = Path(__file__).resolve().parents[2] / 'examples/mem_service_pool/terminal_domain/accepted-E-consumer-pins.json'
    raw = adapter.read(path, 64 << 10)
    adapter.require(hashlib.sha256(raw).hexdigest() == adapter.PIN_SHA, 'accepted external consumer pins required')
    pins = adapter.decode(raw)
    _, metadata = adapter.load_modules(pins)
    return pins, metadata, __import__('uflow_qemu_evidence')


def scope(directory):
    directory = Path(directory)
    base = adapter.ROOT / 'implementation/qemu-domain-v1/consumer-phase-c'
    adapter.require(directory.is_relative_to(base) and directory.parent == base and directory.resolve() == directory,
                    'new single C consumer data namespace required')
    return directory


def save(ev, path, value):
    ev.immutable_json(path, value)
    raw, _ = ev.read(path)
    return {'path': str(path), 'bytes': len(raw), 'sha256': ev.digest(raw)}


def journal_diagnostics(run, reader):
    """Inspect both bounded serial sources before enforcing strict admission.

    Capture completeness describes transport only. E still independently
    validates frames and publication; this diagnostic grants no registration.
    """
    rows, captured = {}, {}
    for node in ('nodeA', 'nodeB'):
        path = Path(run) / (node + '.serial.log')
        row = {'path': str(path), 'complete': False}
        try:
            raw, _ = reader(path, 64 << 20)
        except FileNotFoundError:
            row.update(status='missing-serial-file')
        except (OSError, RuntimeError, ValueError) as error:
            row.update(status='unavailable-serial-file', detail=str(error)[:512])
        else:
            source = {'path': str(path), 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}
            row['source'] = source
            try:
                text = raw.decode('ascii')
            except UnicodeDecodeError:
                row.update(status='non-ascii-serial')
            else:
                summary, data = capture(text)
                row.update(summary)
                captured[node] = (source, text, data)
        rows[node] = row
    return {'schema': 1, 'kind': 'per-node-journal-completeness-v1', 'nodes': rows,
            'all_serial_captures_complete': all(r['complete'] for r in rows.values()),
            'E_registration_or_publication_proven': False, 'SDK_ACK_or_replay_claimed': False,
            'ledger_mutation': False}, captured


def register_and_audit(directory, requests, original_carriers, births, endpoints, modules):
    """Shared actual API path for prepared future runs and HOST fixtures."""
    pins, md, ev = modules
    directory = scope(directory)
    adapter.require(directory.is_dir(), 'prepared C data directory required')
    helper = ev.document(pins['domain_plan'])['fixed_bindings']['physical_helper']['path']
    # Check the complete list before the first append; append itself rechecks.
    entries = [md.validate_registration(row['path'], row['sha256'], helper) for row in requests]
    adapter.require(len({entry['file']['path'] for entry in entries}) == len(entries), 'duplicate metadata request')
    head = '0' * 64
    registry = directory / 'registry'
    receipts = []
    for entry in entries:
        receipt = md.append_registration(registry, head, entry)
        receipts.append(receipt)
        head = receipt['head_sha256']
    plan = {'schema': 1, 'interface': ev.INTERFACE, 'kind': 'readonly-companion-plan-v1',
            'original_carriers': original_carriers, 'registry_directory': str(registry),
            'registry_head_sha256': head, 'helper_path': helper, 'births': births, 'control_endpoints': endpoints}
    reference = save(ev, directory / 'companion-plan.json', plan)
    result = md.companion_audit(reference['path'], reference['sha256'])
    save(ev, directory / 'companion-result.json', result)
    save(ev, directory / 'registration-receipts.json', {'schema': 1, 'receipts': receipts, 'requests': requests,
         'head_sha256': head, 'normal_native_or_SDK_release_ACK_proven': False, 'ledger_mutation': False})
    return result


def complete_run(run, directory,*,qemu_identity=None,runtime_context=None):
    """Called only after the actual future group has reaped its QEMU children.

    The original FM carrier inventory must have been captured before business.
    Dynamic metadata requires real publication, QEMU event and serial journal.
    """
    if qemu_identity is not None:
        from profile_metadata_gate import complete_profile_run
        return complete_profile_run(run,directory,qemu_identity,runtime_context=runtime_context)
    run = Path(run)
    adapter.require(run.parent == adapter.ROOT / 'runs' and re.fullmatch(r'c[0-9]{3}',run.name)
                    and int(run.name[1:])>=63 and run.resolve() == run, 'new reviewed task run only; old c044 stays readonly')
    directory = scope(directory)
    directory.mkdir(parents=True, exist_ok=False)
    modules = accepted()
    pins, md, ev = modules
    diagnostics, captured = journal_diagnostics(run, ev.read)
    save(ev, directory / 'journal-completeness.json', diagnostics)
    adapter.require(diagnostics['all_serial_captures_complete'],
                    'complete actual worker serial journal required; see per-node journal-completeness.json')
    template = ev.document(pins['domain_plan'])
    graph = ev.document(template['source_graph'])
    def ref(path, maximum=64 << 20):
        raw, _ = ev.read(path, maximum)
        return {'path': str(path), 'bytes': len(raw), 'sha256': ev.digest(raw)}
    starts_ref, identities_ref = ref(run / 'guest-starts.json'), ref(run / 'guest-identities.json')
    starts = ev.decode(ev.bound(starts_ref))
    controls = ref(run / 'pool-events.jsonl', 16 << 20)
    control_rows = ev.bound(controls, 16 << 20).splitlines()
    bindings = []
    for index, line in enumerate(control_rows):
        row = ev.decode(line)
        if row.get('event') == 'lower-control' and row.get('operation') == 122 and row.get('status') == 0:
            bindings.append((index, md.descriptor_binding(controls, index)))
    journals, captures, logs, qemu_lines = {}, {}, {}, {}
    for node in ('nodeA', 'nodeB'):
        serial, text, raw = captured[node]
        capture_ref = {'format': 'captured-serial-v1', 'source': serial, 'bytes': len(raw), 'sha256': ev.digest(raw)}
        frames = md.journal_frames(capture_ref)
        end_line = next(line for line in text.splitlines() if line.startswith('UFPOOL_JOURNAL_END '))
        end = dict(word.split('=', 1) for word in end_line.split()[1:])
        journals[node] = {'capture_end': end, 'bytes': len(raw), 'sha256': ev.digest(raw), 'frames': frames}
        captures[node] = capture_ref
        logs[node] = ref(run / (node + '.qemu.log'))
        qemu_lines[node] = ev.bound(logs[node], 64 << 20).decode('ascii').splitlines()
    journal_ref = save(ev, directory / 'journal-summary.json', {'schema': 1, 'journals': journals})
    requests = []
    count = 0
    for kind, folder in (('bootstrap', 'obmm_bootstrap'), ('retired', 'obmm_retired')):
        root = run / 'fm' / folder
        if not root.exists():
            continue
        adapter.require(root.is_dir() and root.resolve() == root, 'metadata directory alias rejected')
        for path in sorted(root.iterdir()):
            count += 1
            adapter.require(count <= 128 and not path.is_symlink(), 'bounded exact metadata candidates required')
            info = path.lstat()
            adapter.require(stat.S_ISREG(info.st_mode) and info.st_uid == os.geteuid() and 0 < info.st_size <= 4096
                            and '.tmp.' not in path.name, 'partial/unknown metadata is not admitted')
            # Read only these two source-defined metadata schemas, never rings.
            raw, identity = ev.read(path, 4096, metadata=True)
            fields = md.parse_metadata(raw, kind)
            adapter.require(path == run / 'fm' / md.relative_name(kind, fields), 'exact metadata filename required')
            matches = [(line, b) for line, b in bindings if all(b['fields'][key] == value for key, value in fields.items())]
            adapter.require(len(matches) == 1, 'unique actual successful worker publication required')
            line, binding = matches[0]
            node = binding['node']
            owner = next(row for row in starts if row['node'] == node)
            helper = ev.frozen_helper(template['fixed_bindings']['physical_helper']['path'])
            file_identity = {**identity, 'path': str(path), 'content_sha256': ev.digest(raw),
                             'kernel_inode_aliases': helper.file_identity_aliases(path, identity)}
            request = {'schema': 1, 'interface': ev.INTERFACE, 'kind': 'metadata-registration-request-v1',
                       'run': run.name, 'fm_root': str(run / 'fm'), 'metadata_kind': kind, 'file_identity': file_identity,
                       'qemu_commit': ev.QEMU_COMMIT, 'qemu_binary_sha256': ev.FIXED['qemu_binary'],
                       'writer_source_sha256': md.SOURCE_SHA, 'writer_source': graph['files']['hw/ub/ub_ubc.c'],
                       'fixed_bindings': template['fixed_bindings'], 'guest_starts': starts_ref,
                       'guest_identities': identities_ref, 'guest_node': node, 'guest_boot_id': binding['boot_id'],
                       'guest_incarnation': binding['incarnation'], 'qemu_owner': {'pid': owner['pid'], 'birth': owner['birth']},
                       'control_log': controls, 'control_line': line, 'qemu_log': logs[node],
                       'worker_stdout': ref(run / ('worker-' + node + '.stdout.log'), 1 << 20),
                       'journal_capture': captures[node], 'journal_summary': journal_ref}
            # Only the E validator can choose the matching successful event.
            candidates = []
            for number, text in enumerate(qemu_lines[node]):
                prefix = 'SIM_DEC: OBMM bootstrap publish ' if kind == 'bootstrap' else 'SIM_DEC: OBMM export retired '
                if not text.startswith(prefix):
                    continue
                proposed = {**request, 'qemu_line': number}
                try:
                    md.verify_publication(proposed, fields)
                except (ev.EvidenceError, KeyError, ValueError):
                    continue
                candidates.append(proposed)
            adapter.require(len(candidates) == 1, 'unique fully validated actual QEMU export event required')
            requests.append(save(ev, directory / ('request-%03d.json' % count), candidates[0]))
    adapter.require(requests, 'no actual dynamic metadata registered; cannot pretend complete union')
    births = {str(row['pid']): row['birth'] for row in starts}
    endpoints = ev.decode(ev.bound(ref(run / 'control-endpoints-active.json')))
    return register_and_audit(directory, requests, ref(run / 'fm-carrier-active.json'), births, endpoints, modules)
