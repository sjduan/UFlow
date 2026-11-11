"""Bounded public metadata CLI; finalize is an E-only local lifecycle action."""
import argparse
import json
from pathlib import Path

from session_protocol import request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('op', choices=('describe', 'register-client', 'line-retired', 'finalize'))
    parser.add_argument('--line', choices=('O1', 'M1'))
    parser.add_argument('--client-session', action='append', default=[])
    parser.add_argument('--owner-root', type=Path)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    values = {}
    if args.op == 'finalize':
        status = request(manifest, 'describe')
        from session_protocol import check_final, pool_record
        check_final(pool_record(status['snapshot'], manifest['pool_descriptor']), status['lines'], manifest['bootstrap_client'])
        if args.owner_root is None or str(args.owner_root.resolve()) != manifest['owner_root']:
            raise ValueError('E must name the exact original private owner root')
        (args.owner_root / 'finalize.request').touch(exist_ok=False)
        print(json.dumps({'finalize_requested': True, 'completion_requires': 'finalized.json and actual owner exit'}))
        return
    if args.op != 'describe':
        if args.line is None:
            raise ValueError('line required')
        values['line'] = args.line
        if args.op == 'register-client':
            if len(args.client_session) != 1:
                raise ValueError('one real client session required')
            values['client_session'] = args.client_session[0]
        else:
            values['client_sessions'] = args.client_session
    print(json.dumps(request(manifest, args.op, **values), indent=2))


if __name__ == '__main__':
    main()
