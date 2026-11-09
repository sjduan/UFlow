"""Actual accepted metadata APIs through C's completion path, CPU fixtures only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import types
import unittest
import pool_run_metadata as consumer


def main(directory):
    directory=consumer.scope(directory);directory.mkdir(parents=True,exist_ok=False)
    modules=consumer.accepted();pins,md,ev=modules
    source=consumer.adapter.ROOT/'implementation/qemu-domain-v1/source-r009/tests/test_uflow_qemu_domain.py'
    raw=consumer.adapter.read(source,128<<10)
    if hashlib.sha256(raw).hexdigest()!='a3616331c9f482f82b34ce862fef80e692dd61e94303486dbc0a663b7057bfb8':raise ValueError('accepted HOST fixture helper changed')
    fixture=types.ModuleType('accepted_E_CPU_fixture');fixture.__file__=str(source)
    exec(compile(raw,str(source),'exec'),fixture.__dict__)
    os.environ['UFLOW_QEMU_DOMAIN_BASE_PLAN']=pins['domain_plan']['path']
    os.environ['UFLOW_QEMU_DOMAIN_TEST_ROOT']=str(directory/'fixtures')
    fixture.LinuxPrivateDomain.setUpClass()
    count=0
    for kind in ('bootstrap','retired'):
        case=fixture.LinuxPrivateDomain('test_both_metadata_schemas_full_publication_and_CLI_retry')
        case.setUp()
        request,helper=case.metadata_fixture(kind)
        request_ref=case.save(request)
        destination=consumer.scope(directory.parent/(directory.name+'-'+kind));destination.mkdir()
        original=case.save({'schema':1,'files':[]})
        result=consumer.register_and_audit(destination,[request_ref],original,{},[],modules)
        assert result['exact_union_files']==1 and result['validated_dynamic_metadata']==1
        assert result['status']=='PASS_READONLY_COMPANION_AUDIT_ONLY' and result['normal_native_or_SDK_release_ACK_proven'] is False
        # Exact retry only via E's idempotence semantics, no duplicate record.
        entry=md.validate_registration(request_ref['path'],request_ref['sha256'],helper)
        retry=md.append_registration(destination/'registry',result['registry_head_sha256'],entry)
        assert retry['idempotent'] and len(md.registry_history(destination/'registry'))==1
        # An unregistered extra must still fail the real exact-union audit.
        extra=Path(request['fm_root'])/'unexpected';extra.write_bytes(b'CPU FIXTURE ONLY')
        plan=consumer.adapter.decode((destination/'companion-plan.json').read_bytes())
        ref=case.save(plan)
        try:md.companion_audit(ref['path'],ref['sha256'])
        except ev.EvidenceError:pass
        else:raise AssertionError('unknown metadata extra admitted')
        extra.unlink();count+=3
        # A fresh fixture root for the second schema.
        fixture.LinuxPrivateDomain.root=directory/'fixtures'/kind
        fixture.LinuxPrivateDomain.root.mkdir()
    consumer.save(ev,directory/'results.json',{'pass':True,'cases':count,'actual_E_API':True,
        'both_bootstrap_and_retired':True,'exact_retry_and_unknown_extra_rejection':True,
        'CPU_fixture_only':True,'original_c044_or_registry_or_ledger_modified':False})
    print(json.dumps({'status':'PASS_C_METADATA_CONSUMER_CPU','cases':count}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--directory',type=Path,required=True);main(p.parse_args().directory)
