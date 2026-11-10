"""Private HOST profile seams only; no real accepted release or Guest boot."""
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import types
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import qemu_profile_gate as gate


class ProductionTests(unittest.TestCase):
    def test_unaccepted_selection_never_reads_or_executes_or_falls_back(self):
        for selection in ({'ready':True},None,{'validator':'caller.py','sha256':'0'*64}):
            with patch.object(gate,'_read',side_effect=AssertionError('no profile IO')):
                with self.assertRaisesRegex(ValueError,'not-ready'):gate.launch_qemu_gate(gate.ROOT,{'qemu_profile':selection,'qemu_binary':str(gate.LEGACY_BINARY)})

    def test_caller_hash_or_host_identity_cannot_enter_production_capture(self):
        for value in ({'binary':str(gate.LEGACY_BINARY),'sha256':gate.LEGACY_SHA},gate._host_fixture_identity(gate.LEGACY_BINARY,gate.LEGACY_SHA)):
            with self.assertRaisesRegex(ValueError,'production-validated'):gate.capture_identity({'qemu_binary':str(gate.LEGACY_BINARY)},value)

    def test_legacy_branch_remains_exact_fixed_path(self):
        with patch.object(gate,'_read',side_effect=AssertionError('no arbitrary executable read')):
            with self.assertRaisesRegex(ValueError,'original exact'):gate.launch_qemu_gate(gate.ROOT,{'qemu_binary':'/caller/other'})


@unittest.skipUnless(sys.platform.startswith('linux'),'Linux ELF HOST fixtures')
class ProfileTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name).resolve()
        def ref(name,raw):
            p=self.root/name;p.write_bytes(raw)
            return {'path':str(p),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
        self.ref=ref
        binary=ref('cpu-executable',Path(sys.executable).resolve().read_bytes())
        members={name:ref(name+'.json',json.dumps({'CPU_fixture':name}).encode()) for name in ('patch','source_closure','object_closure','link_closure')}
        manifest=ref('profile-manifest.json',b'{"CPU_fixture_only":true}')
        self.result={'schema':1,'interface':gate.INTERFACE,'profile_id':gate.PROFILE,'status':'PASS_HOST_QEMU_PROFILE_ONLY',
            'producer_manifest_sha256':manifest['sha256'],'base_qemu_commit':gate.BASE_COMMIT,'base_binary_sha256':gate.LEGACY_SHA,
            'patch_sha256':members['patch']['sha256'],'qemu_binary':binary['path'],'qemu_binary_sha256':binary['sha256'],
            **{k:members[k] for k in ('source_closure','object_closure','link_closure')},'runtime_tested':False,'authority_to_boot_or_replace_existing_binary':False}
        self.anchor={'manifest':manifest,'binary':binary,'members':members,'dependencies':{}}
        self.validator(self.result)
        self.config={'qemu_profile':gate._selection(self.anchor),'qemu_binary':binary['path']}
    def validator(self,result,prefix=''):
        raw=(prefix+'\ndef validate(manifest_path,external_sha,qemu_binary_path):\n    return '+repr(result)+'\n').encode()
        self.anchor['validator']=self.ref('uflow_qemu_profile.py',raw)
        if hasattr(self,'config'):self.config['qemu_profile']=gate._selection(self.anchor)
    def tearDown(self):self.temp.cleanup()
    def validate(self):return gate._host_validate_profile(self.root,self.config,self.anchor)
    def test_actual_bound_sources_execute_fresh_and_provenance_is_host_only(self):
        stale=types.ModuleType('uflow_reviewed_qemu_profile');stale.validate=lambda *a: (_ for _ in ()).throw(AssertionError('stale cache'))
        with patch.dict(sys.modules,{'uflow_reviewed_qemu_profile':stale}):
            value=self.validate();self.assertIs(sys.modules['uflow_reviewed_qemu_profile'],stale)
        self.assertEqual(value.binary,Path(self.anchor['binary']['path']))
        self.assertTrue(value.metadata()['HOST_fixture_only']);self.assertFalse(value.metadata()['boot_authority'])
        with self.assertRaises(ValueError):gate.capture_identity(self.config,value)
    def test_all_dependency_hashes_checked_before_first_validator_exec(self):
        marker=self.root/'must-not-execute'
        self.validator(self.result,"from pathlib import Path\nPath("+repr(str(marker))+").write_text('bad')")
        dep=self.ref('uflow_dependency.py',b'value=1\n');self.anchor['dependencies']={'uflow_dependency':dep}
        Path(dep['path']).write_bytes(b'value=2\n')
        with self.assertRaisesRegex(ValueError,'digest'):self.validate()
        self.assertFalse(marker.exists())
    def test_source_manifest_binary_and_member_drift_rejected(self):
        for ref in (self.anchor['validator'],self.anchor['manifest'],self.anchor['binary'],*self.anchor['members'].values()):
            path=Path(ref['path']);old=path.read_bytes();path.write_bytes(old+b'changed')
            try:
                with self.assertRaises(ValueError):self.validate()
            finally:path.write_bytes(old)
    def test_wrong_result_status_identity_closure_or_authority_rejected(self):
        for key,value in [('status','PASS'),('runtime_tested',True),('authority_to_boot_or_replace_existing_binary',True),
                          ('qemu_binary','/wrong'),('qemu_binary_sha256','0'*64),('base_binary_sha256','0'*64),('patch_sha256','0'*64),('source_closure',{})]:
            changed=copy.deepcopy(self.result);changed[key]=value;self.validator(changed)
            with self.subTest(key=key),self.assertRaises(ValueError):self.validate()
    def test_requested_member_has_no_legacy_fallback(self):
        self.config['qemu_binary']=str(gate.LEGACY_BINARY)
        with self.assertRaisesRegex(ValueError,'member mismatch'):self.validate()


if __name__=='__main__':unittest.main()
