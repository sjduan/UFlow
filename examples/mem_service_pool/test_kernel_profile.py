"""Synthetic Image/profile fixtures; no actual producer/runtime PASS claim."""
import copy,gzip,json,os,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import kernel_profile_gate as g
class KernelProfile(unittest.TestCase):
    def setUp(self):
        allowed=Path(os.environ.get('UFLOW_POOL_CPU_TMP_ROOT',tempfile.gettempdir())).resolve()
        self.tmp=tempfile.TemporaryDirectory(dir=allowed);self.root=Path(self.tmp.name).resolve()
        self.assertEqual(self.root.parent,allowed);self.addCleanup(self.tmp.cleanup)
        self.before=''.join(k+'=y\n' for k in g.REQUIRED if k!='CONFIG_UB_UMMU_SVA_SEPARATED_PAGES')+'# CONFIG_UB_UMMU_SVA_SEPARATED_PAGES is not set\n'
        self.after=self.before.replace('# CONFIG_UB_UMMU_SVA_SEPARATED_PAGES is not set','CONFIG_UB_UMMU_SVA_SEPARATED_PAGES=y')
        self.image=lambda raw:b'SYNTHETIC-CPU-IMAGE\0IKCFG_ST'+gzip.compress(raw)+b'IKCFG_ED'
        self.base=self.root/'base';self.base.write_bytes(self.image(self.before.encode()))
        files={'Image':self.image(self.after.encode()),'.config':self.after.encode(),'vmlinux':b'\x7fELF'+bytes(14)+bytes([183,0])+bytes(12),
            'System.map':b'ffffffc000001000 T ummu_sva_matt_map\n','source-manifest.json':json.dumps({'kernel_commit':g.KERNEL,'files':{'synthetic':'CPU-fixture-only'}}).encode()}
        self.outputs={}
        for n,b in files.items():
            p=self.root/n;p.write_bytes(b);self.outputs[n]={'path':str(p),'bytes':len(b),'sha256':g.sha(b)}
        self.m={'schema':1,'interface':'mem-service-private-kernel-profile-v1','profile_id':'mem-service-gsva-separated-v1','kernel_commit':g.KERNEL,
            'base_Image_sha256':g.sha(self.base.read_bytes()),'base_embedded_config_sha256':g.sha(self.before.encode()),
            'resolved_config_diff':{'CONFIG_UB_UMMU_SVA_SEPARATED_PAGES':{'before':'n','after':'y'}},'source_manifest_sha256':self.outputs['source-manifest.json']['sha256'],
            'compiler_and_build_argv':[['CPU-synthetic-not-a-build']], 'outputs':self.outputs,'embedded_config_sha256':g.sha(self.after.encode()),
            'required_builtin_config':list(g.REQUIRED),'real_symbol_and_obmm_call_evidence':{'synthetic-fixture':True},'old_baseline_unchanged':True,'runtime_tested':False}
        self.manifest=self.root/'manifest.json'
    def check(self):
        self.manifest.write_text(json.dumps(self.m))
        with patch.object(g,'BASE',g.sha(self.base.read_bytes())),patch.object(g,'BASE_CONFIG',g.sha(self.before.encode())):
            return g.check(self.root/'Image',self.manifest,g.sha(self.manifest.read_bytes()),self.base,allowed_root=self.root)
    def test_actual_embedded_config_and_link_symbol(self):self.assertFalse(self.check()['runtime_tested'])
    def test_disabled_old_Image_refused_even_with_manifest_ready_flag(self):
        (self.root/'Image').write_bytes(self.base.read_bytes());self.m['ready']=True
        with self.assertRaises(ValueError):self.check()
    def test_wrong_version_profile_source_provenance_or_runtime_claim(self):
        for key,value in [('schema',2),('profile_id','wrong'),('kernel_commit','wrong'),('source_manifest_sha256','0'*64),('runtime_tested',True),('old_baseline_unchanged',False)]:
            old=self.m[key];self.m[key]=value
            with self.assertRaises(ValueError):self.check()
            self.m[key]=old
    def test_missing_required_config_and_output(self):
        self.m['required_builtin_config']=list(g.REQUIRED[:-1])
        with self.assertRaises(ValueError):self.check()
        self.m['required_builtin_config']=list(g.REQUIRED);self.m['outputs'].pop('vmlinux')
        with self.assertRaises(ValueError):self.check()
    def test_actual_file_hash_and_selected_path_membership(self):
        (self.root/'System.map').write_bytes(b'wrong')
        with self.assertRaises(ValueError):self.check()
    def test_corrupt_truncated_duplicate_or_oversized_embedded_config(self):
        valid=self.image(self.after.encode())
        for b in [b'no-config',valid[:-8],valid+valid,self.image(b'A'*(g.MAX_CONFIG+1)),b'IKCFG_STbadgzipIKCFG_ED']:
            with self.assertRaises(ValueError):g.embedded(b)
    def test_duplicate_config_or_json_key(self):
        with self.assertRaises(ValueError):g.values(b'CONFIG_FOO=y\n# CONFIG_FOO is not set\n')
        with self.assertRaises(ValueError):json.loads('{"x":1,"x":2}',object_pairs_hook=g.unique)
    def test_no_profile_before_any_launch_side_effect(self):
        with self.assertRaises(ValueError):g.launch_gate(self.root,{'kernel':str(self.root/'Image')})
