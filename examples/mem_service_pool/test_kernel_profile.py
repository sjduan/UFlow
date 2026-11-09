"""Lightweight launcher boundary tests; actual producer validation runs on B."""
import os,sys,tempfile,types,unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import kernel_profile_gate as g
class KernelProfile(unittest.TestCase):
    def setUp(self):
        allowed=Path(os.environ.get('UFLOW_POOL_CPU_TMP_ROOT',tempfile.gettempdir())).resolve()
        self.tmp=tempfile.TemporaryDirectory(dir=allowed);self.root=Path(self.tmp.name).resolve();self.assertEqual(self.root.parent,allowed);self.addCleanup(self.tmp.cleanup)
        self.config={'kernel':str(self.root/g.RELEASE/'Image'),'kernel_profile':g.binding(self.root)}
    def result(self):return {'status':'PASS_HOST_PROFILE_ONLY','interface':g.INTERFACE,'profile_id':g.PROFILE,'producer_manifest_sha256':g.MANIFEST_SHA,
        'Image':self.config['kernel'],'Image_sha256':g.IMAGE_SHA,'embedded_config_sha256':g.CONFIG_SHA,'runtime_tested':False,'authority_to_boot_or_replace_existing_Image':False}
    def test_mocked_API_seam_preserves_external_pins_and_no_boot_authority(self):
        calls=[];module=types.SimpleNamespace(validate=lambda *a:calls.append(a) or self.result())
        with patch.object(g,'_load_validator',return_value=module),patch.object(g,'_base_digest',return_value=g.BASE):value=g.launch_gate(self.root,self.config)
        self.assertEqual(calls,[(Path(self.config['kernel_profile']['manifest']),g.MANIFEST_SHA,Path(self.config['kernel']))]);self.assertEqual(value['external_validator_sha256'],g.VALIDATOR_SHA);self.assertFalse(value['runtime_tested'])
    def test_absent_or_extra_ready_fields_are_not_authority(self):
        for selected in [None,{},dict(g.binding(self.root),ready=True)]:
            with self.subTest(selected=selected),self.assertRaises(ValueError):g.launch_gate(self.root,{'kernel':self.config['kernel'],'kernel_profile':selected})
    def test_changed_manifest_or_validator_pin_refused_before_load(self):
        for key in ['manifest_sha256','validator_sha256','manifest','validator']:
            c={'kernel':self.config['kernel'],'kernel_profile':dict(g.binding(self.root),**{key:'wrong'})}
            with patch.object(g,'_load_validator') as load,self.assertRaises(ValueError):g.launch_gate(self.root,c)
            load.assert_not_called()
    def test_old_or_unbound_Image_refused_before_load(self):
        for image in [str(self.root/'src/ub_sim/guest-linux/aarch64/out/Image'),self.config['kernel']+'.other']:
            with patch.object(g,'_load_validator') as load,self.assertRaises(ValueError):g.launch_gate(self.root,dict(self.config,kernel=image))
            load.assert_not_called()
    def test_unreviewed_validator_bytes_never_execute(self):
        p=self.root/'bad-validator.py';p.write_bytes(b'raise RuntimeError("unreviewed code executed")\n')
        with self.assertRaisesRegex(ValueError,'external reviewed validator hash'):g._load_validator(p)
    def test_inconsistent_actual_validator_result_cannot_claim_ready(self):
        for key,value in [('runtime_tested',True),('authority_to_boot_or_replace_existing_Image',True),('Image_sha256','0'*64),('Image','other'),('status','ready')]:
            result=dict(self.result(),**{key:value});module=types.SimpleNamespace(validate=lambda *a:result)
            with patch.object(g,'_load_validator',return_value=module),patch.object(g,'_base_digest',return_value=g.BASE),self.assertRaises(ValueError):g.launch_gate(self.root,self.config)
    def test_changed_old_default_pin_is_not_replaced(self):
        module=types.SimpleNamespace(validate=lambda *a:self.fail('validator should not be called'))
        with patch.object(g,'_load_validator',return_value=module),patch.object(g,'_base_digest',return_value='0'*64),self.assertRaises(ValueError):g.launch_gate(self.root,self.config)
    def test_failed_gate_precedes_lower_Catalog_or_lineage_side_effects(self):
        import run_mem_service_pool_matrix as driver
        out=self.root/'runs/cpu-probe';out.mkdir(parents=True)
        c=dict(self.config,output=str(out));c['kernel_profile']={}
        with patch.dict(os.environ,{'UFLOW_ENV_CLOSE_FD':'999'}),patch.object(driver.subprocess,'Popen') as popen,self.assertRaises(ValueError):driver.matrix(self.root,c,{'source':str(self.root/'source')})
        popen.assert_not_called();self.assertEqual(list(out.iterdir()),[]);self.assertFalse((self.root/'implementation/responsibility-c013').exists())
