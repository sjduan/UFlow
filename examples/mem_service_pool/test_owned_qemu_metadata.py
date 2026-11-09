"""Synthetic CPU-child mechanism tests; never launch or inspect real QEMU."""
import hashlib
import mmap
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tools/phasec'))
import collect_owned_qemu_metadata as collector


class SelectedEnvironmentTests(unittest.TestCase):
    def test_only_individual_approved_keys_are_read_and_inference_is_explicit(self):
        class Selected(dict):
            def __iter__(self):raise AssertionError('must not enumerate full environment')
            def items(self):raise AssertionError('must not read full environment')
        value=collector.selected_launch_environment(Selected({'PRIVATE_SECRET':'not-selected','LD_AUDIT':'cpu-only.so'}),
            {'topology':'/fixture/topology','entity_plan':'/fixture/entities','output':'/fixture/run'},'nodeA')
        self.assertNotIn('PRIVATE_SECRET',value['values']);self.assertEqual(value['values']['UB_FM_NODE_ID'],'nodeA')
        self.assertFalse(value['direct_child_proc_environment_observed']);self.assertFalse(value['full_environment_read'])
    def test_production_scope_rejects_arbitrary_starts_before_proc_access(self):
        with patch.object(collector,'_observe_owned',side_effect=AssertionError('no process reads')):
            with self.assertRaises(ValueError):collector.capture_owned_starts('/tmp/arbitrary/guest-starts.json','0'*64,{'output':'/tmp/arbitrary'},'/tmp/group.py')


@unittest.skipUnless(os.name=='posix','Linux /proc fixture required')
class OwnedProcessTests(unittest.TestCase):
    def setUp(self):
        self.argv=[sys.executable,'-c','import mmap,time; m=mmap.mmap(-1,2<<20,flags=mmap.MAP_PRIVATE|mmap.MAP_ANONYMOUS); print("CPU_METADATA_READY",flush=True); time.sleep(30)']
        self.child=subprocess.Popen(self.argv,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        self.assertEqual(self.child.stdout.readline(),b'CPU_METADATA_READY\n')
        self.identity=collector.process_identity(self.child.pid);self.owner=collector.process_identity(os.getpid())
        self.pin=hashlib.sha256(Path(sys.executable).read_bytes()).hexdigest()
    def tearDown(self):
        if self.child.poll() is None and collector.process_identity(self.child.pid)['birth']==self.identity['birth']:
            self.child.terminate()
            try:self.child.wait(timeout=3)
            except subprocess.TimeoutExpired:
                if collector.process_identity(self.child.pid)['birth']==self.identity['birth']:self.child.kill()
                self.child.wait(timeout=3)
        if self.child.stdout:self.child.stdout.close()
        if self.child.stderr:self.child.stderr.close()
    def capture(self,**changes):
        args={'pid':self.child.pid,'birth':self.identity['birth'],'argv':self.argv,'exe_sha':self.pin,'owner':self.owner,'ram_bytes':1<<20}
        args.update(changes);return collector._observe_owned(**args)
    def test_actual_owned_birth_exe_namespace_anonymous_maps_and_fd_metadata(self):
        value=self.capture();self.assertEqual(value['identity']['pid'],self.child.pid)
        self.assertTrue(value['RAM_sized_anonymous_candidates']);self.assertTrue(value['FD_metadata'])
        self.assertFalse(value['RAM_or_transport_contents_read']);self.assertFalse(value['full_environment_read'])
        self.assertFalse(value['QMP_commands_added']);self.assertNotIn('environment',value)
    def test_wrong_birth_or_nonparent_owner_cannot_select_process(self):
        with self.assertRaises(ValueError):self.capture(birth=self.identity['birth']+1)
        with self.assertRaises(ValueError):self.capture(owner={'pid':os.getpid()+10000000,'birth':1})
    def test_wrong_executable_or_argv_cannot_select_process(self):
        with self.assertRaises(ValueError):self.capture(exe_sha='0'*64)
        with self.assertRaises(ValueError):self.capture(argv=self.argv+['extra'])
    def test_process_birth_change_during_capture_refuses(self):
        original=collector.process_identity;calls=0
        def changing(pid):
            nonlocal calls
            result=original(pid)
            if pid==self.child.pid:
                calls+=1
                if calls>1:result['birth']+=1
            return result
        with patch.object(collector,'process_identity',side_effect=changing),self.assertRaises(ValueError):self.capture()
    def test_missing_ram_sized_vma_is_incomplete_not_fabricated(self):
        value=self.capture(ram_bytes=1<<50);self.assertTrue(value['missing_evidence'])
