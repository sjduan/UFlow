"""CPU-only finalization continuation and independent real POSIX socket regressions."""
from contextlib import contextmanager
import copy
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import session_adopt as adopt
from session_owner import Owner
from session_protocol import GROUP, POOL_BYTES, request, save
from test_recovery import sample_manifest, sample_snapshot, query_drift_cases


def fixture_values(public):
    origin=sample_manifest()
    origin.update(owner_root=str(public/'session002'),helper_root=str(public/'original/src/pool/examples/common_session'),
                  endpoint=str(public/'u.sock'),control_endpoint=str(public/'control.sock'))
    origin['frozen_inputs']={'fixed':'synthetic-CPU-only'}
    current=copy.deepcopy(origin)
    current.update(manifest_version=2,owner_root=str(public/'metadata-v2-001'),
                   helper_root=str(public/'prior/src/pool/examples/common_session'),
                   controller_birth={'pid':98781,'starttime_ticks':1298738805})
    current['frozen_inputs']['metadata_handover']={'predecessor_manifest_sha256':adopt.OLD_MANIFEST_SHA}
    snapshot,_=sample_snapshot(origin)
    lines={'O1':{'clients':[adopt.O1_SESSION],'retired':False},
           'M1':{'clients':[f'synthetic-M1-{i}' for i in range(30)],'retired':True}}
    snapshot['operation_records']=1362
    snapshot['pools'][0]['clients']=[{
        'client_session':s,'state':'Unmapped','children':[],'access':'rw','access_generation':50000+i,
        'closing':True,'birth':{'pid':1000+i,'starttime_ticks':2000+i,'uid':0,'gid':0,
                             'pid_namespace_dev':4,'pid_namespace_ino':42}}
        for i,s in enumerate([origin['bootstrap_client'],adopt.O1_SESSION,*lines['M1']['clients']])]
    cap={'schema':1,'group':GROUP,'mode':adopt.FINAL_MODE,'decision_id':'synthetic-final-only',
         'origin_manifest_sha256':adopt.OLD_MANIFEST_SHA,'current_manifest_sha256':adopt.CURRENT_MANIFEST_SHA,
         'old_controller_birth':current['controller_birth'],'original_owner_births':origin['owner_births'],
         'old_control_identity':{'dev':1,'inode':2,'uid':os.geteuid(),'mode':0o140600},
         'original_snapshot':snapshot,'lines':lines,
         'reservations':{'schema':1,'group':GROUP,'reservations':[{
             'line':'O1','physical_device':10,'board':5,'chip':0,'bdf':'0000:89:00.0',
             'execution_domain':'B/0000:89:00.0/physical-10','lock_path':'/workspace/uflow/locks/run02-npu-10.lock',
             'lock_holder_pid':3285540,'lock_holder_birth':1299093916,
             'evidence_path':'/workspace/uflow/runs/phasef-parallel-20261106/o1/campaign002/calibration002/resource-before.json'}]},
         'new_owner_root':str(public/'metadata-finalize-synthetic'),
         'new_manifest_path':str(public/'manifest-finalization-synthetic.json'),
         'helper_commit':'1'*40,'helper_files':{n:adopt.sha(Path(adopt.__file__).parent/n) for n in adopt.HELPER_FILES},
         'parent_safety':{},'closed_tasks':copy.deepcopy(adopt.FINAL_CLOSED_TASKS),
         'history_evidence':{'path':str(public/'history.json'),'sha256':'2'*64},
         'O1_close_evidence':{'path':str(public/'close.json'),'sha256':'3'*64},
         'origin_source_proof':{'path':str(public/'old-proof.json'),'sha256':'4'*64},
         'new_source_proof':{'path':str(public/'new-proof.json'),'sha256':'5'*64},
         'state_root':str(public/'session002/state'),
         'budget':{'anchor':1362,'finalizer_records':2,'terminal_records':256,'admission_limit':4096,'cleanup_limit':8192}}
    return origin,current,cap


@contextmanager
def final_fixture():
    with tempfile.TemporaryDirectory(prefix='uf-fin-') as temp:
        public=Path(temp)
        origin,current,cap=fixture_values(public)
        secret=Path(cap['state_root'])/'admin.token'
        secret.parent.mkdir(parents=True)
        secret.write_text('SYNTHETIC_CPU_ONLY_NOT_A_LIVE_CAPABILITY')
        secret.chmod(0o600)
        save(public/'manifest.json',origin)
        save(public/'manifest-v2.json',current)
        class RecordedSystem:
            def __init__(self):
                self.calls=[]
                self.value=copy.deepcopy(cap['original_snapshot'])
                self.drift=None
            def verify(self,*args):self.calls.append('verify-recorded-CPU-only')
            def snapshot(self,*args):
                self.calls.append('GetManagedPoolSnapshot')
                self.value['control_counts']['GetManagedPoolSnapshot']+=1
                self.value['revision']+=2
                if self.drift:self.drift(self.value)
                return copy.deepcopy(self.value)
        system=RecordedSystem()
        def run():
            save(public/'capsule.json',cap,exclusive=False)
            return adopt.FinalizationOwner.adopt_finalization(public/'capsule.json',adopt.sha(public/'capsule.json'),system=system)
        with patch.object(adopt,'PUBLIC',public),patch.object(adopt,'OLD_MANIFEST_SHA',adopt.sha(public/'manifest.json')), \
                patch.object(adopt,'CURRENT_MANIFEST_SHA',adopt.sha(public/'manifest-v2.json')):
            cap.update(origin_manifest_sha256=adopt.OLD_MANIFEST_SHA,current_manifest_sha256=adopt.CURRENT_MANIFEST_SHA)
            current['frozen_inputs']['metadata_handover']['predecessor_manifest_sha256']=adopt.OLD_MANIFEST_SHA
            save(public/'manifest-v2.json',current,exclusive=False)
            with patch.object(adopt,'CURRENT_MANIFEST_SHA',adopt.sha(public/'manifest-v2.json')):
                cap['current_manifest_sha256']=adopt.CURRENT_MANIFEST_SHA
                yield SimpleNamespace(public=public,origin=origin,current=current,cap=cap,system=system,run=run)


class FinalizationValidationTests(unittest.TestCase):
    def test_current32_valid_fixture_preserves_reservation_history_and_original_state(self):
        with final_fixture() as f:
            adopt.validate_finalization_capsule(f.cap,f.origin,f.current)
            with patch.object(Owner,'__init__',side_effect=AssertionError('must not construct UF owner')), \
                    patch.object(Owner,'bootstrap',side_effect=AssertionError('must not bootstrap')), \
                    patch.object(subprocess,'Popen',side_effect=AssertionError('must not start UF service')):
                owner=f.run()
            self.assertEqual(owner.state,Path(f.origin['owner_root'])/'state')
            self.assertNotEqual(owner.state,Path(f.current['owner_root'])/'state')
            self.assertFalse(owner.lines['O1']['retired'])
            self.assertTrue(owner.lines['M1']['retired'])
            self.assertEqual(len(owner.lines['M1']['clients']),30)
            self.assertEqual(set(owner.manifest),adopt.MANIFEST_KEYS)
            self.assertEqual(owner.manifest['manifest_version'],2)
            self.assertNotIn('_admin',owner.manifest)
            self.assertEqual(f.system.calls,['verify-recorded-CPU-only','GetManagedPoolSnapshot'])
            self.assertEqual(owner._previous_snapshot['operation_records'],1362)

    def test_snapshot_live_range_missing_extra_bootstrap_and_typed_client_drift_rejected(self):
        changes={
            'Mapped':lambda s:s['pools'][0]['clients'][0].update(state='Mapped'),
            'Unknown':lambda s:s['pools'][0]['clients'][0].update(state='Unknown'),
            'child':lambda s:s['pools'][0]['clients'][0].update(children=[{'unknown':False}]),
            'range':lambda s:s['pools'][0]['ranges']['leases'].append({'client_session':'bootstrap','state':'Known'}),
            'missing':lambda s:s['pools'][0]['clients'].pop(),
            'extra':lambda s:s['pools'][0]['clients'].append(copy.deepcopy(s['pools'][0]['clients'][0])),
            'bootstrap':lambda s:s['pools'][0]['clients'][0].update(client_session='wrong-bootstrap'),
            'birth-bool':lambda s:s['pools'][0]['clients'][0]['birth'].update(uid=False),
            'access-float':lambda s:s['pools'][0]['clients'][0].update(access_generation=50000.0),
            'closing-int':lambda s:s['pools'][0]['clients'][0].update(closing=1),
            'pool-float':lambda s:s['pools'][0].update(nbytes=float(POOL_BYTES)),
            'frozen-int':lambda s:s['pools'][0].update(frozen=0),
            'R-bool':lambda s:s.update(operation_records=True),
            'R-float':lambda s:s.update(operation_records=1362.0),
            'R-other':lambda s:s.update(operation_records=1364),
        }
        with final_fixture() as f:
            for name,change in changes.items():
                with self.subTest(name=name):
                    cap=copy.deepcopy(f.cap);change(cap['original_snapshot'])
                    with self.assertRaises((ValueError,KeyError)):
                        adopt.validate_finalization_capsule(cap,f.origin,f.current)

    def test_capsule_manifest_history_paths_reservation_source_and_budget_drift_rejected(self):
        changes={
            'legacy-mode':lambda c:c.update(mode='adopt-existing'),
            'legacy-R':lambda c:c['original_snapshot'].update(operation_records=312),
            'schema-bool':lambda c:c.update(schema=True),
            'origin-MF':lambda c:c.update(origin_manifest_sha256='0'*64),
            'current-MF':lambda c:c.update(current_manifest_sha256='0'*64),
            'peer':lambda c:c['old_control_identity'].update(uid=os.geteuid()+1),
            'socket-type':lambda c:c['old_control_identity'].update(mode=0o100600),
            'socket-bool':lambda c:c['old_control_identity'].update(inode=True),
            'owner':lambda c:c['old_controller_birth'].update(pid=1),
            'UF-owner':lambda c:c['original_owner_births'].update({'101':1002}),
            'state-v2':lambda c:c.update(state_root=str(Path(f.current['owner_root'])/'state')),
            'state-foreign':lambda c:c.update(state_root='/tmp/foreign-state'),
            'owner-root':lambda c:c.update(new_owner_root='/tmp/metadata-finalize-foreign'),
            'MF-target':lambda c:c.update(new_manifest_path=str(f.public/'manifest-v2.json')),
            'source':lambda c:c['helper_files'].pop('sessionctl.py'),
            'commit':lambda c:c.update(helper_commit='not-a-commit'),
            'M1-flag':lambda c:c['lines']['M1'].update(retired=False),
            'O1-flag':lambda c:c['lines']['O1'].update(retired=True),
            'O1-bool':lambda c:c['lines']['O1'].update(retired=0),
            'O1-client':lambda c:c['lines']['O1'].update(clients=['foreign']),
            'reservation-empty':lambda c:c['reservations'].update(reservations=[]),
            'reservation-device':lambda c:c['reservations']['reservations'][0].update(physical_device=12),
            'reservation-float':lambda c:c['reservations']['reservations'][0].update(physical_device=10.0),
            'reservation-birth':lambda c:c['reservations']['reservations'][0].update(lock_holder_birth=1),
            'budget':lambda c:c['budget'].update(finalizer_records=0),
            'budget-bool':lambda c:c['budget'].update(finalizer_records=True),
            'task-birth':lambda c:c['closed_tasks'][0].update(birth=1),
            'extra':lambda c:c.update(extra=0),
        }
        with final_fixture() as f:
            for name,change in changes.items():
                with self.subTest(name=name):
                    cap=copy.deepcopy(f.cap);change(cap)
                    with self.assertRaises((ValueError,KeyError)):
                        adopt.validate_finalization_capsule(cap,f.origin,f.current)
            for name,change in {
                'host':lambda v:v['host'].update(boot_id='changed'),
                'namespace':lambda v:v['host'].update(pid_namespace='changed'),
                'field':lambda v:v.update(extra=0),
                'schema':lambda v:v.update(schema=True),
                'pool':lambda v:v['pool_descriptor'].update(pool_epoch='other'),
            }.items():
                with self.subTest(name=name):
                    current=copy.deepcopy(f.current);change(current)
                    with self.assertRaises((ValueError,KeyError)):
                        adopt.validate_finalization_capsule(f.cap,f.origin,current)

    def test_observation_all_non_Get_or_revision_drift_is_rejected(self):
        with final_fixture() as f:
            for name,change in query_drift_cases(f.cap['original_snapshot']).items():
                with self.subTest(name=name):
                    f.system.drift=change
                    with self.assertRaises((ValueError,KeyError)):
                        f.run()
                    self.assertFalse(Path(f.cap['new_owner_root']).exists())
                    f.system.value=copy.deepcopy(f.cap['original_snapshot'])

    def test_explicit_mode_forbids_new_client_or_reservation_or_M1_changes(self):
        owner=adopt.FinalizationOwner.__new__(adopt.FinalizationOwner)
        for body in [
            {'op':'register-client','line':'O1'}, {'op':'register-client','line':'M1'},
            {'op':'reserve-devices','line':'O1','reservations':[{}]},
            {'op':'reserve-devices','line':'M1','reservations':[]},
            {'op':'line-retired','line':'M1'},
        ]:
            with self.subTest(body=body),self.assertRaisesRegex(ValueError,'finalization-only'):
                owner.handle(body)

    def test_two_production_modes_do_not_accept_each_others_capsule(self):
        with final_fixture() as f:
            with self.assertRaises(ValueError):adopt.validate_capsule(f.cap,f.origin)
            with self.assertRaises(ValueError):adopt.validate_finalization_capsule({'mode':'adopt-existing'},f.origin,f.current)

    def test_original_v1_state_requires_exact_daemon_argv_birth_and_executable(self):
        with final_fixture() as f:
            daemon={'pid':101,'birth':1001,'exe_sha256':adopt.DAEMON_SHA,
                    'argv':['daemon','--host-pool','--pool-state-root',f.cap['state_root']]}
            adopt.check_finalization_state(f.cap,f.origin,daemon)
            for name,change in {
                'argv-other':lambda d:d['argv'].__setitem__(-1,'/tmp/foreign-state'),
                'argv-missing':lambda d:d.update(argv=['daemon','--host-pool']),
                'argv-duplicate':lambda d:d['argv'].extend(['--pool-state-root',f.cap['state_root']]),
                'argv-truncated':lambda d:d.update(argv=['daemon','--pool-state-root']),
                'birth':lambda d:d.update(birth=1002),
                'exe':lambda d:d.update(exe_sha256='0'*64),
                'pid':lambda d:d.update(pid=102),
            }.items():
                with self.subTest(name=name):
                    value=copy.deepcopy(daemon);change(value)
                    with self.assertRaises(ValueError):adopt.check_finalization_state(f.cap,f.origin,value)

    def test_original_SDK_receipt_requires_actual_ACK_view_native_and_physical_close(self):
        close={'pid':3295917,'birth':1299094309,'operation_id':adopt.O1_UNMAP,
               'ACK':{'unmapped':True,'unknown_content_invalid_not_committed':False},
               'actual_native_close_complete':True,'actual_SDK_view_closed':True,
               'pool_no_VMA_FD':{'pid':3295917,'maps':[],'fds':[]}}
        adopt.check_finalization_close(close)
        for name,change in {
            'operation':lambda c:c.update(operation_id='foreign'),
            'ACK':lambda c:c['ACK'].update(unmapped=False),
            'ACK-int':lambda c:c['ACK'].update(unmapped=1),
            'unknown':lambda c:c['ACK'].update(unknown_content_invalid_not_committed=True),
            'native':lambda c:c.update(actual_native_close_complete=False),
            'view':lambda c:c.update(actual_SDK_view_closed=False),
            'VMA':lambda c:c['pool_no_VMA_FD'].update(maps=['original']),
            'FD':lambda c:c['pool_no_VMA_FD'].update(fds=['3']),
            'pid':lambda c:c.update(pid=1),
            'birth-float':lambda c:c.update(birth=1299094309.0),
        }.items():
            with self.subTest(name=name):
                value=copy.deepcopy(close);change(value)
                with self.assertRaises(ValueError):adopt.check_finalization_close(value)

    def test_finalizer_rechecks_cached_cost_and_full_resource_before_any_Destroy(self):
        with final_fixture() as f:
            owner=f.run()
            observed=copy.deepcopy(owner._previous_snapshot)
            observed['control_counts']['GetManagedPoolSnapshot']+=1;observed['revision']+=2
            with patch.object(owner,'snapshot',return_value=(observed,observed['pools'][0])), \
                    patch.object(adopt.AdoptedOwner,'finalize') as finish:
                owner.finalize()
                finish.assert_called_once_with()
            for name,change in query_drift_cases(owner._previous_snapshot).items():
                with self.subTest(name=name):
                    value=copy.deepcopy(observed);change(value)
                    with patch.object(owner,'snapshot',return_value=(value,value['pools'][0])), \
                            patch.object(adopt.AdoptedOwner,'finalize',side_effect=AssertionError('must not call Destroy')):
                        with self.assertRaises((ValueError,KeyError)):owner.finalize()

    def test_runtime_host_namespace_old_pid_or_frozen_SDK_drift_stops_before_private_capability(self):
        with final_fixture() as f:
            system=adopt.FinalizationSystem()
            def sealed(path):
                return adopt.OLD_MANIFEST_SHA if Path(path).name=='manifest.json' else adopt.CURRENT_MANIFEST_SHA
            for label in ('host','namespace','occupied-old-pid','missing-SDK'):
                with self.subTest(label=label),patch.object(adopt,'read_json',return_value=f.origin), \
                        patch.object(adopt,'sha',side_effect=sealed), \
                        patch.object(adopt,'current_host',return_value={'wrong':label} if label in ('host','namespace') else f.current['host']), \
                        patch.object(adopt,'birth',return_value=1 if label=='occupied-old-pid' else None), \
                        patch('importlib.machinery.PathFinder.find_spec',return_value=None), \
                        patch.object(adopt,'check_parent_safety',side_effect=AssertionError('must reject earlier')):
                    with self.assertRaises(ValueError):system.verify(f.cap,f.current)

    def test_new_mode_source_manifest_seal_and_private_capability_permissions_rejected(self):
        for label in ('source','manifest','private-mode','occupied-root'):
            with self.subTest(label=label),final_fixture() as f:
                if label=='source':f.cap['helper_files']['session_owner.py']='0'*64
                if label=='manifest':
                    changed=copy.deepcopy(f.current);changed['controller_birth']['pid']=1
                    save(f.public/'manifest-v2.json',changed,exclusive=False)
                if label=='private-mode':(Path(f.cap['state_root'])/'admin.token').chmod(0o644)
                if label=='occupied-root':Path(f.cap['new_owner_root']).mkdir()
                with self.assertRaises(ValueError):f.run()

    def test_current32_publication_real_socket_preserves_history_then_normal_retirement_ACKs(self):
        with final_fixture() as f:
            stale=socket.socket(socket.AF_UNIX)
            stale.bind(f.current['control_endpoint']);stale.close()
            info=Path(f.current['control_endpoint']).stat()
            f.cap['old_control_identity']={'dev':info.st_dev,'inode':info.st_ino,'uid':info.st_uid,'mode':info.st_mode}
            save(f.public/'reservations.json',f.cap['reservations'])
            owner=f.run()
            owner.snapshot=lambda label:(copy.deepcopy(f.system.value),copy.deepcopy(f.system.value['pools'][0]))
            owner.finalize=lambda:None  # CPU server stop only, no SDK import/Destroy.
            errors=[]
            def serve():
                try:owner.publish_and_serve()
                except BaseException as error:errors.append(repr(error))
            thread=threading.Thread(target=serve);thread.start()
            try:
                path=Path(f.cap['new_manifest_path']);deadline=time.monotonic()+5
                while not path.exists():
                    assert not errors and time.monotonic()<deadline,errors
                    time.sleep(.005)
                manifest=json.loads(path.read_bytes())
                # Peer check, complete request and reply all use this same socket.
                import struct
                from session_protocol import receive
                with socket.socket(socket.AF_UNIX) as stream:
                    stream.connect(manifest['control_endpoint'])
                    self.assertEqual(struct.unpack('3i',stream.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))[0],os.getpid())
                    stream.sendall(json.dumps({'schema':1,'group':GROUP,'op':'describe'}).encode()+b'\n')
                    response=receive(stream)
                self.assertEqual(response['result']['manifest'],manifest)
                self.assertEqual(response['result']['lines'],f.cap['lines'])
                self.assertEqual(set(manifest),adopt.MANIFEST_KEYS)
                self.assertEqual(manifest['manifest_version'],2)
                self.assertTrue((Path(f.cap['new_owner_root'])/'previous-control.sock').exists())
                self.assertEqual(json.loads((f.public/'manifest-v2.json').read_bytes()),f.current)
                request(manifest,'reserve-devices',line='O1',reservations=[])
                self.assertEqual(json.loads((f.public/'reservations.json').read_bytes())['reservations'],[])
                result=request(manifest,'line-retired',line='O1',client_sessions=[adopt.O1_SESSION])
                self.assertTrue(result['lines']['O1']['retired'])
                self.assertTrue(result['lines']['M1']['retired'])
            finally:
                (owner.root/'finalize.request').touch()
                thread.join(timeout=5)
            self.assertFalse(thread.is_alive());self.assertEqual(errors,[])
            self.assertFalse(Path(owner.control).exists())


def socket_server(root,owner_source=None):
    """A separate CPU process; only the actual Owner.serve socket loop is used."""
    root=Path(root)
    if owner_source:
        spec=importlib.util.spec_from_file_location('baseline_owner',owner_source)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        cls=module.Owner
    else:cls=Owner
    owner=cls.__new__(cls)
    owner.root=root;owner.control=str(root/'c.sock')
    owner.counter=0
    def evidence(label,value):
        owner.counter+=1
        save(root/f'{owner.counter:04d}-{label}.json',value)
    owner.evidence=evidence
    def handle(body):
        with (root/'handle-count.log').open('a') as stream:stream.write('handled\n')
        if (root/'block-next').exists():
            (root/'block-next').unlink()
            (root/'inside-handle').touch()
            deadline=time.monotonic()+5
            while not (root/'release-handle').exists():
                if time.monotonic()>deadline:raise TimeoutError('CPU parent barrier timeout')
                time.sleep(.005)
        return {'CPU_only':True}
    owner.handle=handle
    owner.finalize=lambda:None
    owner.serve(publish=lambda:(root/'ready').touch())


def exercise_disconnect(owner_source=None,expect_survival=True):
    """Real EOF/reset paths; no monkeypatch of receive/sendall or process sockets."""
    with tempfile.TemporaryDirectory(prefix='uf-sock-') as temp:
        root=Path(temp).resolve()
        args=[sys.executable,'-B','-s',str(Path(__file__).resolve()),'--socket-server',str(root)]
        if owner_source:args.append(str(owner_source))
        with (root/'child.stdout').open('wb') as out,(root/'child.stderr').open('wb') as err:
            child=subprocess.Popen(args,stdout=out,stderr=err)
            try:
                deadline=time.monotonic()+5
                while not (root/'ready').exists():
                    assert child.poll() is None,(root/'child.stderr').read_text()
                    assert time.monotonic()<deadline
                    time.sleep(.005)
                path=str(root/'c.sock')
                with socket.socket(socket.AF_UNIX) as stream:stream.connect(path)
                if not expect_survival:
                    code=child.wait(timeout=5)
                    assert code!=0 and 'BrokenPipeError' in (root/'child.stderr').read_text()
                    return {'unpatched_failed':True,'exit_code':code,'stderr':(root/'child.stderr').read_text()}
                def describe():
                    assert request({'control_endpoint':path},'describe')=={'CPU_only':True}
                    assert child.poll() is None
                describe()
                for frame in (b'{"schema":',b'not-json\n'):
                    with socket.socket(socket.AF_UNIX) as stream:
                        stream.connect(path);stream.sendall(frame);stream.shutdown(socket.SHUT_WR)
                    describe()
                before=(root/'handle-count.log').read_text().count('handled')
                (root/'block-next').touch()
                stream=socket.socket(socket.AF_UNIX);stream.connect(path)
                stream.sendall(json.dumps({'schema':1,'group':GROUP,'op':'describe'}).encode()+b'\n')
                deadline=time.monotonic()+5
                while not (root/'inside-handle').exists():
                    assert child.poll() is None and time.monotonic()<deadline
                    time.sleep(.005)
                stream.close();(root/'release-handle').touch()
                describe()
                after=(root/'handle-count.log').read_text().count('handled')
                assert after==before+2,'closed mutating body must never be retried'
                failures=[json.loads(p.read_bytes()) for p in root.glob('*-control-reply-disconnected.json')]
                assert {'ok','error'}<={f['reply_status'] for f in failures}
                assert all(f['delivery_confirmed'] is False and f['body_reexecuted'] is False for f in failures)
                (root/'finalize.request').touch()
                assert child.wait(timeout=5)==0
                assert not (root/'c.sock').exists()
                return {'passed':True,'child_exit':0,'disconnect_reply_records':len(failures),
                        'actual_handle_count':after,'error_and_success_reply_paths':True,
                        'closed_request_handled_exactly_once':True}
            finally:
                if child.poll() is None:
                    (root/'finalize.request').touch()
                    child.wait(timeout=6)  # CPU child normal stop, never kill.


class RealSocketTests(unittest.TestCase):
    def test_independent_child_empty_half_invalid_and_handled_peer_close_survive(self):
        self.assertTrue(exercise_disconnect()['passed'])


if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='--socket-server':
        socket_server(sys.argv[2],sys.argv[3] if len(sys.argv)>3 else None)
    else:unittest.main()
