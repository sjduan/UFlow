"""Fixed original bytes + private semantic negatives, no production verify()."""
import base64,copy,json,os,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import verify_c039_prestart as q
D=Path(__file__).parent/'reconciliation'
class FixedPrestart(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.a=json.loads((D/'closed-c039-prestart-assessment.json').read_bytes())
        cls.raw={n:base64.b64decode(b) for n,b in json.loads((D/'closed-c039-cpu-documents.json').read_bytes())['files_base64'].items()}
    def bad_json(self,name,edit):
        raw=dict(self.raw);value=json.loads(raw[name]);edit(value);raw[name]=json.dumps(value).encode()+b'\n'
        with self.assertRaises((ValueError,KeyError,StopIteration)):q.semantic(self.a,raw)
    def test_actual_fixed286_and_semantic_control_flow_positive(self):
        q.exact(self.a,self.raw);p=q.semantic(self.a,self.raw)
        self.assertEqual(p['before_total_bytes'],80<<20);self.assertEqual(p['carry']['historical_prestart_contexts'][0]['unstarted_reserved_ids'],[108,109])
        self.assertFalse(p['old64_cancelled']);self.assertFalse(p['SDK_ACK_Unmapped_or_canary_close_claimed'])
    def test_each_missing_or_changed_fixed_binding_rejected(self):
        for n in self.raw:
            with self.subTest(binding=n):
                missing=dict(self.raw);missing.pop(n)
                with self.assertRaises(ValueError):q.exact(self.a,missing)
                changed=dict(self.raw);changed[n]=changed[n]+b' '
                with self.assertRaises(ValueError):q.exact(self.a,changed)
    def test_start_delivered_on_either_serial_rejected(self):
        for node in ['nodeA','nodeB']:
            for marker in [b'UFPOOL_STARTED\n',b'UFPOOL_REPLY 1 1 00\n']:
                raw=dict(self.raw);name='runs/c039/'+node+'.serial.log';raw[name]+=marker
                with self.assertRaises(ValueError):q.semantic(self.a,raw)
    def test_missing_extra_reordered_truncated_event_tail(self):
        name='runs/c039/pool-events.jsonl';events=[json.loads(s) for s in self.raw[name].splitlines()]
        for rows in [events[:1],list(reversed(events)),events+events[-1:]]:
            raw=dict(self.raw);raw[name]=b''.join(json.dumps(x).encode()+b'\n' for x in rows)
            with self.assertRaises(ValueError):q.semantic(self.a,raw)
        for data in [self.raw[name][:-1],self.raw[name][:-30],b'']:
            raw=dict(self.raw);raw[name]=data
            with self.assertRaises(ValueError):q.semantic(self.a,raw)
    def test_wrong_literal_failure_or_notice_or_pool_rejected(self):
        name='runs/c039/pool-events.jsonl'
        for key,value in [('detail','other failure'),('original_pool',{'state':'CreationUnknown'})]:
            self.bad_json_lines(name,key,value)
        self.bad_json('runs/c039/guest-first-failure.json',lambda v:v.update(message='after start'))
        self.bad_json('runs/c039/guest-first-failure.json',lambda v:v.update(independent_monitor_notified_before_this_diagnostic=False))
    def bad_json_lines(self,name,key,value):
        raw=dict(self.raw);rows=[json.loads(x) for x in raw[name].splitlines()];rows[1][key]=value;raw[name]=b''.join(json.dumps(v).encode()+b'\n' for v in rows)
        with self.assertRaises(ValueError):q.semantic(self.a,raw)
    def test_any_store_family_even_forged_empty_store_rejected(self):
        q.store_family([])
        for name in ['lower.store','lower.store.next','lower.store.backup']:
            with self.assertRaises(ValueError):q.store_family([Path(name)])
    def test_closed_namespace_rejects_added_worker_state_or_missing_monitor(self):
        for job in ['c039','ca039']:
            prefix='runs/'+job+'/';top={n[len(prefix):].split('/')[0] for n in self.raw if n.startswith(prefix)}
            if job=='c039':top.add('fm')
            prefix+='monitor/';monitor={n[len(prefix):] for n in self.raw if n.startswith(prefix)};state={'authority.lock','admin.token','state.json'} if job=='c039' else set()
            q.closed_names(self.a,job,top,monitor,state)
            with self.assertRaises(ValueError):q.closed_names(self.a,job,top|{'worker-nodeA.stdout.log'},monitor,state)
            with self.assertRaises(ValueError):q.closed_names(self.a,job,top,monitor-{'result.json'},state)
            if job=='c039':
                with self.assertRaises(ValueError):q.closed_names(self.a,job,top,monitor,state|{'worker.conf.state'})
    def test_only_exact_two_unstarted_reservations_and_old64(self):
        name='runs/c039/state/state.json'
        for key,value in [('canary:nodeA',1),('canary:other',2<<20),('old64',64<<20)]:
            self.bad_json(name,lambda v,k=key,x=value:v['resource_ledger']['reservations'].update({k:x}))
        self.bad_json(name,lambda v:v['resource_ledger']['reservations'].pop('canary:nodeB'))
        self.bad_json(name,lambda v:v['resource_ledger']['retained_uncertain_reservations'].clear())
        self.bad_json(name,lambda v:v['provider_loaded']['bootstrap_charge_objects'].update(nodeA=107))
    def test_state_work_or_wrong_budget_is_not_prestart(self):
        name='runs/c039/state/state.json'
        for key,value in [('pools',[{}]),('operations',[{}]),('control_counts',{'Acquire':1}),('bootstrap_readiness',{}),('native_jobs_executed',1),('budget_bytes',79<<20),('creation_reserved_bytes',64<<20)]:
            self.bad_json(name,lambda v,k=key,x=value:v.update({k:x}))
    def test_changed_source_agent_image_or_validator_rejected(self):
        for name in ['implementation/source-host030/tools/phasec/run_mem_service_pool_matrix.py','implementation/source-host030/crates/uf-ub-sim-agent/c/pool_agent.c']:
            raw=dict(self.raw);raw[name]=b'changed\n'+raw[name]
            with self.assertRaises(ValueError):q.exact(self.a,raw)
        for key in ['kernel','validator_sha256','manifest_sha256']:
            def change(v):
                if key=='kernel':v['kernel']='/unreviewed/Image'
                else:v['kernel_profile'][key]='0'*64
            self.bad_json('implementation/config-c039-profile-v1/group.json',change)
    def test_successor_or_state_drift_rejected(self):
        self.bad_json('implementation/responsibility-c013/successor-c033-v1/result.json',lambda v:v.update(phase='Pending'))
        self.bad_json('implementation/responsibility-c013/successor-c033-v1/intent.json',lambda v:v.update(cancel64_repeated=True))
        self.bad_json('runs/c039/state/state.json',lambda v:v['reconciliation'].update(proof_sha256='wrong'))
    def test_monitor_alive_or_uncertain_audit_rejected(self):
        for job in ['c039','ca039']:
            self.bad_json('runs/'+job+'/monitor/result.json',lambda v:v.update(process_tree_empty=False))
        self.bad_json('runs/c039/post-close-audit.json',lambda v:v['holder_audit']['holders'].append({'pid':1}))
        self.bad_json('runs/c039/post-close-audit.json',lambda v:v['holder_audit']['denied'].append('/proc/unknown'))
        with self.assertRaises(ValueError):q._r['nonlive']({'2':123},lambda p:123)
    def test_future_admission_is_one_guest_and_three_matching_locks_not_alternate_authority(self):
        row={'pid':55,'birth':66,'job':'c044','state':'held','kind':'guest'};records=[dict(row) for _ in range(3)];authority=q.ROOT/'runs/c044/state'
        self.assertEqual(q._r['admission_record'](records,{55:66},authority),row)
        for ancestors,path in [({55:67},authority),({55:66},q.ROOT/'runs/c045/state')]:
            with self.assertRaises(ValueError):q._r['admission_record'](records,ancestors,path)
        records[2]['job']='c045'
        with self.assertRaises(ValueError):q._r['admission_record'](records,{55:66},authority)
    def test_limits_and_aggregate_evidence(self):
        raw=dict(self.raw);raw[next(iter(raw))]=b'X'*(q.MAX_FILE+1)
        with self.assertRaises(ValueError):q.exact(self.a,raw)
        self.assertGreater(q.MAX_ASSESSMENT,80548);self.assertGreater(q.MAX_FILE,316073)
    @unittest.skipUnless(os.name=='posix','actual nofollow/FIFO bounded evidence IO requires POSIX')
    def test_actual_bounded_regular_no_symlink_or_FIFO_io(self):
        allowed=Path(os.environ.get('UFLOW_POOL_CPU_TMP_ROOT',tempfile.gettempdir())).resolve()
        with tempfile.TemporaryDirectory(dir=allowed) as tmp:
            root=Path(tmp);self.assertEqual(root.parent,allowed);p=root/'meta';p.write_bytes(b'abc');self.assertEqual(q.read(p,3),b'abc')
            with self.assertRaises(ValueError):q.read(p,2)
            link=root/'link';link.symlink_to(p)
            with self.assertRaises(ValueError):q.read(link,3)
            fifo=root/'fifo';os.mkfifo(fifo)
            with self.assertRaises(ValueError):q.read(fifo,3)
if __name__=='__main__':unittest.main()
