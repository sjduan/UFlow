"""Portable public-metadata parsing/admission negatives; Linux probe is separate."""
import copy
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from mem_service_pool_bridge import LOWER,lower_metadata,lower_response,worker_idle_reply
from pool_startup import validate_readiness,NotReady


class StartupContracts(unittest.TestCase):
    def test_only_same_worker_normal_poll_not_found_is_expected_idle(self):
        identity={'node':'nodeA','incarnation':7};worker={'pid':41,'birth':721}
        origin={'role':'worker',**worker};fields=(0,)*8+(1,1,0)
        public={'node_id':'nodeA','incarnation':'7','after_generation':'0'}
        reply={'status':'not_found','reason':'not_found','key':''}
        self.assertTrue(worker_idle_reply(identity,worker,origin,0x7c,fields,public,reply))
        for bad_origin,bad_op,bad_fields,bad_public,bad_reply in [
            ({**origin,'role':'root'},0x7c,fields,public,reply),({**origin,'birth':722},0x7c,fields,public,reply),
            (origin,0x70,fields,public,reply),(origin,0x7c,(0,)*8+(10,10,0),public,reply),
            (origin,0x7c,fields,{**public,'recovery':'1'},reply),
            (origin,0x7c,fields,public,{**reply,'reason':'managed_store_write_uncertain'})]:
            self.assertFalse(worker_idle_reply(identity,worker,bad_origin,bad_op,bad_fields,bad_public,bad_reply))
    def evidence(self):
        nodes={name:SimpleNamespace(identity={'boot_id':'boot-'+name,'incarnation':index},worker={'pid':40+index,'birth':700+index})
               for index,name in enumerate(('nodeA','nodeB'),1)}
        registrations={name:dict(node=name,boot_id=n.identity['boot_id'],incarnation=n.identity['incarnation'],readiness_generation=1,
            capabilities=1,directory_registered=True,channel_ready=True) for name,n in nodes.items()}
        statuses={name:dict(r,required_count=2,active_count=2,directory_ready=True,directory_epoch=2,lease_ms=1000,
            host_registry_data_plane_ready=False) for name,r in registrations.items()}
        workers={name:dict(worker_pid=n.worker['pid'],worker_birth=n.worker['birth'],worker_alive=True,worker_ready=True,
            worker_refresh_count=1,incarnation=n.identity['incarnation'],readiness_generation=1,capabilities=1,
            refresh_age_ms=10,lease_ms=1000) for name,n in nodes.items()}
        rows=[dict(node=name,incarnation=n.identity['incarnation'],readiness_generation=1,capabilities=1,age_ms=10) for name,n in nodes.items()]
        meta=dict(lease_ms='1000',directory_epoch='2',provider_required_count='2',provider_active_count='2',
            provider_directory_ready='1',data_plane_ready='0',providers=rows)
        directory={name:dict(origin={'role':'root'},operation=0x78,status=0,response_metadata=copy.deepcopy(meta),host_monotonic_ns=999999999)
                   for name in nodes}
        refresh={name:[dict(origin=dict(role='worker',pid=n.worker['pid'],birth=n.worker['birth']),operation=0x77,status=0,
            request_metadata=dict(node_id=name,incarnation=str(n.identity['incarnation']),readiness_generation='1'),host_monotonic_ns=999999999)]
                 for name,n in nodes.items()}
        return [nodes,registrations,workers,statuses,directory,refresh,1000000000]
    def test_raw_optional_host_registry_false_is_preserved(self):
        self.assertEqual(validate_readiness(*self.evidence()),2)
    def test_worker_and_directory_cannot_be_replaced_by_generic_ready(self):
        for section,key,value in [(2,'worker_alive',False),(2,'worker_refresh_count',0),(2,'worker_birth',999),
            (2,'refresh_age_ms',500),(3,'active_count',1),(3,'directory_ready',False),
            (1,'incarnation',99),(1,'boot_id','old'),(1,'channel_ready',False)]:
            data=self.evidence();data[section]['nodeA'][key]=value
            with self.subTest(section=section,key=key),self.assertRaises(NotReady):validate_readiness(*data)
    def test_missing_wrong_generation_or_expired_rows_and_refresh(self):
        for mutation in ('missing','incarnation','generation','expired','refresh','clock','raw-flag'):
            data=self.evidence();meta=data[4]['nodeA']['response_metadata']
            if mutation=='missing':meta['providers'].pop()
            if mutation=='incarnation':meta['providers'][1]['incarnation']=99
            if mutation=='generation':meta['providers'][1]['readiness_generation']=2
            if mutation=='expired':meta['providers'][1]['age_ms']=500
            if mutation=='refresh':data[5]['nodeA']=[]
            if mutation=='clock':data[4]['nodeA']['host_monotonic_ns']=1
            if mutation=='raw-flag':data[3]['nodeA']['host_registry_data_plane_ready']=True
            with self.subTest(mutation=mutation),self.assertRaises(NotReady):validate_readiness(*data)
    def test_public_provider_rows_are_repeated_but_other_keys_unique(self):
        raw=b'status=ok\nprovider=nodeA incarnation=1 readiness_generation=1 capabilities=1 age_ms=9\nprovider=nodeB incarnation=2 readiness_generation=1 capabilities=1 age_ms=8\n'
        self.assertEqual(len(lower_metadata(raw)['providers']),2)
        for bad in (b'status=ok\nstatus=ok\n',b'provider=x incarnation=1\n',b'reason=bad\x00\n'):
            with self.assertRaises(ValueError):lower_metadata(bad)
    def test_error_reply_remains_metadata_not_side_effect_absence(self):
        body=b'status=internal\nreason=store_write_uncertain\n';crc=2166136261
        for byte in body:crc=((crc^byte)*16777619)&0xffffffff
        raw=LOWER.pack(0x4d535643,1,48,7,0x76,0,len(body),crc,10,10,0)+body
        fields,meta=lower_response(raw,7,0x76)
        self.assertEqual((fields[8],meta['reason']),(10,'store_write_uncertain'))
        for bad in (raw[:-1],raw[:-1]+b'!',raw[:48]):
            with self.assertRaises(ValueError):lower_response(bad,7,0x76)
