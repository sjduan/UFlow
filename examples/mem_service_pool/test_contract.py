"""Portable negative contracts; no fake mapping/provider pass is inferred."""
from dataclasses import replace
import copy
import json
from pathlib import Path
import struct
import sys
import unittest
from uflow._pool_bridge_v2 import Frame, HEADER, metadata
from uflow.mem_service_pool_v2 import (access_binding, pool_descriptor,
    MemServicePoolClient, PoolV2Unknown)

sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from mem_service_pool_bridge import LOWER, lower_request, GuestNode


def frame():
    return Frame(17,19,'simulated:test','nodeB','boot-b','sdk-b',7,b'{"schema":2,"op":"inspect"}')


def pool():
    return {'schema':2,'pool_id':'pool','pool_epoch':'epoch','provider_session':'provider',
        'provider_kind':'mem_service_obmm','nbytes':65536,'resource_id':3,'placement_id':4,'binding_id':5,
        'resource_generation':11,'state':'Allocated','canonical':{'schema':2,'phase':'completed',
            'key':'pool','nbytes':65536,'generation':11,'incarnation':23,'state':'active'}}


def binding():
    return {'schema':2,'backend':'mem_service_obmm','realm':'simulated:test','node':'nodeB',
        'boot_id':'boot-b','incarnation':7,'pool_id':'pool','pool_epoch':'epoch','provider_session':'provider',
        'client_session':'client','access_generation':9,'rights':'rw','offset':0,'nbytes':65536,
        'receipt':{'mapped':True,'holder':True,'client_session':'client','generation':11,'nbytes':65536,'guest_pid':123,'pins':0}}


class BridgeTests(unittest.TestCase):
    def test_c02_network_layout_and_v2_completion_identity(self):
        original=frame();data=original.encode()
        self.assertEqual(HEADER.size,68)
        self.assertEqual(data[:16],b'UFSIMB01'+struct.pack('!4H',1,1,14,0))
        self.assertEqual(Frame.decode(data),original)
        reply=original.completion({'schema':2,'phase':'completed','contexts':0,'pins':0,'cleanup_required':False})
        self.assertTrue(original.matches(Frame.decode(reply.encode())))

    def test_checksum_length_protocol_minor_and_truncation(self):
        data=frame().encode()
        for bad in (data[:-1],data+b'x',data[:11]+b'\x02'+data[12:],data[:-1]+b'x',data[:67]):
            with self.subTest(bad=bad[:16]),self.assertRaises(ValueError):Frame.decode(bad)

    def test_replay_correlation_and_boot_incarnation_are_not_optional(self):
        request=frame();reply=request.completion({'schema':2,'phase':'completed'})
        for name,value in [('request',18),('correlation',18),('boot','other'),('generation',8),
                           ('realm','physical:test'),('session','stale'),('node','nodeA')]:
            self.assertFalse(request.matches(replace(reply,**{name:value})))

    def test_no_payload_pointer_nested_float_negative_or_duplicate(self):
        cases=[b'{"schema":2,"schema":2}',b'{"payload":"KV"}',b'{"pointer":12}',b'{"address":12}',
               b'{"op":[1]}',b'{"n":1.5}',b'{"n":-1}',b'{"n":18446744073709551616}',b'{"n":NaN}']
        for raw in cases:
            with self.subTest(raw=raw),self.assertRaises(ValueError):metadata(raw)

    def test_pool_extension_rejects_old_peers_and_zero_ids(self):
        with self.assertRaises(ValueError):replace(frame(),request=0).encode()
        data=bytearray(frame().encode());data[11]=0
        with self.assertRaises(ValueError):Frame.decode(data)

    def test_only_opaque_canary_descriptor_has_explicit_384_hex_bound(self):
        self.assertEqual(metadata(json.dumps({'canary_descriptor':'ab'*192}).encode())['canary_descriptor'],'ab'*192)
        for value in ({'canary_descriptor':'ab'*193},{'canary_descriptor':'xyz'},
                      {'peer_canary_descriptor':123},{'op':'a'*257}):
            with self.assertRaises(ValueError):metadata(json.dumps(value).encode())

    def test_foreign_realm_is_rejected_before_any_serial_access(self):
        node=GuestNode(None,{'node':'nodeB','boot_id':'boot-b','agent_session':'sdk-b','incarnation':7},
                       'not-connected',lambda event:None,'simulated:other')
        with self.assertRaisesRegex(ValueError,'realm'):node.exchange(frame())

    def test_lower_relay_accepts_only_public_managed_metadata(self):
        header=LOWER.pack(0x4d535643,1,48,5,0x74,0,0,0,0,0,0)
        self.assertEqual(lower_request(header),(5,0x74))
        for op in (16,17,19,0x7e,0x7f):
            with self.assertRaises(ValueError):lower_request(LOWER.pack(0x4d535643,1,48,5,op,0,0,0,0,0,0))
        with self.assertRaises(ValueError):lower_request(header+b'KV')


class BindingTests(unittest.TestCase):
    def test_explicit_backend_and_matching_lower_allocation(self):
        self.assertEqual(pool_descriptor(pool()),pool())
        for name,value in [('schema',1),('provider_kind','posix_shm_simpler'),('nbytes',129<<20),('resource_generation',12)]:
            changed=pool();changed[name]=value
            with self.assertRaises(ValueError):pool_descriptor(changed)

    def test_epoch_node_rights_range_and_receipt_are_all_bound(self):
        self.assertEqual(access_binding(binding(),pool(),'nodeB','rw'),binding())
        for name,value in [('pool_epoch','other'),('backend','posix_shm_simpler'),('node','nodeA'),
                           ('rights','r'),('offset',65536),('nbytes',131072),('incarnation',False),('realm','physical:test')]:
            changed=binding();changed[name]=value
            with self.subTest(name=name),self.assertRaises(ValueError):access_binding(changed,pool(),'nodeB','rw')
        changed=binding();changed['receipt']['holder']=False
        with self.assertRaises(ValueError):access_binding(changed,pool(),'nodeB','rw')

    def test_constructor_unknown_keeps_original_client_and_operation(self):
        def transport(*args,**kwargs):raise PoolV2Unknown('unknown map','original-map')
        with self.assertRaises(PoolV2Unknown) as caught:MemServicePoolClient('not-connected',pool(),'nodeB',transport=transport)
        client=caught.exception.client
        self.assertIn('original-map',client.unknown)
        self.assertFalse(client.closed)

    def test_close_requires_actual_unmap_and_does_not_self_ack(self):
        def transport(endpoint,op,values):
            if op=='RegisterPoolClient':return {'client_session':'client','access_generation':9,'access_binding_v2':binding()}
            return {'stopping':True}
        client=MemServicePoolClient('not-connected',pool(),'nodeB',transport=transport)
        with self.assertRaises(PoolV2Unknown):client.close()
        self.assertFalse(client.closed)

    def test_wrong_context_cannot_execute_and_readonly_cannot_write(self):
        def transport(endpoint,op,values):return {'client_session':'client','access_generation':9,'access_binding_v2':binding()}
        client=MemServicePoolClient('not-connected',pool(),'nodeB',transport=transport)
        with self.assertRaises(ValueError):client.execute_pattern({'client_session':'foreign'},9)
        client.binding['rights']='r'
        with self.assertRaises(ValueError):client.acquire({'generation':1},'w')

    def test_each_request_carries_issued_full_binding(self):
        seen=[]
        def transport(endpoint,op,values):
            seen.append((op,copy.deepcopy(values)))
            if op=='RegisterPoolClient':return {'client_session':'client','access_generation':9,'access_binding_v2':binding()}
            return {'snapshot':True}
        client=MemServicePoolClient('not-connected',pool(),'nodeB',transport=transport)
        client.snapshot()
        self.assertEqual(seen[-1][1]['access_binding_v2'],binding())
        self.assertEqual(seen[-1][1]['client_session'],'client')


if __name__=='__main__':unittest.main()
