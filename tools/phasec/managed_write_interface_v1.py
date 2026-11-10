"""Pinned stable E W codec/wrapper, with explicit HOST-only construction.

Production remains closed until final ELF/profile/current-c101 admission. The
stable interface handoff alone never grants original/control IO permission.
"""
import copy,hashlib,time,types
from pathlib import Path
from daemon_peer_auth import DaemonPeer
from managed_interface_v2 import source
from managed_cache_control import check,native_u64

SNAPSHOT=Path(__file__).parents[2]/'examples/mem_service_pool/managed_write_contract/v1'
CODEC_SHA='e5627711dc76a5441a7c957733983bff10dfd7d38fe1cf787b3b535e0de05c08'
WRAPPER_SHA='9bd5df060abfa26b2527557e14b755c701b9b3323cb57d6c5466ffa6691765b2'
R_CODEC_SHA='7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db'

def _host_snapshot():
    # Verify all top-level executed dependency bytes before executing any.
    raws={name:source(SNAPSHOT/name,pin) for name,pin in [('uflow_managed_write_codec_v1.py',CODEC_SHA),('uflow_managed_cache_codec_v2.py',R_CODEC_SHA),('uflow_managed_guest_group_v2.py',WRAPPER_SHA)]}
    def load(name):
        m=types.ModuleType('_C_HOST_actual_E_W_'+name[:-3]);m.__file__=str(SNAPSHOT/name);exec(compile(raws[name],m.__file__,'exec'),m.__dict__);return m
    return load('uflow_managed_write_codec_v1.py'),load('uflow_managed_guest_group_v2.py')

class _NativeProtocol:
    VERSION=1
    def __init__(self,c):
        check(c.FEATURE=='uflow-managed-write-v1' and c.PROPERTY=='uflow-managed-write-control-v1' and c.WIRE.size==220 and c.OPS=={'probe':1,'begin':2,'end':3,'status':4,'flush':5},'actual stable W codec required')
        self.c=c;self.FIELDS64,self.FIELDS32=c.FIELDS64,c.FIELDS32
    def encode(self,body,public,op,alias):
        command={'version':1,'operation':op,'control_generation':body['write_control_generation'],'authority_sha256':body['write_authority_sha256'],'correlation_sha256':body['write_frame_sha256'],
                 **public,'offset':body['offset'],'length':body['nbytes'],'map_id':0,'lease_epoch':0}
        if alias is not None:command.update(map_id=native_u64(alias['map_id']),lease_epoch=native_u64(alias['lease_epoch']))
        return self.c.encode(command)
    def request_sha256(self,wire):return self.c.request_sha256(wire)
    def validate_reply(self,wire,value,pid):
        # Actual E wrapper's public method returns a dict. The unchanged
        # codec validates every byte hash, native alias and both IO counters.
        check(type(value) is dict,'actual typed W wrapper reply required')
        native=self.c.validate_reply(wire,value,pid);op=self.c.decode(wire)['operation']
        check(native['code']=='ok' and native['uncertain'] is False and native['write_counters']['failures']=='0','uncertain native W cannot acknowledge normal completion')
        if op in ('begin','flush','end'):check(native['write_in_flight']==0 and native['dirty_bytes']==0 and native['in_flight']==0,'native W flush/IO obligations remain')
        return {**copy.deepcopy(native),'mode':'w','flush_confirmed':op in ('flush','end'),'native_reply':copy.deepcopy(native)}

def _host_native_protocol(codec):return _NativeProtocol(codec)

def _host_serial_binding(wrapper,channel,peer):
    check(type(channel) is wrapper.ManagedSerialChannel and type(peer) is DaemonPeer,'actual E W channel and actual owned HOST peer required');peer.recheck()
    def control(wire,deadline):
        check(deadline-time.monotonic()>=5,'original W deadline cannot fit bounded metadata pair');peer.recheck();r=channel.managed_write(wire);peer.recheck();check(time.monotonic()<deadline,'source-defined W wrapper completion is late');return r
    return {'pid':peer._expected['pid'],'recheck':peer.recheck,'control':control,'requires_full_five_seconds':True}

def production_binding(*args,admission=None,**kwargs):
    from managed_gate_v4 import require_runtime
    admission=require_runtime(admission)
    check(not kwargs and len(args)==3 and args[0] is admission.modules['wrapper'],'only accepted c1e W wrapper can control its owning channel')
    return _host_serial_binding(*args)
