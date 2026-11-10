"""Final E wire/wrapper API; production requires accepted v6 E0 admission."""
import hashlib,inspect,json,os,stat,time,types
from pathlib import Path
from daemon_peer_auth import DaemonPeer

SNAPSHOT=Path(__file__).parents[2]/'examples/mem_service_pool/managed_cache_contract/v2'
CODEC_SHA='7cfbdc76f1ab8d696ae3349b6bf5510d5b8e6b4f047f50ae90e579b72c4e62db'
WRAPPER_SHA='8aebe66479b8eec36841fb8b3295ccd56362ab02bf7b6b4d6a63edbd83b86f3c'
def check(ok,why):
    if not ok:raise ValueError(why)
def source(path,pin,size=None):
    p=Path(path);s=p.lstat();check(p.resolve()==p and stat.S_ISREG(s.st_mode) and (os.name=='nt' or stat.S_IMODE(s.st_mode)==0o644) and 0<s.st_size<=128<<10,'ordinary exact-mode proposed source required')
    raw=p.read_bytes();check(hashlib.sha256(raw).hexdigest()==pin and (size is None or len(raw)==size),'proposed source byte pin mismatch');return raw
def _host_wrapper_snapshot():
    # Verify all top-level executed peer bytes before the first exec. This
    # snapshot is exclusively a HOST fixture, never an alternate runtime path.
    source(SNAPSHOT/'uflow_managed_cache_codec_v2.py',CODEC_SHA)
    raw=source(SNAPSHOT/'uflow_managed_guest_group_v1.py',WRAPPER_SHA)
    module=types.ModuleType('_C_HOST_actual_E_wrapper');module.__file__=str(SNAPSHOT/'uflow_managed_guest_group_v1.py')
    exec(compile(raw,module.__file__,'exec'),module.__dict__);return module
def _serial_binding(wrapper,channel,peer):
    check(type(channel) is wrapper.ManagedSerialChannel and type(peer) is DaemonPeer,'actual source-defined channel and owned HOST peer required')
    peer.recheck();pid=peer._expected['pid']
    def control(wire,deadline):
        # E's real public signature is managed_cache(wire), returning a dict.
        # It owns a bounded five-second serialized set/get. Never start it
        # with less time than that inside the original operation deadline.
        check(deadline-time.monotonic()>=5,'insufficient original deadline for bounded wrapper metadata pair')
        peer.recheck();result=channel.managed_cache(wire);peer.recheck()
        check(time.monotonic()<deadline,'source-defined wrapper completion is late');return result
    return {'pid':pid,'recheck':peer.recheck,'control':control,'requires_full_five_seconds':True}
def _host_managed_serial_binding(wrapper,channel,peer):return _serial_binding(wrapper,channel,peer)

def production_binding(*args,admission=None,**kwargs):
    from managed_gate_v3 import require_runtime
    admission=require_runtime(admission)
    check(len(args)==3 and args[0] is admission.modules['wrapper'],'only actually selected release-v2 wrapper can control its owning channel')
    return _serial_binding(*args)

class HostTypedAPIs:
    """Signature-checked dispatch on explicit HOST doubles; no live admission."""
    SHAPES={'evaluate':2,'validate_candidate':2,'validate_registration':5,'append_registration':3,'registry_history':2,'validate_closed_union':4}
    def __init__(self,terminal,metadata,ownership,*,source_only=False):
        self.source_only=source_only
        self.functions={k:getattr(terminal if k in ('evaluate','validate_candidate') else metadata,k) for k in self.SHAPES}
        for name,n in self.SHAPES.items():inspect.signature(self.functions[name]).bind(*[object() for _ in range(n)])
        check(getattr(metadata,'companion_audit') is self.functions['validate_closed_union'],'actual companion alias must be exact closed-union API')
        inspect.signature(ownership.validate_descriptor).bind(object(),object(),object(),starts=None,capture=None)
        self.ownership=ownership.validate_descriptor
    def _host_dispatch(self,name,*args):
        check(not self.source_only,'source-only proposed API cannot perform original/control IO')
        check(name in self.SHAPES and len(args)==self.SHAPES[name],'exact typed positional interface required');return self.functions[name](*args)

def _host_source_only_apis(recovery_root=None):
    pins=json.loads((SNAPSHOT/'proposal-pins.json').read_bytes());check(pins['manager_accepted'] is False and pins['production_IO_permitted'] is False,'proposal is never production admission')
    root=Path(recovery_root or pins['recovery_source_root']);raw={}
    for name,row in pins['recovery_sources'].items():raw[name]=source(root/name,row['sha256'],row['bytes'])
    def load(name):
        module=types.ModuleType('_C_HOST_proposed_'+name[:-3]);module.__file__=str(root/name);exec(compile(raw[name],module.__file__,'exec'),module.__dict__);return module
    # Actual imports execute source-only dependency verification. The HOST
    # probe audit forbids opening original run/authority/control paths.
    terminal=load('uflow_recovery_context_v3.py');metadata=load('uflow_profile_metadata_v3.py');ownership=load('uflow_monitor_ownership_v3.py')
    apis=HostTypedAPIs(terminal,metadata,ownership,source_only=True)
    return apis,{'modules_verified_before_exec':len(raw),'source_only':True,'original_or_control_IO':False,'manager_accepted':False,'production_ready':False}
