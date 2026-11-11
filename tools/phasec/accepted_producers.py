"""Fixed manager-accepted E f5a5 sources. Case/config hashes are not anchors.

Also embedded before terminal_current_adapter.py in a fresh isolated Rust
adapter interpreter. That caller supplies the same compiled raw anchor bytes.
"""
import hashlib,json,os,stat,sys,types
from pathlib import Path
from contextlib import contextmanager

AP_ROOT=Path('/workspace/uflow/runs/mem-service-shared-pool-20261108')
AP_SHA='d16b3922afc9c33f589318634413527981f641e5a8b4bd1c1252ca39a6cae80c'
AP_ORDER=('uflow_qemu_evidence','uflow_qemu_metadata','uflow_qemu_fixed_launch','uflow_terminal_domain_v2')

def ap_require(ok,message):
    if not ok:raise ValueError('accepted E producer: '+message)

def ap_unique(pairs):
    result={}
    for key,value in pairs:
        ap_require(key not in result,'duplicate JSON key');result[key]=value
    return result

def ap_decode(raw):
    return json.loads(raw,object_pairs_hook=ap_unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError('nonfinite JSON')))

def ap_canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()

def ap_read(path,maximum=4<<20,mode=None):
    path=Path(path)
    ap_require(path.is_absolute() and path.resolve(strict=True)==path and path.is_relative_to(AP_ROOT) and 'fm' not in path.parts,'canonical task-local nonpayload evidence required')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    with os.fdopen(fd,'rb') as f:
        first=os.fstat(f.fileno())
        ap_require(stat.S_ISREG(first.st_mode) and first.st_uid==os.geteuid() and first.st_nlink==1 and 0<first.st_size<=maximum,'owned bounded single-link evidence required')
        ap_require(mode is None or stat.S_IMODE(first.st_mode)==mode,'accepted source mode mismatch')
        raw=f.read(maximum+1);last=os.fstat(f.fileno());named=path.lstat()
    identity=lambda s:(s.st_dev,s.st_ino,s.st_uid,s.st_mode,s.st_size,s.st_mtime_ns,s.st_ctime_ns,s.st_nlink)
    ap_require(identity(first)==identity(last)==identity(named) and len(raw)==first.st_size,'evidence changed while observed')
    return raw

def ap_bound(row,maximum=4<<20,mode=None):
    ap_require(type(row) is dict and set(row)=={'path','bytes','sha256'} and type(row['bytes']) is int,'exact external reference required')
    raw=ap_read(row['path'],maximum,mode)
    ap_require(len(raw)==row['bytes'] and hashlib.sha256(raw).hexdigest()==row['sha256'],'accepted raw reference drift')
    return raw

def accepted_anchors(raw=None):
    if raw is None:
        raw=(Path(__file__).resolve().parents[2]/'examples/mem_service_pool/current_terminal_production/accepted-producer-anchors.json').read_bytes()
    if isinstance(raw,str):raw=raw.encode('utf-8')
    ap_require(len(raw)==12067 and hashlib.sha256(raw).hexdigest()==AP_SHA,'fixed external manager anchor bytes mismatch')
    return ap_decode(raw)

@contextmanager
def ap_fresh_modules(rows,modes,order):
    ap_require(set(rows)==set(order) and len(order)<=16,'exact module dependency closure required')
    # Check all code bytes AND modes before the first source is executed.
    bodies={name:ap_bound(rows[name],256<<10,int(modes[name],8)) for name in order}
    previous={name:sys.modules.get(name) for name in order};modules={name:types.ModuleType(name) for name in order}
    try:
        for name,module in modules.items():module.__file__=rows[name]['path'];sys.modules[name]=module
        for name in order:exec(compile(bodies[name],rows[name]['path'],'exec'),modules[name].__dict__)
        yield modules
        for name in order:ap_bound(rows[name],256<<10,int(modes[name],8))
    finally:
        for name,old in previous.items():
            if old is None:sys.modules.pop(name,None)
            else:sys.modules[name]=old

def ap_check_terminal_result(result,anchor,recorded):
    # Compare the whole externally pinned typed result, not just PASS or hash.
    ap_require(type(result) is dict and result==recorded,'fresh complete typed terminal result differs from accepted candidate')
    ap_require(result['schema']==1 and result['interface']==anchor['expected_interface'] and result['method']==anchor['expected_method'] and result['status']==anchor['expected_status'],'typed terminal interface/status/method mismatch')
    ap_require(result['assessment_sha256']==anchor['expected_assessment_sha256'] and result['state']==anchor['expected_state'] and
               result['eligible_rows']==anchor['expected_eligible_rows'] and result['prior_chain']==anchor['expected_prior_chain'] and
               result['request']==anchor['request'] and result['source_graph']==anchor['source_graph'],'current/source/backing/prior reference mismatch')
    ap_require(result['current_eligible_capacity_bytes']==52<<20 and result['protected_historical_bytes']==76<<20 and
               result['B_responsibility_unchanged_bytes']==128<<20 and result['A_historical_unqueried_bytes']==128<<20 and
               result['missing_evidence']==[] and result['v1_metadata_verdict']=='INCOMPLETE' and result['original_unknown_client_operation_history_preserved'] is True,'exact liability/history scope required')
    for key in ('normal_metadata_publication_proven','normal_native_or_SDK_release_ACK','journal_A_complete','ledger_mutated','fences_applied','new_allocation_authority','FM_payload_destroyed','authority_to_mutate_ledger','authority_to_launch_new_Guest'):
        ap_require(result[key] is False,'producer cannot grant ledger/Guest/normal ACK authority: '+key)
    physical=result['physical_closure']
    ap_require(physical['retained_files']==62 and physical['exact_original_births_gone']==117 and physical['holders']==[] and physical['denied']==[],'complete accepted current private-domain physical closure required')
    ap_require(result['source_runtime_closure']['current_source_files']==239 and result['source_runtime_closure']['installed_SDK_members']==53 and result['source_runtime_closure']['official_worker_source_link_inputs']==233,'full actual current runtime source closure required')
    without=dict(result);without.pop('assessment_sha256')
    ap_require(hashlib.sha256(ap_canonical(without)).hexdigest()==result['assessment_sha256'],'typed assessment canonical binding mismatch')
    return result

def fresh_terminal_proof(pins):
    a=pins['terminal']
    for key in ('release_manifest','candidate','request','source_graph'):ap_bound(a[key],4<<20)
    recorded=ap_decode(ap_bound(a['candidate'],2<<20))
    modes={name:a['module_mode'] for name in AP_ORDER}
    with ap_fresh_modules(a['modules'],modes,AP_ORDER) as modules:
        result=modules['uflow_terminal_domain_v2'].validate_candidate(Path(a['candidate']['path']),a['candidate']['sha256'])
        ap_check_terminal_result(result,a,recorded)
    for key in ('release_manifest','candidate','request','source_graph'):ap_bound(a[key],4<<20)
    return {'schema':2,'kind':'EValidatedCurrentTerminalProofV2','consumer_anchor_sha256':AP_SHA,
            'producer_commit':pins['producer_commit'],'release_manifest':a['release_manifest'],'candidate_reference':a['candidate'],
            'verified_module_sources':a['modules'],'verified_module_mode':a['module_mode'],'validated_candidate':result}
