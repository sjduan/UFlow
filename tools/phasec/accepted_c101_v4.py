"""Externally accepted c1e E inputs, distinct observed/current/future graphs.
Embedded in the new daemon; data hashes never select caller Python code.
"""
import hashlib,json,os,re,stat,sys,types
from pathlib import Path
if 'ap_read' not in globals():
    from accepted_producers import ap_read,ap_bound,ap_decode,ap_require,ap_canonical,AP_ROOT
V4_SHA='584f34c0a8acaaeca826fbf37d47b07fff514c8906fd3b127e31272f12a660d9'
POLICY_V4_SHA='48e7153e479acd0b033681d32a2ea8c28beacd584d18db634649d7b7b26fa746'
V4_CONTEXT_SHA='996f1ecf6abf9755090ff3c1122322c87873be44324e4261c1eec5ea925d83a8'
V4_CASE_SHA='5d61fe240f01c6fd31f2b4a3721a4532ea92be1b1a181e5de8c294fa71f9020f'
V4_HOST_CLOSEOUT_SHA='d089fa7bc5c95789c8c103b6d611177719c0304486d23fa8d2937d123d0fdcb1'

def v4_host_closeout(a,raw=None):
    if raw is None:raw=(Path(__file__).parents[2]/'examples/mem_service_pool/accepted_c101_v4/host-closeout.json').read_bytes()
    if type(raw) is str:raw=raw.encode()
    ap_require(hashlib.sha256(raw).hexdigest()==V4_HOST_CLOSEOUT_SHA,'compiled independently accepted fixed-guard R closeout required')
    c=ap_decode(raw)
    ap_require(c['parent_accepted_host_inputs_sha256']==V4_SHA and c['guard_closeout_manifest_sha256']=='9af7dd0a079ac9bf7ec30a1c6dfffa95ce6efd1a2cf430b0fed3a9d7894a203f' and c['pending']==[] and c['acceptance']=='E_COMPLETE_HOST_INPUTS_ACCEPTED_FOR_C_FINAL_BINDING_ONLY' and c['new_terminal_claim_Guest_authorized'] is False,'complete HOST acceptance is separate from runtime release')
    for key in ['source_commit','accepted_context','current_candidate','current_context','current_state_sha256','future_W_ELF','future_W_profile','guest_image','code_and_data','same_release_python_dependencies']:
        ap_require(c[key]==a[key],'accepted closeout changed original production input '+key)
    return c

def v4_anchors(raw=None,closeout=None):
    if raw is None:raw=(Path(__file__).parents[2]/'examples/mem_service_pool/accepted_c101_v4/anchors.json').read_bytes()
    if type(raw) is str:raw=raw.encode()
    ap_require(len(raw)==9419 and hashlib.sha256(raw).hexdigest()==V4_SHA,'compiled externally accepted c101-v4 anchor bytes required')
    a=ap_decode(raw)
    ap_require(a['acceptance']=='E_HOST_INPUTS_ACCEPTED_FOR_C_FINAL_BINDING_ONLY' and a['source_commit']=='c1e73ed2115e7a32400098a99c3b5bbf84405968' and a['accepted_context']['sha256']==V4_CONTEXT_SHA and a['current_state_sha256']=='934b30b07df72ab28e96ef173a639f55e64584db46dbdc13518828246ab48796' and a['new_terminal_claim_Guest_authorized'] is False,'accepted actual current/future binding only, not execution permission')
    v4_host_closeout(a,closeout)
    # Aliases are fixed C adapter roles, never additional unverified inputs.
    d=a['code_and_data'];d.update(wrapper=d['runtime_wrapper'],codec=d['R_codec'],write_codec=d['W_codec'],profile=a['future_W_profile'],qemu_binary=a['future_W_ELF'],terminal_candidate=a['current_candidate'],terminal_context=a['current_context'],ownership_producer=a['same_release_python_dependencies']['uflow_monitor_ownership_v3.py'])
    return a

def v4_policy(raw=None):
    if raw is None:raw=(Path(__file__).parents[2]/'examples/mem_service_pool/accepted_c101_v4/execution-policy.json').read_bytes()
    if type(raw) is str:raw=raw.encode()
    ap_require(hashlib.sha256(raw).hexdigest()==POLICY_V4_SHA,'compiled current-c101 execution namespace/nonce required');return ap_decode(raw)

def v4_source_rows(a):
    rows={x['path']:x for x in a['same_release_python_dependencies'].values()};d=a['code_and_data']
    for name in ['metadata_producer','terminal_producer','ownership_producer','profile_validator','wrapper','codec','write_codec']:rows[d[name]['path']]=d[name]
    runtime=Path(d['wrapper']['path']).parent
    for name in ['uflow_managed_qemu_profile_v1.py','uflow_qemu_evidence.py','uflow_qemu_profile.py']:
        row=a['same_release_python_dependencies'][name];path=str(runtime/name);rows[path]={**row,'path':path}
    return rows

def v4_sources(a):
    rows=v4_source_rows(a)
    def source_bytes(row):return ap_bound({k:row[k] for k in ('path','bytes','sha256')},128<<10,0o644)
    bodies={p:source_bytes(row) for p,row in rows.items()}
    def load(role):
        ref=a['code_and_data'][role];m=types.ModuleType('_accepted_c1e_'+role);m.__file__=ref['path'];exec(compile(bodies[ref['path']],ref['path'],'exec'),m.__dict__);return m
    result={role:load(role) for role in ['terminal_producer','metadata_producer','ownership_producer','profile_validator','wrapper']}
    for p,row in rows.items():ap_require(source_bytes(row)==bodies[p],'accepted v4 source changed during loading')
    return result

def v4_context(a,modules):
    accepted=ap_decode(ap_bound(a['accepted_context'],128<<10));d=a['code_and_data']
    ap_require(accepted['schema']==4 and accepted['profile_manifest']==d['profile'] and accepted['terminal_candidate']==d['terminal_candidate'] and accepted['terminal_context']==d['terminal_context'] and accepted['guest_image']==a['guest_image'],'whole externally accepted v4 data roles required')
    result=modules['metadata_producer'].accepted_context(a['accepted_context']['path'],a['accepted_context']['sha256'])
    ap_require(result[0]==accepted and result[1]['qemu_binary']==d['qemu_binary']['path'] and result[1]['qemu_binary_sha256']==d['qemu_binary']['sha256'],'actual E API must select the same W/R profile')
    ap_require(result[2]==ap_decode(ap_bound(d['terminal_candidate'],128<<10)),'exact externally accepted recorded c101 candidate')
    return result

def v4_terminal_proof(a):
    modules=v4_sources(a);accepted,profile,recorded=v4_context(a,modules);d=a['code_and_data']
    fresh=modules['terminal_producer'].validate_candidate(Path(d['terminal_candidate']['path']),d['terminal_candidate']['sha256'])
    ap_require(fresh==recorded and fresh['schema']==4 and fresh['run']=='c101' and fresh['state']['sha256']==a['current_state_sha256'] and fresh['current_eligible_capacity_bytes']==52<<20 and fresh['protected_historical_bytes']==76<<20,'whole actual c101 current predicate result required')
    physical=fresh['physical_closure']
    ap_require([x['object_id'] for x in fresh['eligible_rows']]==[119,120,121] and len(physical['files'])==84 and len(physical['births'])==179 and physical['holders']==[] and physical['denied']==[] and fresh['native_W_grant_observed_in_c101'] is False,'actual frozen W failure/84 files/179 births required')
    for key in ['normal_metadata_publication_proven','normal_native_or_SDK_release_ACK','journal_A_complete','journal_B_complete','ledger_mutated','fences_applied','new_allocation_authority','FM_payload_destroyed','authority_to_mutate_ledger','authority_to_launch_new_Guest']:
        ap_require(fresh[key] is False,'E proof is not normal ACK or C accounting/execution authority')
    return {'schema':4,'kind':'EAcceptedCurrentC101ProofV4','consumer_anchor_sha256':V4_SHA,'accepted_context':a['accepted_context'],'candidate':d['terminal_candidate'],'verified_sources':v4_source_rows(a),'validated_candidate':fresh}
