"""Manager-accepted E v6 anchors and fresh source loading; no ledger actions.
Also embedded verbatim in the new daemon's isolated admission interpreter.
"""
import hashlib,json,os,re,stat,sys,types
from pathlib import Path
if 'ap_read' not in globals():
    from accepted_producers import ap_read,ap_bound,ap_decode,ap_require,ap_canonical,AP_ROOT
V3_SHA='e63ff8efde507b49b5fefc7849dce4eee508bbd0a116fd2e88902833f4de46fc'
POLICY_SHA='824dd32fedde7d8b17fe4fd6d592fca7cf8cacbfd5745818d14e68606b348592'
V3_CONTEXT_SHA='e6241411f9217661c6ccf5376120c67a5c096629c501c48f02bfb4238fb8175c'
def v3_anchors(raw=None):
    if raw is None:raw=(Path(__file__).parents[2]/'examples/mem_service_pool/accepted_c081_v3/anchors.json').read_bytes()
    if type(raw) is str:raw=raw.encode()
    ap_require(len(raw)==6273 and hashlib.sha256(raw).hexdigest()==V3_SHA,'compiled externally accepted v6 anchor bytes required')
    a=ap_decode(raw);ap_require(a['accepted_context']['sha256']==V3_CONTEXT_SHA and a['acceptance']=='E_MANAGED_CACHE_PROFILE_REAL_CURRENT_WITNESS_AND_V6_CONTEXT_ACCEPTED_FOR_C_PRODUCTION_INTEGRATION','only corrected manager-accepted v6 context')
    return a
def v3_policy(raw=None):
    if raw is None:raw=(Path(__file__).parents[2]/'examples/mem_service_pool/accepted_c081_v3/execution-policy.json').read_bytes()
    if type(raw) is str:raw=raw.encode()
    ap_require(hashlib.sha256(raw).hexdigest()==POLICY_SHA,'compiled exact C execution namespace/nonce required');return ap_decode(raw)
def v3_source_rows(a):
    d=a['code_and_data'];base=Path(d['terminal_producer']['path']).parent;runtime=Path(d['wrapper']['path']).parent
    rows={r['path']:r for r in a['fixed_old_dependencies'].values()}
    for role in ['metadata_producer','terminal_producer','ownership_producer']:rows[d[role]['path']]=d[role]
    profile={**d['profile_validator'],'path':str(base/'uflow_managed_qemu_profile_v1.py')};rows[profile['path']]=profile
    for role in ['wrapper','codec','profile_validator']:rows[d[role]['path']]=d[role]
    for name in ['uflow_qemu_evidence.py','uflow_qemu_profile.py']:
        ref={**a['fixed_old_dependencies'][name],'path':str(runtime/name)};rows[ref['path']]=ref
    return rows
def v3_sources(a):
    # Complete source closure is checked BEFORE first execution, including
    # same-release dependency copies used inside E's fresh module loaders.
    rows=v3_source_rows(a);bodies={p:ap_bound(row,128<<10,0o755) for p,row in rows.items()}
    def load(role):
        row=a['code_and_data'][role];module=types.ModuleType('_accepted_v6_'+role);module.__file__=row['path']
        exec(compile(bodies[row['path']],row['path'],'exec'),module.__dict__);return module
    result={role:load(role) for role in ['terminal_producer','metadata_producer','ownership_producer','profile_validator','wrapper']}
    for p,row in rows.items():ap_require(ap_bound(row,128<<10,0o755)==bodies[p],'accepted source changed during fresh loading')
    return result
def v3_context(a,modules):
    raw=ap_bound(a['accepted_context'],128<<10);value=ap_decode(raw);d=a['code_and_data']
    ap_require(value['profile_manifest']==d['profile'] and value['terminal_candidate']==d['terminal_candidate'] and value['terminal_context']==d['terminal_context'], 'v6 complete selected profile/current-candidate linkage required; v5 cannot mix with release-v2')
    ap_require(value['guest_image']['image']=={'path':str(AP_ROOT/'implementation/build-host066/pool-agent.cpio.gz'),'bytes':13859680,'sha256':'5c00fbbd5359ce8bf44876f8a610c579a7eef9cefe62c56c8009c49ba284539f'},'accepted actual image066 binding required')
    result=modules['metadata_producer'].accepted_context(a['accepted_context']['path'],a['accepted_context']['sha256'])
    ap_require(result[0]==value and result[1]['qemu_binary']==d['qemu_binary']['path'] and result[1]['qemu_binary_sha256']==d['qemu_binary']['sha256'],'actual accepted_context API must select the same final v2 profile')
    return result
def v3_terminal_proof(a):
    modules=v3_sources(a);anchor,profile,recorded=v3_context(a,modules);d=a['code_and_data']
    ap_require(recorded==ap_decode(ap_bound(d['terminal_candidate'],128<<10)),'exact externally accepted recorded candidate')
    fresh=modules['terminal_producer'].validate_candidate(Path(d['terminal_candidate']['path']),d['terminal_candidate']['sha256'])
    ap_require(fresh==recorded and fresh['schema']==3 and fresh['run']=='c081' and fresh['state']['sha256']==a['current']['sha256'] and fresh['current_eligible_capacity_bytes']==52<<20 and fresh['protected_historical_bytes']==76<<20,'whole genuine actual c081 typed predicate/result required')
    ap_require([r['object_id'] for r in fresh['eligible_rows']]==[116,117,118] and fresh['physical_closure']['retained_files']==73 and fresh['physical_closure']['exact_original_births_gone']==144 and fresh['physical_closure']['holders']==[] and fresh['physical_closure']['denied']==[],'full actual current resources/73 files/144 births required')
    for key in ['normal_metadata_publication_proven','normal_native_or_SDK_release_ACK','journal_A_complete','journal_B_complete','ledger_mutated','fences_applied','new_allocation_authority','FM_payload_destroyed','authority_to_mutate_ledger','authority_to_launch_new_Guest']:
        ap_require(fresh[key] is False,'E evidence does not mint normal ACK or C execution/accounting authority')
    return {'schema':3,'kind':'EAcceptedCurrentC081ProofV3','consumer_anchor_sha256':V3_SHA,'accepted_context':a['accepted_context'],'candidate':d['terminal_candidate'],'verified_sources':v3_source_rows(a),'validated_candidate':fresh}
