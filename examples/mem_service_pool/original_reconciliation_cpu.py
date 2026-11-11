"""B-only fresh verifier regressions. Original control bytes never leave B."""
import copy
import json
import os
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tools/phasec'))
from verify_original_pool import ROOT,verify,structure,audit_and_events,family,nonlive,current_birth,read

def rejected(action):
    try:action()
    except (ValueError,KeyError,FileNotFoundError):return
    raise AssertionError('expected narrow verifier rejection')

def main():
    fixture=Path(__file__).parent/'reconciliation'
    # Fresh original SHA/binary/source/SDK/image/birth checks in place only.
    proof=verify(fixture/'assessment.json',fixture/'bindings.json')
    a=json.loads((fixture/'assessment.json').read_bytes())
    state=json.loads((ROOT/'runs/c013/state/state.json').read_bytes())
    store=(ROOT/'runs/c013/lower.store').read_bytes();journal=(ROOT/'runs/c013/lower.store.journal').read_bytes()
    events=[json.loads(s) for s in (ROOT/'runs/c013/pool-events.jsonl').read_bytes().splitlines()]
    checks=1
    changes=[('canonical',{'state':'active'}),('clients',[{'state':'Mapped'}]),
             ('ranges',{'allocations':[{}],'leases':[]}),('release_result',{'closed':True}),('state','Allocated')]
    for key,value in changes:
        bad=copy.deepcopy(state);bad['pools'][0][key]=value;rejected(lambda:structure(bad,a));checks+=1
    for key,value in [('charged_bytes',0),('creation_reserved_bytes',0),('native_jobs_executed',1),('catalog_range_leases',[{}])]:
        bad=copy.deepcopy(state);bad[key]=value;rejected(lambda:structure(bad,a));checks+=1
    for changed in [journal[:-1],journal+b'idem_begin\n',journal+b'managed_begin\n']:
        rejected(lambda:audit_and_events(store,changed,events,a));checks+=1
    for reason in ['store_write_uncertain','managed_store_write_uncertain','different_internal']:
        bad=copy.deepcopy(a);bad['gate']['response_body_hex']=('status=internal\nreason='+reason+'\n').encode().hex()
        rejected(lambda:audit_and_events(store,journal,events,bad));checks+=1
    for names in [[],['lower.store'],['lower.store','lower.store.journal','lower.store.managed']]:
        rejected(lambda:family(names,a));checks+=1
    own=str(os.getpid());rejected(lambda:nonlive({own:{'expected_birth':current_birth(own)}},current_birth));checks+=1
    rejected(lambda:read(ROOT/'runs/c013/never-created-required-proof',16384));checks+=1
    print(json.dumps({'scope':'fresh B read-only original proof and negative parser/liveness checks; no import/cancel or raw export',
        'checks':checks,'pass':True,'proof_sha256':proof['proof_sha256'],'original_responsibility_unchanged_bytes':68<<20}))

if __name__=='__main__':main()
