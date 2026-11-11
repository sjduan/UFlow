"""Narrow c013 verifier, embedded verbatim in the Rust responsibility importer.

Reads original metadata in place on B. It does not export original file contents,
modify the old authority, contact its SDK, signal processes, or cancel capacity.
Only the one Catalog may consume this proof and call cancel_uncreated.
"""
import hashlib
import json
from pathlib import Path
import sys

ROOT=Path('/workspace/uflow/runs/mem-service-shared-pool-20261108')
ASSESSMENT_SHA='f4e89d644df46d78cf4b611ed51f10efc5bc0b99591281b95f5a26a65e57aba4'
def sha(data):return hashlib.sha256(data).hexdigest()
def require(condition,reason):
    if not condition:raise ValueError('c013 reconciliation rejected: '+reason)
def fnv(data):
    h=2166136261 if data else 0
    for byte in data:h=((h^byte)*16777619)&0xffffffff
    return h
def read(path,maximum):
    require(path.is_file() and not path.is_symlink(),'regular original file required')
    require(path.stat().st_size<=maximum,'bounded evidence file')
    return path.read_bytes()

def family(names,a):
    require(set(names)==set(a['original_lower_store_family']),'no extra original store/managed/history family')

def nonlive(records,observe):
    for raw_pid,record in records.items():
        require(observe(raw_pid)!=record['expected_birth'],'original exact birth still live')

def current_birth(raw_pid):
    try:return int(Path('/proc',raw_pid,'stat').read_text().rsplit(')',1)[1].split()[19])
    except FileNotFoundError:return None


def structure(state,a):
    identity=a['original_identity']
    require(state['schema']==1 and len(state['pools'])==len(state['operations'])==1,'exact original pool/operation count')
    require(state['charged_bytes']==4<<20 and state['creation_reserved_bytes']==64<<20 and state['budget_bytes']==128<<20,'original68MiB in sole128MiB ledger')
    require(not state['catalog_range_leases'] and state['native_jobs_executed']==0 and state['payload_rpc_bytes']==0,'no native/range/payload work')
    pool=state['pools'][0];op=state['operations'][0]
    for key in ('pool_id','pool_epoch','provider_session','nbytes','resource_id','placement_id','binding_id'):
        require(pool[key]==identity[key],'original '+key)
    require(pool['state']=='CreationUnknown' and pool['canonical'] is None and not pool['clients'] and
            pool['ranges']=={'allocations':[],'leases':[]} and pool['release_result'] is None,'no created backing/client/range/release')
    require(op['key']==identity['operation_record_key'] and op['state']=='Unknown' and op['response']['code']=='Unknown' and
            op['response']['operation_id']==identity['operation_id'],'original actor/operation Unknown history')
    request=json.loads(op['request'])
    require(request=={'nbytes':64<<20,'op':'CreateManagedPool','operation_id':identity['operation_id'],'schema':2},'original upper request')
    loaded=state['provider_loaded']
    require(loaded['session']==identity['provider_session'] and loaded['source']==a['frozen_mem_service_commit'] and
            set(loaded['nodes'])=={'nodeA','nodeB'} and len(a['retained_canary_objects'])==2,'original provider/canary identity')
    for canary in a['retained_canary_objects']:
        node=loaded['nodes'][canary['node']]
        require(loaded['bootstrap_charge_objects'][canary['node']]==canary['object_id'] and node['boot_id']==canary['boot_id'] and
                node['incarnation']==canary['backing']['generation'] and loaded['realm']==canary['backing']['realm'] and
                node['canary']['canary_bytes']==2<<20 and node['canary']['canary_prepared'] is True and node['ready']['canary_ready'] is True,
                'retained real canary obligation')
        require(canary['nbytes']==2<<20 and canary['state']=='UnknownCanaryClose' and canary['SDK_checked_close_confirmed'] is False,
                'no inferred canary close')
    return pool,op


def audit_and_events(store,journal,events,a):
    family=a['original_lower_store_family']
    for name,raw in [('lower.store',store),('lower.store.journal',journal)]:
        require(len(raw)==family[name]['bytes'] and sha(raw)==family[name]['sha256'],'exact complete '+name)
    require(store.count(b'audit_begin\n')==store.count(b'audit_end\n')==1 and
            journal==b'mem_service_journal_v1\n'+store[store.index(b'audit_begin\n'):],'exact sole audit journal, no managed/history records')
    fields={}
    for line in store[store.index(b'audit_begin\n')+12:store.index(b'audit_end\n')].decode().splitlines():
        k,v=line.split('=',1);require(k not in fields,'unique audit fields');fields[k]=v
    gate=a['gate'];body=bytes.fromhex(gate['request_body_hex']);response=bytes.fromhex(gate['response_body_hex'])
    session='owner-'+a['original_identity']['pool_id']
    expected=('key='+a['original_identity']['pool_id']+'\nidempotency_key='+session+'-allocate-1\nsession_id='+session+
              '\nhome_node=nodeA\nsize_bytes=67108864\nalignment_bytes=65536\ncapabilities=1\n').encode()
    require(body==expected and response==b'status=internal\nreason=data_plane_not_ready\n','this exact pre-dispatch reason/request only')
    require((len(body),fnv(body),len(response),fnv(response))==(238,281788076,44,3845867773),'complete request/reply lengths and checksums')
    for key,value in {'operation':'112','status':'10','key':a['original_identity']['pool_id'],
            'idempotency_key':session+'-allocate-1','request_checksum':'281788076','response_checksum':'3845867773'}.items():
        require(fields.get(key)==value,'audit '+key)
    calls=[e for e in events if e['event']=='lower-control']
    done=[e for e in events if e['event']=='guest-completion' and e['op']=='allocate']
    require(len(calls)==len(done)==1 and calls[0]['operation']==112 and calls[0]['status']==10 and
            calls[0]['request_bytes']==286 and calls[0]['response_bytes']==92 and done[0]['request']==done[0]['correlation']==1024 and
            done[0]['node']=='nodeA' and done[0]['receipt']['phase']=='unknown' and done[0]['receipt']['owner_present'] is False and
            done[0]['receipt']['mapping_pending'] is False,'original bridge transaction and no mapping')


def verify(assessment_path,bindings_path):
    raw=read(assessment_path,32768);require(sha(raw)==ASSESSMENT_SHA,'approved assessment SHA')
    a=json.loads(raw);bindings_raw=read(bindings_path,262144)
    require(sha(bindings_raw)==a['B_readonly_receipt_sha256'],'approved source/controller binding receipt')
    bindings=json.loads(bindings_raw);require(len(bindings['files'])==247,'complete historical binding set')
    evidence={}
    for name,row in bindings['files'].items():
        path=Path(name);require(path.is_relative_to(ROOT),'only task-local bindings')
        data=read(path,64<<20)
        require(len(data)==row['bytes'] and sha(data)==row['sha256'],'fresh historical source/controller/SDK/image binding '+path.name)
        evidence[str(path.relative_to(ROOT))]=sha(data)
    old=ROOT/'runs/c013'
    family_names={p.name for p in old.iterdir() if p.name.startswith('lower.store') or
            any(word in p.name.lower() for word in ('managed','checkpoint','idempotency','history'))}
    family(family_names,a)
    require(not (old/'state/state-next.json').exists(),'no partial original snapshot')
    state_bytes=read(old/'state/state.json',16384);events_bytes=read(old/'pool-events.jsonl',16384)
    for name,data in [('runs/c013/state/state.json',state_bytes),('runs/c013/pool-events.jsonl',events_bytes)]:
        require(sha(data)==a['original_files'][name],'exact original state/events')
    state=json.loads(state_bytes);_,op=structure(state,a)
    store=read(old/'lower.store',16384);journal=read(old/'lower.store.journal',16384)
    audit_and_events(store,journal,[json.loads(s) for s in events_bytes.splitlines()],a)
    evidence['runs/c013/lower.store.journal']=sha(journal)
    nonlive(a['original_nonlive_births'],current_birth)
    original=a['original_identity']
    result={'schema':1,'kind':'verified-c013-uncreated-reservation','assessment_sha256':ASSESSMENT_SHA,
        'identity':original,'before_charged_bytes':4<<20,'before_reserved_bytes':64<<20,
        'retained_canaries':a['retained_canary_objects'],'historical_unknown_operation':op,
        'historical_pool':state['pools'][0],'evidence_sha256':evidence,
        'reason':'exact original pre-dispatch data_plane_not_ready; no managed allocation or idempotency entered',
        'SDK_ACK_Unmapped_or_canary_close_claimed':False}
    result['proof_sha256']=sha(json.dumps(result,sort_keys=True,separators=(',',':')).encode())
    return result


if __name__=='__main__':
    require(len(sys.argv)==3,'assessment and binding paths required')
    print(json.dumps(verify(Path(sys.argv[1]),Path(sys.argv[2])),separators=(',',':')))
