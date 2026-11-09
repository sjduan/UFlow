"""Fixed c027 conservative succession proof. No restore, ACK or data access.

Only the Rust service invokes verify() with the embedded old verifier while
holding the original authority lock. This never offers a fixture/root flag.
Semantic helpers are separately exercised using isolated CPU documents.
"""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys
import time

ROOT=Path('/workspace/uflow/runs/mem-service-shared-pool-20261108')
ASSESSMENT_SHA='b283ec290bb2487d3e81380cc5dc8cd0b5fd9db34b76eb0488171f8066600151'
C013_PHYSICAL_BINDINGS={'runs/c013/fm-carrier-active.json': {'bytes': 4252, 'sha256': '82045b4d78c0584f3913cf0ede23053e1020e44fd78627f87b307d0d06c9c551'}, 'runs/c013/post-close-audit.json': {'bytes': 5519, 'sha256': '711af4991e2ecaff2cad5f132d03b0885a7692d67a6beb4f1b115b81c4272cbb'}, 'runs/c013/control-endpoints-active.json': {'bytes': 612, 'sha256': '14f9e1ed1844d5f39c3791b561d2551784101c522aeec5637dfbd28970fdbaf9'}}
OLD_PROOF='9f80af5f1dab8cce48e9b3853c8f0f4628b3b6e6697d31cd6cc24253a8796dfe'
def require(ok,message):
    if not ok:raise ValueError(message)
def sha(raw):return hashlib.sha256(raw).hexdigest()
def store_family(names):require(not list(names),'no c027 lower store/managed/journal family')
def nonlive(records,observe):
    for pid,expected in records.items():require(observe(pid)!=expected,'old exact birth live')
def read(path,maximum=4<<20):
    require(path.is_file() and not path.is_symlink(),'regular bounded evidence required')
    require(path.stat().st_size<=maximum,'evidence size bound')
    raw=path.read_bytes();require(len(raw)<=maximum,'evidence grew');return raw
def birth(pid):
    try:return int(proc_read(Path('/proc',str(pid),'stat'),65536).decode().rsplit(')',1)[1].split()[19])
    except (FileNotFoundError,ProcessLookupError):return None
def proc_read(p,maximum):
    with p.open('rb') as f:raw=f.read(maximum+1)
    require(len(raw)<=maximum,'bounded proc read');return raw

def documents(a,raw,old_proof):
    """Derive identities from complete known bytes, not assessment safe flags."""
    expected=a['files_requiring_fresh_exact_byte_validation_on_B']
    require(set(raw)==set(expected),'complete exact c027 evidence set')
    j=lambda name:json.loads(raw[name])
    state=j('runs/c027/state/state.json');intent=j('implementation/responsibility-c013/intent.json')
    result=j('implementation/responsibility-c013/result.json')
    require(old_proof['proof_sha256']==OLD_PROOF==a['original_c013_proof_sha256'],'original proof continuity')
    require(intent['phase']=='Restored68PendingProofCommit' and result['phase']=='VerifiedUncreatedTerminal','completed predecessor only')
    require(intent['verified_proof']==result['verified_proof']==old_proof and result['proof_sha256']==OLD_PROOF,'original evidence bytes/proof lineage')
    require(result['cancelled_uncreated_bytes']==64<<20 and not result['SDK_ACK_Unmapped_or_canary_close_claimed'],'cancel64 history, no ACK')
    require(state['reconciliation']['proof_sha256']==OLD_PROOF and state['reconciliation']['phase']=='VerifiedUncreatedTerminal','predecessor state lineage')
    require(state['charged_bytes']==state['retained_bytes']==state['creation_reserved_bytes']==4<<20 and state['active_bytes']==0,'predecessor total8')
    require(state['pools']==state['operations']==state['catalog_range_leases']==[] and state['native_jobs_executed']==0,'no business obligation hidden')
    require(state['resource_ledger']['reservations']=={'canary:nodeA':2<<20,'canary:nodeB':2<<20},'actual created canary reservations retained')
    require(state['resource_ledger']['retained_bytes']==state['resource_ledger']['charged_bytes']==4<<20,'old charged4')
    require(result['retained_canaries']==old_proof['retained_canaries']==a['retained_objects'][:2],'old typed identities')
    require(old_proof['identity']==a['terminal_historical_business_identity'],'complete terminal identity')
    for old in result['retained_canaries']:
        row=next((r for r in state['resource_ledger']['resources'] if r['object_id']==old['object_id']),None)
        require(row is not None and row['retained']==old and row['nbytes']==2<<20,'old retained ledger witness')
        require({k:row['backing'][k] for k in old['backing']}==old['backing'],'old BackingId')
    require(len(state['resource_ledger']['resources'])==2,'no extra old backings')
    provider=j('runs/c027/provider.json');loaded=state['provider_loaded']
    require(provider['realm']==loaded['realm'] and provider['session']==loaded['session'] and loaded['source']=='546985e26bce60798fa3f88329c3fbed8350c798','bound current provider')
    identities=j('runs/c027/guest-identities.json');require(len(identities)==2 and {i['node'] for i in identities}=={'nodeA','nodeB'},'exact two boot identities')
    events=[json.loads(s) for s in raw['runs/c027/pool-events.jsonl'].splitlines()]
    calls=[e for e in events if e['event']=='lower-control']
    require(len(calls)==2 and all(e['operation']==0x76 and e['status']==0 and e['error_code']==0 for e in calls),'only directory register, no business/replay')
    require(not any(e['event'].startswith(('E1-','E2-','E3-','E4-')) for e in events),'no completed business matrix')
    retained=list(result['retained_canaries'])
    for name,id in [('nodeA',103),('nodeB',104)]:
        identity=next(i for i in identities if i['node']==name)
        prepare=[e['receipt'] for e in events if e['event']=='bootstrap-prepared' and e['node']==name]
        verify=[e['receipt'] for e in events if e['event']=='bootstrap-verified' and e['node']==name]
        require(len(prepare)==len(verify)==1,'one prepare/peer verify per actual Guest')
        prepare,verify=prepare[0],verify[0]
        require(prepare['phase']==verify['phase']=='completed' and prepare['canary_prepared'] and verify['canary_ready'],'actual completed canary')
        seed=17 if name=='nodeA' else 29;h=1469598103934665603
        for i in range(4096):h=((h^((seed+i*29)&255))*1099511628211)&((1<<64)-1)
        require(prepare['seed']==seed and prepare['checksum']==h and prepare['canary_bytes']==2<<20 and prepare['visible_bytes']==4096,'independent visible checksum/size')
        peer=next(e['receipt'] for e in events if e['event']=='bootstrap-prepared' and e['node']!=name)
        require(verify['verified_checksum']==peer['checksum'] and verify['guest_pid']==prepare['guest_pid'],'same root verified opposite canary')
        serial=raw['runs/c027/'+name+'.serial.log'].decode()
        serial_id=[json.loads(s.split(' ',1)[1]) for s in serial.splitlines() if s.startswith('UFLOW_ENV_IDENTITY ')]
        # The accepted E0 serial capture repeats nodeB's identical initial
        # identity once. Repetition is not a new boot; differing identity fails.
        require(1<=len(serial_id)<=2 and all(v==identity for v in serial_id),'serial boot identity')
        replies=[]
        for line in serial.splitlines():
            if line.startswith('UFPOOL_REPLY '):
                fields=line.split(' ',3);require(fields[1]==fields[2],'serial correlation')
                replies.append(json.loads(bytes.fromhex(fields[3])))
        require(replies.count(prepare)==replies.count(verify)==1,'serial exact receipts')
        witness={'object_id':id,'nbytes':2<<20,'state':'UnknownCanaryClose','SDK_checked_close_confirmed':False,
            'node':name,'boot_id':identity['boot_id'],'agent_session':identity['agent_session'],'incarnation':identity['incarnation'],
            'provider_session':provider['session'],'backing':{'realm':provider['realm'],'provider':'mem_service_obmm',
                'resource':'canary:'+name+':'+identity['boot_id'],'generation':identity['incarnation']},
            'canary_prepare':prepare,'peer_verify':verify}
        require(loaded['bootstrap_charge_objects'][name]==id and witness==a['retained_objects'][len(retained)],'exact new reserved object/descriptor/boot/session witness')
        retained.append(witness)
    require(len({c['object_id'] for c in retained})==4 and len({json.dumps(c['backing'],sort_keys=True) for c in retained})==4,'unique four objects/backings')
    monitor=j('runs/c027/monitor/result.json');audit=j('runs/c027/post-close-audit.json')
    require(monitor['actual_task_exit_code']==1 and monitor['process_tree_empty'] and monitor['task_resources_closed'] and not monitor['remaining'] and not monitor['evidence_errors'],'closed failed predecessor only')
    require(audit['original_births_gone'] and not audit['holder_audit']['holders'] and not audit['holder_audit']['denied'] and not audit['logical_allocation_ACK_inferred'],'historical physical audit, not logical ACK')
    return {'schema':1,'kind':'verified-c027-retained8-succession','assessment_sha256':ASSESSMENT_SHA,
        'original_proof_sha256':OLD_PROOF,'predecessor_intent':intent,'predecessor_result':result,'predecessor_state_sha256':sha(raw['runs/c027/state/state.json']),
        'identity':old_proof['identity'],'historical_pool':old_proof['historical_pool'],
        'historical_unknown_operation':old_proof['historical_unknown_operation'],'retained_canaries':retained,
        'before_total_bytes':8<<20,'charged_bytes':8<<20,'reserved_bytes':0,
        'cancel64_repeated':False,'SDK_ACK_Unmapped_or_canary_close_claimed':False,
        'evidence_sha256':{n:sha(b) for n,b in raw.items()}}

def holder_audit(inodes):
    deadline=time.monotonic()+15;processes=descriptors=maps_bytes=0
    for p in Path('/proc').iterdir():
        if not p.name.isdigit():continue
        processes+=1;require(processes<=6000 and time.monotonic()<deadline,'holder audit process/time bound')
        try:
            with os.scandir(p/'fd') as stream:
                for e in stream:
                    descriptors+=1;require(descriptors<=1000000 and time.monotonic()<deadline,'holder FD/time bound')
                    try:
                        info=e.stat();require((info.st_dev,info.st_ino) not in inodes,'old FM FD still held')
                    except (FileNotFoundError,ProcessLookupError):pass
            raw=proc_read(p/'maps',16<<20);maps_bytes+=len(raw)
            require(maps_bytes<=256<<20,'total maps byte bound')
            for line in raw.decode().splitlines():
                f=line.split(maxsplit=5);major,minor=map(lambda x:int(x,16),f[3].split(':'))
                require((os.makedev(major,minor),int(f[4])) not in inodes,'old FM VMA still held')
        except (FileNotFoundError,ProcessLookupError):continue
        # Permission/parse/read uncertainty is never a successful empty audit.
    return {'processes':processes,'FD_entries':descriptors,'maps_bytes':maps_bytes,'deadline_seconds':15,'holders':[]}

def verify(old_assessment,old_bindings,assessment):
    import fcntl
    raw_a=read(assessment,32768);require(sha(raw_a)==ASSESSMENT_SHA,'pinned retained8 assessment')
    a=json.loads(raw_a);ns={'__name__':'fixed_old_verifier'};exec(ORIGINAL_SOURCE,ns)
    old=ns['verify'](old_assessment,old_bindings)
    raw={}
    for n,row in a['files_requiring_fresh_exact_byte_validation_on_B'].items():
        p=ROOT/n;require(p.is_relative_to(ROOT),'only task evidence')
        b=read(p);require(len(b)==row['bytes'] and sha(b)==row['sha256'],'exact retained8 bytes: '+n);raw[n]=b
    store_family((ROOT/'runs/c027').glob('lower.store*'))
    proof=documents(a,raw,old)
    monitor=json.loads(raw['runs/c027/monitor/result.json'])
    nonlive(monitor['owned_births'],birth)
    extra={}
    for n,row in C013_PHYSICAL_BINDINGS.items():
        data=read(ROOT/n);require(len(data)==row['bytes'] and sha(data)==row['sha256'],'original c013 physical binding drift')
        extra[n]=json.loads(data);proof['evidence_sha256'][n]=sha(data)
    endpoints=json.loads(raw['runs/c027/control-endpoints-active.json'])+extra['runs/c013/control-endpoints-active.json']
    live={s.split()[-1] for s in proc_read(Path('/proc/net/unix'),16<<20).decode().splitlines()[1:] if len(s.split())>=8}
    require(not any(row['path'] in live for row in endpoints),'old control endpoint kernel-active')
    inventory=json.loads(raw['runs/c027/fm-carrier-active.json'])['files']+extra['runs/c013/fm-carrier-active.json']['files'];inodes=set()
    require(len(inventory)==20,'complete two-generation FM inventory')
    for row in inventory:
        p=Path(row['path']);require(any(p.is_relative_to(ROOT/'runs'/j/'fm') for j in ['c013','c027']),'exact FM namespace')
        info=p.lstat();require((info.st_dev,info.st_ino,info.st_uid,stat.S_IFMT(info.st_mode))==
            (row['dev'],row['inode'],row['uid'],row['file_type']),'retained FM identity drift')
        aliases=row['kernel_inode_aliases'];require([row['dev'],row['inode']] in aliases,'bound inode aliases')
        inodes.update(map(tuple,aliases))
    require({str(p) for j in ['c013','c027'] for p in (ROOT/'runs'/j/'fm').rglob('*') if not p.is_dir()}=={r['path'] for r in inventory},'extra or missing FM')
    proof['fresh_physical_audit']=holder_audit(inodes)
    with (ROOT/'coordination/guest-execution.lock').open('rb') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);fcntl.flock(lock,fcntl.LOCK_UN)
    proof['proof_sha256']=sha(json.dumps(proof,sort_keys=True,separators=(',',':')).encode())
    return proof

if __name__=='__main__':
    require(len(sys.argv)==4,'fixed old assessment/bindings and retained8 assessment required')
    print(json.dumps(verify(*map(Path,sys.argv[1:])),separators=(',',':')))
