"""Fixed closed c039 control-flow proof. No generic absence or restore API.

verify() is invoked only by the embedded Rust path under original authority
lock and fresh E0 Guest admission. semantic() is pure isolated CPU evidence QA.
"""
import ast,gzip,hashlib,json,os,stat,sys
from pathlib import Path
if 'R8_SOURCE' in globals():
    _r={'__name__':'fixed_retained_helpers'};exec(R8_SOURCE,_r)
    _q={'__name__':'fixed_q76_helpers','R8_SOURCE':R8_SOURCE};exec(Q_SOURCE,_q)
    _k={'__name__':'fixed_profile_helpers'};exec(K_SOURCE,_k)
else:
    import verify_retained8_pool,verify_quarantine76_pool,kernel_profile_gate
    _r=vars(verify_retained8_pool);_q=vars(verify_quarantine76_pool);_k=vars(kernel_profile_gate)
ROOT=_r['ROOT'];require=_r['require'];sha=_r['sha']
SHA='aaa11ce813b19a5c325352a3d108497a8cb552683d1a67b05160d4e37ecd9ff1'
OLD_JOBS=('c013','c027','c033','c039')
MAX_ASSESSMENT=128<<10
MAX_FILE=2<<20
MAX_TOTAL=16<<20
def read(path,maximum=MAX_FILE):
    require(path.is_absolute() and path==path.resolve(),'canonical fixed evidence path')
    require(not any(p.is_symlink() for p in [path,*path.parents] if p.is_relative_to(ROOT)),'no symlink evidence component')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    try:
        before=os.fstat(fd);require(stat.S_ISREG(before.st_mode) and before.st_size<=maximum,'bounded regular evidence')
        data=bytearray()
        while len(data)<=maximum:
            part=os.read(fd,min(65536,maximum+1-len(data)))
            if not part:break
            data.extend(part)
        after=os.fstat(fd)
        require((before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns)==(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns) and len(data)==before.st_size,'stable complete evidence')
        return bytes(data)
    finally:os.close(fd)
def exact(a,raw):
    expected=a['files_requiring_fresh_exact_byte_validation'];require(len(expected)==286 and set(raw)==set(expected),'complete fixed286 evidence')
    require(sum(len(b) for b in raw.values())<=MAX_TOTAL,'aggregate evidence bound')
    for n,b in raw.items():
        row=expected[n];require(len(b)<=MAX_FILE and len(b)==row['bytes'] and sha(b)==row['sha256'],'exact evidence '+n)
def semantic(a,raw):
    """Consume complete original data; CPU negatives may edit private copies."""
    require(set(raw)==set(a['files_requiring_fresh_exact_byte_validation']),'complete semantic input')
    j=lambda n:json.loads(raw[n]);prefix='implementation/responsibility-c013/'
    closure=a['executed_runtime_closure'];group=j('implementation/config-c039-profile-v1/group.json')
    require(group['kernel']==closure['selected_kernel']['path'] and group['kernel_profile']==closure['kernel_profile'],'exact original Image/validator binding')
    require(sha(raw['implementation/build-host030/build-receipt.json'])==closure['build_receipt_sha256'],'exact executed build receipt')
    s=j('runs/c039/state/state.json');d=j(prefix+'successor-c033-v1/result.json');intent=j(prefix+'successor-c033-v1/intent.json')
    pending=dict(d);pending['phase']='ClosedC033Quarantine76Pending'
    require(intent==pending and d['phase']=='ClosedC033Quarantine76','complete prior succession')
    proof=d['verified_proof'];p=dict(proof);digest=p.pop('proof_sha256')
    require(sha(json.dumps(p,sort_keys=True,separators=(',',':')).encode())==digest==a['completed_succession_proof_sha256']==d['proof_sha256'],'prior proof checksum')
    require(proof['kind']=='verified-closed-c033-quarantine76' and proof['current_E0_guest_admission']['job']=='c039' and proof['current_E0_guest_admission']['exclusive_locks']==3,'actual completed E0 succession')
    for n,value in proof['predecessors'].items():require(j(n)==value,'full original/retained8 continuity')
    require(d['retained_canaries']==a['old_retained_canaries'] and d['quarantined_creation']==a['old_uncertain_business_reservation'],'exact old12 and attempted64')
    require([s[k] for k in ['charged_bytes','retained_bytes','active_bytes','creation_reserved_bytes','retained_reserved_bytes','active_reserved_bytes','budget_bytes']]==[12<<20,12<<20,0,68<<20,64<<20,4<<20,128<<20],'full current80 state')
    expected=dict(d);expected.pop('verified_proof');expected['evidence_file_count']=33
    require(s['reconciliation']==expected,'current state actually inherited prior proof')
    u=a['old_uncertain_business_reservation'];ledger=s['resource_ledger']
    require(ledger['reservations']=={u['key']:64<<20,'canary:nodeA':2<<20,'canary:nodeB':2<<20} and ledger['retained_uncertain_reservations']==[{'key':u['key'],'object_id':107,'witness':u}],'only exact three reservations')
    require(s['quarantined_pools']==[u['pool']] and u['pool']['state']=='CreationUnknown' and u['pool']['canonical'] is None,'historical attempted context inaccessible')
    require(len(ledger['resources'])==6,'six old backing entries')
    for c in a['old_retained_canaries']:
        row=next(r for r in ledger['resources'] if r['object_id']==c['object_id'])
        require(row['retained']==c and row['nbytes']==2<<20 and all(row['backing'][k]==v for k,v in c['backing'].items()),'full retained backing witness')
    require(s['pools']==s['operations']==s['catalog_range_leases']==[] and s['control_counts']=={} and s['bootstrap_readiness'] is None and s['native_jobs_executed']==s['payload_rpc_bytes']==0,'no current work/unknown')
    require(s['provider_loaded']['bootstrap_charge_objects']=={'nodeA':108,'nodeB':109},'unstarted IDs never reused')
    provider=j('runs/c039/provider.json');require(provider['realm']==s['provider_loaded']['realm'] and provider['session']==s['provider_loaded']['session'],'actual reserved context')
    lines=raw['runs/c039/pool-events.jsonl'];require(lines.endswith(b'\n'),'complete event tail')
    events=[json.loads(line) for line in lines.splitlines()]
    require([e['event'] for e in events]==['quarantine76-succession-established-before-Guest','matrix-failure'],'only ordered two events')
    require(events[0]['proof_sha256']==digest and events[0]['current_canary_reserved_bytes']==4<<20,'actual prestart reservation boundary')
    failure=j('runs/c039/guest-first-failure.json')
    require(events[1]['detail']==failure['message']=='actual new regular FM identity' and events[1]['original_pool'] is None and failure['type']=='ValueError' and failure['independent_monitor_notified_before_this_diagnostic'],'exact failure before start')
    source='implementation/source-host030/';matrix=ast.parse(raw[source+'tools/phasec/run_mem_service_pool_matrix.py'])
    calls=[n for n in ast.walk(matrix) if isinstance(n,ast.Call)]
    positions=[next(n.lineno for n in calls if isinstance(n.func,ast.Name) and n.func.id==name) for name in ['verify_new_fm','GuestNode']]
    positions.append(next(n.lineno for n in calls if isinstance(n.func,ast.Attribute) and n.func.attr=='send' and n.args and isinstance(n.args[0],ast.Constant) and n.args[0].value=='start'))
    require(positions==[141,145,147] and positions[0]<positions[1]<positions[2],'fixed positive source control flow')
    verifier=raw[source+'tools/phasec/verify_quarantine76_pool.py'];require(verifier.count(b"'actual new regular FM identity'")==1,'unique failure literal')
    agent=raw[source+'crates/uf-ub-sim-agent/c/pool_agent.c']
    require(b'if(!strcmp(input,"start")){if(create_client(0,"owner")||pool_emit("UFPOOL_STARTED\\n"))' in agent and b'if(!strcmp(input,"sdk-check")){if(pool_emit("UFLOW_ENV_SDK_OK\\n"))' in agent,'actual agent start boundary versus ABI check')
    identities=j('runs/c039/guest-identities.json');require(identities==a['c039_guest_identities'],'exact two identities')
    for identity in identities:
        text=raw['runs/c039/'+identity['node']+'.serial.log'].decode();ids=[json.loads(x.split(' ',1)[1]) for x in text.splitlines() if x.startswith('UFLOW_ENV_IDENTITY ')]
        require(1<=len(ids)<=2 and all(x==identity for x in ids) and 'UFLOW_ENV_SDK_OK\n' in text,'actual boot/ABI trace')
        require('UFPOOL_STARTED' not in text and 'UFPOOL_REPLY' not in text,'no start receipt or protocol operation')
    starts=j('runs/c039/guest-starts.json');require(len(starts)==2 and {x['node'] for x in starts}=={'nodeA','nodeB'},'two pure QEMU starts')
    for start in starts:
        argv=start['argv'];copy={'node':start['node'],'argv':list(argv)}
        require(argv[argv.index('-kernel')+1]==a['executed_runtime_closure']['selected_kernel']['path'],'actual selected Image')
        copy['argv'][argv.index('-kernel')+1]=(ROOT/'src/ub_sim/guest-linux/aarch64/out/Image').as_posix()
        _q['pure_qemu']([copy,{**copy,'node':'nodeB' if copy['node']=='nodeA' else 'nodeA'}])
    for job in ['c039','ca039']:
        m=j('runs/'+job+'/monitor/result.json');require(m['actual_task_exit_code']==(1 if job=='c039' else 0) and m['process_tree_empty'] and m['task_resources_closed'] and not m['remaining'] and not m['evidence_errors'],'actual completed bounded task')
    audit=j('runs/c039/post-close-audit.json');require(audit['original_births_gone'] and not audit['holder_audit']['holders'] and not audit['holder_audit']['denied'] and not audit['shared_FM_unlinked'] and not audit['logical_allocation_ACK_inferred'],'physical close not SDK ACK')
    carry={k:d[k] for k in ['identity','historical_pool','historical_unknown_operation','retained_canaries','quarantined_creation']}
    carry['historical_prestart_contexts']=[{'realm':provider['realm'],'provider_session':provider['session'],'guest_identities':identities,'unstarted_reserved_ids':[108,109]}]
    return {'schema':1,'kind':'verified-closed-c039-prestart','assessment_sha256':SHA,'predecessor_state_sha256':a['predecessor_state_sha256'],'completed_succession_proof_sha256':digest,'before_total_bytes':80<<20,'cancel_keys':a['current_unstarted_canary_reservations'],'carry':carry,'evidence_sha256':{n:sha(b) for n,b in raw.items()},'SDK_ACK_Unmapped_or_canary_close_claimed':False,'old64_cancelled':False}
def store_family(names):require(not list(names),'no c039 lower.store family')
def closed_names(a,job,top,monitor,state=()):
    prefix='runs/'+job+'/';expected={n[len(prefix):].split('/')[0] for n in a['files_requiring_fresh_exact_byte_validation'] if n.startswith(prefix)}
    optional={'u.sock','l.sock'} if job=='c039' else set()
    if job=='c039':expected.add('fm')
    require(set(top)-optional==expected,'no added/missing closed prestart files')
    prefix+='monitor/';wanted={n[len(prefix):] for n in a['files_requiring_fresh_exact_byte_validation'] if n.startswith(prefix)}
    require(set(monitor)==wanted,'complete closed monitor namespace')
    if job=='c039':require(set(state)=={'authority.lock','admin.token','state.json'},'no hidden state/journal/store family')
def closed_filesystem(a):
    for job in ['c039','ca039']:
        out=ROOT/'runs'/job;require(out.is_dir() and not out.is_symlink() and not (out/'monitor').is_symlink(),'real closed directories')
        state=[p.name for p in (out/'state').iterdir()] if job=='c039' else []
        closed_names(a,job,(p.name for p in out.iterdir()),(p.name for p in (out/'monitor').iterdir()),state)
        for n in ['u.sock','l.sock']:
            p=out/n
            if p.exists():require(stat.S_ISSOCK(p.lstat().st_mode) and p.lstat().st_uid==os.geteuid(),'only inactive known control socket nodes')
def inventory(a,raw):
    aliases=set();all_rows=[]
    for job in OLD_JOBS:
        require(not (ROOT/'runs'/job).is_symlink() and not (ROOT/'runs'/job/'fm').is_symlink(),'fixed old FM directory')
        rows=json.loads(raw['runs/'+job+'/fm-carrier-active.json'])['files'];require(len(rows)==10,'complete old ten FM')
        _r['fm_namespace']((ROOT/'runs'/job/'fm').rglob('*'),(x['path'] for x in rows))
        for row in rows:
            p=Path(row['path']);require(p.is_relative_to(ROOT/'runs'/job/'fm'),'fixed old FM path');m=p.lstat()
            require((m.st_dev,m.st_ino,m.st_uid,stat.S_IFMT(m.st_mode))==(row['dev'],row['inode'],row['uid'],row['file_type']),'old40 identity/type/uid')
            expected=next(x for x in a['FM40'] if x['path']==row['path']);require(m.st_blocks*512==expected['actual_blocks_bytes'],'old FM blocks drift')
            require([m.st_dev,m.st_ino] in row['kernel_inode_aliases'],'old FM kernel aliases');aliases.update(map(tuple,row['kernel_inode_aliases']));all_rows.append(row)
    require(len(all_rows)==40,'all40 retained');return aliases
def runtime(a,raw):
    c=a['executed_runtime_closure'];source=ROOT/'implementation/source-host030';b=json.loads(raw['implementation/build-host030/build-receipt.json'])
    require(sha(read(source/'.uflow-source.json',4<<20))==c['source_manifest_sha256'],'executed source manifest')
    def digest(path,pin):
        # Paths/pins come only from the fixed bound build receipt; no caller list.
        p=Path(path);require(p.is_file() and p.stat().st_size<=256<<20,'bounded closure input');h=hashlib.sha256()
        with p.open('rb') as f:
            for part in iter(lambda:f.read(1<<20),b''):h.update(part)
        require(h.hexdigest()==pin,'runtime closure '+str(p))
    for key in ['image','daemon','fence_shim']:digest(b[key],b[key+'_sha256'])
    for n,pin in b['installed_sdk_manifest'].items():digest(ROOT/'env/mem-service-sdk'/n,pin)
    require(len(b['installed_sdk_manifest'])==53 and len(b['official_worker']['source_and_headers_link_inputs'])==233,'SDK53 worker233')
    for n,row in b['official_worker']['source_and_headers_link_inputs'].items():digest(n,row['sha256'])
    w=b['official_worker'];digest(w['ELF'],w['ELF_sha256'])
    for n,pin in b['platform_gva_inputs'].items():digest(n,pin)
    group=json.loads(raw['implementation/config-c039-profile-v1/group.json']);require(group['kernel_profile']==c['kernel_profile'] and group['kernel']==c['selected_kernel']['path'],'fixed profile binding')
    capability=_k['launch_gate'](ROOT,group)
    digest(ROOT/'env/guest-kit004/uflow_guest_group.py',c['frozen_group_sha256'])
    for n,pin in c['frozen_supervisor_sha256'].items():digest(ROOT/'env/supervisor'/n,pin)
    digest(_r['RUNNER'],c['frozen_runner_sha256'])
    with gzip.open(b['image'],'rb') as f:data=f.read((128<<20)+1)
    require(len(data)<=128<<20,'bounded actual initramfs');pos=0;found={};targets={'bin/uflow_env_payload':b['launcher']['sha256'],'bin/uflow_pool_agent':b['agent_sha256'],'bin/linqu_mem_service_provider_obmm':w['ELF_sha256']}
    while pos+110<=len(data):
        header=data[pos:pos+110];require(header[:6]==b'070701','cpio newc');size=int(header[54:62],16);length=int(header[94:102],16);name=data[pos+110:pos+110+length-1].decode().removeprefix('./');start=(pos+110+length+3)&~3
        if name=='TRAILER!!!':break
        if name in targets:require(name not in found and sha(data[start:start+size])==targets[name],'actual cpio ELF');found[name]=targets[name]
        pos=(start+size+3)&~3
    require(found==targets,'all actual cpio members');return capability
def verify(assessment,authority):
    b=read(assessment,MAX_ASSESSMENT);require(sha(b)==SHA,'fixed prestart assessment');a=json.loads(b)
    raw={n:read(ROOT/n) for n in a['files_requiring_fresh_exact_byte_validation']};exact(a,raw);p=semantic(a,raw)
    store_family((ROOT/'runs/c039').glob('lower.store*'));closed_filesystem(a);p['runtime_closure']=runtime(a,raw)
    for job in (*OLD_JOBS,'ca039'):
        m=json.loads(raw['runs/'+job+'/monitor/result.json']) if 'runs/'+job+'/monitor/result.json' in raw else None
        if m:_r['nonlive']({**m['owned_births'],str(m['supervisor_pid']):m['supervisor_birth']},_r['birth'])
    original=json.loads(read(ROOT/'implementation/source-host030/examples/mem_service_pool/reconciliation/assessment.json',65536))
    _r['nonlive']({k:v['expected_birth'] for k,v in original['original_nonlive_births'].items()},_r['birth'])
    def old_admissions(value):
        if isinstance(value,dict):
            if 'current_E0_guest_admission' in value:
                old=value['current_E0_guest_admission'];_r['nonlive']({str(old['pid']):old['birth']},_r['birth'])
            for child in value.values():old_admissions(child)
        elif isinstance(value,list):
            for child in value:old_admissions(child)
    old_admissions(json.loads(raw['implementation/responsibility-c013/successor-c033-v1/result.json']))
    live={s.split()[-1] for s in _r['proc_read'](Path('/proc/net/unix'),16<<20).decode().splitlines()[1:] if len(s.split())>=8}
    endpoints={x['path'] for j in OLD_JOBS for x in json.loads(raw['runs/'+j+'/control-endpoints-active.json'])};endpoints.update(str(ROOT/'runs'/j/n) for j in OLD_JOBS for n in ['u.sock','l.sock','a.sock','b.sock'])
    require(not endpoints.intersection(live),'old controls live');aliases=inventory(a,raw);p['fresh_physical_audit']=_r['holder_audit'](aliases)
    owner=_r['guest_admission'](authority);p['current_E0_guest_admission']=owner
    argv=_r['proc_read'](Path('/proc',str(owner['pid']),'cmdline'),16384).rstrip(b'\0').decode().split('\0');outer=json.loads(read(Path(argv[4]),65536));cmd=outer['commands']
    require(len(cmd)==1 and len(cmd[0]['argv'])==6 and Path(cmd[0]['argv'][3]).name=='run_mem_service_pool_matrix.py','actual admitted matrix')
    group=json.loads(read(Path(cmd[0]['argv'][4]),65536));_q['namespace'](group,authority);require(authority.parent.name not in OLD_JOBS,'four old contexts never reused')
    p['new_namespace']=str(authority.parent);p['proof_sha256']=sha(json.dumps(p,sort_keys=True,separators=(',',':')).encode());return p
def verify_new_fm(out,assessment):
    b=read(assessment,MAX_ASSESSMENT);require(sha(b)==SHA,'fixed new FM assessment');a=json.loads(b);raw={}
    for j in OLD_JOBS:
        n='runs/'+j+'/fm-carrier-active.json';b=read(ROOT/n);row=a['files_requiring_fresh_exact_byte_validation'][n];require(len(b)==row['bytes'] and sha(b)==row['sha256'],'fixed old FM inventory');raw[n]=b
    old=inventory(a,raw);require(out.parent==ROOT/'runs' and out.name not in OLD_JOBS and not out.is_symlink() and not (out/'fm').is_symlink(),'new FM namespace')
    rows=json.loads(read(out/'fm-carrier-active.json',65536))['files'];require(len(rows)==10,'new complete10');seen=set()
    for row in rows:
        path=Path(row['path']);require(path.is_relative_to(out/'fm'),'new FM path');m=path.lstat();kind=stat.S_IFMT(m.st_mode)
        require(kind in (stat.S_IFREG,stat.S_IFIFO,stat.S_IFSOCK) and kind==row['file_type'] and m.st_uid==row['uid']==os.geteuid() and (m.st_dev,m.st_ino)==(row['dev'],row['inode']),'new typed FM identity')
        aliases=set(map(tuple,row['kernel_inode_aliases']));require((m.st_dev,m.st_ino) in aliases and not aliases.intersection(old|seen),'new aliases disjoint from all40');seen.update(aliases)
    _r['fm_namespace']((out/'fm').rglob('*'),(x['path'] for x in rows));return {'old_files':40,'new_files':10,'independent':True}
if __name__=='__main__':
    require(len(sys.argv)==3,'fixed assessment and admitted authority only');print(json.dumps(verify(*map(Path,sys.argv[1:])),separators=(',',':')))
