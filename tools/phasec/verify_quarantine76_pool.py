"""Only the fixed closed c033 pure-CPU context; never a release/replay proof.

Production is embedded in Rust and invoked under the existing authority lock.
Semantic helpers are exercised separately; there is no fixture CLI/root switch.
"""
import hashlib,json,os,stat,sys
from pathlib import Path
if 'R8_SOURCE' in globals():
    helpers={'__name__':'fixed_retained8_helpers'};exec(R8_SOURCE,helpers)
else:
    import verify_retained8_pool
    helpers=vars(verify_retained8_pool)
for name in ['ROOT','read','require','sha','birth','proc_read','nonlive','holder_audit','guest_admission','fm_namespace']:
    globals()[name]=helpers[name]
ASSESSMENT_SHA='0c147898be722733e5d48b4c27fa60a3fb5a97f1a836394eccf3307ff4e57e13'
OLD_JOBS=('c013','c027','c033')

def pure_qemu(starts):
    require(len(starts)==2 and {s['node'] for s in starts}=={'nodeA','nodeB'},'two fixed pure-QEMU starts')
    for s in starts:
        a=s['argv'];j=Path(a[a.index('-kernel')+1])
        expected=[(ROOT/'build/qemu/qemu-system-aarch64').as_posix(),'-M','virt,gic-version=3,its=on,ummu=on,ub-cluster-mode=on',
            '-cpu','cortex-a57','-m','2048M','-smp','2','-nodefaults','-nographic','-kernel',j.as_posix(),
            '-qmp',a[14],'-chardev',a[16],'-serial','chardev:ser0','-initrd',a[20],'-append',a[22]]
        require(a==expected,'only known TCG CPU QEMU argv, no passthrough/KVM/device option')
        require(j==ROOT/'src/ub_sim/guest-linux/aarch64/out/Image','failed fixed kernel path')

def documents(a,raw):
    require(set(raw)==set(a['files_requiring_fresh_exact_byte_validation']),'complete33 bound evidence')
    j=lambda n:json.loads(raw[n])
    state=j('runs/c033/state/state.json');r8=j('implementation/responsibility-c013/successor-c027-v1/result.json')
    require(r8['successor_phase']=='Retained8' and r8['cancel64_repeated'] is False and
        r8['SDK_ACK_Unmapped_or_canary_close_claimed'] is False,'complete previous retained8 succession')
    require(r8['verified_proof']['predecessor_result']==j('implementation/responsibility-c013/result.json') and
        r8['verified_proof']['predecessor_intent']==j('implementation/responsibility-c013/intent.json'),'complete original predecessor continuity')
    i8=j('implementation/responsibility-c013/successor-c027-v1/intent.json');pending=dict(r8);pending['successor_phase']='Retained8Pending'
    require(i8==pending,'exact completed retained8 intent/result')
    expected_recon=dict(r8);proof=expected_recon.pop('verified_proof');expected_recon['evidence_file_count']=len(proof['evidence_sha256'])
    require(state['reconciliation']==expected_recon,'c033 actually inherited this exact lineage')
    require([state[k] for k in ['charged_bytes','retained_bytes','active_bytes','creation_reserved_bytes','budget_bytes']]==
        [12<<20,8<<20,4<<20,64<<20,128<<20],'complete failed76 ledger')
    u=a['uncertain_business_reservation'];require(state['pools']==[u['pool']] and u['pool']['state']=='CreationUnknown' and
        u['pool']['canonical'] is None and u['pool']['release_result'] is None,'retain exact attempted Unknown, not uncreated')
    require(u['operation'] in state['operations'] and state['resource_ledger']['reservations']=={u['key']:64<<20},'exact original operation and reservation key')
    require(not state['catalog_range_leases'] and state['native_jobs_executed']==state['payload_rpc_bytes']==0,'no hidden ranges/native/payload work')
    canaries=a['retained_canaries'];require(len(canaries)==6 and canaries[:4]==r8['retained_canaries'],'six identities continuous')
    rows=state['resource_ledger']['resources'];require(len(rows)==6,'exact six real canary ledger entries')
    identities=j('runs/c033/guest-identities.json');events=[json.loads(s) for s in raw['runs/c033/pool-events.jsonl'].splitlines()]
    for c in canaries:
        row=next(r for r in rows if r['object_id']==c['object_id'])
        require(row['nbytes']==c['nbytes']==2<<20 and all(row['backing'][k]==v for k,v in c['backing'].items()),'complete typed backing')
        if c['object_id']<105:require(row['retained']==c,'old retained witness unchanged')
        else:
            require(row['retained'] is None,'current charge is not already claimed checked close')
            identity=next(i for i in identities if i['node']==c['node'])
            require(all(identity[k]==c[k] for k in ['node','boot_id','agent_session','incarnation']),'actual c033 boot/incarnation')
            require([e['receipt'] for e in events if e['event']=='bootstrap-prepared' and e['node']==c['node']]==[c['canary_prepare']] and
                [e['receipt'] for e in events if e['event']=='bootstrap-verified' and e['node']==c['node']]==[c['peer_verify']],'actual current receipts')
    require(len({c['object_id'] for c in canaries})==6 and all(c['SDK_checked_close_confirmed'] is False for c in canaries),'no duplicate/close inference')
    for job in ['c027','c033']:
        monitor=j('runs/'+job+'/monitor/result.json');audit=j('runs/'+job+'/post-close-audit.json')
        require(monitor['actual_task_exit_code']==1 and monitor['process_tree_empty'] and monitor['task_resources_closed'] and
            not monitor['remaining'] and not monitor['evidence_errors'],'actual finite closed failed context')
        require(audit['original_births_gone'] and not audit['holder_audit']['holders'] and not audit['holder_audit']['denied'] and
            not audit['shared_FM_unlinked'] and not audit['logical_allocation_ACK_inferred'],'physical audit is not ACK')
        pure_qemu(j('runs/'+job+'/guest-starts.json'))
    return {'schema':1,'kind':'verified-closed-c033-quarantine76','assessment_sha256':ASSESSMENT_SHA,
        'predecessor_state_sha256':sha(raw['runs/c033/state/state.json']),
        'predecessors':{n:j(n) for n in ['implementation/responsibility-c013/intent.json','implementation/responsibility-c013/result.json',
            'implementation/responsibility-c013/successor-c027-v1/intent.json','implementation/responsibility-c013/successor-c027-v1/result.json']},
        'identity':r8['identity'],'historical_pool':r8['historical_pool'],'historical_unknown_operation':r8['historical_unknown_operation'],
        'retained_canaries':canaries,'uncertain_business_reservation':u,'before_total_bytes':76<<20,
        'charged_bytes':12<<20,'reserved_bytes':64<<20,'cancel64_repeated':False,'SDK_ACK_Unmapped_or_canary_close_claimed':False,
        'evidence_sha256':{n:sha(b) for n,b in raw.items()}}

def inventory(raw):
    rows=[r for job in OLD_JOBS for r in json.loads(raw['runs/'+job+'/fm-carrier-active.json'])['files']]
    require(len(rows)==30,'three complete old FM inventories');inodes=set()
    for row in rows:
        p=Path(row['path']);require(any(p.is_relative_to(ROOT/'runs'/j/'fm') for j in OLD_JOBS),'fixed FM namespaces')
        m=p.lstat();require((m.st_dev,m.st_ino,m.st_uid,stat.S_IFMT(m.st_mode))==
            (row['dev'],row['inode'],row['uid'],row['file_type']),'old FM identity drift')
        require([row['dev'],row['inode']] in row['kernel_inode_aliases'],'FM alias complete');inodes.update(map(tuple,row['kernel_inode_aliases']))
    fm_namespace((p for j in OLD_JOBS for p in (ROOT/'runs'/j/'fm').rglob('*')),(r['path'] for r in rows))
    return rows,inodes

def namespace(group,authority):
    require(set(group)<= {'root','output','qemu_binary','kernel','topology','entity_plan','nodes','boot_seconds','kernel_profile'},'no extra Guest control options')
    out=authority.parent;require(out.parent==ROOT/'runs' and out.name not in OLD_JOBS and group['output']==str(out) and group['root']==str(ROOT),'fresh independent run namespace')
    require(group['qemu_binary']==str(ROOT/'build/qemu/qemu-system-aarch64') and len(group['nodes'])==2,'fixed pure CPU emulator')
    require({n['id'] for n in group['nodes']}=={'nodeA','nodeB'},'two logical nodes')
    for n in group['nodes']:
        require(set(n)=={'id','memory_mib','vcpus','boot_argv'} and n['memory_mib']==2048 and n['vcpus']==2,'fixed resources')
        argv=n['boot_argv'];require(len(argv)==4 and argv[0]=='-initrd' and argv[2]=='-append','no passthrough/extra boot flags')
        require(Path(argv[1]).is_relative_to(ROOT/'implementation') and argv[3]==
            'console=ttyAMA0 rdinit=/bin/run_app linqu_urma_dp_role='+n['id']+' uflow_node='+n['id']+
            ' linqu_probe_skip=1 linqu_probe_load_helper=1 obmm.skip_cache_maintain=1 rcupdate.rcu_cpu_stall_timeout=300','fixed CPU kernel arguments')
    require(not (out/'fm').exists() and not (out/'fm').is_symlink(),'no old/current FM before fresh Guest')

def verify(assessment,authority):
    a_bytes=read(assessment,65536);require(sha(a_bytes)==ASSESSMENT_SHA,'fixed quarantine assessment bytes');a=json.loads(a_bytes)
    raw={}
    for n,row in a['files_requiring_fresh_exact_byte_validation'].items():
        p=ROOT/n;require(p.is_relative_to(ROOT),'task evidence only');b=read(p)
        require(len(b)==row['bytes'] and sha(b)==row['sha256'],'exact predecessor bytes '+n);raw[n]=b
    require({p.name for p in (ROOT/'runs/c033').glob('lower.store*')}=={'lower.store'},'exact original metadata family')
    p=documents(a,raw)
    original=json.loads(read(assessment.parent/'assessment.json',65536))
    require(sha(read(assessment.parent/'assessment.json',65536))=='f4e89d644df46d78cf4b611ed51f10efc5bc0b99591281b95f5a26a65e57aba4','fixed c013 births')
    nonlive({k:v['expected_birth'] for k,v in original['original_nonlive_births'].items()},birth)
    for j in ['c027','c033']:
        m=json.loads(raw['runs/'+j+'/monitor/result.json']);nonlive({**m['owned_births'],str(m['supervisor_pid']):m['supervisor_birth']},birth)
    # The old c033 outer runner is bound by the completed retained8 proof.
    old_owner=json.loads(raw['implementation/responsibility-c013/successor-c027-v1/result.json'])['verified_proof']['current_E0_guest_admission']
    nonlive({str(old_owner['pid']):old_owner['birth']},birth)
    live={s.split()[-1] for s in proc_read(Path('/proc/net/unix'),16<<20).decode().splitlines()[1:] if len(s.split())>=8}
    endpoints={r['path'] for j in OLD_JOBS for r in json.loads(raw['runs/'+j+'/control-endpoints-active.json'])}
    endpoints.update(str(ROOT/'runs'/j/n) for j in OLD_JOBS for n in ['u.sock','l.sock','a.sock','b.sock'])
    require(not endpoints.intersection(live),'old controls still active')
    rows,inodes=inventory(raw);p['fresh_physical_audit']=holder_audit(inodes)
    admission=guest_admission(authority);p['current_E0_guest_admission']=admission
    argv=proc_read(Path('/proc',str(admission['pid']),'cmdline'),16384).rstrip(b'\0').decode().split('\0')
    outer=json.loads(read(Path(argv[4]),65536));cmd=outer['commands']
    require(len(cmd)==1 and len(cmd[0]['argv'])==6 and Path(cmd[0]['argv'][3]).name=='run_mem_service_pool_matrix.py','actual admitted matrix command')
    group_path=Path(cmd[0]['argv'][4]);require(group_path.is_relative_to(ROOT/'implementation') and group_path.name=='group.json','actual controlled group')
    group=json.loads(read(group_path,65536));namespace(group,authority)
    p['new_namespace']=str(authority.parent);p['old_FM_aliases']=sorted([list(x) for x in inodes])
    p['proof_sha256']=sha(json.dumps(p,sort_keys=True,separators=(',',':')).encode());return p

def verify_new_fm(out,assessment):
    """After E0 creates new FM, before sending any canary/worker start."""
    a_bytes=read(assessment,65536);require(sha(a_bytes)==ASSESSMENT_SHA,'fixed FM proof input');a=json.loads(a_bytes)
    raw={}
    for j in OLD_JOBS:
        for n in ['fm-carrier-active.json']:
            rel='runs/'+j+'/'+n;b=read(ROOT/rel);row=a['files_requiring_fresh_exact_byte_validation'][rel]
            require(len(b)==row['bytes'] and sha(b)==row['sha256'],'old FM inventory byte drift');raw[rel]=b
    rows,old=inventory(raw)
    require(out.parent==ROOT/'runs' and out.name not in OLD_JOBS and not out.is_symlink(),'new isolated output')
    current=json.loads(read(out/'fm-carrier-active.json',65536))['files'];require(len(current)==10,'new complete FM inventory')
    seen=set()
    for row in current:
        p=Path(row['path']);require(p.is_relative_to(out/'fm'),'new FM namespace')
        m=p.lstat();identity=(m.st_dev,m.st_ino);require(stat.S_ISREG(m.st_mode) and identity==(row['dev'],row['inode']),'actual new regular FM identity')
        aliases=set(map(tuple,row['kernel_inode_aliases']));require(identity in aliases and not aliases.intersection(old|seen),'new FM independent inode/alias')
        seen.update(aliases)
    fm_namespace((out/'fm').rglob('*'),(r['path'] for r in current));return {'old_files':30,'new_files':10,'independent':True}

if __name__=='__main__':
    require(len(sys.argv)==3,'fixed assessment and current admitted authority only')
    print(json.dumps(verify(*map(Path,sys.argv[1:])),separators=(',',':')))
