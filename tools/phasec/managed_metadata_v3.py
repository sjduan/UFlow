"""Real post-monitor v3 registration/union; never reconstruct missing evidence."""
import hashlib,json,os,stat,sys
from pathlib import Path
from accepted_producers import AP_ROOT,ap_read,ap_bound,ap_decode,ap_require
from accepted_c081_v3 import v3_anchors,v3_policy,v3_sources,v3_context
from c081_execution_adapter import c081_manifest_gate,c081_execution
from profile_metadata_completion import reference,require_closed

INTERFACE='mem-service-profile-metadata-v3'
PREFIX=AP_ROOT/'implementation/c081-recovery-v1'

def consumption_receipt(context,a):
    """Use C's actual compiled pure ledger re-derivation, never E accounting."""
    import subprocess
    plan=context['plan'];path=Path(v3_policy()['config_directory'])/'execution.json'
    result=subprocess.run([plan['daemon']['path'],'--pool-terminal-case-consumption-audit',str(path),context['execution_manifest_sha256']],
        pass_fds=(int(os.environ['UFLOW_ENV_CLOSE_FD']),),stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=45,check=False)
    ap_require(result.returncode==0 and len(result.stdout)<=4096 and len(result.stderr)<=4096,'C compiled consumed-accounting audit failed: '+result.stderr[:1200].decode(errors='replace'))
    ref=ap_decode(result.stdout)
    ap_require(ref==reference(Path(plan['authority'])/plan['successor']/'commit.json'),'actual original C commit receipt required')
    return ref


def runtime_case(context,a,modules,profile):
    plan=context['plan'];run=AP_ROOT/'runs'/v3_policy()['phases']['successor'];source=Path(plan['source'])
    source_manifest=plan['source_manifest'];m=ap_decode(ap_bound(source_manifest,8<<20))
    roles={'collector':'tools/phasec/collect_owned_qemu_metadata.py','matrix':'tools/phasec/run_mem_service_pool_matrix.py',
        'metadata_consumer':'tools/phasec/managed_metadata_v3.py','profile_gate':'tools/phasec/qemu_profile_gate.py'}
    sources={role:reference(source/rel) for role,rel in roles.items()}
    for role,rel in roles.items():ap_require(sources[role]['sha256']==m['files'][rel]['sha256'],'actual runtime source drift')
    ap_require(Path(__file__).resolve()==source/roles['metadata_consumer'],'actual bound audit consumer required')
    launch=plan['phases']['successor']['config'];config=ap_decode(ap_bound(launch));op=modules['ownership_producer']
    runner=reference(AP_ROOT/'env/runners/3d3616a3f5ac25ab2ab6f40aaea75c41c30518d590cf3c22fd7451382495decb/uflow_env_runner.py')
    ap_require(runner['sha256']==op.RUNNER_SHA,'actual frozen E0 runner required')
    sources.update(source_manifest=source_manifest,launch_config=reference(Path(v3_policy()['config_directory'])/'guest-group.json'),group=a['code_and_data']['wrapper'],E0_runner=runner)
    paths={'starts':'guest-starts.json','identities':'guest-identities.json','capture':'own-qemu-metadata.json','control':'pool-events.jsonl',
        'carriers':'fm-carrier-active.json','endpoints':'control-endpoints-active.json','result':'result.json'}
    artifacts={k:reference(run/n,16<<20) for k,n in paths.items()}
    top=ap_decode(ap_bound(artifacts['result'],16<<20));require_closed(top['processes'])
    ap_require(top['processes']['actual_task_exit_code']==0 and ap_decode(ap_read(run/'E1-E4.json'))['state']=='passed','original Guest FAIL must never enter normal registry')
    history=run/'monitor/ownership-history.json'
    owned={'schema':3,'interface':op.INTERFACE,'kind':'monitor-result-ownership-v3','run':run.name,'expected_exit':0,
        'monitor_result':reference(run/'monitor/result.json'),'run_result':artifacts['result'],'monitor_config':reference(run/'monitor/config.json'),
        'supervisor_start':reference(run/'monitor/supervisor-start.json'),'launch_config':launch,'E0_runner':runner,
        'supervisor_sources':{n:reference(Path(config['supervisor_root'])/n) for n in op.SUPERVISOR},
        'optional_history':reference(history) if history.exists() else None}
    terminal=ap_decode(ap_bound(a['code_and_data']['terminal_context'],128<<10));helper=terminal['helper']['path']
    op.validate_descriptor(owned,run.name,helper,artifacts['starts'],artifacts['capture'])
    return {'schema':3,'interface':INTERFACE,'kind':'captured-runtime-case-v3','evidence_mode':'captured-runtime','run_path':str(run),
        'accepted_context':a['accepted_context'],'profile_manifest':a['code_and_data']['profile'],'writer_source_sha256':profile['writer_source']['sha256'],
        'runtime_sources':sources,'registry_directory':str(PREFIX/'registry'/run.name),'artifacts':artifacts,'ownership':owned},helper

def complete(context,a,modules,profile,directory):
    # All evidence reads/validation precede any new normal registry append.
    p=modules['metadata_producer'];ev,md=p.ev,p.md
    case,helper_path=runtime_case(context,a,modules,profile);receipt=consumption_receipt(context,a)
    run=Path(case['run_path']);directory.mkdir(exist_ok=False)
    from pool_run_metadata import journal_diagnostics,save
    diagnostics,captured=journal_diagnostics(run,ev.read);save(ev,directory/'journal-completeness.json',diagnostics)
    ap_require(diagnostics['all_serial_captures_complete'],'complete genuine per-node journals required')
    case_ref=save(ev,directory/'runtime-case.json',case);controls=case['artifacts']['control'];starts=ev.decode(ev.bound(case['artifacts']['starts']))
    bindings=[]
    for n,line in enumerate(ev.bound(controls,16<<20).splitlines()):
        row=ev.decode(line)
        if row.get('event')=='lower-control' and row.get('operation')==122 and row.get('status')==0:bindings.append((n,md.descriptor_binding(controls,n)))
    journals,captures,logs,lines={},{},{},{}
    for node in ('nodeA','nodeB'):
        serial,text,raw=captured[node];captures[node]={'format':'captured-serial-v1','source':serial,'bytes':len(raw),'sha256':ev.digest(raw)}
        end=next(s for s in text.splitlines() if s.startswith('UFPOOL_JOURNAL_END '))
        journals[node]={'capture_end':dict(w.split('=',1) for w in end.split()[1:]),'bytes':len(raw),'sha256':ev.digest(raw),'frames':md.journal_frames(captures[node])}
        logs[node]=reference(run/(node+'.qemu.log'));lines[node]=ev.bound(logs[node],64<<20).decode('ascii').splitlines()
    summary=save(ev,directory/'journal-summary.json',{'schema':2,'journals':journals});helper=ev.frozen_helper(helper_path)
    entries=[];requests=[];count=0;accepted=a['accepted_context']
    for kind,folder in (('bootstrap','obmm_bootstrap'),('retired','obmm_retired')):
        root=run/'fm'/folder
        if not root.exists():continue
        ap_require(root.is_dir() and root.resolve()==root,'exact normal metadata directory required')
        for path in sorted(root.iterdir()):
            count+=1;info=path.lstat()
            ap_require(count<=128 and stat.S_ISREG(info.st_mode) and not path.is_symlink() and info.st_uid==os.geteuid() and 0<info.st_size<=4096 and '.tmp.' not in path.name,'partial/unknown metadata refused')
            raw,identity=ev.read(path,4096,metadata=True);fields=md.parse_metadata(raw,kind)
            ap_require(path==run/'fm'/md.relative_name(kind,fields),'exact metadata name required')
            matches=[(n,b) for n,b in bindings if all(b['fields'][k]==v for k,v in fields.items())];ap_require(len(matches)==1,'unique actual successful worker descriptor required')
            n,b=matches[0];node=b['node'];owner=next(s for s in starts if s['node']==node)
            request={'schema':3,'interface':INTERFACE,'kind':'metadata-registration-request-v3','case':case_ref,'run':run.name,'fm_root':str(run/'fm'),
                'metadata_kind':kind,'file_identity':{**identity,'path':str(path),'content_sha256':ev.digest(raw),'kernel_inode_aliases':helper.file_identity_aliases(path,identity)},
                'qemu_binary_sha256':profile['qemu_binary_sha256'],'writer_source_sha256':profile['writer_source']['sha256'],'control_log':controls,'control_line':n,
                'guest_node':node,'guest_boot_id':b['boot_id'],'guest_incarnation':b['incarnation'],'qemu_owner':{'pid':owner['pid'],'birth':owner['birth']},
                'qemu_log':logs[node],'worker_stdout':reference(run/('worker-'+node+'.stdout.log'),1<<20),'journal_summary':summary,'journal_captures':captures}
            candidates=[];prefix='SIM_DEC: OBMM bootstrap publish ' if kind=='bootstrap' else 'SIM_DEC: OBMM export retired '
            for number,text in enumerate(lines[node]):
                if not text.startswith(prefix):continue
                ref=save(ev,directory/('candidate-%03d-%06d.json'%(count,number)),{**request,'qemu_line':number})
                try:entry=p.validate_registration(ref['path'],ref['sha256'],helper_path,accepted['path'],accepted['sha256'])
                except (ev.EvidenceError,KeyError,ValueError):continue
                ap_require(entry['kind']=='validated-profile-metadata-v3' and entry['HOST_fixture_only'] is False,'actual production typed entry required')
                candidates.append((ref,entry))
            ap_require(len(candidates)==1,'unique E-validated genuine publication event required');ref,entry=candidates[0];requests.append(ref);entries.append(entry)
    ap_require(entries and {e['metadata_kind'] for e in entries}=={'bootstrap','retired'} and len({e['file']['path'] for e in entries})==len(entries),'complete distinct normal bootstrap+retired metadata required')
    registry=Path(case['registry_directory']);ap_require(not registry.exists(),'new registry only; partial append requires reconciliation')
    head='0'*64;receipts=[]
    for entry in entries:
        row=p.append_registration(registry,head,entry);receipts.append(row);head=row['head_sha256']
    save(ev,directory/'registration-receipts.json',{'schema':3,'case':case_ref,'requests':requests,'receipts':receipts,'head_sha256':head})
    plan={'schema':3,'interface':INTERFACE,'kind':'closed-union-plan-v3','helper_path':helper_path,'historical_consumption_receipt':receipt,
        'accepted_context':accepted,'cases':[case_ref],'registry_heads':{run.name:head}}
    ref=save(ev,directory/'companion-plan.json',plan);result=p.validate_closed_union(ref['path'],ref['sha256'],accepted['path'],accepted['sha256'])
    ap_require(result['status']=='PASS_READONLY_CLOSED_UNION_ONLY' and result['HOST_fixture_only'] is False and result['historical_retained_files']==73 and
        result['historical_quarantine_normal_registered'] is False and result['new_normal_metadata']==len(entries) and result['exact_union_files']==83+len(entries) and
        result['historical_accounting_semantics_verified_by_E'] is False and all(result[k] is False for k in p.TRUTHS),'complete exact old73/new10/normal union required')
    save(ev,directory/'companion-result.json',result);return result

def main(path):
    a=v3_anchors();policy=v3_policy();path=Path(path)
    ap_require(path==Path(policy['config_directory'])/'execution.json','exact compiled audit namespace required before IO')
    pin=hashlib.sha256(ap_read(path,1<<20)).hexdigest();c081_manifest_gate(path,pin,policy)
    context=c081_execution({'execution_action':'postclose-audit','execution_manifest':{'path':str(path),'sha256':pin},'actor_pid':os.getpid()},a,policy)
    # Metadata context derives immutable historical inventory; terminal eligibility
    # is deliberately NOT re-evaluated after the original proof was consumed.
    modules=v3_sources(a);_,profile,_=v3_context(a,modules)
    run=AP_ROOT/'runs'/policy['phases']['postclose-audit'];directory=run/'metadata-v3'
    try:result=complete(context,a,modules,profile,directory)
    except Exception as error:
        from pool_run_metadata import save
        save(modules['metadata_producer'].ev,run/'audit-incomplete.json',{'schema':3,'status':'INCOMPLETE','original_Guest_verdict_preserved':True,
            'detail':str(error)[:1800],'normal_ACK_inferred_from_death':False,'normal_registration_complete':False})
        raise
    print(json.dumps({'status':result['status'],'exact_union_files':result['exact_union_files']}))
if __name__=='__main__':main(sys.argv[1])
