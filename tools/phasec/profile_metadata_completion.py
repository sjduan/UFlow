"""Post-E0 actual case, normal registration and historical62/new exact union."""
import hashlib
import os
from pathlib import Path
import re
import stat
from accepted_producers import AP_ROOT,ap_read,ap_bound,ap_decode,ap_require,accepted_anchors
from profile_metadata_gate import metadata_producer

INTERFACE='mem-service-profile-metadata-v2'
PREFIX=AP_ROOT/'implementation/c064-profile-metadata-v1'

def reference(path,maximum=64<<20):
    raw=ap_read(path,maximum)
    return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}

def consumption_receipt():
    root=AP_ROOT/'implementation/responsibility-c013/successor-c064-terminal-domain-loss-v2'
    names={'intent.json','historical-state.json','before128.json','permanent-fences.json',
        'inherited-backing-identities.json','resulting76.json','commit.json','service-admission-claim.json'}
    ap_require({p.name for p in root.iterdir()}==names,'complete once-consumed current transaction required')
    ref=reference(root/'commit.json');commit=ap_decode(ap_bound(ref))
    ap_require(commit['version']==2 and commit['original_state_sha256']==accepted_anchors()['terminal']['expected_state']['sha256']
        and commit['normal_SDK_ACK'] is False,'actual current terminal commit required')
    ap_require(set(commit['files'])==names-{'commit.json','service-admission-claim.json'} and
        commit['kind']=='TerminalDomainAuthorityCommitted','complete exact committed metadata required')
    for name,pin in commit['files'].items():
        ap_require(name in names and reference(root/name)['sha256']==pin,'current commit file drift')
    ap_require(reference(root/'historical-state.json')['sha256']==commit['original_state_sha256'],'current history drift')
    result=ap_decode(ap_read(root/'resulting76.json'));ledger=result['resource_ledger']
    ap_require(result['B_conservative_bytes']==76<<20 and ledger['charged_bytes']==ledger['retained_bytes']==12<<20 and
        ledger['reserved_bytes']==ledger['retained_reserved_bytes']==64<<20 and
        set(ledger['terminal_domain_loss_tombstones'])=={'110','111','112','113','114','115'},'protected76/six tombstones drift')
    claim=ap_decode(ap_read(root/'service-admission-claim.json'))
    ap_require(claim['source_terminal_commit_sha256']==ref['sha256'] and claim['conservative_B_bytes']==80<<20 and
        claim['canary_charge_objects']=={'nodeA':116,'nodeB':117} and claim['native_SDK_ACK'] is False,'actual once-claim/reserve4 required')
    return ref

def require_closed(result):
    ap_require(result['process_tree_empty'] is True and result['task_resources_closed'] is True and not result['remaining'] and
        not result['evidence_errors'] and result['isolation_error'] is None,'actual E0 physical closure required before final case')

def runtime_case(run,context,a):
    ap_require(type(context) is dict and set(context)=={'execution','launch_config','build_receipt'},'reviewed actual runtime context references required')
    execution=ap_decode(ap_bound(context['execution']));build=ap_decode(ap_bound(context['build_receipt']))
    source=Path(build['source']);manifest=reference(source/'.uflow-source.json');m=ap_decode(ap_bound(manifest,8<<20))
    ap_require(manifest['sha256']==execution['source_manifest_sha256']==build['source_manifest_sha256'] and
        m['commit']==execution['source_commit']==build['source_commit'],'reviewed source/execution/receipt closure mismatch')
    config=ap_decode(ap_bound(context['launch_config']))
    ap_require(config['terminal_current_execution']=={'path':context['execution']['path'],'sha256':context['execution']['sha256']} and
        config['output']==str(run) and Path(context['launch_config']['path']).parent==Path(context['execution']['path']).parent,
        'actual current execution/group selection mismatch')
    roles={'collector':'tools/phasec/collect_owned_qemu_metadata.py','matrix':'tools/phasec/run_mem_service_pool_matrix.py',
        'metadata_consumer':'tools/phasec/profile_metadata_completion.py','profile_gate':'tools/phasec/qemu_profile_gate.py'}
    sources={role:reference(source/rel) for role,rel in roles.items()}
    for role,rel in roles.items():ap_require(sources[role]['sha256']==m['files'][rel]['sha256'],'actual runtime source drift')
    ap_require(Path(__file__).resolve()==source/roles['metadata_consumer'],'different metadata consumer source')
    graph=ap_decode(ap_bound(accepted_anchors()['terminal']['source_graph']));fixed=graph['fixed_bindings']
    sources.update(source_manifest=manifest,launch_config=context['launch_config'],group=fixed['guest_group'],E0_runner=fixed['E0_runner'])
    artifacts={'starts':'guest-starts.json','identities':'guest-identities.json','capture':'own-qemu-metadata.json',
        'control':'pool-events.jsonl','carriers':'fm-carrier-active.json','endpoints':'control-endpoints-active.json',
        'result':'result.json','ownership':'monitor/ownership-history.json'}
    refs={role:reference(run/name,16<<20) for role,name in artifacts.items()}
    result=ap_decode(ap_bound(refs['result'],16<<20))['processes']
    require_closed(result)
    case={'schema':2,'interface':INTERFACE,'kind':'captured-runtime-case-v2','evidence_mode':'captured-runtime',
        'run_path':str(run),'profile_manifest_sha256':a['profile_manifest_sha256'],'writer_source_sha256':a['writer_source_sha256'],
        'runtime_sources':sources,'registry_directory':str(PREFIX/'registry'/run.name),'artifacts':refs}
    return case,fixed['physical_helper']['path']

def complete(run,directory,context):
    run=Path(run);directory=Path(directory)
    ap_require(run.parent==AP_ROOT/'runs' and re.fullmatch('c[0-9]{3}',run.name) and int(run.name[1:])>68 and
        run.resolve()==run,'new reviewed actual run required')
    base=AP_ROOT/'implementation/qemu-domain-v1/consumer-phase-c'
    ap_require(directory.parent==base and directory.resolve()==directory and not directory.exists(),'new scoped post-close data directory required')
    with metadata_producer() as (a,p):
        ev,md=p.ev,p.md
        case,helper_path=runtime_case(run,context,a);receipt=consumption_receipt()
        directory.mkdir(parents=True,exist_ok=False)
        from pool_run_metadata import journal_diagnostics,save
        diagnostics,captured=journal_diagnostics(run,ev.read)
        save(ev,directory/'journal-completeness.json',diagnostics)
        ap_require(diagnostics['all_serial_captures_complete'],'complete actual per-node journals required; diagnostic preserved')
        case_ref=save(ev,directory/'runtime-case.json',case)
        controls=case['artifacts']['control'];starts=ev.decode(ev.bound(case['artifacts']['starts']))
        bindings=[]
        for n,line in enumerate(ev.bound(controls,16<<20).splitlines()):
            row=ev.decode(line)
            if row.get('event')=='lower-control' and row.get('operation')==122 and row.get('status')==0:
                bindings.append((n,md.descriptor_binding(controls,n)))
        journals,captures,logs,lines={},{},{},{}
        for node in ('nodeA','nodeB'):
            serial,text,raw=captured[node]
            captures[node]={'format':'captured-serial-v1','source':serial,'bytes':len(raw),'sha256':ev.digest(raw)}
            end=next(s for s in text.splitlines() if s.startswith('UFPOOL_JOURNAL_END '))
            journals[node]={'capture_end':dict(w.split('=',1) for w in end.split()[1:]),'bytes':len(raw),'sha256':ev.digest(raw),
                'frames':md.journal_frames(captures[node])}
            logs[node]=reference(run/(node+'.qemu.log'));lines[node]=ev.bound(logs[node],64<<20).decode('ascii').splitlines()
        summary=save(ev,directory/'journal-summary.json',{'schema':2,'journals':journals})
        helper=ev.frozen_helper(helper_path);entries=[];requests=[];count=0
        for kind,folder in (('bootstrap','obmm_bootstrap'),('retired','obmm_retired')):
            root=run/'fm'/folder
            if not root.exists():continue
            ap_require(root.is_dir() and root.resolve()==root,'exact normal metadata directory required')
            for path in sorted(root.iterdir()):
                count+=1;info=path.lstat()
                ap_require(count<=128 and stat.S_ISREG(info.st_mode) and not path.is_symlink() and info.st_uid==os.geteuid() and
                    0<info.st_size<=4096 and '.tmp.' not in path.name,'unknown/partial normal metadata refused')
                raw,identity=ev.read(path,4096,metadata=True);fields=md.parse_metadata(raw,kind)
                ap_require(path==run/'fm'/md.relative_name(kind,fields),'exact metadata name required')
                matches=[(n,b) for n,b in bindings if all(b['fields'][k]==v for k,v in fields.items())]
                ap_require(len(matches)==1,'unique successful actual worker descriptor required')
                n,b=matches[0];node=b['node'];owner=next(s for s in starts if s['node']==node)
                request={'schema':2,'interface':INTERFACE,'kind':'metadata-registration-request-v2','case':case_ref,'run':run.name,
                    'fm_root':str(run/'fm'),'metadata_kind':kind,'file_identity':{**identity,'path':str(path),'content_sha256':ev.digest(raw),
                        'kernel_inode_aliases':helper.file_identity_aliases(path,identity)},
                    'qemu_binary_sha256':a['qemu_binary_sha256'],'writer_source_sha256':a['writer_source_sha256'],
                    'control_log':controls,'control_line':n,'guest_node':node,'guest_boot_id':b['boot_id'],'guest_incarnation':b['incarnation'],
                    'qemu_owner':{'pid':owner['pid'],'birth':owner['birth']},'qemu_log':logs[node],
                    'worker_stdout':reference(run/('worker-'+node+'.stdout.log'),1<<20),'journal_summary':summary,'journal_captures':captures}
                candidates=[]
                prefix='SIM_DEC: OBMM bootstrap publish ' if kind=='bootstrap' else 'SIM_DEC: OBMM export retired '
                for number,text in enumerate(lines[node]):
                    if not text.startswith(prefix):continue
                    ref=save(ev,directory/('candidate-%03d-%06d.json'%(count,number)),{**request,'qemu_line':number})
                    try:entry=p.validate_registration(ref['path'],ref['sha256'],helper_path)
                    except (ev.EvidenceError,KeyError,ValueError):continue
                    ap_require(entry['kind']==a['expected_production_entry_kind'] and entry['HOST_fixture_only'] is False,'production typed entry required')
                    candidates.append((ref,entry))
                ap_require(len(candidates)==1,'unique actual E-validated publication event required')
                ref,entry=candidates[0];requests.append(ref);entries.append(entry)
        ap_require(entries and {e['metadata_kind'] for e in entries}=={'bootstrap','retired'} and
            len({e['file']['path'] for e in entries})==len(entries),'complete distinct bootstrap and retired normal entries required before append')
        registry=Path(case['registry_directory']);ap_require(not registry.exists(),'new registry only; interrupted run needs explicit reconciliation')
        head='0'*64;receipts=[]
        for entry in entries:
            row=p.append_registration(registry,head,entry);receipts.append(row);head=row['head_sha256']
        save(ev,directory/'registration-receipts.json',{'schema':2,'case':case_ref,'requests':requests,'receipts':receipts,'head_sha256':head})
        plan={'schema':2,'interface':INTERFACE,'kind':'closed-union-plan-v2','helper_path':helper_path,
            'historical_candidate_sha256':accepted_anchors()['terminal']['candidate']['sha256'],
            'historical_consumption_receipt':receipt,'cases':[case_ref],'registry_heads':{run.name:head}}
        ref=save(ev,directory/'companion-plan.json',plan);result=p.validate_closed_union(ref['path'],ref['sha256'])
        ap_require(result['status']==a['expected_production_union_status'] and result['HOST_fixture_only'] is False and
            result['historical_retained_files']==62 and result['historical_quarantine_normal_registered'] is False and
            result['new_normal_metadata']==len(entries) and result['exact_union_files']==72+len(entries) and
            result['historical_candidate_replayed'] is False and result['current_balance_derived'] is False and
            all(result[k] is False for k in p.TRUTHS),'complete typed exact union result required')
        save(ev,directory/'companion-result.json',result)
        return result
