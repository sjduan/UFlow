"""Fresh accepted E v2 proof and exact E0 current-case execution binding.

Rust prepends its compiled accepted_producers.py; ordinary HOST tests import
the same helper. This file never locks, mutates accounting or launches Guests.
"""
import re
if 'accepted_anchors' not in globals():
    from accepted_producers import (accepted_anchors,fresh_terminal_proof,AP_ROOT,AP_SHA,
        ap_require,ap_read,ap_bound,ap_decode,ap_canonical)
    import hashlib,json,os,stat,sys
    from pathlib import Path

def current_execution_context(request,pins):
    row=request['execution_manifest'];path=Path(row['path'])
    ap_require(path.name=='execution.json' and path.parent.parent==AP_ROOT/'implementation' and
        re.fullmatch(r'config-terminal-c[0-9]{3}-v[1-9][0-9]{0,2}',path.parent.name),'production-not-ready: exact reviewed current execution namespace required; no descriptor/state/authority/verifier IO')
    plan=ap_decode(ap_bound(row,1<<20));a=pins['terminal']
    ap_require(plan['schema']==2 and plan['kind']=='C064TerminalResolvedExecutionV2' and plan['consumer_anchor_sha256']==AP_SHA and
        plan['state']==a['expected_state'] and plan['candidate']==a['candidate'] and plan['original_authority']==str(AP_ROOT/'implementation/responsibility-c013') and
        plan['eligible_resource_ids']==[113,114,115] and plan['terminal_bytes']==52<<20 and plan['protected_bytes']==76<<20,'exact accepted current-c064 execution scope required')
    case_ref=plan['case'];ap_require(case_ref['path']==str(path.parent/'current-case.json'),'case outside reviewed execution namespace')
    case=ap_decode(ap_bound(case_ref,1<<20))
    ap_require(case['kind']=='C064TerminalProductionCaseV2' and case['consumer_anchor_sha256']==AP_SHA and
        case['accepted_candidate']==a['candidate'] and 'CPU_fixture_only_not_E_accepted_proof' not in case,'actual production case descriptor required; fixture cannot be promoted')
    action=request['execution_action'];ap_require(action in ('terminal-only','successor') and set(plan['phases'])=={'terminal-only','successor'},'exact two explicit execution phases required')
    terminal=plan['phases']['terminal-only']['job'];successor=plan['phases']['successor']['job']
    ap_require(re.fullmatch(r'c[0-9]{3}',terminal) and re.fullmatch(r'c[0-9]{3}',successor) and terminal!=successor and
        path.parent.name.startswith('config-terminal-'+terminal+'-v'),'unique manifest-bound current phase jobs required')
    for phase,item in plan['phases'].items():
        ap_require(item['config']['path']==str(path.parent/(item['job']+'.json')),'phase config outside exact namespace')
        config=ap_decode(ap_bound(item['config'],1<<20))
        ap_require(config['root']==str(AP_ROOT) and config['job']==item['job'] and config['kind']==('build' if phase=='terminal-only' else 'guest'),'actual E0 phase job/kind binding mismatch')
    exe=Path(request['daemon_path']);ap_require(str(exe)==plan['daemon']['path'] and hashlib.sha256(ap_read(exe,128<<20)).hexdigest()==plan['daemon']['sha256'],'reviewed single-link runtime executable required')
    item=plan['phases'][action];job=item['job'];monitor=AP_ROOT/'runs'/job/'monitor'
    launch=ap_decode(ap_read(monitor/'launch-once.json'))
    ap_require(launch['argv'][-2:]==['--actor',item['config']['path']],'wrong actual E0 actor configuration')
    def proc(pid):
        raw=Path('/proc/%d/stat'%pid).read_bytes();ap_require(len(raw)<=8192,'bounded process identity')
        fields=raw.decode().rsplit(')',1)[1].split();return int(fields[1]),int(fields[3]),int(fields[19])
    parent,session,_=proc(os.getpid());ap_require(proc(launch['supervisor_pid'])[2]==launch['supervisor_birth'],'supervisor birth drift')
    leader_parent,leader_session,_=proc(session);ap_require(leader_parent==launch['supervisor_pid'] and leader_session==session,'actual dedicated supervised session required')
    ancestry=[]
    for _ in range(16):
        ancestry.append(parent)
        if parent==session:break
        parent,child_session,_=proc(parent);ap_require(child_session==session,'adapter escaped supervised session')
    ap_require(session in ancestry and request['daemon_pid'] in ancestry,'actual daemon outside E0 ancestry')
    ap_require(os.readlink('/proc/%d/exe'%request['daemon_pid'])==str(exe),'daemon executable changed')
    ap_require(stat.S_ISFIFO(os.fstat(int(os.environ['UFLOW_ENV_CLOSE_FD'])).st_mode),'actual inherited independent-supervisor close pipe required')
    return {'execution_manifest_sha256':row['sha256'],'phase':action,'job':job,'case':case_ref}

def current_main(request):
    pins=accepted_anchors(request['accepted_anchor_bytes'])
    if request['action']=='execution-context':return current_execution_context(request,pins)
    ap_require(request['action']=='verify-current','only explicit current v2 read-only verification permitted')
    return fresh_terminal_proof(pins)

if __name__=='__main__':
    try:
        raw=sys.stdin.buffer.read((128<<10)+1);ap_require(len(raw)<=128<<10,'bounded current adapter request')
        print(ap_canonical(current_main(ap_decode(raw))).decode())
    except Exception as error:
        sys.stderr.write('REJECT_C_CURRENT_TERMINAL_ADAPTER: '+str(error)[:1600]+'\n');raise SystemExit(2)
