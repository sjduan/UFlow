"""Exact compiled C c100/c101/c102 E0 admission; no original lock or writes."""
if 'v3_anchors' not in globals():
    from accepted_c081_v3 import *
    from accepted_producers import ap_read,ap_bound,ap_decode,ap_require,ap_canonical,AP_ROOT

def c081_manifest_gate(path,external_sha,policy):
    path=Path(path)
    ap_require(path==Path(policy['config_directory'])/'execution.json' and type(external_sha) is str and re.fullmatch('[0-9a-f]{64}',external_sha),'production-not-ready: exact compiled reviewed execution namespace required; no descriptor/state/authority/verifier IO')
    return {'path':str(path),'bytes':len(ap_read(path,1<<20)),'sha256':external_sha}

def c081_execution(request,a,policy):
    phase=request['execution_action'];ap_require(phase in policy['phases'],'exact compiled execution phase required')
    ref=c081_manifest_gate(request['execution_manifest']['path'],request['execution_manifest']['sha256'],policy);plan=ap_decode(ap_bound(ref,1<<20));directory=Path(policy['config_directory'])
    ap_require(plan['schema']==3 and plan['kind']=='C081ResolvedExecutionV3' and plan['nonce']==policy['nonce'] and plan['consumer_anchor_sha256']==V3_SHA and plan['accepted_context']==a['accepted_context'] and plan['state_sha256']==policy['state_sha256'] and plan['authority']==policy['authority'] and plan['successor']==policy['successor'],'compiled complete context/current/nonce binding required')
    ap_require(set(plan['phases'])==set(policy['phases']) and all(plan['phases'][k]['job']==v for k,v in policy['phases'].items()),'compiled exact three phase jobs required')
    configs={}
    for key,item in plan['phases'].items():
        ap_require(item['config']['path']==str(directory/(item['job']+'.json')),'phase config outside exact namespace')
        config=ap_decode(ap_bound(item['config'],1<<20));configs[key]=config
        ap_require(config['root']==str(AP_ROOT) and config['job']==item['job'] and config['kind']=={'terminal-only':'build','successor':'guest','postclose-audit':'host'}[key],'exact actual E0 phase/kind required')
    case=ap_decode(ap_bound(plan['case'],1<<20));ap_require(plan['case']['path']==str(directory/'current-case.json') and case['kind']=='C081TerminalProductionCaseV3' and case['consumer_anchor_sha256']==V3_SHA and case['accepted_context']==a['accepted_context'] and case['accepted_candidate']==a['code_and_data']['terminal_candidate'] and 'CPU_fixture_only_not_E_accepted_proof' not in case,'actual typed current case, never fixture admission')
    source=Path(plan['source']);m=ap_decode(ap_bound(plan['source_manifest'],8<<20));ap_require(plan['source_manifest']['path']==str(source/'.uflow-source.json') and m['commit']==plan['source_commit'],'actual source snapshot identity required')
    commands={
      'terminal-only':['/usr/local/python3.11.14/bin/python3.11','-B','-s',str(source/'tools/phasec/c081_terminal_phase.py'),str(directory/'execution.json')],
      'successor':['/usr/local/python3.11.14/bin/python3.11','-B','-s',str(source/'tools/phasec/run_mem_service_pool_matrix.py'),str(directory/'guest-group.json'),plan['build']['path']],
      'postclose-audit':['/usr/local/python3.11.14/bin/python3.11','-B','-s',str(source/'tools/phasec/managed_metadata_v3.py'),str(directory/'execution.json')]}
    frozen={'task_supervisor.py':'7b23344b2cd9c51504e2389682f31b6dfa2e9567ed5a8bfda49dfb73ddf70827','task_lifecycle.py':'9370260295844d433ac9273a255304b6b5e66d52c26b58cde758460dd594ad8c','task_resources.py':'bdc057af7a5790c7ab181289072326b7a81fc397d6d08c6a6b5e08066e01f607'}
    for key,config in configs.items():
        ap_require(config['commands']==[{'argv':commands[key],'cwd':str(AP_ROOT)}] and config['supervisor_sha256']==frozen and config['supervisor_root']==str(AP_ROOT/'env/supervisor') and 0<config['run_seconds']<=3600,'exact source-bound actual phase actor and frozen bounded supervisor required')
    for rel,row in m['files'].items():
        ap_require(hashlib.sha256(ap_read(source/rel,8<<20,int(row['mode'][-3:],8))).hexdigest()==row['sha256'],'actual bound Git source/mode drift')
    build=ap_decode(ap_bound(plan['build'],8<<20));ap_require(build['source_commit']==plan['source_commit'] and build['source_manifest_sha256']==plan['source_manifest']['sha256'] and build['managed_cache_enabled'] is True and build['accepted_context']==a['accepted_context'],'actual newly compiled production daemon/source receipt required')
    daemon=plan['daemon'];ap_require(daemon['path']==build['daemon'] and daemon['sha256']==build['daemon_sha256'],'actual compiled runtime executable binding')
    ap_bound(daemon,128<<20)
    # Group digest excludes only its back-reference to this manifest, avoiding
    # a circular hash without omitting any actual launch selection.
    group=ap_decode(ap_read(directory/'guest-group.json',1<<20));normalized=dict(group);selected=normalized.pop('terminal_case_execution')
    ap_require(selected=={'path':ref['path'],'sha256':ref['sha256']} and hashlib.sha256(ap_canonical(normalized)).hexdigest()==plan['group_without_execution_sha256'] and group['output']==str(AP_ROOT/'runs'/policy['phases']['successor']),'full group launch selection and exact manifest back-reference required')
    ap_require(group['managed_cache_profile']=={'manifest':a['code_and_data']['profile']['path'],'manifest_sha256':a['code_and_data']['profile']['sha256'],'validator_sha256':a['code_and_data']['profile_validator']['sha256']} and group['qemu_binary']==a['code_and_data']['qemu_binary']['path'],'only accepted actual release-v2 profile selected')
    job=policy['phases'][phase];launch=ap_decode(ap_read(AP_ROOT/'runs'/job/'monitor/launch-once.json',128<<10));item=plan['phases'][phase]
    ap_require(launch['argv'][-2:]==['--actor',item['config']['path']],'actual frozen E0 actor configuration required')
    def proc(pid):
        raw=Path('/proc/%d/stat'%pid).read_bytes();ap_require(len(raw)<=8192,'bounded actual process identity');f=raw.rsplit(b')',1)[1].split();return int(f[1]),int(f[3]),int(f[19])
    parent,session,_=proc(os.getpid());ap_require(proc(launch['supervisor_pid'])[2]==launch['supervisor_birth'],'actual supervisor birth mismatch')
    pp,ss,_=proc(session);ap_require(pp==launch['supervisor_pid'] and ss==session,'dedicated actual supervised session required')
    ancestry=[os.getpid()]
    for _ in range(16):
        ancestry.append(parent)
        if parent==session:break
        parent,ss,_=proc(parent);ap_require(ss==session,'admission escaped owned E0 session')
    ap_require(session in ancestry and request['actor_pid'] in ancestry,'actual invoking actor outside E0 ancestry')
    if request.get('daemon_path') is not None:
        ap_require(request['daemon_path']==daemon['path'] and os.readlink('/proc/%d/exe'%request['actor_pid'])==daemon['path'],'actual same single-link compiled daemon required')
    ap_require(stat.S_ISFIFO(os.fstat(int(os.environ['UFLOW_ENV_CLOSE_FD'])).st_mode),'real inherited independent supervisor close pipe required')
    return {'phase':phase,'job':job,'case':plan['case'],'execution_manifest_sha256':ref['sha256'],'source':str(source),'accepted_context':a['accepted_context'],'plan':plan,'group':group}

def c081_main(request):
    a=v3_anchors(request.get('accepted_anchor_bytes'));policy=v3_policy(request.get('execution_policy_bytes'))
    if request['action']=='execution-context':return c081_execution(request,a,policy)
    ap_require(request['action']=='verify-current','explicit accepted current read-only action only');return v3_terminal_proof(a)
if __name__=='__main__':
    try:
        raw=sys.stdin.buffer.read((128<<10)+1);ap_require(len(raw)<=128<<10,'bounded compiled admission request');print(ap_canonical(c081_main(ap_decode(raw))).decode())
    except Exception as error:
        sys.stderr.write('REJECT_C081_ACCEPTED_ADAPTER: '+str(error)[:1800]+'\n');raise SystemExit(2)
