"""Own-QEMU metadata only, for a later independently accepted Guest package.

Call inline from the same matrix process that owns the frozen Guest starts.
Never read RAM, transport contents, /proc/*/mem, or full process environments.
Selected environment values are source-derived from the frozen launch overlay
and only individually selected parent keys. They are labelled as inference.
"""
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
from qemu_profile_gate import capture_identity, launch_qemu_gate, _require_host_identity, _require_production_identity

ROOT=Path('/workspace/uflow/runs/mem-service-shared-pool-20261108')
QEMU_SHA='56fab4e5d8c50c559f76c8ddbae9d6e1661e4764a6f927db2c3706e8199e5d4c'
GROUP_SHA='05c2dcc01e6b24aed7e14bd6a77fcf8078f5c4f5e0fccd412833e58280ebdcd1'
PARENT_KEYS=('LD_PRELOAD','LD_AUDIT','QEMU_RAM_PATH','LD_LIBRARY_PATH')


def bounded(path,limit):
    fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_CLOEXEC|os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):raise ValueError('ordinary metadata only')
        data=bytearray()
        while len(data)<=limit:
            part=os.read(fd,min(65536,limit+1-len(data)))
            if not part:break
            data.extend(part)
        if len(data)>limit:raise ValueError('metadata bound')
        return bytes(data)
    finally:os.close(fd)


def process_identity(pid):
    raw=bounded(Path('/proc')/str(pid)/'stat',8192).decode()
    fields=raw.rsplit(')',1)[1].split()
    return {'pid':pid,'birth':int(fields[19]),'ppid':int(fields[1]),'session':int(fields[3])}


def _observe_owned(pid,birth,argv,exe_sha,owner,ram_bytes):
    """Private shared mechanism; tests use a named synthetic CPU child only."""
    proc=Path('/proc')/str(pid);first=process_identity(pid)
    if first['birth']!=birth or first['ppid']!=owner['pid'] or process_identity(owner['pid'])['birth']!=owner['birth']:
        raise ValueError('exact owned process PID/birth/parent required')
    status=bounded(proc/'status',65536).decode()
    uid=re.search(r'^Uid:\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)',status,re.M)
    if not uid or any(int(v)!=os.geteuid() for v in uid.groups()):raise ValueError('same task uid required')
    namespaces={name:os.readlink(proc/'ns'/name) for name in ('pid','mnt','user')}
    if any(value!=os.readlink(Path('/proc/self/ns')/name) for name,value in namespaces.items()):raise ValueError('foreign namespace')
    actual_argv=bounded(proc/'cmdline',16384).split(b'\0')
    if actual_argv[-1:]==[b'']:actual_argv.pop()
    if [v.decode() for v in actual_argv]!=list(argv):raise ValueError('frozen actual argv mismatch')
    exe=os.readlink(proc/'exe')
    if exe.endswith(' (deleted)') or Path(exe).resolve()!=Path(argv[0]).resolve():raise ValueError('executable alias/deletion')
    # /proc/exe is a kernel-owned magic link to the proven process executable.
    # Read code bytes only, never a process FD or any mapped/RAM/transport file.
    with (proc/'exe').open('rb') as stream:
        code=stream.read((256<<20)+1)
    if len(code)>256<<20 or hashlib.sha256(code).hexdigest()!=exe_sha:raise ValueError('executable pin')
    maps=bounded(proc/'maps',2<<20).decode().splitlines();anonymous=[]
    for line in maps:
        fields=line.split(None,5)
        if len(fields)<5:raise ValueError('malformed VMA metadata')
        lo,hi=(int(v,16) for v in fields[0].split('-'));name=fields[5] if len(fields)==6 else ''
        if fields[3]=='00:00' and fields[4]=='0' and not name and fields[1].startswith('rw'):
            anonymous.append({'start':lo,'end':hi,'bytes':hi-lo,'permissions':fields[1],'file_backed':False})
    candidates=[v for v in anonymous if v['bytes']>=ram_bytes]
    fds=[];entries=sorted((proc/'fd').iterdir(),key=lambda p:int(p.name))
    if len(entries)>128:raise ValueError('owned FD inventory bound')
    for entry in entries:
        try:
            target=os.readlink(entry);info=entry.stat()
        except FileNotFoundError:raise ValueError('owned FD changed during capture') from None
        fds.append({'fd':int(entry.name),'target':target,'dev':info.st_dev,'inode':info.st_ino,'uid':info.st_uid,
                    'type':stat.S_IFMT(info.st_mode),'file_contents_read':False})
    if process_identity(pid)!=first or os.readlink(proc/'exe')!=exe:raise ValueError('process changed during capture')
    return {'identity':first,'executable':exe,'executable_sha256':exe_sha,'namespaces':namespaces,'FD_metadata':fds,
            'anonymous_writable_VMA_metadata':anonymous,'RAM_sized_anonymous_candidates':candidates,
            'missing_evidence':[] if candidates else ['RAM-sized anonymous mapping not visible in this capture'],
            'RAM_or_transport_contents_read':False,'full_environment_read':False,'QMP_commands_added':False}


def selected_launch_environment(parent,config,node):
    # Individual lookups only. Do not enumerate or serialize os.environ.
    selected={key:parent.get(key) for key in PARENT_KEYS}
    selected.update({'UB_FM_NODE_ID':node,'UB_FM_TOPOLOGY_FILE':config['topology'],
        'UB_FM_SHARED_DIR':str(Path(config['output'])/'fm'),'UB_SIM_ENTITY_COUNT':'2',
        'UB_FM_ENTITY_PLAN_FILE':config['entity_plan'],'UB_SIM_EXPERIMENTAL_FEATURES':'gsva','GSVA_COH_UB_LINK_TX':'0'})
    return {'values':selected,'basis':'source-derived frozen group overlay plus individually selected actual parent keys',
            'direct_child_proc_environment_observed':False,'full_environment_read':False}


def _observe_profiled(record,node,owner,identity,*,_host_fixture=False):
    """Same process/argv/FD/VMA guards, independently bound executable identity."""
    if _host_fixture:identity=_require_host_identity(identity)
    else:identity=_require_production_identity(identity)
    if Path(record['argv'][0])!=identity.binary:raise ValueError('exact validated QEMU executable path required')
    value=_observe_owned(record['pid'],record['birth'],record['argv'],identity.sha256,owner,node['memory_mib']<<20)
    value['qemu_profile_provenance']=identity.metadata()
    return value


def capture_owned_starts(starts_path,expected_starts_sha,config,group_source,qemu_identity=None):
    """Inline owner callback, enabled only in a reviewed future terminal run."""
    starts_path=Path(starts_path);run=starts_path.parent
    if (run.parent!=ROOT/'runs' or not re.fullmatch(r'c\d{3}',run.name) or starts_path.name!='guest-starts.json' or
            run.resolve()!=run or Path(config['output'])!=run):raise ValueError('exact new task Guest starts scope')
    identity=capture_identity(config,qemu_identity if qemu_identity is not None else launch_qemu_gate(ROOT,config))
    raw=bounded(starts_path,65536)
    if hashlib.sha256(raw).hexdigest()!=expected_starts_sha:raise ValueError('frozen starts pin')
    group_sha=GROUP_SHA
    if config.get('managed_write_profile') is not None:
        from accepted_c101_v4 import v4_anchors
        ref=v4_anchors()['code_and_data']['wrapper'];group_sha=ref['sha256']
        if str(group_source)!=ref['path']:raise ValueError('actual c1e W/R wrapper runtime path required')
    elif config.get('managed_cache_profile') is not None:
        from accepted_c081_v3 import v3_anchors
        ref=v3_anchors()['code_and_data']['wrapper'];group_sha=ref['sha256']
        if str(group_source)!=ref['path']:raise ValueError('actual managed wrapper runtime path required')
    if hashlib.sha256(bounded(group_source,65536)).hexdigest()!=group_sha:raise ValueError('frozen group source pin')
    starts=json.loads(raw)
    if len(starts)!=2 or {v['node'] for v in starts}!={'nodeA','nodeB'} or len({(v['pid'],v['birth']) for v in starts})!=2:
        raise ValueError('exact two distinct owned Guest records')
    owner=process_identity(os.getpid());result=[];begin=time.monotonic_ns()
    for record in starts:
        node=next(v for v in config['nodes'] if v['id']==record['node'])
        value=_observe_profiled(record,node,owner,identity)
        value['node']=record['node'];value['selected_launch_environment']=selected_launch_environment(os.environ,config,record['node'])
        result.append(value)
    report={'schema':1,'kind':'own-QEMU-metadata-capture','owner':owner,'starts_sha256':expected_starts_sha,'group_source_sha256':group_sha,
            'qemu_profile_provenance':identity.metadata(),
            'start_host_ns':begin,'end_host_ns':time.monotonic_ns(),'clock_domain':'B-host','processes':result,
            'scope':'process/namespace/argv/executable/FD/VMA metadata; environment is explicit source-derived inference',
            'normal_SDK_ACK':False,'authority_to_mutate_ledger':False,'authority_to_launch_Guest':False}
    target=run/'own-qemu-metadata.json'
    with target.open('x') as stream:json.dump(report,stream,indent=2);stream.flush();os.fsync(stream.fileno())
    return report
