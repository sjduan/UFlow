#!/usr/bin/env python3
"""Guarded private growth-history diagnostic. NEVER uses the real G authority.

Fixture expansion is only allowed while this driver's daemon is stopped, in its
inherited private namespace/root, preserving the private pinned inodes. Complete
normal generations from a real drained seed are re-encoded with unique broker
names; the production daemon must independently replay and C/R/D every byte.
This is synthetic legal history, NOT thousands of real process drain claims.
"""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import time
from types import SimpleNamespace

import t3_normal_authority_probe as g
import t35_calibrate as c

V = Path('/workspace/uflow/runs/eagain-private')
ROOT = Path('/workspace/uflow/authority/external-ddr-v0')
ENDPOINT = str(V/'u.sock')
ZERO = ('committed_bytes','backing_fds','owner_leases','active_views','pending_handoffs','in_flight')
POLY = 0x42f0e1eba9ea3693
MASK = (1 << 64)-1
TABLE = []
for value in range(256):
    crc = value << 56
    for _ in range(8):
        crc = ((crc << 1) ^ (POLY if crc & (1 << 63) else 0)) & MASK
    TABLE.append(crc)

def crc64(data):
    crc = 0
    for byte in data:
        crc = ((crc << 8) & MASK) ^ TABLE[(crc >> 56) ^ byte]
    return crc

def decode(data):
    offset, previous, rows = 0, 0, []
    while offset < len(data):
        magic, version, kind, seq, prev, size = struct.unpack_from('>4sHHQQI', data, offset)
        assert magic == b'UDRJ' and version == 1 and seq == len(rows)+1 and prev == previous
        end = offset+28+size
        check = struct.unpack_from('>Q',data,end)[0]
        assert check == crc64(data[offset:end]) and data[end+8:end+12] == b'JEND'
        payload=data[offset+28:end]
        fields=dict(item.split('=',1) for item in payload.decode().strip().split(';'))
        assert payload.decode()==';'.join(k+'='+v for k,v in sorted(fields.items()))+'\n'
        rows.append(dict(kind=kind,sequence=seq,fields=fields))
        previous,offset=check,end+12
    return rows,previous

def journal():
    raw=(ROOT/'recovery.journal').read_bytes()
    rows,previous=decode(raw)
    head=dict(line.split('=',1) for line in (ROOT/'recovery.head').read_text().splitlines()[1:])
    assert int(head['sequence'])==len(rows) and int(head['offset'])==len(raw) and int(head['checksum'],16)==previous
    return raw,rows,head

def control(op,**values):
    raw=g.control(ENDPOINT,(';'.join(f'{k}={v}' for k,v in {'op':op,**values}.items())+'\n').encode())
    fields=dict(item.split('=',1) for item in raw.decode().strip().split(';') if '=' in item)
    assert fields['status']=='ok',fields
    return fields

def exercise(rootfd,nsfd,parent,label,sizes):
    assert os.getppid()==parent and g.identity(Path('/workspace/uflow')) == (os.fstat(rootfd).st_dev,os.fstat(rootfd).st_ino)
    assert os.stat('/proc/self/ns/mnt').st_ino!=os.fstat(nsfd).st_ino
    from uflow import ExternalDdrClient
    client=ExternalDdrClient(ENDPOINT,timeout=90)
    out=V/label; out.mkdir(mode=0o700)
    peers=[]; rows=[]; errors=[]
    start=time.monotonic()
    try:
        cpus=sorted(os.sched_getaffinity(0))[:3]; assert len(cpus)==3
        args=SimpleNamespace(output=out,endpoint=ENDPOINT)
        for nbytes in sizes:
            source=c.Peer(args,cpus[0],f'source-{nbytes}'); peers.append(source)
            src=source.command('source',bytes=nbytes)
            targets=[]
            for i in range(2):
                p=c.Peer(args,cpus[i+1],f'target-{nbytes}-{i}'); targets.append(p); peers.append(p)
            extra=32*1024*1024 if nbytes>4096 else 0
            prepared=[p.command('prepare',bytes=nbytes,source=src['handle'],round=0,target_extra_bytes=extra) for p in targets]
            at=time.monotonic()+.05
            for p in targets: p.send('map',bytes=nbytes,at=at,source_pid=source.proc.pid)
            mapped=[p.receive() for p in targets]
            at=time.monotonic()+.05
            for i,p in enumerate(targets):
                p.send('copy',bytes=nbytes,at=at,concurrency=2,round=0,requester=i,sample_id=f'{label}-n{nbytes}-q{i}')
            copied=[]
            for p in targets:
                try: copied.append(p.receive())
                except Exception as error: copied.append({'failure':str(error)})
            for i,p in enumerate(targets):
                if 'failure' in copied[i]:
                    c.save_json(out/f'failure-{nbytes}-{i}.json',copied[i]); continue
                consumed=p.command('consume_copy',bytes=nbytes)
                row=dict(bytes=nbytes,requester=i,preparation=prepared[i],**mapped[i],**copied[i],**consumed)
                assert row['full_payload_and_complement_exact'] and row['map_read']['crc32']==row['copy_read']['crc32']
                row['target_complement_bytes']=prepared[i]['target']['size_bytes']-nbytes
                if nbytes>4096: assert row['target_complement_bytes']>16*1024*1024
                assert row['done']['copied_bytes']==str(nbytes) and row['done']['drained']=='true'
                _,records,head=journal(); target=str(prepared[i]['target']['object_id'])
                assert len([r for r in records if r['fields']['broker']==head['broker'] and r['fields']['type']=='work_intent' and r['fields']['target_id']==target])==1
                c.append_json(out/'samples.jsonl',row); rows.append(row)
            for p in reversed(peers): p.close()
            peers.clear()
    finally:
        for p in reversed(peers):
            try: p.close()
            except Exception as error: errors.append(str(error))
        zero=client.snapshot()
        c.save_json(out/'result.json',dict(rows=len(rows),sizes=sizes,seconds=time.monotonic()-start,errors=errors,zero=zero))
    assert not errors and all(zero[k]==0 for k in ZERO)
    # A baseline may fail: evidence retained, cleanup still mandatory.
    print(json.dumps(dict(label=label,rows=len(rows),seconds=time.monotonic()-start)),flush=True)

def grow(seed,min_frames,min_bytes):
    """Internal fixture encoder; caller holds guarded private-root lock."""
    raw,rows,head=journal()
    assert head['mode']=='clean' and rows[-1]['kind']==16
    state=json.loads((ROOT/'state').read_text())
    assert state['mode']=='clean' and state['closure']=='normal'
    template,_=decode(seed)
    assert template[0]['kind']==22 and template[-2]['kind']==15 and template[-1]['kind']==16
    prefix=raw; sequence=len(rows); previous=int(head['checksum'],16); added=bytearray(); generations=0
    while sequence < min_frames or len(prefix)+len(added)<min_bytes:
        broker=f'fixture-{sequence:028d}'
        for row in template:
            fields=dict(row['fields'],broker=broker)
            payload=(';'.join(k+'='+v for k,v in sorted(fields.items()))+'\n').encode()
            sequence+=1
            frame=struct.pack('>4sHHQQI',b'UDRJ',1,row['kind'],sequence,previous,len(payload))+payload
            previous=crc64(frame)
            added.extend(frame+struct.pack('>Q',previous)+b'JEND')
        generations+=1
    if not added: return
    with (ROOT/'recovery.journal').open('ab') as f: f.write(added); f.flush(); os.fsync(f.fileno())
    head.update(broker=broker,sequence=str(sequence),offset=str(len(prefix)+len(added)),checksum=f'{previous:016x}',clean_sequence=str(sequence))
    headtext='uflow-ddr-recovery-head-v1\n'+''.join(k+'='+head[k]+'\n' for k in ('mode','broker','sequence','offset','checksum','clean_sequence'))
    with (ROOT/'recovery.head').open('w') as f: f.write(headtext); f.flush(); os.fsync(f.fileno())
    # Exact normal-head binding in the private versioned state. All other fields
    # and every pinned inode remain untouched; production startup replays it.
    state['journal_head']=headtext
    with (ROOT/'state').open('w') as f: f.write(json.dumps(state,sort_keys=True,separators=(',',':'))+'\n'); f.flush(); os.fsync(f.fileno())
    _,records,newhead=journal()
    assert (ROOT/'recovery.journal').read_bytes().startswith(prefix)
    c.append_json(V/'growth.jsonl',dict(before_frames=len(rows),before_bytes=len(prefix),after_frames=len(records),after_bytes=int(newhead['offset']),generations=generations,template_frames=len(template),template_bytes=len(seed),head=newhead,state=state,synthetic_fixture=True))

def guarded_grow(args,seed):
    # Recheck BEFORE authority paths, even when called independently. The
    # inherited root/namespace descriptors cannot be supplied by shell flags
    # alone; the pinned outer parent must still be this process's parent.
    g.require_isolated_namespace(args.nsfd,args.outer_pid)
    expected=os.fstat(args.rootfd)
    assert g.identity(Path('/workspace/uflow'))==(expected.st_dev,expected.st_ino)
    assert not Path(ENDPOINT).exists() and not Path(ENDPOINT+'.external-ddr').exists()
    pinned={name:g.identity(ROOT/name) for name in g.FILES}
    with (ROOT/'authority.lock').open('rb') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        grow(seed,args.frames,args.history_bytes)
        assert pinned=={name:g.identity(ROOT/name) for name in g.FILES}

def negative(rootfd,nsfd,parent,label):
    assert os.getppid()==parent and g.identity(Path('/workspace/uflow'))==(os.fstat(rootfd).st_dev,os.fstat(rootfd).st_ino)
    assert os.stat('/proc/self/ns/mnt').st_ino!=os.fstat(nsfd).st_ino
    from uflow import ExternalDdrClient,ExternalDdrError
    client=ExternalDdrClient(ENDPOINT,timeout=90)
    out=V/label; out.mkdir(mode=0o700)
    fds=[]; handles=[]; result=[]
    try:
        sf=c.memfd('negative-source',c.SRC_OFFSET+4096+c.SUFFIX,37,True); fds.append(sf)
        source=client.register(sf,name='negative-source',owner_name='negative',size_bytes=c.SRC_OFFSET+4096+c.SUFFIX); handles.append(source)
        for mechanism in ('socket','memfd'):
            size=c.DST_OFFSET+4096+c.SUFFIX
            fd=c.memfd('negative-target',size,83,False); fds.append(fd)
            target=client.stage(fd,name='negative-target',owner_name='negative',size_bytes=size); handles.append(target)
            before=client.snapshot()
            # Skip first and guard's two queries each; delay the post-probe
            # first query. This ensures a REAL probe has already drained.
            (V/(mechanism+'.arm')).write_text('4\n' if mechanism=='socket' else '0\n')
            started=time.monotonic()
            try:
                client.plan(source,target,src_offset_bytes=c.SRC_OFFSET,dst_offset_bytes=c.DST_OFFSET,nbytes=4096)
            except ExternalDdrError as error:
                text=str(error)
                assert ('broker.read-response' if mechanism=='socket' else 'probe.memfd-create') in text,text
                assert 'WouldBlock' in text and 'Some(11)' in text,text
                assert not c.retry_rejection(error,'plan',None)
            else: raise AssertionError('controlled Plan failure accepted')
            elapsed=time.monotonic()-started
            if mechanism=='socket': assert elapsed>=5
            assert not (V/(mechanism+'.arm')).exists()
            assert os.pread(fd,size,0)==bytes([83])*size
            try: client.acquire(target,offset_bytes=c.DST_OFFSET,nbytes=4096)
            except ExternalDdrError: pass
            else: raise AssertionError('unsubmitted target Ready')
            after=client.snapshot()
            assert all(before[k]==after[k] for k in ZERO)
            _,records,head=journal(); current=[r['fields'] for r in records if r['fields']['broker']==head['broker']]
            work=[r for r in current if r['type']=='work_intent']
            assert not any(r['target_id']==str(target.object_id) for r in work)
            probes=[r for r in work if r['purpose']=='probe_only']
            assert len(probes)==1
            probe=probes[0]; life=[r['type'] for r in current if r.get('work_id')==probe['work_id']]
            assert all(k in life for k in ('work_drained','work_io_completed','work_verified'))
            assert any(r['type']=='backing_close_done' and r.get('object_id')==probe['target_id'] for r in current)
            assert next(b for b in after['backings'] if str(b['object_id'])==probe['target_id'])['reclaimed']
            result.append(dict(mechanism=mechanism,error=text,seconds=elapsed,full_target_unchanged=True,business_work_count=0,probe_lifecycle=life,zero_delta=True,before=before,after=after))
    finally:
        for h in reversed(handles): client.revoke(h)
        for fd in fds: os.close(fd)
        zero=client.snapshot(); c.save_json(out/'negative.json',dict(results=result,zero=zero))
    assert all(zero[k]==0 for k in ZERO)

def inner(args):
    g.require_isolated_namespace(args.nsfd,args.outer_pid)
    expected=os.fstat(args.rootfd)
    assert g.identity(args.nsroot)==(expected.st_dev,expected.st_ino)
    subprocess.run(['mount','--bind',str(args.nsroot),'/workspace/uflow'],check=True)
    assert g.identity(Path('/workspace/uflow'))==(expected.st_dev,expected.st_ino)
    assert not os.path.lexists(ROOT)
    env=dict(os.environ,UF_EXTERNAL_DDR_HOST_ID='eagain-private',UF_EXTERNAL_DDR_OWNER_MAX_BYTES='536870912',
             UF_EXTERNAL_DDR_TOTAL_MAX_BYTES='1073741824',UF_HBM_STARTUP_PROBE_BYTES='0',UF_DDR_DIAGNOSTICS='1',
             LD_LIBRARY_PATH=str(V/'shim')+':/usr/local/Ascend/cann-9.0.0/lib64',PYTHONPATH=str(V/'src/sdk/python'),PYTHONDONTWRITEBYTECODE='1')
    if (V/'delay.so').exists():
        env.update(LD_PRELOAD=str(V/'delay.so'),UF_T4_PRIVATE_SOCKET_ARM=str(V/'socket.arm'),UF_T4_PRIVATE_MEMFD_ARM=str(V/'memfd.arm'))
    binary=V/'uf-daemon'
    with (V/'bootstrap.log').open('x') as log:
        p=subprocess.Popen([str(binary),'--socket',ENDPOINT,'--initialize-external-ddr-authority'],env=env,stdout=log,stderr=subprocess.STDOUT)
        assert p.wait(timeout=30)==0
    pinned={name:g.identity(ROOT/name) for name in g.FILES}
    def run(label,sizes,negative_test=False):
        before=journal()[2]; daemon=None; report={'before':before,'binary_sha256':g.digest(binary)}
        try:
            with (V/(label+'-daemon.log')).open('x') as log:
                daemon=subprocess.Popen([str(binary),'--socket',ENDPOINT,'--execution-mode','simulated','--startup-probe-bytes','0'],env=env,stdout=log,stderr=subprocess.STDOUT)
                report['daemon_pid']=daemon.pid
                deadline=time.monotonic()+120
                while not Path(ENDPOINT+'.external-ddr').exists():
                    assert daemon.poll() is None and time.monotonic()<deadline; time.sleep(.02)
                libs={line.split()[-1] for line in Path(f'/proc/{daemon.pid}/maps').read_text().splitlines() if any(s in line for s in ('libuf_acl_shim','libascendcl','delay.so'))}
                report['loaded_libraries']={path:g.digest(Path(path)) for path in libs}
                report['trace_start']=control('StartTrace',run_id=label,mode='timeline',include_args='true',output_dir=str(V/'trace'))
                proc=subprocess.Popen([str(V/'venv/bin/python'),str(V/'src/examples/ddr_v0/test_t4_eagain_private.py'),'negative' if negative_test else 'exercise',str(args.rootfd),str(args.nsfd),str(os.getpid()),label,*map(str,sizes)],env=env,pass_fds=(args.rootfd,args.nsfd))
                report['exercise_pid']=proc.pid; report['exercise_exit']=proc.wait(timeout=1800)
                report['stats']=control('GetStats'); report['trace']=control('ExportTrace')
                trace=json.loads(Path(report['trace']['trace_artifact']).read_text())['traceEvents']
                terminal={e['args']['event_id']:e['args'] for e in trace if e['name']=='transfer.external_terminal'}
                submitted=[e['args']['event_id'] for e in trace if e['name']=='transfer.external_submit']
                assert set(submitted)==set(terminal) and len(submitted)==len(terminal)
                assert sum(int(e['copied_bytes']) for e in terminal.values())==int(report['stats']['external_copied_bytes'])
                for key in ('external_ddr_committed_bytes','external_unfinalized_workers','external_cleanup_unconfirmed'): assert report['stats'][key]=='0'
                assert report['stats']['external_cost_version']=='unknown' and report['stats']['external_automatic_ranking']=='disabled'
                if not negative_test:
                    samplefile=V/label/'samples.jsonl'
                    samples=[json.loads(line) for line in samplefile.read_text().splitlines()] if samplefile.exists() else []
                    keys=('event_id','event_status','bytes_done','mapped_bytes','accessed_bytes','copied_bytes','planned_path','planned_engine','actual_path','actual_engine','drained','phase')
                    for row in samples:
                        done=row['done']; assert all(done[k]==terminal[done['event_id']][k] for k in keys)
                    if args.require_complete or label=='seed': assert len(samples)==2*len(sizes)
                    assert int(report['stats']['external_mapped_bytes'])==2*sum(sizes)+sum(r['bytes'] for r in samples)
                    assert int(report['stats']['external_accessed_bytes'])==2*int(report['stats']['external_mapped_bytes'])
                report['reconciliation']=dict(submits=len(submitted),terminals=len(terminal),all_12_terminal_fields_match=True)
        finally:
            if daemon and daemon.poll() is None:
                report['shutdown']=control('ShutdownDaemon'); report['daemon_exit']=daemon.wait(timeout=300)
            report['after']=journal()[2]
            report['state']=json.loads((ROOT/'state').read_text())
            c.save_json(V/(label+'-audit.json'),report)
        assert report['exercise_exit']==0 and report['daemon_exit']==0
        assert report['state']['mode']=='clean' and report['state']['closure']=='normal'
        assert pinned=={name:g.identity(ROOT/name) for name in g.FILES}
        assert not Path(ENDPOINT).exists() and not Path(ENDPOINT+'.external-ddr').exists()
        fd=os.open(ROOT/'authority.lock',os.O_RDONLY|os.O_CLOEXEC|os.O_NOFOLLOW)
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB); os.close(fd)
    run('seed',[4096])
    seed=(ROOT/'recovery.journal').read_bytes()
    (V/'seed.journal').write_bytes(seed)
    (V/'seed-state.json').write_bytes((ROOT/'state').read_bytes())
    if args.seed_only: return
    guarded_grow(args,seed)
    run(args.label,[4096,32*1024*1024])
    # A new independent daemon must replay/resync the same original clean root
    # and actually copy nonzero bytes; not just an in-process authority reopen.
    run(args.label+'-restart',[4096])
    if (V/'delay.so').exists(): run(args.label+'-negative',[],True)

def outer(args):
    # Metadata only: no opening, locking, reading or writing real G files.
    def outer_domain():
        paths=[ROOT,*[ROOT/name for name in g.FILES],Path('/workspace/uflow/runs/external-ddr-v0/u.sock'),Path('/workspace/uflow/runs/external-ddr-v0/u.sock.external-ddr')]
        result={}
        for path in paths:
            try:
                st=path.lstat(); result[str(path)]=(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_mode)
            except FileNotFoundError: result[str(path)]=None
        return result
    outer_before=outer_domain()
    run=args.run_dir.resolve(); assert run==args.run_dir and run.is_relative_to(Path('/workspace/uflow/runs'))
    run.mkdir(mode=0o700); nsroot=run/'nsroot'; nsroot.mkdir(mode=0o700)
    (nsroot/'authority').mkdir(mode=0o700); (nsroot/'runs').mkdir(mode=0o700)
    clone=nsroot/V.relative_to('/workspace/uflow'); clone.mkdir(mode=0o700)
    src=Path(__file__).resolve().parents[2]
    for name in ('examples/ddr_v0','sdk/python'): shutil.copytree(src/name,clone/'src'/name)
    (clone/'shim').mkdir(mode=0o700)
    shutil.copy2(args.daemon,clone/'uf-daemon'); shutil.copy2(args.shim,clone/'shim/libuf_acl_shim.so')
    if args.delay_so: shutil.copy2(args.delay_so,clone/'delay.so')
    subprocess.run(['python3','-m','venv','--without-pip',str(clone/'venv')],check=True)
    nsfd=os.open('/proc/self/ns/mnt',os.O_RDONLY|os.O_CLOEXEC)
    rootfd=os.open(nsroot,os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC|os.O_NOFOLLOW)
    before=Path('/proc/self/mountinfo').read_bytes(); original=g.identity(Path('/workspace/uflow'))
    cmd=[sys.executable,str(Path(__file__).resolve()),'inner','--nsroot',str(nsroot),'--nsfd',str(nsfd),'--rootfd',str(rootfd),'--outer-pid',str(os.getpid()),'--frames',str(args.frames),'--history-bytes',str(args.history_bytes),'--label',args.label]
    if args.seed_only: cmd.append('--seed-only')
    if args.require_complete: cmd.append('--require-complete')
    try:
        with (run/'negative-direct.log').open('x') as log:
            proc=subprocess.Popen(cmd,pass_fds=(nsfd,rootfd),stdout=log,stderr=subprocess.STDOUT); assert proc.wait(timeout=30)!=0
        assert 'mount namespace was not isolated' in (run/'negative-direct.log').read_text()
        with (run/'outer.log').open('x') as log:
            proc=subprocess.Popen(['unshare','--mount','--propagation','private',*cmd],pass_fds=(nsfd,rootfd),stdout=log,stderr=subprocess.STDOUT)
            rc=proc.wait(timeout=3600)
        same=before==Path('/proc/self/mountinfo').read_bytes() and original==g.identity(Path('/workspace/uflow'))
        outer_after=outer_domain()
        c.save_json(run/'isolation.json',dict(exit=rc,outer_mounts_and_identity_unchanged=same,outer_namespace=os.fstat(nsfd).st_ino,outer_G_metadata_before=outer_before,outer_G_metadata_after=outer_after,outer_G_unchanged=outer_before==outer_after))
        assert rc==0 and same and outer_before==outer_after,(run/'outer.log').read_text()
    finally: os.close(rootfd); os.close(nsfd)
    print('GUARDED_GROWTH_PRIVATE_PASS',flush=True)

def main():
    if len(sys.argv)>1 and sys.argv[1]=='negative':
        return negative(*map(int,sys.argv[2:5]),sys.argv[5])
    if len(sys.argv)>1 and sys.argv[1]=='exercise':
        return exercise(*map(int,sys.argv[2:5]),sys.argv[5],list(map(int,sys.argv[6:])))
    p=argparse.ArgumentParser(description=__doc__); p.add_argument('mode',choices=('outer','inner'))
    p.add_argument('--run-dir',type=Path); p.add_argument('--daemon',type=Path); p.add_argument('--shim',type=Path)
    p.add_argument('--nsroot',type=Path); p.add_argument('--nsfd',type=int); p.add_argument('--rootfd',type=int); p.add_argument('--outer-pid',type=int)
    p.add_argument('--frames',type=int,default=12489); p.add_argument('--history-bytes',type=int,default=5097305)
    p.add_argument('--label',default='baseline'); p.add_argument('--seed-only',action='store_true')
    p.add_argument('--delay-so',type=Path); p.add_argument('--require-complete',action='store_true')
    args=p.parse_args(); return outer(args) if args.mode=='outer' else inner(args)
if __name__=='__main__': main()
