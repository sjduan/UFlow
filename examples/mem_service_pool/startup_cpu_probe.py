#!/usr/bin/env python3
"""Admitted B build only: production agent/SDK + CPU canary/worker doubles.

Actual subprocesses, SO_PEERCRED, PID/birth, pipes and directory service run.
No QEMU/OBMM/device or business allocation; no c013 authority is opened.
"""
import argparse
import ctypes
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import threading
import time

sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tools/phasec'))
from mem_service_pool_bridge import GuestNode, LOWER, exact, lower_request
from pool_startup import Startup, NotReady
from pool_matrix_contract import bootstrap_nodes


def birth(pid):
    try:return int(Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19])
    except FileNotFoundError:return None


class Channel:
    def __init__(self,process,raw):
        self.process,self.raw,self.buffer=process,raw,bytearray()
        os.set_blocking(process.stdout.fileno(),False);os.set_blocking(process.stdin.fileno(),False)
        self.bytes=0
    def line(self,deadline):
        while b'\n' not in self.buffer:
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('fixture serial deadline')
            if not select.select([self.process.stdout],[],[],remaining)[0]:raise TimeoutError('fixture serial deadline')
            data=os.read(self.process.stdout.fileno(),16384)
            if not data:raise RuntimeError('actual fixture agent EOF')
            self.bytes+=len(data)
            if self.bytes>8<<20:raise RuntimeError('fixture raw log bound')
            self.raw.write(data);self.raw.flush();self.buffer.extend(data)
        value,_,rest=self.buffer.partition(b'\n');self.buffer=bytearray(rest)
        return value.decode()
    def send(self,line):
        data=(line+'\n').encode();deadline=time.monotonic()+3
        while data:
            if time.monotonic()>=deadline:raise TimeoutError('bounded fixture serial write')
            if select.select([],[self.process.stdin],[],.05)[1]:
                try:data=data[os.write(self.process.stdin.fileno(),data):]
                except BlockingIOError:pass


class Harness:
    def __init__(self,args,index,mode='normal',lower=True):
        self.args,self.path,self.mode=args,args.scratch/str(index),mode
        self.path.mkdir();self.output=args.output/str(index);self.output.mkdir()
        self.children=[];self.nodes={};self.events=[];self.lock=threading.Lock()
        self.log=(self.output/'events.jsonl').open('x');self.raw=[]
        self.lower_socket=str(self.path/'l');self.config=self.path/'lower.conf'
        self.config.write_text('listen=unix:'+self.lower_socket+'\nstore='+str(self.path/'metadata.store')+
            '\nrequired_provider=nodeA\nrequired_provider=nodeB\nprovider_lease_ms=1000\nallocation_home_provider=nodeA\n')
        if lower:
            f=(self.output/'lower.log').open('xb');self.raw.append(f)
            process=self.spawn([args.controller,'serve','--config',self.config],stdout=f,stderr=subprocess.STDOUT)
            until=time.monotonic()+5
            while not Path(self.lower_socket).exists():
                if process.poll() is not None or time.monotonic()>until:raise RuntimeError('metadata controller start failed')
                time.sleep(.02)
    def event(self,value):
        with self.lock:
            self.events.append(value);self.log.write(json.dumps(value)+'\n');self.log.flush()
            if value.get('event')=='worker-log':
                with (self.output/(value['node']+'.'+value['stream']+'.log')).open('ab') as f:f.write(bytes.fromhex(value['hex']))
    def notice(self):self.event({'event':'first-notice','host_monotonic_ns':time.monotonic_ns()})
    def spawn(self,argv,**kwargs):
        p=subprocess.Popen(list(map(str,argv)),start_new_session=True,**kwargs)
        self.children.append((p,birth(p.pid)));return p
    def start(self):
        for name in ('nodeA','nodeB'):
            raw=(self.output/(name+'.serial.log')).open('xb');self.raw.append(raw)
            err=(self.output/(name+'.agent.stderr')).open('xb');self.raw.append(err)
            p=self.spawn([self.args.agent,self.path/name,name,'cpu-boot-'+str(self.path.name)+'-'+name,'metadata-only-no-OBMM'],
                stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=err,
                env={**os.environ,'UFLOW_POOL_FIXTURE_WORKER':str(self.args.worker),'UFLOW_POOL_FIXTURE_MODE':self.mode})
            channel=Channel(p,raw);line=channel.line(time.monotonic()+5)
            if not line.startswith('UFLOW_ENV_IDENTITY '):raise RuntimeError('actual agent identity absent: '+line)
            identity=json.loads(line.split(' ',1)[1])
            if identity['engine']!='cpu-fixture':raise RuntimeError('CPU fixture compile marker required')
            # Explicitly forbid every business lifecycle op before forwarding.
            class MetadataNode(GuestNode):
                def relay(node,identifier,data,origin):
                    if lower_request(data)[1] not in (1,2,3,0x76,0x77,0x78,0x79):
                        raise RuntimeError('CPU regression forbids business backing operations')
                    return super().relay(identifier,data,origin)
            node=MetadataNode(channel,identity,self.lower_socket,self.event,'simulated:cpu-startup',notify=self.notice,lower_timeout=.4)
            self.nodes[name]=node;node.thread.start();node.send('start')
            if not node.started.wait(3) or node.failure:raise RuntimeError('actual root start failed')
        self.startup=Startup(self.nodes,'simulated:cpu-startup',self.event,timeout=3)
        return self.startup
    def close(self):
        for node in self.nodes.values():node.stop.set()
        for node in self.nodes.values():node.thread.join(1)
        # Subreaper + exact owned descendant births. No scan/kill by name.
        def descendants(pid):
            try:ids=Path('/proc',str(pid),'task',str(pid),'children').read_text().split()
            except FileNotFoundError:return []
            result=[]
            for raw in ids:
                child=int(raw);result.extend(descendants(child));result.append((child,birth(child)))
            return result
        owned=dict(descendants(os.getpid()))
        for signo in (signal.SIGTERM,signal.SIGKILL):
            for pid,stamp in owned.items():
                if stamp is not None and birth(pid)==stamp:
                    try:os.kill(pid,signo)
                    except ProcessLookupError:pass
            until=time.monotonic()+1
            while time.monotonic()<until:
                try:
                    while os.waitpid(-1,os.WNOHANG)[0]:pass
                except ChildProcessError:break
                time.sleep(.02)
        remaining={pid:stamp for pid,stamp in owned.items() if stamp is not None and birth(pid)==stamp}
        self.event({'event':'fixture-owned-tree-closed','exact_births':owned,'remaining':remaining})
        for p,_ in self.children:
            # waitpid above may already have reaped Popen's direct child.
            p.wait(timeout=1)
            if p.stdin:p.stdin.close()
            if p.stdout:p.stdout.close()
        for f in self.raw:f.close()
        self.log.close()
        if remaining:raise RuntimeError('actual fixture tree not closed')


def rejected(action):
    try:action()
    except (RuntimeError,ValueError,KeyError):return
    raise AssertionError('expected actual gate rejection')


def packet(request,op,body,status=0,error=0):
    data=body.encode();crc=2166136261 if data else 0
    for byte in data:crc=((crc^byte)*16777619)&0xffffffff
    return LOWER.pack(0x4d535643,1,48,request,op,0,len(data),crc,status,error,0)+data


def run_case(h,kind):
    startup=h.start()
    if kind=='before-canary':
        rejected(lambda:startup.control.call('nodeA',{'op':'provider_register'}));return
    if kind=='worker-before-register':
        bootstrap_nodes(h.nodes,startup.control.realm,h.event,control=startup.control)
        rejected(lambda:startup.control.call('nodeA',{'op':'worker_start'}));return
    if kind=='generic-only':
        result=subprocess.run([str(h.args.controller),'ready','--config',str(h.config)],capture_output=True,timeout=2)
        h.event({'event':'generic-ready','exit':result.returncode,'stdout_hex':result.stdout.hex(),'stderr_hex':result.stderr.hex()})
        assert result.returncode==0
        bootstrap_nodes(h.nodes,startup.control.realm,h.event,control=startup.control)
        for name in h.nodes:startup.registrations[name]={}
        rejected(startup.check_once)
        assert not any(e.get('event')=='managed-admission-ready' for e in h.events);return
    if kind=='missing-node':
        bootstrap_nodes(h.nodes,startup.control.realm,h.event,control=startup.control)
        startup.registrations['nodeA']=startup.control.call('nodeA',{'op':'provider_register'})
        startup.registrations['nodeB']={}
        rejected(startup.check_once);return
    if kind=='expired-node':
        startup.bootstrap_and_register();time.sleep(1.15);rejected(startup.check_once);return
    startup.bootstrap_and_register();startup.start_workers()
    if kind in ('exit','wrong-inc','wrong-generation','burst'):
        deadline=time.monotonic()+8
        while not any(n.failure for n in h.nodes.values()) and time.monotonic()<deadline:time.sleep(.02)
        assert any(n.failure for n in h.nodes.values())
        if kind=='burst':
            assert all(n.log_bytes<=256*1024 for n in h.nodes.values())
            assert any(e.get('event')=='first-notice' for e in h.events)
        if kind=='exit':
            first=next(i for i,e in enumerate(h.events) if e['event']=='first-notice')
            diagnostic=next(i for i,e in enumerate(h.events) if e['event']=='worker-exit')
            assert first<diagnostic
        rejected(startup.check_once);return
    if kind=='no-refresh':
        rejected(startup.check_once)
        deadline=time.monotonic()+6
        while not any(n.failure for n in h.nodes.values()) and time.monotonic()<deadline:time.sleep(.02)
        assert any(n.failure for n in h.nodes.values());return
    proof=startup.wait_ready(3)
    assert all(p['managed_ready'] for p in proof.values())
    assert all(p['directory']['host_registry_data_plane_ready'] is False for p in proof.values())
    if kind=='stale':
        time.sleep(1.2);rejected(startup.check_once)
        assert any(n.failure for n in h.nodes.values());return
    if kind=='wrong-directory-inc':
        # Actual public metadata registration replaces one directory identity;
        # it does not create backing. The next real root SDK status must reject.
        identity=h.nodes['nodeB'].identity
        data=packet(999,0x76,'node_id=nodeB\nincarnation='+str(identity['incarnation']+1)+'\nreadiness_generation=1\ncapabilities=1\n')
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(1);s.connect(h.lower_socket);s.sendall(data);header=exact(s,48);exact(s,LOWER.unpack(header)[6])
        rejected(startup.check_once);return
    startup.stop_infrastructure()
    for node in h.nodes.values():node.prepare_close()
    for name,node in h.nodes.items():
        for stream in ('stdout','stderr'):
            raw=(h.output/(name+'.'+stream+'.log')).read_bytes()
            assert ('fixture '+stream).encode() in raw
    assert sum(e.get('event')=='worker-refresh' for e in h.events)>=2
    assert any(e.get('operation')==0x79 and e.get('status')==0 for e in h.events)


def transport_case(h,mode):
    listener=socket.socket(socket.AF_UNIX);listener.bind(h.lower_socket);listener.listen(1)
    def serve():
        with listener:
            connection,_=listener.accept()
            with connection:
                connection.settimeout(1);header=exact(connection,48);fields=LOWER.unpack(header);exact(connection,fields[6])
                response=packet(fields[3],fields[4],'status=ok\n')
                if mode=='eof':return
                if mode=='truncated':connection.sendall(response[:53]);return
                if mode=='timeout':connection.sendall(response[:5]);time.sleep(.6);return
                if mode=='checksum':response=response[:-1]+b'!'
                if mode=='identity':response=packet(fields[3]+1,fields[4],'status=ok\n')
                if mode in ('internal','store-uncertain'):
                    response=packet(fields[3],fields[4],'status=internal\nreason='+('store_write_uncertain' if mode=='store-uncertain' else 'data_plane_not_ready')+'\n',10,10)
                connection.sendall(response)
    server=threading.Thread(target=serve);server.start()
    startup=h.start();bootstrap_nodes(h.nodes,startup.control.realm,h.event,control=startup.control)
    rejected(lambda:startup.control.call('nodeA',{'op':'provider_register'}))
    server.join(2);assert not server.is_alive()
    bad=[e for e in h.events if e.get('event') in ('lower-control','lower-control-unconfirmed')]
    assert len(bad)==1 and bad[0]['outcome']=='Unknown' and bad[0]['side_effect_absence_inferred'] is False
    notice=next(i for i,e in enumerate(h.events) if e['event']=='first-notice')
    diagnostic=next(i for i,e in enumerate(h.events) if e.get('event') in ('lower-control','lower-control-unconfirmed','guest-bridge-failure'))
    assert notice<diagnostic


def main():
    p=argparse.ArgumentParser();p.add_argument('--agent',type=Path,required=True);p.add_argument('--worker',type=Path,required=True)
    p.add_argument('--controller',type=Path,required=True);p.add_argument('--scratch',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    if os.name!='posix' or 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('B admitted build only')
    if ctypes.CDLL(None).prctl(36,1,0,0,0):raise RuntimeError('fixture subreaper required')
    a.scratch.mkdir(exist_ok=False);a.output.mkdir(exist_ok=False)
    cases=['positive','before-canary','worker-before-register','generic-only','missing-node','expired-node',
           'exit','no-refresh','wrong-inc','wrong-generation','stale','wrong-directory-inc','burst']
    modes=['eof','truncated','timeout','checksum','identity','internal','store-uncertain']
    results=[]
    for index,kind in enumerate(cases+modes):
        h=Harness(a,index,kind if kind in ('exit','no-refresh','wrong-inc','wrong-generation','stale','burst') else 'normal',lower=kind not in modes)
        try:
            if kind in modes:transport_case(h,kind)
            else:run_case(h,kind)
            results.append({'case':kind,'pass':True,'scope':'CPU canary double; actual agent/SDK/process/socket/directory'})
        finally:h.close()
        (a.output/'results.json').write_text(json.dumps({'cases':results,'no_business_backing':True,'new_Guest':False},indent=2))
        print('actual CPU startup case PASS '+kind,flush=True)


if __name__=='__main__':main()
