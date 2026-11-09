#!/usr/bin/env python3
"""CPU A/B regression of real agent/SDK refresh under costly UART reads.

The legacy file is the exact f39 accepted agent, not a rewritten failure model.
Only this fixture preloads syscall cost; production leases/guards are unchanged.
No Guest, OBMM, business backing or production authority is opened.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import time

from startup_cpu_probe import Harness


class CostHarness(Harness):
    def spawn(self,argv,**kwargs):
        if Path(argv[0])==self.args.agent:
            kwargs['env']={**kwargs['env'],'LD_PRELOAD':str(self.args.cost),
                           'UFLOW_CPU_RX_COST_MARKER':str(self.path/'read-cost-enabled')}
        return super().spawn(argv,**kwargs)


def run(args,agent,index,legacy):
    args.agent=agent
    h=CostHarness(args,index)
    try:
        startup=h.start();startup.run()
        startup.control.timeout=3
        before=startup.control.call('nodeA',{'op':'worker_state'})
        assert before['worker_ready'] and before['lease_ms']==1000
        marker=h.path/'read-cost-enabled';marker.touch(exist_ok=False)
        # Legal flat V2 request just below the existing 1800-byte body bound.
        body={'op':'worker_state',**{'padding'+str(i):'a'*350 for i in range(4)}}
        encoded=json.dumps({'schema':2,**body},separators=(',',':')).encode()
        assert 1400<len(encoded)<=1800
        started=time.monotonic();failure=None;reply=None
        try:reply=startup.control.call('nodeA',body)
        except (RuntimeError,TimeoutError,OSError) as exc:failure=str(exc)
        elapsed=time.monotonic()-started
        if legacy:
            until=time.monotonic()+2
            while not h.nodes['nodeA'].failure and time.monotonic()<until:time.sleep(.01)
            raw=(h.output/'nodeA.serial.log').read_bytes()
            assert failure and reply is None
            assert b'UFPOOL_FATAL missing or expired worker refresh' in raw
            result={'case':'exact-f39-byte-RX','expected_guard_failure':True,'error':failure}
        else:
            assert failure is None and reply['worker_ready'] and elapsed<1
            time.sleep(1.2)
            after=startup.control.call('nodeA',body)
            assert after['worker_ready'] and after['worker_refresh_count']>=before['worker_refresh_count']+3
            assert not any(n.failure for n in h.nodes.values())
            # Multiple commands/worker wire replies share the buffered stream;
            # every completion still uses the real Control sequence and identity.
            for _ in range(4):
                assert startup.control.call('nodeA',body)['worker_ready']
            marker.unlink()
            startup.stop_infrastructure()
            for node in h.nodes.values():node.prepare_close()
            result={'case':'bounded-256-byte-RX','completed':True,'before':before,'after':after,
                    'normal_infrastructure_close':True}
        result.update({'pass':True,'body_bytes':len(encoded),'read_syscall_cost_us':3000,
                       'first_command_seconds':elapsed,'lease_ms_unchanged':1000})
        return result
    finally:h.close()


def main():
    p=argparse.ArgumentParser()
    for name in ('agent','legacy-agent','legacy-source','worker','controller','cost','scratch','output'):
        p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args()
    if os.name!='posix' or 'UFLOW_ENV_CLOSE_FD' not in os.environ:raise RuntimeError('admitted B build only')
    if ctypes.CDLL(None).prctl(36,1,0,0,0):raise RuntimeError('fixture subreaper required')
    a.scratch.mkdir(exist_ok=False);a.output.mkdir(exist_ok=False)
    current=a.agent
    results=[]
    for index,(agent,legacy) in enumerate(((a.legacy_agent,True),(current,False))):
        results.append(run(a,agent,index,legacy))
        receipt={'cases':results,'legacy_commit':'f39aa116ac5c3e244bafd873539e1c9ba0463196',
                 'legacy_source_sha256':hashlib.sha256(a.legacy_source.read_bytes()).hexdigest(),
                 'CPU_delay_model_only':True,'actual_agent_SDK_processes':True,
                 'production_guard_or_lease_changed':False,'business_backing':False,'new_Guest':False}
        (a.output/'results.json').write_text(json.dumps(receipt,indent=2))
        print('actual CPU serial RX case PASS '+results[-1]['case'],flush=True)


if __name__=='__main__':main()
