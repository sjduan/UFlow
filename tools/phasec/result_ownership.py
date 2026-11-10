"""Source-bound actual monitor-result observation; never invent history.

This C observation is not an E metadata permit. The next accepted E interface
must bind its actual tagged reference; old history-only API stays unchanged.
"""
from pathlib import Path
from accepted_producers import AP_ROOT,ap_bound,ap_decode,ap_read,ap_require
import hashlib
SUPERVISOR_PINS={'task_supervisor.py':'7b23344b2cd9c51504e2389682f31b6dfa2e9567ed5a8bfda49dfb73ddf70827',
    'task_lifecycle.py':'9370260295844d433ac9273a255304b6b5e66d52c26b58cde758460dd594ad8c',
    'task_resources.py':'bdc057af7a5790c7ab181289072326b7a81fc397d6d08c6a6b5e08066e01f607'}

def validate_observation(top,monitor,owner,starts):
    ap_require(type(top) is dict and type(monitor) is dict and type(top.get('processes')) is dict,'actual E0 result objects required')
    processes=top['processes']
    ap_require(set(processes)==set(monitor)|{'supervisor_actual_exit_code'} and all(processes[k]==v for k,v in monitor.items()),'top-level and actual monitor result disagree')
    ap_require(type(processes['supervisor_actual_exit_code']) is int and type(monitor['actual_task_exit_code']) is int and
        monitor['process_tree_empty'] is True and monitor['task_resources_closed'] is True and
        monitor['remaining']=={} and monitor['evidence_errors']==[] and monitor['isolation_error'] is None,'actual monitor physical closure required')
    births=monitor['owned_births'];ap_require(type(births) is dict and 0<len(births)<=4096,'bounded actual observed owned births required')
    observed=set()
    for pid,birth in births.items():
        ap_require(type(pid) is str and pid.isdecimal() and int(pid)>0 and type(birth) is int and birth>0,'typed actual PID/birth observation required')
        observed.add((int(pid),birth))
    ap_require(type(starts) is list and len(starts)==2 and {s['node'] for s in starts}=={'nodeA','nodeB'} and
        len({(s['pid'],s['birth']) for s in starts})==2,'two distinct actual QEMU observations required')
    required=[owner,*starts]
    for row in required:
        ap_require(type(row['pid']) is int and (row['pid'],row['birth']) in observed,'capture owner/QEMU outside real supervisor observations')
    for key in ['supervisor','task']:
        ap_require(type(monitor[key+'_pid']) is int and type(monitor[key+'_birth']) is int and monitor[key+'_birth']>0,'actual supervisor/task birth required')
        observed.add((monitor[key+'_pid'],monitor[key+'_birth']))
    return {'observed_births':sorted(observed),'actual_task_exit_code':monitor['actual_task_exit_code'],
        'supervisor_actual_exit_code':processes['supervisor_actual_exit_code'],'physical_closure':True,
        'ownership_history_reconstructed':False,'SDK_ACK_or_normal_release_inferred':False}

def reference(path):
    raw=ap_read(path,16<<20);return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}

def collect(run):
    run=Path(run);ap_require(run.parent==AP_ROOT/'runs' and run.resolve()==run,'exact task run required')
    sources={}
    for name,pin in SUPERVISOR_PINS.items():
        row=reference(AP_ROOT/'env/supervisor'/name);ap_require(row['sha256']==pin,'frozen actual supervisor source drift');ap_bound(row);sources[name]=row
    refs={key:reference(run/name) for key,name in {'result':'result.json','monitor_result':'monitor/result.json','capture':'own-qemu-metadata.json','starts':'guest-starts.json'}.items()}
    capture=ap_decode(ap_bound(refs['capture']));starts=ap_decode(ap_bound(refs['starts']))
    value=validate_observation(ap_decode(ap_bound(refs['result'])),ap_decode(ap_bound(refs['monitor_result'])),capture['owner'],starts)
    return {'schema':1,'kind':'CActualMonitorResultOwnershipObservation','format':'captured-monitor-result-v1',
        'supervisor_sources':sources,'artifacts':refs,**value,'E_registration_permit':False,'current_terminal_permit':False}
