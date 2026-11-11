"""Small diagnostic prerequisites; full original matrix remains required."""
import json
import time


def worker_serial_preflight(startup,readiness,event,*,clock=time.monotonic_ns):
    body={'op':'worker_state',**{'padding'+str(i):'a'*230 for i in range(6)}}
    encoded=json.dumps({'schema':2,**body},separators=(',',':')).encode()
    if len(encoded)!=1496:raise RuntimeError('fixed legal serial preflight body required')
    proofs={}
    for name in ('nodeA','nodeB'):
        baseline=readiness[name]['worker'];begin=clock()
        event({'event':'stage-worker-serial-preflight-start','node':name,'body_bytes':len(encoded),
               'start_host_ns':begin,'clock_domain':'B-host','Guest_acceptance_proved':False})
        receipt=startup.control.call(name,body);end=clock()
        lease=receipt.get('lease_ms',0)
        if (receipt.get('schema')!=2 or receipt.get('phase')!='completed' or receipt.get('worker_alive') is not True or
                receipt.get('worker_ready') is not True or receipt.get('worker_pid')!=baseline['worker_pid'] or
                receipt.get('worker_birth')!=baseline['worker_birth'] or receipt.get('lease_ms')!=baseline['lease_ms'] or
                receipt.get('worker_refresh_count',0)<baseline['worker_refresh_count'] or receipt.get('worker_refresh_count',0)<1 or
                not 0<=receipt.get('refresh_age_ms',-1)<lease//2 or not 0<=end-begin<lease*500000):
            raise RuntimeError('actual large serial completion/worker identity/freshness/Host RTT failed before business')
        proofs[name]={'receipt':receipt,'start_host_ns':begin,'end_host_ns':end,'body_bytes':len(encoded)}
        event({'event':'stage-worker-serial-preflight-completed','node':name,**proofs[name],
               'clock_domain':'B-host','Guest_acceptance_proved':True,'pre_send_not_used_as_acceptance':True})
    return proofs
