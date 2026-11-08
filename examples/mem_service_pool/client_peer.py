"""Independent host-side control client; all data access is inside its Guest child."""
import argparse
import json
import sys
from uflow.mem_service_pool_v2 import MemServicePoolClient, PoolV2Error


def main():
    p=argparse.ArgumentParser();p.add_argument('endpoint');p.add_argument('node');a=p.parse_args()
    client=None
    for line in sys.stdin:
        req=json.loads(line)
        try:
            op=req.pop('op')
            if op=='register':
                client=MemServicePoolClient(a.endpoint,req['pool'],a.node,req.get('access','rw'))
                result=client.binding
            elif client is None:
                raise ValueError('registration required')
            elif op=='allocate':result=client.allocate(**req)
            elif op=='acquire':result=client.acquire(**req)
            elif op=='execute':result=client.execute_pattern(**req)
            elif op=='release':result=client.release(**req)
            elif op=='close':result=client.close()
            elif op=='snapshot':result=client.snapshot()
            elif op=='raw':result=client._call(req['method'],**req['values'])
            else:raise ValueError('unknown control request')
            print(json.dumps({'status':'ok','result':result}),flush=True)
        except PoolV2Error as error:
            if getattr(error,'client',None) is not None:client=error.client
            print(json.dumps({'status':'error','class':type(error).__name__,'detail':str(error),
                              'operation_id':error.operation_id,'response':error.response}),flush=True)
    if client is not None and not client.closed:
        raise RuntimeError('control EOF with original mapping/holder responsibility retained')


if __name__=='__main__':main()
