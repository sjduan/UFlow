"""One owned QEMU peer set and actual channels shared by admitted R and W."""
from pathlib import Path
from daemon_peer_auth import DaemonPeer
from managed_cache_control import check,PublicationBook
from managed_gate_v4 import require_runtime
from managed_interface_v2 import _serial_binding
from managed_write_interface_v1 import production_binding

_SEAL=object()
class _Routes:
    def __init__(self,owner,mode):self.owner,self.mode=owner,mode
    def __getitem__(self,key):
        pool,node=key;o=self.owner
        rows=[r for (p,_),r in o.publication._rows.items() if p==pool]
        check(not o.closed and len(rows)==1 and node in o.by_node,'unique actual native publication and open owning routes required')
        return {role:o.by_node[n][self.mode] for role,n in [('home',rows[0]['home']),('importer',node)]}

class _NativeRoutes:
    def __init__(self,admission,publication,channels,starts):
        self.admission=require_runtime(admission)
        check(type(publication) is PublicationBook,'shared actual publication book required')
        check(type(starts) is list and len(starts)==2 and {r['node'] for r in starts}=={'nodeA','nodeB'} and set(channels)=={'nodeA','nodeB'},'exact two actual owning QEMU starts/channels required')
        self.publication=publication;self.peers=[];self.by_node={};self.closed=False;self._seal=_SEAL
        ref=admission.anchors['code_and_data']['qemu_binary'];wrapper=admission.modules['wrapper']
        try:
            for record in starts:
                peer=DaemonPeer.capture(record['pid'],Path(ref['path']),ref['sha256']);self.peers.append(peer)
                check(peer._expected['birth']==record['birth'] and record['argv'][0]==ref['path'],'actual accepted QEMU birth/executable mismatch')
                channel=channels[record['node']]
                self.by_node[record['node']]={'r':_serial_binding(wrapper,channel,peer),'w':production_binding(wrapper,channel,peer,admission=admission)}
        except BaseException:
            self.close();raise
        self.read,self.write=_Routes(self,'r'),_Routes(self,'w')
    def close(self):
        # Closing authentication handles never acknowledges a native grant.
        if not self.closed:
            self.closed=True
            for peer in self.peers:peer.close()

def production_routes(admission,publication,channels,starts):return _NativeRoutes(admission,publication,channels,starts)
def require_routes(routes,admission,publication):
    require_runtime(admission)
    check(type(routes) is _NativeRoutes and routes._seal is _SEAL and not routes.closed and routes.admission is admission and routes.publication is publication,'actual same-admission R/W owning peer set required')
    return routes
