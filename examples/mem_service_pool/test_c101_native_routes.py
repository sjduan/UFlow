"""Explicit HOST admission fixture, actual Linux peers and accepted E W API.

No original context, production E0 admission, QEMU payload or Guest is used.
"""
import hashlib,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
import managed_gate_v4 as gate
from managed_native_routes_v4 import production_routes,require_routes
from managed_cache_control import production_controller as read_controller
from managed_write_control import production_controller as write_controller
from managed_write_interface_v1 import CODEC_SHA,SNAPSHOT
import test_managed_write_connection as connection

@unittest.skipUnless(sys.platform=='linux','actual owned Linux endpoint peers')
class NativeRoutesTests(unittest.TestCase):
    def test_actual_production_factories_share_owned_peers_channels_and_publication(self):
        f=connection.WriteWrapperConnectionTests();f.setUp();routes=None
        try:
            bindings=[f.binding('home'),f.binding('importer')]
            channels={};starts=[]
            for node,peer,qmp,binding in zip(('nodeA','nodeB'),f.peers,f.qmps,bindings):
                channel=f.wrapper.ManagedSerialChannel.__new__(f.wrapper.ManagedSerialChannel);channel._managed_qmp=qmp;channels[node]=channel
                starts.append({'node':node,'pid':binding['pid'],'birth':peer._expected['birth'],'argv':[str(f.exe)]})
            path=SNAPSHOT/'uflow_managed_write_codec_v1.py'
            anchors={'code_and_data':{'qemu_binary':{'path':str(f.exe),'sha256':f.pin},'write_codec':{'path':str(path),'bytes':path.stat().st_size,'sha256':CODEC_SHA}}}
            # Private, labelled HOST fixture only. The actual production entry
            # point creates its seal only after real manifest/E0/source checks.
            admission=gate._RuntimeAdmission({'HOST_fixture_only':True},anchors,{'wrapper':f.wrapper},{},gate._RUNTIME)
            routes=production_routes(admission,f.f.f.book,channels,starts)
            r=read_controller(f.f.f.book,admission=admission,native_routes=routes)
            # Codec source is a task-local pinned fixture; production ap_bound
            # constrains it to the real immutable remote task root.
            from unittest.mock import patch
            import accepted_producers
            with patch.object(accepted_producers,'ap_bound',return_value=path.read_bytes()):
                w=write_controller(f.f.f.book,admission=admission,native_routes=routes)
            self.assertIs(r.publication,w.publication);self.assertIs(r.bindings.owner,w.bindings.owner)
            self.assertEqual(len(routes.peers),2);self.assertFalse(r.owned_peers)
            f.execute(w);self.assertEqual([row['operation'] for row in w.audit],['probe','probe','begin','flush','end'])
            r.close();w.close();routes.close()
            with self.assertRaises(ValueError):require_routes(routes,admission,f.f.f.book)
            for qmp in f.qmps:qmp.assert_no_managed_grants()
        finally:
            if routes is not None:routes.close()
            f.tearDown()

if __name__=='__main__':unittest.main()
