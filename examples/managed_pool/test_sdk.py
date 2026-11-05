"""Portable SDK fixtures; these do not stand in for the real Linux pool tests."""
from dataclasses import asdict, replace
import json
import mmap
import os
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from uflow._managed_pool_wire import decode, fields, integer
from uflow.managed_pool import (
    ManagedPoolClient, ManagedPoolError, ManagedPoolUnknown, ManagedPoolView, PoolBlock, PoolLease,
)


def block():
    return PoolBlock('pool', 'epoch', 'provider', 1, 1, 0, 4*65536)


def lease(mode='w'):
    return PoolLease('pool', 'epoch', 'provider', 3, 'lease', 4, 'client', 1, 1, 0, 65536,
                     mode, 0 if mode == 'w' else 7, '' if mode == 'w' else 'sha256-content')


def fixture_client():
    client = ManagedPoolClient()
    client.endpoint, client.timeout = 'fixture-only', 1.0
    client._descriptor = {'pool_id': 'pool', 'pool_epoch': 'epoch', 'provider_session': 'provider',
                          'client_session': 'client', 'access_generation': 3}
    client._registered, client._stopping, client._unmapped = True, False, False
    client._children, client._child_operations, client._leases = {}, {}, {}
    client.view = SimpleNamespace(closed=False)
    client._ack_operation_id = None
    return client


class SchemaTests(unittest.TestCase):
    def test_duplicate_nested_unknown_and_non_object_rejected(self):
        for raw in (b'{"schema":1,"schema":1}', b'{"nested":{"a":1,"a":2}}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode(raw)
        for value in ({'schema': 1, 'raw_va': 123}, [], None):
            with self.assertRaises(ValueError):
                fields(value, ('schema',))

    def test_bool_negative_overflow_float_and_nonfinite_rejected(self):
        for raw in (b'{"n":-1}', b'{"n":18446744073709551616}', b'{"n":1.0}', b'{"n":NaN}', b'{"n":Infinity}'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                decode(raw)
        for value in (True, False, -1, 1 << 64, '1', 1.0):
            with self.subTest(value=value), self.assertRaises(ValueError):
                integer(value)

    def test_frame_limit_and_dataclass_unknown_fields(self):
        with self.assertRaises(ValueError):
            decode(b' ' * 65537)
        with self.assertRaises(ValueError):
            PoolBlock.from_dict({**asdict(block()), 'owner_va': 123})
        with self.assertRaises(ValueError):
            PoolLease.from_dict({**asdict(lease()), 'native_handle': 123})

    def test_no_zero_or_boolean_generations_or_overflow(self):
        for value in (0, True, 1 << 64):
            with self.assertRaises(ValueError):
                replace(block(), generation=value)
        with self.assertRaises(ValueError):
            replace(block(), offset=(1 << 64)-1)

    def test_read_requires_committed_version_and_write_cannot_claim_ready(self):
        lease('r')
        with self.assertRaises(ValueError):
            replace(lease('r'), content_version=0)
        with self.assertRaises(ValueError):
            replace(lease(), content_version=5)

    def test_fresh_normal_sdk_import_actively_forbids_tensor_and_provider_modules(self):
        code = '''import importlib.abc,json,sys
class Deny(importlib.abc.MetaPathFinder):
 def find_spec(self,name,path=None,target=None):
  if name.split('.')[0] in ('torch','torch_npu','pypto','simpler','_task_interface'):
   raise RuntimeError('forbidden '+name)
sys.meta_path.insert(0,Deny())
from uflow import ManagedPoolClient, PoolLease
print(json.dumps({'sdk':ManagedPoolClient.__module__,'tensor_provider_modules':[]}))
'''
        result = subprocess.run([sys.executable, '-c', code], capture_output=True, timeout=20, check=True)
        self.assertEqual(json.loads(result.stdout)['sdk'], 'uflow.managed_pool')


class ResultTests(unittest.TestCase):
    def test_foreign_allocation_or_response_lease_is_unknown_not_usable(self):
        client = fixture_client()
        client._call = Mock(return_value=asdict(replace(block(), pool_id='different')))
        with self.assertRaises(ManagedPoolUnknown) as caught:
            client.allocate_range(65536, operation_id='allocate-op')
        self.assertEqual(caught.exception.operation_id, 'allocate-op')
        for wrong in (replace(lease(), offset=65536), replace(lease(), block_id=9), replace(lease(), generation=2)):
            client._call = Mock(return_value=asdict(wrong))
            with self.assertRaises(ManagedPoolUnknown):
                client.acquire_range(block(), 'w', 1, nbytes=65536, operation_id='acquire-op')
        self.assertEqual(client._leases, {})

    def test_bad_commit_ack_keeps_original_obligation(self):
        client, held = fixture_client(), lease()
        client._leases[held.lease_id] = 'Active'
        for response in ({}, {'committed': True, 'content_version': True, 'lease_id': 'lease', 'content_id': 'hash'},
                         {'committed': False, 'content_version': 1, 'lease_id': 'lease', 'content_id': 'hash'}):
            client._call = Mock(return_value=response)
            with self.assertRaises(ManagedPoolUnknown) as caught:
                client.commit_write(held, 'hash', 65536, 'commit-op')
            self.assertEqual(caught.exception.operation_id, 'commit-op')
            self.assertEqual(client._leases[held.lease_id], 'Active')

    def test_bad_release_ack_or_unknown_lease_never_frees_local_responsibility(self):
        client, held = fixture_client(), lease('r')
        client._leases[held.lease_id] = 'Active'
        client._call = Mock(return_value={'released': False, 'lease_id': 'lease'})
        with self.assertRaises(ManagedPoolUnknown):
            client.release(held, 'release-op')
        self.assertEqual(client._leases[held.lease_id], 'Active')
        client._leases[held.lease_id] = 'Unknown'
        client._call.reset_mock()
        with self.assertRaises(RuntimeError):
            client.release(held)
        client._call.assert_not_called()

    def test_registration_idempotency_reuses_view_and_rejects_changed_arguments(self):
        client = fixture_client()
        client._requested_registration = ('pool', 'epoch', 'rw')
        key = (os.getpid(), 'fixture-only', 'registration-op')
        ManagedPoolClient._registrations[key] = client
        with patch('uflow.managed_pool.describe_pool') as describe:
            self.assertIs(ManagedPoolClient.register('fixture-only', 'pool', 'epoch', operation_id='registration-op'), client)
            with self.assertRaises(ManagedPoolError):
                ManagedPoolClient.register('fixture-only', 'pool', 'different', operation_id='registration-op')
            describe.assert_not_called()

    def test_ack_unmapped_refuses_live_view_child_or_no_explicit_stop(self):
        client = fixture_client()
        client._call = Mock()
        with self.assertRaises(RuntimeError):
            client.ack_unmapped()
        client._stopping = True
        with self.assertRaises(RuntimeError):
            client.ack_unmapped(native_drained=True)
        client.view.closed = True
        client._children[123] = {'fixture': True}
        with self.assertRaises(RuntimeError):
            client.ack_unmapped(native_drained=True)
        client._call.assert_not_called()

    def test_child_pid_observation_alone_is_not_a_native_stop_proof(self):
        client = fixture_client()
        client._call = Mock()
        with self.assertRaises(ValueError):
            client.detach_child(123, {'fixture': True}, native_drained=False)
        client._call.assert_not_called()

    def test_lost_commit_query_confirms_same_operation_without_faking_native_completion(self):
        client = fixture_client()
        client._leases['lease'] = 'Active'
        response = {'state': 'Completed', 'response': {'schema': 1, 'status': 'ok', 'operation_id': 'lost',
                    'result': {'committed': True, 'lease_id': 'lease', 'content_version': 5, 'content_id': 'hash'}}}
        with patch('uflow.managed_pool._exchange', return_value=(response, [])):
            self.assertEqual(client.query_operation('lost'), response)
        self.assertEqual(client._leases['lease'], 'Closed')

    def test_stale_or_malformed_query_does_not_confirm_a_release(self):
        client = fixture_client()
        client._leases['lease'] = 'Active'
        for response in (
            {'state': 'Completed', 'response': {'status': 'ok', 'result': {'released': True, 'lease_id': 'lease'}}},
            {'state': 'Completed', 'response': {'schema': 1, 'operation_id': 'different', 'status': 'ok',
                                              'result': {'released': True, 'lease_id': 'lease'}}},
            {'state': 'Pending', 'response': {'schema': 1, 'operation_id': 'lost', 'status': 'ok',
                                            'result': {'released': True, 'lease_id': 'lease'}}},
        ):
            with patch('uflow.managed_pool._exchange', return_value=(response, [])):
                with self.assertRaises(ManagedPoolUnknown):
                    client.query_operation('lost')
            self.assertEqual(client._leases['lease'], 'Active')

    def test_integer_true_in_queried_terminal_ack_is_not_a_release_proof(self):
        client = fixture_client()
        client._leases['lease'] = 'Active'
        response = {'state': 'Completed', 'response': {'schema': 1, 'operation_id': 'lost', 'status': 'ok',
                                                      'result': {'released': 1, 'lease_id': 'lease'}}}
        with patch('uflow.managed_pool._exchange', return_value=(response, [])):
            with self.assertRaises(ManagedPoolUnknown):
                client.query_operation('lost')
        self.assertEqual(client._leases['lease'], 'Active')

    def test_lost_child_attach_and_detach_acks_retain_holder_until_exact_query(self):
        client = fixture_client()
        client._call = Mock(side_effect=ManagedPoolUnknown('lost', operation_id='attach-op', client=client))
        with self.assertRaises(ManagedPoolUnknown):
            client.attach_child(123, {'fixture': True}, operation_id='attach-op')
        self.assertIn(123, client._children)
        reply = {'state': 'Completed', 'response': {'schema': 1, 'operation_id': 'attach-op', 'status': 'ok',
                                                 'result': {'attached': True}}}
        with patch('uflow.managed_pool._exchange', return_value=(reply, [])):
            client.query_operation('attach-op')
        client._call = Mock(side_effect=ManagedPoolUnknown('lost', operation_id='detach-op', client=client))
        with self.assertRaises(ManagedPoolUnknown):
            client.detach_child(123, {'fixture': True}, native_drained=True, operation_id='detach-op')
        self.assertIn(123, client._children)
        reply['response'] = {'schema': 1, 'operation_id': 'detach-op', 'status': 'ok',
                             'result': {'detached': True, 'native_drain_attested': True}}
        with patch('uflow.managed_pool._exchange', return_value=(reply, [])):
            client.query_operation('detach-op')
        self.assertNotIn(123, client._children)

    def test_unconfirmed_provider_release_cannot_become_success(self):
        from uflow.managed_pool import destroy_pool
        descriptor = {'pool_id': 'pool', 'pool_epoch': 'epoch', 'provider_session': 'provider',
                      'canonical': {'dev': 1, 'inode': 2}}
        for result in ({}, {'released': True, 'daemon_fd_closed': True,
                           'provider_release': {'closed': True, 'unlinked': False, 'name_absent': True, 'dev': 1, 'inode': 2}}):
            with patch('uflow.managed_pool._exchange', return_value=(result, [])):
                with self.assertRaises(ManagedPoolUnknown) as error:
                    destroy_pool('fixture-only', descriptor, admin_token='fixture-capability', operation_id='destroy-op')
            self.assertEqual(error.exception.operation_id, 'destroy-op')


class ActualCloseTests(unittest.TestCase):
    def make_view(self, descriptor):
        client = fixture_client()
        client.report_unknown = Mock(return_value={'unknown_retained': True})
        view = ManagedPoolView.__new__(ManagedPoolView)
        view.client, view.descriptor = client, {'fixture': 'anonymous local mmap, not real pool registration'}
        view._fd = os.dup(descriptor)
        view._mapping = mmap.mmap(-1, 65536)
        view._map_closed, view._fd_closed, view.close_error = False, False, None
        client.view = view
        return client, view

    def test_real_exported_mmap_close_failure_retains_fd_until_explicit_retry(self):
        with tempfile.TemporaryFile() as file:
            client, view = self.make_view(file.fileno())
            exported = memoryview(view.buffer)
            with self.assertRaises(BufferError):
                view.close()
            self.assertFalse(view.closed)
            os.fstat(view.fileno())
            client.report_unknown.assert_called_once()
            exported.release()
            view.close()
            self.assertTrue(view.closed)
            with self.assertRaises(OSError):
                os.fstat(view._fd)

    def test_live_child_guard_does_not_unmap_or_silently_clear_registration(self):
        with tempfile.TemporaryFile() as file:
            client, view = self.make_view(file.fileno())
            client._children[123] = {'fixture': 'held child'}
            with self.assertRaises(RuntimeError):
                view.close()
            self.assertFalse(view._mapping.closed)
            client.report_unknown.assert_not_called()
            client._children.clear()
            view.close()


if __name__ == '__main__':
    unittest.main()
