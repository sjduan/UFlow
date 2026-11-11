import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'tools/phasec'))
import terminal_domain_adapter as adapter


class TerminalAdapter(unittest.TestCase):
    def test_duplicate_and_nonfinite_request_fail(self):
        for raw in [b'{"action":1,"action":2}',b'{"value":NaN}']:
            with self.assertRaises(ValueError):adapter.decode(raw)

    def test_changed_external_pins_fail_before_module_load(self):
        with patch.object(adapter,'load_modules',side_effect=AssertionError('must not execute')):
            with self.assertRaisesRegex(ValueError,'consumer pins drift'):
                adapter.main({'action':'verify-current','consumer_pins':'{}'})

    def test_all_four_modules_verified_before_first_exec(self):
        pins=json.loads((Path(__file__).parent/'terminal_domain/accepted-E-consumer-pins.json').read_bytes())
        with patch.object(adapter,'read',return_value=b'raise AssertionError("untrusted code must not execute")'):
            with self.assertRaisesRegex(ValueError,'source hash drift'):adapter.load_modules(pins)
        self.assertFalse(any(name in sys.modules for name in adapter.ORDER))

    def test_explicit_execution_path_rejects_before_any_evidence_read(self):
        with patch.object(adapter,'reference',side_effect=AssertionError('must not read')):
            with self.assertRaisesRegex(ValueError,'fixed resolved'):
                adapter.execution_context({'execution_manifest':{'path':'/tmp/force.json'}},{})

    def test_reference_external_digest_checked(self):
        with patch.object(adapter,'read',return_value=b'{}'):
            self.assertEqual(adapter.reference({'path':'input.json','bytes':2,'sha256':hashlib.sha256(b'{}').hexdigest()}),b'{}')
            with self.assertRaisesRegex(ValueError,'pin drift'):
                adapter.reference({'path':'input.json','bytes':2,'sha256':'0'*64})


if __name__=='__main__':unittest.main()
