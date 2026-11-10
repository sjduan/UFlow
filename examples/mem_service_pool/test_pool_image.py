"""HOST newc parser/overlay safety; no filesystem extraction or Guest."""
import stat,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parents[2]/'tools/phasec'))
from pool_image import parse,record,overlay,inventory,REPLACEMENTS
def archive(rows):return b''.join(record(n,b,i+1) for i,(n,b) in enumerate(rows))+record('TRAILER!!!',b'',999)
class ImageTests(unittest.TestCase):
    def test_exact_overlay_preserves_all_unrelated_records(self):
        base=archive([('init',b'fixed init'),('bin/run_app',b'old runner'),('lib/sdk',b'fixed SDK')]);new={n:n.encode() for n in REPLACEMENTS};out=overlay(base,new)
        before={e.name:e for e in parse(base)};after={e.name:e for e in parse(out)}
        for n in ['init','lib/sdk']:self.assertEqual(before[n].record,after[n].record)
        for n,data in new.items():self.assertEqual(after[n].data,data);self.assertEqual(after[n].fields[1],stat.S_IFREG|0o755)
        self.assertEqual(set(inventory(out)),set(before)|REPLACEMENTS)
    def test_absolute_parent_duplicate_and_truncation_rejected(self):
        for raw in [archive([('/outside',b'x')]),archive([('../outside',b'x')]),archive([('a',b'x'),('a',b'y')]),archive([('a',b'x')])[:-5],archive([('a',b'x')])+b'payload']:
            with self.assertRaises(ValueError):parse(raw)
    def test_replace_symlink_or_alias_and_arbitrary_addition_rejected(self):
        original=bytearray(record('bin/run_app',b'outside',1));original[14:22]=f'{stat.S_IFLNK|0o777:08x}'.encode();base=bytes(original)+record('TRAILER!!!',b'',2)
        with self.assertRaisesRegex(ValueError,'symlink'):overlay(base,{n:b'x' for n in REPLACEMENTS})
        with self.assertRaises(ValueError):overlay(archive([('init',b'x')]),{'arbitrary':b'x'})
if __name__=='__main__':unittest.main()
