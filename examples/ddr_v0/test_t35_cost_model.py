import unittest
from t35_cost_model import TRAIN_SIZES, HELD_OUT, linear, percentile, summarize


class CostTests(unittest.TestCase):
    def test_nonphysical_fit_is_unknown_not_zero(self):
        self.assertIsNone(linear([(1, 3), (2, 2), (3, 1)])['bytes_per_second'])
        self.assertIsNone(linear([(1, 1), (2, 3), (3, 5)])['setup_seconds'])

    def test_percentiles_are_explicit_linear_interpolation(self):
        self.assertEqual(percentile([1, 2, 3, 4], .5), 2.5)
        self.assertAlmostEqual(percentile([1, 2, 3, 4], .9), 3.7)

    def test_held_out_failure_does_not_enable_ranking_or_change_fit(self):
        # Arithmetic fixtures ONLY, never hardware calibration evidence.
        rows = []
        for size in (*TRAIN_SIZES, HELD_OUT):
            for i in range(10):
                row = {'bytes': size, 'concurrency': 1, 'round': i, 'warmup': False,
                       'map_setup_seconds': .001, 'map_first_seconds': size / 1e9,
                       'map_reuse_seconds': size / 2e9, 'copy_setup_seconds': .002,
                       'copy_execute_seconds': .0001 + size / 1e9, 'copy_queue_seconds': .00001,
                       'copy_first_seconds': size / 1e9, 'copy_reuse_seconds': size / 2e9,
                       'probe_plan_seconds': .01, 'explicit_validation_seconds': .01,
                       'map_total_seconds': .001 + size / 1e9 + size / 2e9,
                       'copy_total_seconds': .00211 + size / 1e9 + size / 1e9 + size / 2e9}
                rows.append(row)
        good = summarize(rows, {})
        self.assertTrue(good['profiles'][0]['ranking_validated'])
        self.assertEqual(good['automatic_ranking'], 'disabled')
        for row in rows:
            if row['bytes'] == HELD_OUT:
                row['copy_total_seconds'] *= 10
        bad = summarize(rows, {})
        self.assertFalse(bad['profiles'][0]['ranking_validated'])
        self.assertEqual(good['profiles'][0]['parameters'], bad['profiles'][0]['parameters'])
        self.assertEqual(bad['profiles'][0]['automatic_ranking'], 'disabled')

    def test_warmups_are_not_training_or_validation(self):
        self.assertEqual(summarize([{'concurrency': 1, 'bytes': 4096, 'warmup': True}], {})['profiles'], [])


if __name__ == '__main__':
    unittest.main()
