#!/usr/bin/env python3
"""Offline, non-authoritative DDR calibration. Never enables broker ranking.

Matches ddr_cost.rs seconds/bytes model; fit only training sizes, evaluate every
held-out p50 against max(actual * .30, 5us). Unknown is None, never zero.
"""
from __future__ import annotations
import math
import statistics

TRAIN_SIZES = (4096, 1024 * 1024, 64 * 1024 * 1024)
HELD_OUT = 8 * 1024 * 1024
METRICS = ('map_setup_seconds', 'map_first_seconds', 'map_reuse_seconds',
           'copy_setup_seconds', 'copy_execute_seconds', 'copy_queue_seconds',
           'copy_first_seconds', 'copy_reuse_seconds', 'probe_plan_seconds',
           'explicit_validation_seconds', 'map_total_seconds', 'copy_total_seconds')


def percentile(values, fraction):
    values = sorted(values)
    position = (len(values) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    return values[low] + (values[high] - values[low]) * (position - low)


def linear(points):
    """Unweighted least squares of per-training-size p50: seconds = a + N/B."""
    xbar = statistics.mean(x for x, _ in points)
    ybar = statistics.mean(y for _, y in points)
    denominator = sum((x - xbar) ** 2 for x, _ in points)
    slope = sum((x - xbar) * (y - ybar) for x, y in points) / denominator
    intercept = ybar - slope * xbar
    if not math.isfinite(slope) or slope <= 0 or intercept < 0:
        return {'setup_seconds': None, 'bytes_per_second': None,
                'raw_intercept_seconds': intercept, 'raw_seconds_per_byte': slope,
                'reason': 'nonphysical fit; no clamping or fabricated zero'}
    return {'setup_seconds': intercept, 'bytes_per_second': 1 / slope}


def summarize(samples, metadata):
    groups = []
    for concurrency in (1, 2):
        for size in (*TRAIN_SIZES, HELD_OUT):
            rows = [r for r in samples if r['concurrency'] == concurrency and
                    r['bytes'] == size and not r['warmup']]
            if not rows:
                continue
            assert len(rows) >= 10 * concurrency
            groups.append({'concurrency': concurrency, 'bytes': size,
                           'role': 'held-out' if size == HELD_OUT else 'training',
                           'samples': len(rows), 'rounds': len({r['round'] for r in rows}),
                           'metrics': {key: {'p50': statistics.median(r[key] for r in rows),
                                             'p90': percentile([r[key] for r in rows], .9)}
                                       for key in METRICS}})
    profiles = []
    for concurrency in (1, 2):
        training = [g for g in groups if g['concurrency'] == concurrency and g['role'] == 'training']
        held = next((g for g in groups if g['concurrency'] == concurrency and g['role'] == 'held-out'), None)
        if len(training) != 3 or held is None:
            continue
        def med(key):
            return statistics.median(g['metrics'][key]['p50'] for g in training)
        def bandwidth(key):
            # One size-normalized per-size p50 per training group, no held-out leakage.
            return statistics.median(g['bytes'] / g['metrics'][key]['p50'] for g in training)
        execution_fit = linear([(g['bytes'], g['metrics']['copy_execute_seconds']['p50']) for g in training])
        parameters = {'map_setup_seconds': med('map_setup_seconds'),
                      'map_first_read_bps': bandwidth('map_first_seconds'),
                      'map_reuse_read_bps': bandwidth('map_reuse_seconds'),
                      'copy_setup_seconds': med('copy_setup_seconds'),
                      'copy_primitive_setup_seconds': execution_fit['setup_seconds'],
                      'copy_bps': execution_fit['bytes_per_second'],
                      'copy_queue_seconds': med('copy_queue_seconds'),
                      'copy_first_read_bps': bandwidth('copy_first_seconds'),
                      'copy_reuse_read_bps': bandwidth('copy_reuse_seconds')}
        size = held['bytes']
        predicted = {'map_total_seconds': parameters['map_setup_seconds'] +
                     size / parameters['map_first_read_bps'] + size / parameters['map_reuse_read_bps']}
        predicted['copy_total_seconds'] = None if parameters['copy_bps'] is None else (
            parameters['copy_setup_seconds'] + parameters['copy_primitive_setup_seconds'] +
            size / parameters['copy_bps'] + parameters['copy_queue_seconds'] +
            size / parameters['copy_first_read_bps'] + size / parameters['copy_reuse_read_bps'])
        errors = []
        for metric, estimate in predicted.items():
            actual = held['metrics'][metric]['p50']
            tolerance = max(actual * .30, 5e-6)
            error = None if estimate is None else abs(estimate - actual)
            errors.append({'metric': metric, 'actual_p50_seconds': actual, 'predicted_seconds': estimate,
                           'absolute_error_seconds': error, 'tolerance_seconds': tolerance,
                           'passed': error is not None and error <= tolerance})
        validated = all(e['passed'] for e in errors)
        profiles.append({'concurrency': concurrency, 'parameters': parameters,
                         'execution_fit': execution_fit, 'held_out': errors,
                         'ranking_validated': validated,
                         'automatic_ranking': 'disabled',
                         'reason': ('offline measurements only; not installed in broker' if validated else
                                    'held-out error/unknown: explicit semantic path only; auto refused')})
    return {'schema': 'uflow-ddr-cost-calibration-v1', 'metadata': metadata,
            'groups': groups, 'profiles': profiles,
            'automatic_ranking': 'disabled', 'public_cost_version': 'unknown',
            'decision': 'Do not compare map/copy until semantics permit both. Explicit read-only access maps; '
                        'independent destination uses verified copy; automatic mode rejected. Expired or '
                        'environment/concurrency/NUMA-mismatched offline profiles are not usable.'}
