# PhaseE-08 Low-Overhead Direct Executor

device: 7

mode: auto

repeats: 3

## Direct executor stats

- direct_d2h_busy_lanes: 0
- direct_d2h_lane_count: 1
- direct_d2h_max_lanes: 1
- direct_event_create_count: 2
- direct_event_reuse_count: 26
- direct_h2d_busy_lanes: 0
- direct_h2d_lane_count: 1
- direct_h2d_max_lanes: 1
- direct_idle_reaped_lanes: 0
- direct_idle_ttl_ms: 30000
- direct_lane_acquires: 28
- direct_lane_count: 2
- direct_lane_wait_count: 22
- direct_lane_wait_us_total: 1933.453
- direct_stream_create_count: 2
- direct_transfer_completed_jobs: 28
- direct_transfer_queue_depth: 0
- direct_transfer_queue_depth_high_watermark: 1
- direct_transfer_queue_wait_count: 28
- direct_transfer_queue_wait_us_total: 347.112
- direct_transfer_submitted_jobs: 28
- direct_transfer_worker_execute_us_total: 488471.346
- direct_transfer_workers: 1

## Result rows

- h2d 64MiB iter=0 channel=42.987GiB/s event=23.920GiB/s stream_create=1 event_reuse=0 queue_wait_us=41.950
- h2d 64MiB iter=1 channel=47.847GiB/s event=46.769GiB/s stream_create=0 event_reuse=1 queue_wait_us=6.270
- h2d 64MiB iter=2 channel=48.163GiB/s event=47.023GiB/s stream_create=0 event_reuse=2 queue_wait_us=6.850
- d2h 64MiB iter=0 channel=34.421GiB/s event=22.658GiB/s stream_create=1 event_reuse=0 queue_wait_us=8.050
- d2h 64MiB iter=1 channel=36.240GiB/s event=35.662GiB/s stream_create=0 event_reuse=1 queue_wait_us=5.350
- d2h 64MiB iter=2 channel=35.991GiB/s event=35.433GiB/s stream_create=0 event_reuse=2 queue_wait_us=5.020
- h2d 256MiB iter=0 channel=53.355GiB/s event=52.603GiB/s stream_create=0 event_reuse=4 queue_wait_us=15.140
- h2d 256MiB iter=1 channel=54.333GiB/s event=53.948GiB/s stream_create=0 event_reuse=5 queue_wait_us=6.120
- h2d 256MiB iter=2 channel=54.233GiB/s event=53.858GiB/s stream_create=0 event_reuse=6 queue_wait_us=5.420
- d2h 256MiB iter=0 channel=37.833GiB/s event=37.556GiB/s stream_create=0 event_reuse=3 queue_wait_us=8.780
- d2h 256MiB iter=1 channel=39.595GiB/s event=39.359GiB/s stream_create=0 event_reuse=4 queue_wait_us=6.600
- d2h 256MiB iter=2 channel=39.667GiB/s event=39.483GiB/s stream_create=0 event_reuse=5 queue_wait_us=6.120
- h2d 1GiB iter=0 channel=56.059GiB/s event=55.799GiB/s stream_create=0 event_reuse=8 queue_wait_us=14.430
- h2d 1GiB iter=1 channel=56.512GiB/s event=56.334GiB/s stream_create=0 event_reuse=9 queue_wait_us=16.000
- h2d 1GiB iter=2 channel=56.604GiB/s event=56.453GiB/s stream_create=0 event_reuse=10 queue_wait_us=13.530
- d2h 1GiB iter=0 channel=40.221GiB/s event=40.128GiB/s stream_create=0 event_reuse=6 queue_wait_us=15.320
- d2h 1GiB iter=1 channel=40.563GiB/s event=40.475GiB/s stream_create=0 event_reuse=7 queue_wait_us=13.380
- d2h 1GiB iter=2 channel=40.575GiB/s event=40.495GiB/s stream_create=0 event_reuse=8 queue_wait_us=11.531
- h2d 2GiB iter=0 channel=53.471GiB/s event=53.390GiB/s stream_create=0 event_reuse=12 queue_wait_us=16.310
- h2d 2GiB iter=1 channel=57.012GiB/s event=56.922GiB/s stream_create=0 event_reuse=13 queue_wait_us=13.590
- h2d 2GiB iter=2 channel=57.021GiB/s event=56.937GiB/s stream_create=0 event_reuse=14 queue_wait_us=12.131
- d2h 2GiB iter=0 channel=40.578GiB/s event=40.532GiB/s stream_create=0 event_reuse=9 queue_wait_us=16.290
- d2h 2GiB iter=1 channel=40.733GiB/s event=40.687GiB/s stream_create=0 event_reuse=10 queue_wait_us=14.820
- d2h 2GiB iter=2 channel=40.744GiB/s event=40.707GiB/s stream_create=0 event_reuse=11 queue_wait_us=12.020
