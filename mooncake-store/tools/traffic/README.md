# Trace load and regression gate

`traffic_load` replays a recorded Master RPC session. `--rate` is the base
request rate; `--amplify` multiplies it without changing the recorded request
mix:

```bash
traffic_load --dir ./session --target 127.0.0.1:50051 \
  --allow-all --rate 2000 --amplify 2 --duration 10 --warmup 2
```

An amplification of `1` is the baseline, `2` sends the same request sequence at
twice the rate, and `3` at three times. The report records `base_rate`,
`amplify`, and the effective `rate_target`, so a gate cannot compare reports
from different amplification levels.

`traffic_load` keeps one request in flight per connection and wraps the
recorded sequence until `--duration` ends. Use a longer duration to measure a
sustained run; use `--amplify` to measure peak-load behavior. `--saturate`
ignores pacing and measures the ceiling for a given concurrency.

`traffic_gate` compares a baseline report with a candidate report from the same
plan:

```bash
traffic_gate --baseline 1x.json --current 1x-new.json \
  --max-rps-drop-pct 5 --max-p99-increase-pct 10
```

Exit code `0` passes, `1` fails a threshold, and `2` means the reports or
arguments are invalid. The plan check includes rate, amplification, connection
count, duration, and warmup.

The recorder captures Master metadata RPC only. A store data-plane trace needs
a client-side recording point; `memory_segment_bench` remains the independent
100M-key Memory Segment benchmark.
