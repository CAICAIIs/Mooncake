# `memory_segment_bench`

`memory_segment_bench` is a standalone Store data-plane benchmark for one
Memory Segment. It does not use traffic recording or replay. The default
workload fills 100 million keys and then reads them back.

The benchmark is meant to answer one question: can one Store client fill and
serve a large Memory Segment at a useful rate, and where does latency go when
the key count grows?

## Workload

1. The provider client mounts one or more segments.
2. Fill phase: every key is written once with `BatchPut`; keys are partitioned
   across `--fill-threads`.
3. Read phase: the consumer client mounts a small read segment and reads the
   keys with `BatchGet`; `--read-threads` workers read random batches, or
   consecutive keys with `--sequential-read`.
4. The JSON report contains fill and read throughput, latency percentiles,
   payload size, segment count, and error counts.

The default payload is 512 B. The Store allocates about 1 KiB for a 512 B
object, so 100 million keys need about 100 GiB of segment capacity. The tool
mounts two 64 GiB segments by default for this workload. Use `--value-bytes` to
model a different object size.

A model KV page is much larger than 512 B. For example, a Qwen3-8B BF16 KV page
of 64 tokens is about 9 MiB, so 100 million such pages do not fit in one 1 TiB
segment. Use the model page size with a key count that fits, or use the 512 B
workload to measure the 100-million-key metadata and segment scale. The
two measurements answer different questions and should not be compared
directly.

## Build

```bash
cmake --build build --target memory_segment_bench -j"$(nproc)"
```

## Run

Start one Master without HA, then run the benchmark against it:

```bash
./build/mooncake-store/src/mooncake_master \
  --port=50051 \
  --rpc_thread_num=16 \
  --memory_allocator=offset

./build/mooncake-store/benchmarks/memory_segment_bench \
  --master 127.0.0.1:50051 \
  --keys 100000000 \
  --value-bytes 512 \
  --batch-size 64 \
  --fill-threads 8 \
  --read-threads 16
```

The benchmark mounts 64 GiB per segment for payloads above 8 GiB, and uses two
segments for payloads above 32 GiB. Pass `--segment-bytes` and `--segments`
explicitly when the Master has a different capacity budget. Each segment must
not exceed the Store's 1 TiB per-segment limit.

The Master evicts at 90% usage by default. That silently removes objects before
the read phase and makes the benchmark look like it has missing keys. Disable
eviction for this workload, or leave enough headroom:

```bash
--eviction_high_watermark_ratio=1.0 --allow_evict_soft_pinned_objects=false
```

For a smaller smoke run:

```bash
./build/mooncake-store/benchmarks/memory_segment_bench \
  --keys 1000000 \
  --value-bytes 512 \
  --batch-size 64 \
  --fill-threads 8 \
  --read-threads 8
```

For a model-shaped smoke run with a Qwen3-8B 64-token KV page:

```bash
./build/mooncake-store/benchmarks/memory_segment_bench \
  --keys 5000 \
  --value-bytes 9437184 \
  --batch-size 4 \
  --fill-threads 4 \
  --read-threads 4
```

## Output

The tool writes one JSON object to stdout. Important fields:

- `keys`, `value_bytes`, `dataset_gib`, `segments`, `segment_bytes`;
- `fill_completed`, `fill_errors`, `fill_seconds`, `fill_rps`, `fill_gib_s`;
- `read_completed`, `read_errors`, `read_seconds`, `read_rps`, `read_gib_s`;
- `read_us.p50`, `read_us.p90`, `read_us.p99`, `read_us.p999`;
- `random_read`, `seed`, `key_prefix`.

The process exits 0 only when both phases have zero errors. A failed mount,
failed allocation, or a failed batch exits non-zero.

## Scope

This is a single-process benchmark that mounts several provider segments. It
does not cover HA, eviction, offload, or model serving. Those need separate
workloads. The benchmark is deliberately independent of the traffic recorder so
it can be used as a release gate without enabling recording.
