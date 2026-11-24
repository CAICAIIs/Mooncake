# SGLang HiCache Master Benchmark

`master_bench --workload=sglang_hicache` measures the Master metadata path used
by SGLang HiCache. It is independent of the traffic recorder and replay tools.

The benchmark replays one multi-turn conversation workload per SGLang node and
calls the same batch APIs SGLang uses:

- `BatchExistKey` for a growing prefix and for pages about to be written;
- `BatchGetReplicaList` for the prefix range that exists;
- `BatchPutStart` and `BatchPutEnd` for each newly committed page.

No KV data is transferred. The measured traffic is Master RPC and metadata
work, not Store data-plane throughput.

## Workload

The default profile follows the SGLang HiCache baseline used by the RFC:

| Item | Value |
| --- | --- |
| Model | Qwen3-235B-A22B-Instruct-2507 |
| Tensor parallel | 8 ranks |
| Page size | 64 tokens |
| Tokens per round | 2,048 |
| Pages per round | 32 |
| Objects per page | 16 (K and V for 8 ranks) |
| Conversations | 80 per node |
| Rounds per conversation | 10 |
| Logical request slots | 4 per node |
| Request rate | 16 logical requests/s per node |
| Object size | 1,540,096 bytes |
| Objects per node | 409,600 |
| Logical allocation | 587.5 GiB per node |

Each round runs the prefix `BatchExistKey`, the hit-range
`BatchGetReplicaList`, the new-page miss check, `BatchPutStart`, and
`BatchPutEnd`. A barrier separates rounds. Logical requests are closed-loop and
arrive with exponential inter-arrival times at the configured rate.

## Build

```bash
cmake --build build --target master_bench -j
```

## Run

Start a Master with the memory allocator and eviction settings the workload
expects:

```bash
./build/mooncake-store/src/mooncake_master \
  --port=50051 \
  --rpc_thread_num=16 \
  --memory_allocator=offset \
  --eviction_high_watermark_ratio=1.0 \
  --allow_evict_soft_pinned_objects=false
```

Then run one node:

```bash
./build/mooncake-store/benchmarks/master_bench \
  --workload=sglang_hicache \
  --node_count=1 \
  --session_count=80 \
  --round_count=10 \
  --object_bytes=1540096 \
  --segment_size=137438953472 \
  --warmup_objects_per_node=512 \
  --result_file=sglang.csv
```

`--node_count` multiplies the number of independent nodes, clients, segments,
and total traffic. `--request_rate` is per node. The benchmark exits nonzero if
the RPC count differs from the generated workload, any RPC fails, or the final
replica validation fails.

The CSV has one row per `(phase, rpc, batch_keys)` group:

```text
phase,rpc,batch_keys,rpcs,items,p50_ms,p95_ms,p99_ms,max_ms
```

## Reference result

One node on the dgx host (2x EPYC 7742, 1007 GiB RAM, TCP), Master with 16 RPC
threads, 8 x 128 GiB synthetic segments, offset allocator, eviction disabled:
40,320 business RPCs in 48.68 s, 828.3 RPC/s; 409,600/409,600 objects validated;
zero RPC errors.

| phase | rpc | batch keys | RPCs | p50 ms | p99 ms |
| --- | --- | ---: | ---: | ---: | ---: |
| get | BatchGetReplicaList | 64 | 1,920 | 0.591 | 2.388 |
| get | BatchGetReplicaList | 128 | 1,280 | 0.897 | 2.925 |
| get | BatchGetReplicaList | 192 | 1,280 | 1.283 | 3.147 |
| get | BatchGetReplicaList | 256 | 5,120 | 1.589 | 3.750 |
| prefix | BatchExistKey | 64 | 1,920 | 0.514 | 1.735 |
| prefix | BatchExistKey | 128 | 1,920 | 0.873 | 2.357 |
| prefix | BatchExistKey | 192 | 1,280 | 1.298 | 2.588 |
| prefix | BatchExistKey | 256 | 6,400 | 1.753 | 4.414 |
| prewrite | BatchExistKey | 64 | 6,400 | 0.343 | 2.477 |
| put_end | BatchPutEnd | 64 | 6,400 | 0.490 | 2.358 |
| put_start | BatchPutStart | 64 | 6,400 | 2.761 | 4.795 |

## Comparing a PR with its base

Run the same command against the base and the PR build on the same Master and
same machine. Compare the per-phase p50, p95, and p99 columns, plus the final
`business_rpcs` and `active_rpc_per_sec` lines. The workload is deterministic
for a fixed `--seed`, so a difference in RPC counts or validation totals points
to a correctness regression before latency is considered.

## Scope and limits

This is a Master metadata benchmark. It does not exercise KV block reads or
writes, the transfer engine, RDMA, or multi-node discovery. The object size is
the SGLang page size, so a 100M-key run at this size would require far more
memory than one host; the supported scale knob is `--node_count`.
