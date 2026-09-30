#pragma once

namespace mooncake {

struct MemcpyWorkerPoolConfig {
    // A batched read submits one memcpy task per key, so the pool carries the
    // queue handoff of many small tasks in addition to the copies themselves.
    // Eight workers remove that handoff from the critical path while staying
    // clear of the point where more workers stop helping small objects.
    int worker_count = 8;

    static MemcpyWorkerPoolConfig FromEnvironment();
};

}  // namespace mooncake
