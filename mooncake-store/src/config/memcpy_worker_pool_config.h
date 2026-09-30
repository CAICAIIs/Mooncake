#pragma once

namespace mooncake {

struct MemcpyWorkerPoolConfig {
    // A batched read submits one memcpy task per key, so the pool carries the
    // queue handoff of many small tasks in addition to the copies themselves.
    // Raising the count shortens that queue; the default stays at one worker
    // because every client builds this pool, including clients whose
    // transfers never reach it.
    int worker_count = 1;

    static MemcpyWorkerPoolConfig FromEnvironment();
};

}  // namespace mooncake
