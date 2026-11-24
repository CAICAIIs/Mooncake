// Copyright 2025 Alibaba Cloud and its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <latch>
#include <map>
#include <mutex>
#include <numeric>
#include <random>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <openssl/sha.h>

#include "gflags/gflags.h"
#include "glog/logging.h"

#include "master_client.h"

// Size units for better readability
static constexpr size_t KiB = 1024;
static constexpr size_t MiB = 1024 * KiB;
static constexpr size_t GiB = 1024 * MiB;
static constexpr uintptr_t kSegmentBase = 0x100000000ULL;

DEFINE_string(master_server, "127.0.0.1:50051", "Master server address");
DEFINE_uint64(num_segments, 128, "Number of segments to mount");
DEFINE_uint64(segment_size, 64 * GiB, "Size of each segment");
DEFINE_uint64(num_clients, 4, "Number of clients to perform operations");
DEFINE_uint64(num_threads, 1,
              "Number of threads in each client to perform operations");
DEFINE_string(operation, "BatchPut", "Operation to perform");
DEFINE_uint64(num_keys, 10 * 1000,
              "Number of keys to prefill for Get operations on each thread");
DEFINE_uint64(batch_size, 128, "Batch size for batch operations");
DEFINE_uint64(value_size, 4096, "Size of object values");
DEFINE_uint64(duration, 60, "Test duration in seconds");
DEFINE_double(
    prefill_ratio, 0.0,
    "Ratio of segment capacity to prefill before test (0.0-1.0). "
    "E.g., 0.95 means fill segments to 95% before starting benchmark");

static inline void unset_cpu_affinity() {
    // Ensure that the worker threads are not bound to any CPU cores.
    cpu_set_t cpuset;
    memset(&cpuset, -1, sizeof(cpuset));
    pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
}

class SegmentClient {
   public:
    SegmentClient(const std::string& name, const std::string& master_server,
                  uintptr_t segment_base, uint64_t segment_size)
        : master_client_(mooncake::generate_uuid()) {
        auto ec = master_client_.Connect(master_server);
        if (ec != mooncake::ErrorCode::OK) {
            throw std::invalid_argument("Cannot connect to master server at " +
                                        master_server + ", ec=" + toString(ec));
        }

        segment_.id = mooncake::generate_uuid();
        segment_.name = name;
        segment_.base = segment_base;
        segment_.size = segment_size;
        segment_.te_endpoint = name;
        auto mount_ec = master_client_.MountSegment(segment_);
        if (!mount_ec.has_value()) {
            throw std::runtime_error("Failed to mount segment " + name +
                                     ", ec=" + toString(mount_ec.error()));
        }
    }

    ~SegmentClient() {
        if (remount_future_.valid()) {
            remount_future_.wait();
        }

        auto unmount_result = master_client_.UnmountSegment(segment_.id);
        if (!unmount_result.has_value()) {
            LOG(ERROR) << "Failed to unmount segment " << segment_.name;
        }
    }

    void Ping() {
        if (remount_future_.valid() &&
            remount_future_.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_future_.get();
            remount_future_ = std::future<void>();
        }

        auto ping_result = master_client_.Ping();
        if (!ping_result.has_value()) {
            throw std::runtime_error("Failed to ping master server");
        }

        if (ping_result.value().client_status ==
                mooncake::ClientStatus::NEED_REMOUNT &&
            !remount_future_.valid()) {
            remount_future_ = std::async(std::launch::async, [&]() {
                auto remount_ec = master_client_.ReMountSegment({segment_});
                if (!remount_ec.has_value()) {
                    throw std::runtime_error("Failed to remount segment");
                }
            });
        }
    }

   private:
    mooncake::MasterClient master_client_;
    mooncake::Segment segment_;
    std::future<void> remount_future_;
};

static std::atomic<uint64_t> gCompletedOperations = 0;

enum class BenchOperation {
    PUT,
    GET,
    BATCH_PUT,
    BATCH_GET,
};

static inline BenchOperation ParseOperation(const std::string& operation_str) {
    if (operation_str == "Put") {
        return BenchOperation::PUT;
    } else if (operation_str == "Get") {
        return BenchOperation::GET;
    } else if (operation_str == "BatchPut") {
        return BenchOperation::BATCH_PUT;
    } else if (operation_str == "BatchGet") {
        return BenchOperation::BATCH_GET;
    } else {
        throw std::invalid_argument("Invalid operation");
    }
}

class BenchClient {
   public:
    BenchClient(const std::string& master_server)
        : master_client_(mooncake::generate_uuid()), running_(false) {
        auto ec = master_client_.Connect(master_server);
        if (ec != mooncake::ErrorCode::OK) {
            throw std::invalid_argument("Cannot connect to master server at " +
                                        master_server + ", ec=" + toString(ec));
        }
    }

    void StartBench(std::shared_ptr<std::latch> barrier,
                    BenchOperation operation, uint64_t num_threads,
                    uint64_t batch_size, uint64_t value_size,
                    uint64_t num_keys) {
        if (running_.load()) {
            return;
        }

        running_.store(true);
        for (size_t i = 0; i < num_threads; i++) {
            threads_.push_back(std::thread(
                std::bind(&BenchClient::BenchFn, this, barrier, operation,
                          batch_size, value_size, num_keys)));
        }
    }

    void StopBench() {
        if (!running_.load()) {
            return;
        }

        running_.store(false);
        for (auto& thread : threads_) {
            if (thread.joinable()) {
                thread.join();
            }
        }

        threads_.clear();
    }

   private:
    bool Put(const std::string& key, const std::vector<uint64_t>& slice_lengths,
             const mooncake::ReplicateConfig& config) {
        auto put_start_result =
            master_client_.PutStart(key, slice_lengths, config);
        if (!put_start_result.has_value()) {
            return false;
        }

        auto put_end_result = master_client_.PutEnd(
            {key, std::nullopt}, mooncake::ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            return false;
        }

        return true;
    }

    bool Get(const std::string& key) {
        auto get_result = master_client_.GetReplicaList(key);
        return get_result.has_value() ||
               get_result.error() == mooncake::ErrorCode::OBJECT_NOT_FOUND;
    }

    uint64_t BatchPut(const std::vector<std::string>& keys,
                      const std::vector<std::vector<uint64_t>>& slice_lengths,
                      const mooncake::ReplicateConfig& config) {
        std::vector<std::string> started_keys;
        uint64_t success_cnt = 0;

        auto put_start_result =
            master_client_.BatchPutStart(keys, slice_lengths, config);
        for (size_t i = 0; i < keys.size(); i++) {
            if (put_start_result[i].has_value()) {
                started_keys.push_back(keys[i]);
            }
        }

        if (started_keys.empty()) {
            return 0;
        }

        std::vector<mooncake::ObjectMeta> object_metas;
        object_metas.reserve(started_keys.size());
        for (const auto& key : started_keys) {
            object_metas.emplace_back(mooncake::ObjectMeta{key, std::nullopt});
        }
        auto put_end_result = master_client_.BatchPutEnd(object_metas);
        for (auto& result : put_end_result) {
            if (result.has_value()) {
                success_cnt++;
            }
        }

        return success_cnt;
    }

    uint64_t BatchGet(const std::vector<std::string>& keys) {
        uint64_t success_cnt = 0;
        auto get_results = master_client_.BatchGetReplicaList(keys);
        for (auto& get_result : get_results) {
            if (get_result.has_value() ||
                get_result.error() == mooncake::ErrorCode::OBJECT_NOT_FOUND) {
                success_cnt++;
            }
        }
        return success_cnt;
    }

    std::vector<std::string> GeneratePutKeys(uint64_t& key_id,
                                             uint64_t num_keys) {
        const auto self_id = std::this_thread::get_id();
        std::vector<std::string> keys;
        keys.reserve(num_keys);

        for (size_t i = 0; i < num_keys; i++, key_id++) {
            std::stringstream ss;
            ss << self_id << "_" << key_id;
            keys.push_back(ss.str());
        }

        return keys;
    }

    std::vector<std::string> GenerateGetKeys(uint64_t key_id,
                                             uint64_t num_keys) {
        static thread_local std::mt19937 generator(std::random_device{}());

        const auto self_id = std::this_thread::get_id();
        std::vector<std::string> keys;
        keys.reserve(num_keys);

        if (key_id == 0) {
            // No keys have been created yet; return empty vector.
            return keys;
        }

        std::uniform_int_distribution<uint64_t> distribution(0, key_id - 1);
        for (size_t i = 0; i < num_keys; i++) {
            std::stringstream ss;
            ss << self_id << "_" << distribution(generator);
            keys.push_back(ss.str());
        }

        return keys;
    }

    void PrefillKeys(uint64_t& key_id, uint64_t batch_size, uint64_t value_size,
                     uint64_t num_keys) {
        const mooncake::ReplicateConfig config;

        uint64_t num_to_prefill = std::min(batch_size, num_keys);
        while (num_to_prefill > 0) {
            auto keys = GeneratePutKeys(key_id, num_to_prefill);
            std::vector<std::vector<uint64_t>> slice_lengths(num_to_prefill,
                                                             {value_size});
            BatchPut(keys, slice_lengths, config);
            num_keys -= num_to_prefill;
            num_to_prefill = std::min(batch_size, num_keys);
        }
    }

    void BenchFn(std::shared_ptr<std::latch> barrier, BenchOperation operation,
                 uint64_t batch_size, uint64_t value_size, uint64_t num_keys) {
        unset_cpu_affinity();

        uint64_t key_id = 0;
        const mooncake::ReplicateConfig config;

        std::vector<std::string> keys;
        std::vector<std::vector<uint64_t>> slice_lengths;
        slice_lengths.reserve(batch_size);
        for (size_t i = 0; i < batch_size; i++) {
            slice_lengths.push_back({value_size});
        }

        if (operation == BenchOperation::GET ||
            operation == BenchOperation::BATCH_GET) {
            PrefillKeys(key_id, batch_size, value_size, num_keys);
        }

        barrier->arrive_and_wait();

        while (running_.load()) {
            switch (operation) {
                case BenchOperation::PUT:
                    keys = GeneratePutKeys(key_id, 1);
                    if (Put(keys[0], slice_lengths[0], config)) {
                        gCompletedOperations.fetch_add(1);
                    }
                    break;
                case BenchOperation::GET:
                    keys = GenerateGetKeys(key_id, 1);
                    if (Get(keys[0])) {
                        gCompletedOperations.fetch_add(1);
                    }
                    break;
                case BenchOperation::BATCH_PUT:
                    keys = GeneratePutKeys(key_id, batch_size);
                    gCompletedOperations.fetch_add(
                        BatchPut(keys, slice_lengths, config));
                    break;
                case BenchOperation::BATCH_GET:
                    keys = GenerateGetKeys(key_id, batch_size);
                    gCompletedOperations.fetch_add(BatchGet(keys));
                    break;
                default:
                    break;
            }
        }
    }

    mooncake::MasterClient master_client_;

    std::atomic<bool> running_;
    std::vector<std::thread> threads_;
};

// ---------------------------------------------------------------------------
// SGLang HiCache metadata replay (--workload=sglang_hicache)
//
// Replays the metadata traffic a SGLang HiCache deployment sends to one master
// for a multi-turn workload: every round re-queries its growing history prefix,
// fetches replica metadata for the hits, then allocates and commits the pages
// added in that round. Node count is the independent variable.
// ---------------------------------------------------------------------------

DEFINE_string(workload, "",
              "Workload profile; empty selects the legacy operation mode, "
              "sglang_hicache selects the SGLang HiCache metadata replay");
DEFINE_uint64(node_count, 1, "Number of SGLang nodes (instances) to replay");
DEFINE_uint64(session_count, 80, "Conversations replayed per node");
DEFINE_uint64(round_count, 10, "Rounds per conversation");
DEFINE_uint64(page_tokens, 64, "Tokens per logical page");
DEFINE_uint64(tokens_per_round, 2048, "New input tokens per round");
DEFINE_uint64(object_bytes, 1540096, "Bytes per K or V object");
DEFINE_uint64(request_rate, 16, "Logical requests per second per node");
DEFINE_uint64(slots_per_node, 4, "Logical request slots per node");
DEFINE_uint64(ranks_per_node, 8, "TP ranks per node");
DEFINE_uint64(page_batch, 128, "Logical pages per batched exists/get RPC");
DEFINE_uint64(seed, 1, "Seed for arrival intervals and page keys");
DEFINE_string(model_config_digest, "qwen3-235b-a22b-instruct-2507",
              "Model digest mixed into page keys");
DEFINE_uint64(warmup_objects_per_node, 512,
              "Auxiliary objects warmed up per node");
DEFINE_bool(round_barrier, true, "Barrier between rounds");
DEFINE_bool(disable_validation, false,
            "Skip the final validation pass over all measured keys");
DEFINE_string(result_file, "",
              "Write per-group results to this CSV file; empty prints only");

namespace sglang_hicache {

using Clock = std::chrono::steady_clock;

static uint64_t ElapsedNs(Clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                                start)
        .count();
}

static std::string HexSha256(const std::string& input) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(input.data()), input.size(),
           digest);
    static const char* kHex = "0123456789abcdef";
    std::string out;
    out.reserve(2 * SHA256_DIGEST_LENGTH);
    for (unsigned char byte : digest) {
        out.push_back(kHex[byte >> 4]);
        out.push_back(kHex[byte & 0x0F]);
    }
    return out;
}

struct Profile {
    uint64_t nodes;
    uint64_t sessions;
    uint64_t rounds;
    uint64_t ranks;
    uint64_t slots;
    uint64_t pages_per_round;
    uint64_t page_batch;
    uint64_t object_bytes;
    uint64_t warmup_objects;

    uint64_t ObjectsPerPage() const { return ranks * 2; }
    uint64_t PagesBeforeRound(uint64_t round) const {
        return pages_per_round * (round - 1);
    }
    uint64_t PagesAtRound(uint64_t round) const {
        return pages_per_round * round;
    }
    // Batched RPCs needed for `pages` logical pages.
    uint64_t Batches(uint64_t pages) const {
        return pages == 0 ? 0 : (pages + page_batch - 1) / page_batch;
    }
};

struct GroupKey {
    std::string phase;
    std::string rpc;
    uint64_t batch_keys;

    bool operator<(const GroupKey& other) const {
        if (phase != other.phase) return phase < other.phase;
        if (rpc != other.rpc) return rpc < other.rpc;
        return batch_keys < other.batch_keys;
    }
};

struct GroupStats {
    std::atomic<uint64_t> rpc_count{0};
    std::atomic<uint64_t> logical_pages{0};
    std::atomic<uint64_t> object_keys{0};
    std::atomic<uint64_t> hit{0};
    std::atomic<uint64_t> miss{0};
    std::atomic<uint64_t> error{0};
    std::atomic<uint64_t> start_ok{0};
    std::atomic<uint64_t> start_fail{0};
    std::atomic<uint64_t> start_exists{0};
    std::atomic<uint64_t> start_no_handle{0};
    std::atomic<uint64_t> end_ok{0};
    std::atomic<uint64_t> end_fail{0};
    std::mutex latency_mutex;
    std::vector<uint64_t> latency_ns;

    void RecordLatency(uint64_t ns) {
        std::lock_guard<std::mutex> guard(latency_mutex);
        latency_ns.push_back(ns);
    }
    std::vector<uint64_t> Latencies() {
        std::lock_guard<std::mutex> guard(latency_mutex);
        return latency_ns;
    }
};

class Registry {
   public:
    GroupStats& Get(const std::string& phase, const std::string& rpc,
                    uint64_t batch_keys) {
        std::lock_guard<std::mutex> guard(mutex_);
        return groups_[GroupKey{phase, rpc, batch_keys}];
    }

    template <typename Fn>
    void ForEach(Fn&& fn) {
        std::lock_guard<std::mutex> guard(mutex_);
        for (auto& entry : groups_) {
            fn(entry.first, entry.second);
        }
    }

   private:
    std::mutex mutex_;
    std::map<GroupKey, GroupStats> groups_;
};

static double Percentile(const std::vector<uint64_t>& sorted, double q) {
    if (sorted.empty()) return 0.0;
    size_t idx = static_cast<size_t>(q * (sorted.size() - 1));
    return static_cast<double>(sorted[idx]) / 1e6;  // ns -> ms
}

struct CounterSnapshot {
    uint64_t rpc = 0;
    uint64_t keys = 0;
    uint64_t hit = 0;
    uint64_t miss = 0;
    uint64_t error = 0;
    uint64_t put_start = 0;
    uint64_t put_end = 0;
};

// One MasterClient identity: a single TP rank of one node in one request slot.
class RankChannel {
   public:
    RankChannel(const std::string& master, uint64_t node, uint64_t slot,
                uint64_t rank, uintptr_t base, uint64_t segment_size,
                Registry& registry)
        : client_(mooncake::generate_uuid()),
          node_(node),
          slot_(slot),
          rank_(rank),
          registry_(registry),
          segment_base_(base),
          segment_size_(segment_size) {
        auto ec = client_.Connect(master);
        if (ec != mooncake::ErrorCode::OK) {
            throw std::runtime_error("connect failed: " + toString(ec));
        }
        segment_.id = mooncake::generate_uuid();
        segment_.name = "sg_node" + std::to_string(node) + "_slot" +
                        std::to_string(slot) + "_rank" + std::to_string(rank);
        segment_.base = base;
        segment_.size = segment_size;
        segment_.te_endpoint = segment_.name;
        auto mount_ec = client_.MountSegment(segment_);
        if (!mount_ec.has_value()) {
            throw std::runtime_error("mount failed: " +
                                     toString(mount_ec.error()));
        }
    }

    ~RankChannel() {
        auto result = client_.UnmountSegment(segment_.id);
        if (!result.has_value()) {
            LOG(ERROR) << "unmount failed for " << segment_.name;
        }
    }

    RankChannel(const RankChannel&) = delete;
    RankChannel& operator=(const RankChannel&) = delete;

    mooncake::MasterClient& client() { return client_; }
    uint64_t rank() const { return rank_; }
    uintptr_t segment_base() const { return segment_base_; }
    uint64_t segment_size() const { return segment_size_; }
    Registry& registry() { return registry_; }

    void Ping() { (void)client_.Ping(); }

   private:
    mooncake::MasterClient client_;
    mooncake::Segment segment_;
    uint64_t node_;
    uint64_t slot_;
    uint64_t rank_;
    Registry& registry_;
    uintptr_t segment_base_;
    uint64_t segment_size_;
};

// Per-slot rendezvous shared by the rank threads of one request slot.
struct SlotControl {
    std::mutex mutex;
    std::condition_variable cv;
    uint64_t generation = 0;
    uint64_t session = 0;
    uint64_t round = 0;
    std::atomic<uint64_t> pending{0};
    std::atomic<bool> busy{false};
    bool stop = false;
};

struct ValidationTotals {
    uint64_t checked = 0;
    uint64_t missing = 0;
    uint64_t wrong_type = 0;
    uint64_t wrong_size = 0;
    uint64_t foreign_segment = 0;
    uint64_t warmup_unexpected_hit = 0;
    uint64_t warmup_put_failures = 0;
    uint64_t warmup_get_failures = 0;
};

static std::string ObjectKey(const std::string& page_key, uint64_t rank,
                             bool is_k) {
    std::string key = page_key;
    key += '_';
    key += std::to_string(rank);
    key += is_k ? "_k" : "_v";
    return key;
}

static std::vector<std::string> BuildObjectKeys(
    const std::vector<std::string>& page_keys, uint64_t begin, uint64_t end,
    uint64_t rank) {
    std::vector<std::string> keys;
    keys.reserve((end - begin) * 2);
    for (uint64_t i = begin; i < end; ++i) {
        keys.push_back(ObjectKey(page_keys[i], rank, true));
        keys.push_back(ObjectKey(page_keys[i], rank, false));
    }
    return keys;
}

// Result counters of one round of one rank, used by the expected-count oracle.
struct RoundCounters {
    uint64_t prefix_exists_rpcs = 0;
    uint64_t get_rpcs = 0;
    uint64_t prewrite_rpcs = 0;
    uint64_t put_start_rpcs = 0;
    uint64_t put_end_rpcs = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t put_start_ok = 0;
    uint64_t put_end_ok = 0;
};

static RoundCounters ExecuteRankRound(RankChannel& channel,
                                      const Profile& profile,
                                      const std::vector<std::string>& pages,
                                      uint64_t round, Registry& registry,
                                      const mooncake::ReplicateConfig& config) {
    RoundCounters counters;
    const uint64_t rank = channel.rank();
    const uint64_t history_pages = profile.PagesBeforeRound(round);
    const uint64_t total_pages = profile.PagesAtRound(round);

    // Phase 1: prefix exists, 128 pages per RPC. A page counts as hit only when
    // both its K and V objects exist; the first page that fails ends the
    // contiguous hit range and no further batches are issued.
    uint64_t contiguous_hit_pages = 0;
    bool stop = false;
    for (uint64_t begin = 0; begin < total_pages && !stop;
         begin += profile.page_batch) {
        const uint64_t end = std::min(begin + profile.page_batch, total_pages);
        auto keys = BuildObjectKeys(pages, begin, end, rank);
        const uint64_t batch_keys = keys.size();

        auto start = Clock::now();
        auto results = channel.client().BatchExistKey(keys);
        const uint64_t elapsed = ElapsedNs(start);

        GroupStats& stats =
            registry.Get("prefix_exists", "BatchExistKey", batch_keys);
        stats.rpc_count.fetch_add(1);
        stats.logical_pages.fetch_add(end - begin);
        stats.object_keys.fetch_add(batch_keys);
        stats.RecordLatency(elapsed);
        counters.prefix_exists_rpcs++;

        for (uint64_t page = begin; page < end; ++page) {
            const size_t offset = static_cast<size_t>(page - begin) * 2;
            bool page_hit = true;
            for (size_t component = 0; component < 2; ++component) {
                const auto& result = results[offset + component];
                if (!result.has_value()) {
                    stats.error.fetch_add(1);
                    page_hit = false;
                } else if (result.value()) {
                    stats.hit.fetch_add(1);
                    counters.hits++;
                } else {
                    stats.miss.fetch_add(1);
                    counters.misses++;
                    page_hit = false;
                }
            }
            if (page_hit) {
                contiguous_hit_pages = page + 1;
            } else {
                stop = true;
                break;
            }
        }
    }

    // Phase 2: replica metadata for the contiguous hit range.
    for (uint64_t begin = 0; begin < contiguous_hit_pages;
         begin += profile.page_batch) {
        const uint64_t end =
            std::min(begin + profile.page_batch, contiguous_hit_pages);
        auto keys = BuildObjectKeys(pages, begin, end, rank);
        const uint64_t batch_keys = keys.size();

        auto start = Clock::now();
        auto results = channel.client().BatchGetReplicaList(keys);
        const uint64_t elapsed = ElapsedNs(start);

        GroupStats& stats =
            registry.Get("get_metadata", "BatchGetReplicaList", batch_keys);
        stats.rpc_count.fetch_add(1);
        stats.logical_pages.fetch_add(end - begin);
        stats.object_keys.fetch_add(batch_keys);
        stats.RecordLatency(elapsed);
        counters.get_rpcs++;

        for (const auto& result : results) {
            if (!result.has_value()) {
                stats.error.fetch_add(1);
            } else {
                stats.hit.fetch_add(1);
                counters.hits++;
            }
        }
    }

    // Phase 3: pre-write dedup check on the pages added in this round.
    auto new_keys = BuildObjectKeys(pages, history_pages, total_pages, rank);
    const uint64_t new_batch_keys = new_keys.size();
    {
        auto start = Clock::now();
        auto results = channel.client().BatchExistKey(new_keys);
        const uint64_t elapsed = ElapsedNs(start);

        GroupStats& stats =
            registry.Get("prewrite_exists", "BatchExistKey", new_batch_keys);
        stats.rpc_count.fetch_add(1);
        stats.logical_pages.fetch_add(total_pages - history_pages);
        stats.object_keys.fetch_add(new_batch_keys);
        stats.RecordLatency(elapsed);
        counters.prewrite_rpcs++;

        for (const auto& result : results) {
            if (!result.has_value()) {
                stats.error.fetch_add(1);
            } else if (result.value()) {
                stats.hit.fetch_add(1);
            } else {
                stats.miss.fetch_add(1);
                counters.misses++;
            }
        }
    }

    // Phase 4: allocate, then Phase 5: commit.
    std::vector<std::vector<uint64_t>> slice_lengths(
        new_batch_keys, std::vector<uint64_t>{profile.object_bytes});
    std::vector<std::string> started;
    {
        auto start = Clock::now();
        auto results =
            channel.client().BatchPutStart(new_keys, slice_lengths, config);
        const uint64_t elapsed = ElapsedNs(start);

        GroupStats& stats =
            registry.Get("put_start", "BatchPutStart", new_batch_keys);
        stats.rpc_count.fetch_add(1);
        stats.logical_pages.fetch_add(total_pages - history_pages);
        stats.object_keys.fetch_add(new_batch_keys);
        stats.RecordLatency(elapsed);
        counters.put_start_rpcs++;

        for (size_t i = 0; i < results.size(); ++i) {
            if (results[i].has_value()) {
                stats.start_ok.fetch_add(1);
                started.push_back(new_keys[i]);
                counters.put_start_ok++;
                continue;
            }
            const auto error = results[i].error();
            if (error == mooncake::ErrorCode::OBJECT_ALREADY_EXISTS) {
                stats.start_exists.fetch_add(1);
                started.push_back(new_keys[i]);
            } else if (error == mooncake::ErrorCode::NO_AVAILABLE_HANDLE) {
                stats.start_no_handle.fetch_add(1);
                stats.start_fail.fetch_add(1);
            } else {
                stats.start_fail.fetch_add(1);
            }
        }
    }

    if (!started.empty()) {
        std::vector<mooncake::ObjectMeta> object_metas;
        object_metas.reserve(started.size());
        for (const auto& key : started) {
            object_metas.emplace_back(mooncake::ObjectMeta{key, std::nullopt});
        }

        auto start = Clock::now();
        auto results = channel.client().BatchPutEnd(
            object_metas, mooncake::ReplicaType::MEMORY);
        const uint64_t elapsed = ElapsedNs(start);

        GroupStats& stats =
            registry.Get("put_end", "BatchPutEnd", started.size());
        stats.rpc_count.fetch_add(1);
        stats.logical_pages.fetch_add(total_pages - history_pages);
        stats.object_keys.fetch_add(started.size());
        stats.RecordLatency(elapsed);
        counters.put_end_rpcs++;

        for (const auto& result : results) {
            if (result.has_value()) {
                stats.end_ok.fetch_add(1);
                counters.put_end_ok++;
            } else {
                stats.end_fail.fetch_add(1);
            }
        }
    }

    return counters;
}

static std::vector<std::string> BuildPageKeys(const Profile& profile,
                                              uint64_t node, uint64_t session) {
    const uint64_t total = profile.PagesAtRound(profile.rounds);
    std::vector<std::string> pages;
    pages.reserve(total);
    for (uint64_t page = 1; page <= total; ++page) {
        std::string material = "mc-sglang-master-v1";
        material.push_back('\0');
        material += FLAGS_model_config_digest;
        material.push_back('\0');
        material += std::to_string(FLAGS_seed);
        material.push_back('\0');
        material += std::to_string(node);
        material.push_back('\0');
        material += std::to_string(session);
        material.push_back('\0');
        material += std::to_string(page);
        pages.push_back(HexSha256(material));
    }
    return pages;
}

struct NodeContext {
    uint64_t node = 0;
    std::vector<std::unique_ptr<RankChannel>> ranks;
    std::vector<std::unique_ptr<SlotControl>> slots;
    std::vector<std::vector<std::string>> pages;  // [session][page]
    std::vector<std::thread> workers;
    uint64_t active_window_ns = 0;
    uint64_t logical_requests = 0;
};

static void WarmupChannel(RankChannel& channel, const Profile& profile,
                          uint64_t objects_per_channel,
                          ValidationTotals* totals) {
    if (objects_per_channel == 0) return;
    const uint64_t batch_objects = 16;
    uint64_t done = 0;
    uint64_t batch_id = 0;
    while (done < objects_per_channel) {
        const uint64_t count =
            std::min(batch_objects, objects_per_channel - done);
        std::vector<std::string> keys;
        keys.reserve(count);
        for (uint64_t i = 0; i < count; ++i) {
            keys.push_back("sgw_" + std::to_string(channel.rank()) + "_" +
                           std::to_string(batch_id) + "_" + std::to_string(i));
        }
        // Warmup traffic is reported separately and excluded from load totals.
        auto exist_results = channel.client().BatchExistKey(keys);
        if (totals != nullptr) {
            for (const auto& result : exist_results) {
                if (result.has_value() && result.value()) {
                    totals->warmup_unexpected_hit++;
                }
            }
        }

        std::vector<std::vector<uint64_t>> slice_lengths(
            keys.size(), std::vector<uint64_t>{profile.object_bytes});
        mooncake::ReplicateConfig config;
        config.replica_num = 1;
        auto start_results =
            channel.client().BatchPutStart(keys, slice_lengths, config);
        std::vector<mooncake::ObjectMeta> metas;
        for (size_t i = 0; i < start_results.size(); ++i) {
            if (start_results[i].has_value()) {
                metas.emplace_back(mooncake::ObjectMeta{keys[i], std::nullopt});
            }
        }
        if (!metas.empty()) {
            auto end_results = channel.client().BatchPutEnd(
                metas, mooncake::ReplicaType::MEMORY);
            for (const auto& result : end_results) {
                if (!result.has_value() && totals != nullptr) {
                    totals->warmup_put_failures++;
                }
            }
        }
        std::vector<std::string> committed;
        committed.reserve(metas.size());
        for (const auto& meta : metas) {
            committed.push_back(meta.key);
        }
        if (!committed.empty()) {
            auto get_results = channel.client().BatchGetReplicaList(committed);
            for (const auto& result : get_results) {
                if (!result.has_value() && totals != nullptr) {
                    totals->warmup_get_failures++;
                }
            }
        }
        done += count;
        batch_id++;
    }
}

static void WorkerLoop(NodeContext& ctx, uint64_t slot_index, uint64_t rank,
                       const Profile& profile, Registry& registry,
                       const mooncake::ReplicateConfig& config) {
    SlotControl& control = *ctx.slots[slot_index];
    uint64_t seen = 0;
    for (;;) {
        uint64_t session = 0;
        uint64_t round = 0;
        {
            std::unique_lock<std::mutex> lock(control.mutex);
            control.cv.wait(lock, [&] {
                return control.stop || control.generation != seen;
            });
            if (control.stop) {
                return;
            }
            seen = control.generation;
            session = control.session;
            round = control.round;
        }
        ExecuteRankRound(*ctx.ranks[rank], profile, ctx.pages[session], round,
                         registry, config);
        if (control.pending.fetch_sub(1) == 1) {
            control.busy.store(false, std::memory_order_release);
        }
    }
}

static void RunNodeRounds(NodeContext& ctx, const Profile& profile,
                          Registry& registry,
                          const mooncake::ReplicateConfig& config) {
    std::mt19937_64 rng(static_cast<uint64_t>(FLAGS_seed) * 1000003ULL +
                        ctx.node);
    std::vector<uint64_t> order(profile.sessions);
    std::iota(order.begin(), order.end(), 0ULL);
    std::exponential_distribution<double> arrival(
        static_cast<double>(FLAGS_request_rate));

    const auto window_start = Clock::now();
    for (uint64_t round = 1; round <= profile.rounds; ++round) {
        std::shuffle(order.begin(), order.end(), rng);
        auto next_dispatch = Clock::now();
        for (uint64_t dispatched = 0; dispatched < order.size(); ++dispatched) {
            int64_t free_slot = -1;
            while (free_slot < 0) {
                for (uint64_t s = 0; s < ctx.slots.size(); ++s) {
                    if (!ctx.slots[s]->busy.load(std::memory_order_acquire)) {
                        free_slot = static_cast<int64_t>(s);
                        break;
                    }
                }
                if (free_slot < 0) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
            const auto now = Clock::now();
            if (next_dispatch > now) {
                std::this_thread::sleep_until(next_dispatch);
            }
            next_dispatch += std::chrono::duration_cast<Clock::duration>(
                std::chrono::duration<double>(arrival(rng)));

            SlotControl& control = *ctx.slots[free_slot];
            {
                std::lock_guard<std::mutex> guard(control.mutex);
                control.session = order[dispatched];
                control.round = round;
                control.pending.store(profile.ranks);
                control.busy.store(true, std::memory_order_release);
                control.generation++;
            }
            control.cv.notify_all();
            ctx.logical_requests++;
        }

        // Round barrier: the next round starts only after every slot is idle.
        for (;;) {
            bool all_idle = true;
            for (auto& control : ctx.slots) {
                if (control->busy.load(std::memory_order_acquire)) {
                    all_idle = false;
                    break;
                }
            }
            if (all_idle) break;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    ctx.active_window_ns = ElapsedNs(window_start);
}

static ValidationTotals ValidateMeasuredKeys(
    const Profile& profile, const std::vector<NodeContext>& contexts,
    const std::string& master, uint64_t segment_size) {
    ValidationTotals totals;
    mooncake::MasterClient validator(mooncake::generate_uuid());
    auto ec = validator.Connect(master);
    if (ec != mooncake::ErrorCode::OK) {
        LOG(ERROR) << "validation client failed to connect: " << toString(ec);
        totals.missing = 1;
        return totals;
    }

    const uint64_t batch_keys = 256;
    for (const auto& ctx : contexts) {
        for (uint64_t session = 0; session < profile.sessions; ++session) {
            const auto& pages = ctx.pages[session];
            std::vector<std::string> pending;
            pending.reserve(batch_keys);
            auto flush = [&]() {
                if (pending.empty()) return;
                auto results = validator.BatchGetReplicaList(pending);
                for (size_t i = 0; i < results.size(); ++i) {
                    totals.checked++;
                    if (!results[i].has_value()) {
                        totals.missing++;
                        continue;
                    }
                    const auto& replicas = results[i].value().replicas;
                    if (replicas.empty()) {
                        totals.missing++;
                        continue;
                    }
                    bool memory_found = false;
                    for (const auto& replica : replicas) {
                        if (!replica.is_memory_replica()) continue;
                        memory_found = true;
                        const auto& descriptor =
                            replica.get_memory_descriptor();
                        if (descriptor.buffer_descriptor.size_ !=
                            profile.object_bytes) {
                            totals.wrong_size++;
                        }
                        const uintptr_t address =
                            descriptor.buffer_descriptor.buffer_address_;
                        bool inside = false;
                        for (uint64_t identity = 0;
                             identity < profile.nodes * profile.ranks;
                             ++identity) {
                            const uintptr_t base =
                                kSegmentBase + identity * segment_size;
                            if (address >= base &&
                                address < base + segment_size) {
                                inside = true;
                                break;
                            }
                        }
                        if (!inside) {
                            totals.foreign_segment++;
                        }
                    }
                    if (!memory_found) {
                        totals.wrong_type++;
                    }
                }
                pending.clear();
            };
            for (uint64_t rank = 0; rank < profile.ranks; ++rank) {
                for (uint64_t page = 0; page < pages.size(); ++page) {
                    pending.push_back(ObjectKey(pages[page], rank, true));
                    pending.push_back(ObjectKey(pages[page], rank, false));
                    if (pending.size() >= batch_keys) flush();
                }
            }
            flush();
        }
    }
    return totals;
}

static int RunSglangHicache() {
    if (FLAGS_page_tokens == 0 ||
        FLAGS_tokens_per_round % FLAGS_page_tokens != 0) {
        LOG(ERROR) << "tokens_per_round must be a multiple of page_tokens";
        return 1;
    }
    if (FLAGS_node_count == 0 || FLAGS_request_rate == 0 ||
        FLAGS_ranks_per_node == 0 || FLAGS_slots_per_node == 0 ||
        FLAGS_session_count == 0 || FLAGS_round_count == 0) {
        LOG(ERROR) << "nodes, rate, ranks, slots, sessions and rounds must be "
                      "positive";
        return 1;
    }

    Profile profile;
    profile.nodes = FLAGS_node_count;
    profile.sessions = FLAGS_session_count;
    profile.rounds = FLAGS_round_count;
    profile.ranks = FLAGS_ranks_per_node;
    profile.slots = FLAGS_slots_per_node;
    profile.pages_per_round = FLAGS_tokens_per_round / FLAGS_page_tokens;
    profile.page_batch = FLAGS_page_batch;
    profile.object_bytes = FLAGS_object_bytes;
    profile.warmup_objects = FLAGS_warmup_objects_per_node;

    const uint64_t identities = profile.nodes * profile.ranks;
    LOG(INFO) << "sglang_hicache: nodes=" << profile.nodes
              << " sessions=" << profile.sessions
              << " rounds=" << profile.rounds
              << " pages_per_round=" << profile.pages_per_round
              << " ranks=" << profile.ranks << " slots=" << profile.slots
              << " identities=" << identities
              << " object_bytes=" << profile.object_bytes;

    Registry registry;
    mooncake::ReplicateConfig config;
    config.replica_num = 1;

    std::vector<NodeContext> contexts(profile.nodes);
    for (uint64_t node = 0; node < profile.nodes; ++node) {
        NodeContext& ctx = contexts[node];
        ctx.node = node;
        ctx.ranks.reserve(profile.ranks);
        for (uint64_t rank = 0; rank < profile.ranks; ++rank) {
            const uint64_t identity = node * profile.ranks + rank;
            const uintptr_t base = kSegmentBase + identity * FLAGS_segment_size;
            ctx.ranks.push_back(std::make_unique<RankChannel>(
                FLAGS_master_server, node, 0, rank, base, FLAGS_segment_size,
                registry));
        }
        ctx.slots.reserve(profile.slots);
        for (uint64_t slot = 0; slot < profile.slots; ++slot) {
            ctx.slots.push_back(std::make_unique<SlotControl>());
        }
        ctx.pages.reserve(profile.sessions);
        for (uint64_t session = 0; session < profile.sessions; ++session) {
            ctx.pages.push_back(BuildPageKeys(profile, node, session));
        }
    }
    LOG(INFO) << "mounted " << identities << " segments";

    // Master offboards idle clients after client_active_ttl_sec, which unmounts
    // their segments. Keep every rank identity alive for the whole replay.
    std::jthread heartbeat([&contexts](std::stop_token stop) {
        while (!stop.stop_requested()) {
            for (auto& ctx : contexts) {
                for (auto& channel : ctx.ranks) {
                    channel->Ping();
                }
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    });

    const uint64_t channels_per_node = profile.slots * profile.ranks;
    // Warmup keys are per rank identity, and each identity serves every request
    // slot, so one identity warms up slots * (objects / (slots * ranks)).
    const uint64_t warmup_per_channel =
        profile.warmup_objects / std::max<uint64_t>(1, channels_per_node) *
        profile.slots;
    LOG(INFO) << "warming up " << profile.warmup_objects
              << " objects per node (" << warmup_per_channel
              << " per rank identity across " << profile.ranks
              << " identities)";
    ValidationTotals warmup_totals;
    for (auto& ctx : contexts) {
        for (auto& channel : ctx.ranks) {
            WarmupChannel(*channel, profile, warmup_per_channel,
                          &warmup_totals);
        }
    }
    LOG(INFO) << "warmup done: unexpected_hit="
              << warmup_totals.warmup_unexpected_hit
              << " put_failures=" << warmup_totals.warmup_put_failures
              << " get_failures=" << warmup_totals.warmup_get_failures;

    for (auto& ctx : contexts) {
        for (uint64_t slot = 0; slot < profile.slots; ++slot) {
            for (uint64_t rank = 0; rank < profile.ranks; ++rank) {
                ctx.workers.emplace_back(WorkerLoop, std::ref(ctx), slot, rank,
                                         std::cref(profile), std::ref(registry),
                                         std::cref(config));
            }
        }
    }

    std::vector<std::thread> schedulers;
    schedulers.reserve(contexts.size());
    for (auto& ctx : contexts) {
        schedulers.emplace_back(RunNodeRounds, std::ref(ctx),
                                std::cref(profile), std::ref(registry),
                                std::cref(config));
    }
    for (auto& scheduler : schedulers) scheduler.join();

    for (auto& ctx : contexts) {
        for (auto& control : ctx.slots) {
            {
                std::lock_guard<std::mutex> guard(control->mutex);
                control->stop = true;
            }
            control->cv.notify_all();
        }
        for (auto& worker : ctx.workers) worker.join();
    }

    uint64_t active_window_ns = 0;
    uint64_t logical_requests = 0;
    for (const auto& ctx : contexts) {
        active_window_ns = std::max(active_window_ns, ctx.active_window_ns);
        logical_requests += ctx.logical_requests;
    }

    ValidationTotals validation;
    if (!FLAGS_disable_validation) {
        LOG(INFO) << "validating measured keys...";
        validation = ValidateMeasuredKeys(
            profile, contexts, FLAGS_master_server, FLAGS_segment_size);
        LOG(INFO) << "validation: checked=" << validation.checked
                  << " missing=" << validation.missing
                  << " wrong_type=" << validation.wrong_type
                  << " wrong_size=" << validation.wrong_size
                  << " foreign_segment=" << validation.foreign_segment;
    }

    // Expected counts from the profile, used to cross-check the generator.
    uint64_t expected_prefix_rpcs = 0;
    uint64_t expected_get_rpcs = 0;
    uint64_t expected_per_round_rpcs = 0;
    for (uint64_t round = 1; round <= profile.rounds; ++round) {
        expected_prefix_rpcs += profile.Batches(profile.PagesAtRound(round));
        expected_get_rpcs += profile.Batches(profile.PagesBeforeRound(round));
    }
    expected_prefix_rpcs *= profile.sessions * profile.ranks * profile.nodes;
    expected_get_rpcs *= profile.sessions * profile.ranks * profile.nodes;
    expected_per_round_rpcs =
        profile.sessions * profile.ranks * profile.nodes * profile.rounds;

    uint64_t business_rpcs = 0;
    uint64_t total_errors = 0;
    std::ostringstream table;
    table << "\nphase,rpc,batch_keys,rpcs,items,p50_ms,p95_ms,p99_ms,max_ms\n";
    registry.ForEach([&](const GroupKey& key, GroupStats& stats) {
        auto latencies = stats.Latencies();
        std::sort(latencies.begin(), latencies.end());
        const uint64_t rpcs = stats.rpc_count.load();
        business_rpcs += rpcs;
        total_errors += stats.error.load();
        table << key.phase << "," << key.rpc << "," << key.batch_keys << ","
              << rpcs << "," << stats.object_keys.load() << "," << std::fixed
              << std::setprecision(3) << Percentile(latencies, 0.50) << ","
              << Percentile(latencies, 0.95) << ","
              << Percentile(latencies, 0.99) << ","
              << (latencies.empty()
                      ? 0.0
                      : static_cast<double>(latencies.back()) / 1e6)
              << "\n";
    });
    std::cout << table.str() << std::flush;

    if (!FLAGS_result_file.empty()) {
        std::ofstream out(FLAGS_result_file);
        if (!out.is_open()) {
            LOG(ERROR) << "cannot open result file " << FLAGS_result_file;
        } else {
            out << table.str();
            out.close();
            LOG(INFO) << "results written to " << FLAGS_result_file;
        }
    }

    const double active_seconds = active_window_ns / 1e9;
    LOG(INFO) << "business_rpcs=" << business_rpcs
              << " expected_prefix=" << expected_prefix_rpcs
              << " expected_get=" << expected_get_rpcs
              << " expected_per_round=" << expected_per_round_rpcs;
    LOG(INFO) << "logical_requests=" << logical_requests
              << " active_window_s=" << active_seconds << " active_rpc_per_sec="
              << (active_seconds > 0 ? business_rpcs / active_seconds : 0.0);

    bool ok = true;
    if (business_rpcs != expected_prefix_rpcs + expected_get_rpcs +
                             3 * expected_per_round_rpcs) {
        LOG(ERROR) << "RPC count does not match the expected workload";
        ok = false;
    }
    if (total_errors != 0) {
        LOG(ERROR) << "unexpected RPC errors: " << total_errors;
        ok = false;
    }
    if (!FLAGS_disable_validation &&
        (validation.missing || validation.wrong_type || validation.wrong_size ||
         validation.foreign_segment)) {
        LOG(ERROR) << "validation failed";
        ok = false;
    }

    LOG(INFO) << (ok ? "sglang_hicache replay finished OK"
                     : "sglang_hicache replay finished with failures");
    return ok ? 0 : 1;
}

}  // namespace sglang_hicache

int main(int argc, char** argv) {
    std::vector<std::unique_ptr<SegmentClient>> segment_clients;
    std::mutex segment_clients_mutex;
    std::jthread ping_thread;
    std::vector<std::unique_ptr<BenchClient>> bench_clients;

    google::InitGoogleLogging("MasterBench");
    FLAGS_logtostderr = true;

    gflags::ParseCommandLineFlags(&argc, &argv, false);

    if (!FLAGS_workload.empty()) {
        if (FLAGS_workload != "sglang_hicache") {
            LOG(ERROR) << "unknown workload profile: " << FLAGS_workload;
            google::ShutdownGoogleLogging();
            return 1;
        }
        const int result = sglang_hicache::RunSglangHicache();
        google::ShutdownGoogleLogging();
        return result;
    }

    ping_thread = std::jthread([&](std::stop_token stop_token) {
        static const auto OneSecond = std::chrono::seconds(1);

        unset_cpu_affinity();

        while (!stop_token.stop_requested()) {
            std::chrono::nanoseconds time_elapsed;
            auto start_time = std::chrono::steady_clock::now();
            {
                std::lock_guard<std::mutex> guard(segment_clients_mutex);
                for (auto& segment_client : segment_clients) {
                    segment_client->Ping();
                }
            }
            time_elapsed = std::chrono::steady_clock::now() - start_time;

            if (OneSecond > time_elapsed) {
                std::this_thread::sleep_for(OneSecond - time_elapsed);
            }
        }
    });

    LOG(INFO) << "Mounting " << FLAGS_num_segments << " segments...";
    for (size_t i = 0; i < FLAGS_num_segments; i++) {
        auto segment_client = std::make_unique<SegmentClient>(
            "segment_client_" + std::to_string(i), FLAGS_master_server,
            kSegmentBase + i * FLAGS_segment_size, FLAGS_segment_size);
        {
            std::lock_guard<std::mutex> guard(segment_clients_mutex);
            segment_clients.push_back(std::move(segment_client));
        }
    }
    LOG(INFO) << "Segments mounted";

    // Prefill segments if requested
    if (FLAGS_prefill_ratio > 0.0) {
        LOG(INFO) << "Prefilling segments to " << (FLAGS_prefill_ratio * 100)
                  << "% capacity...";

        // Calculate total capacity and target fill size
        uint64_t total_capacity = FLAGS_num_segments * FLAGS_segment_size;
        uint64_t target_bytes =
            static_cast<uint64_t>(total_capacity * FLAGS_prefill_ratio);
        uint64_t bytes_per_object = FLAGS_value_size;
        uint64_t target_objects = target_bytes / bytes_per_object;

        LOG(INFO) << "Target: " << target_objects << " objects ("
                  << (target_bytes / GiB) << " GiB)";

        // Create a temporary client for prefilling
        mooncake::MasterClient prefill_client(mooncake::generate_uuid());
        auto ec = prefill_client.Connect(FLAGS_master_server);
        if (ec != mooncake::ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect prefill client";
            return 1;
        }

        const mooncake::ReplicateConfig config;
        uint64_t filled_objects = 0;
        uint64_t key_id = 0;

        while (filled_objects < target_objects) {
            uint64_t batch =
                std::min(FLAGS_batch_size, target_objects - filled_objects);
            std::vector<std::string> keys;
            std::vector<std::vector<uint64_t>> slice_lengths;

            keys.reserve(batch);
            slice_lengths.reserve(batch);

            for (uint64_t i = 0; i < batch; i++) {
                keys.push_back("prefill_" + std::to_string(key_id++));
                slice_lengths.push_back({bytes_per_object});
            }

            auto put_start_result =
                prefill_client.BatchPutStart(keys, slice_lengths, config);
            std::vector<std::string> started_keys;
            for (size_t i = 0; i < keys.size(); i++) {
                if (put_start_result[i].has_value()) {
                    started_keys.push_back(keys[i]);
                }
            }

            if (!started_keys.empty()) {
                std::vector<mooncake::ObjectMeta> object_metas;
                object_metas.reserve(started_keys.size());
                for (const auto& key : started_keys) {
                    object_metas.emplace_back(
                        mooncake::ObjectMeta{key, std::nullopt});
                }
                auto put_end_result = prefill_client.BatchPutEnd(object_metas);
                for (auto& result : put_end_result) {
                    if (result.has_value()) {
                        filled_objects++;
                    }
                }
            }

            if (filled_objects % 10000 == 0) {
                double progress =
                    (double)filled_objects / target_objects * 100.0;
                LOG(INFO) << "Prefill progress: " << filled_objects << "/"
                          << target_objects << " (" << std::fixed
                          << std::setprecision(1) << progress << "%)";
            }
        }

        LOG(INFO) << "Prefill completed: " << filled_objects << " objects";
    }

    LOG(INFO) << "Starting " << FLAGS_num_clients << " bench clients with "
              << FLAGS_num_threads << " threads for each...";
    for (size_t i = 0; i < FLAGS_num_clients; i++) {
        bench_clients.emplace_back(
            std::make_unique<BenchClient>(FLAGS_master_server));
    }

    const auto operation = ParseOperation(FLAGS_operation);
    auto num_threads = FLAGS_num_clients * FLAGS_num_threads;
    auto barrier = std::make_shared<std::latch>(num_threads +
                                                1);  // +1 for the main thread

    for (auto& bench_client : bench_clients) {
        bench_client->StartBench(barrier, operation, FLAGS_num_threads,
                                 FLAGS_batch_size, FLAGS_value_size,
                                 FLAGS_num_keys);
    }
    barrier->arrive_and_wait();
    LOG(INFO) << "Clients started";

    uint64_t last_completed = 0;
    for (size_t i = 0; i < FLAGS_duration; i++) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto curr_completed = gCompletedOperations.load();
        LOG(INFO) << "Completed operations: "
                  << curr_completed - last_completed;
        last_completed = curr_completed;
    }

    auto num_completed_operations = gCompletedOperations.load();

    LOG(INFO) << "Stopping bench clients...";
    for (auto& bench_client : bench_clients) {
        bench_client->StopBench();
    }
    LOG(INFO) << "Clients stopped";

    LOG(INFO) << "Stopping ping thread...";
    if (ping_thread.joinable()) {
        ping_thread.request_stop();
        ping_thread.join();
    }
    LOG(INFO) << "Ping thread stopped";

    LOG(INFO) << "Disconnecting from master...";
    bench_clients.clear();
    segment_clients.clear();
    LOG(INFO) << "Disconnected from master";

    std::cout << "Operations per second: " << std::fixed << std::setprecision(2)
              << num_completed_operations / (double)FLAGS_duration << "\n";

    google::ShutdownGoogleLogging();

    return 0;
}
