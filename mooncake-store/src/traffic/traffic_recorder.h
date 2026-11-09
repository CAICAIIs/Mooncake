#pragma once

#include <sys/uio.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

#include "traffic_format.h"
#include "traffic_redaction.h"

namespace Json {
class Value;
}

namespace cinatra {
class coro_http_server;
}

namespace mooncake::traffic {

class TrafficSink;

struct RequestView {
    std::uint32_t route_key = 0;
    std::string_view payload;
    std::uint64_t conn_id = 0;
    std::uint32_t seq_num = 0;
    std::uint32_t attach_length = 0;
};

struct RecorderStatus {
    std::string state = "disabled";
    std::uint64_t observed_requests = 0;
    std::uint64_t recorded_requests = 0;
    std::uint64_t dropped_requests = 0;
    std::uint64_t bytes_written = 0;
    std::uint64_t segments = 0;
    std::uint64_t ring_used_bytes = 0;
    std::uint64_t ring_capacity_bytes = 0;
    bool stop_timed_out = false;
    bool format_complete = false;
    bool capture_complete = false;
    std::string session_dir;
};

class TrafficRecorder {
   public:
    static TrafficRecorder& Instance();

    // 读取配置并启动控制面；由 RegisterRpcService 在注册完成后调用。
    void Install(std::size_t rpc_threads = 0,
                 RedactionRegistry redaction_registry = {});
    bool Start();
    void Stop();
    void StopAsync();
    void OnRequest(const RequestView& view) noexcept;
    bool Recording() const noexcept;
    RecorderStatus GetStatus() const;
    void Shutdown();

   private:
    struct Shard;

    TrafficRecorder();
    ~TrafficRecorder();
    TrafficRecorder(const TrafficRecorder&) = delete;
    TrafficRecorder& operator=(const TrafficRecorder&) = delete;

    void WriterLoop();
    bool DrainShardDirect(Shard& shard, std::vector<struct iovec>& iov);
    bool DrainShardRedacted(Shard& shard);
    bool FlushRecords(Shard& shard, std::vector<struct iovec>& iov,
                      std::uint64_t bytes, std::uint32_t records,
                      std::uint64_t commit_tail);
    void StopInternal();
    void ResetConfig();
    bool ApplyOptions(const Json::Value& root, std::size_t rpc_threads);
    bool ApplyControl(const Json::Value& root);
    bool ApplyRoutes(const Json::Value& root);
    void SetState(const char* state);
    void StartControlServer();

    mutable std::mutex lifecycle_mu_;
    mutable std::mutex status_mu_;
    mutable std::mutex shards_mu_;
    mutable std::mutex writer_mu_;
    std::condition_variable writer_cv_;

    std::atomic<bool> active_{false};
    std::atomic<bool> shutdown_{false};
    std::atomic<bool> writer_stop_{false};
    std::atomic<std::uint32_t> in_request_{0};
    std::atomic<std::uint64_t> generation_{0};
    std::atomic<std::uint64_t> next_shard_{0};
    std::atomic<std::uint64_t> dropped_no_shard_{0};

    std::vector<std::unique_ptr<Shard>> shards_;
    std::thread writer_thread_;
    std::thread stop_thread_;

    std::unique_ptr<TrafficSink> sink_;
    std::uint64_t block_bytes_;
    std::uint64_t segment_bytes_;
    std::uint64_t segment_seconds_;
    std::uint64_t ring_bytes_;
    std::uint64_t max_record_bytes_;
    std::uint64_t max_total_bytes_;
    std::uint32_t ring_shards_;
    std::uint64_t stop_timeout_ms_;
    std::atomic<std::uint64_t> stop_deadline_ms_{0};
    std::atomic<bool> stop_timed_out_{false};
    std::atomic<bool> writer_failed_{false};

    std::string output_dir_;
    bool config_loaded_ = false;
    RedactionMode redaction_mode_ = RedactionMode::kNone;
    RedactionRegistry redaction_registry_;
    std::unordered_set<std::uint32_t> allowed_routes_;
    std::string control_host_;
    std::uint16_t control_port_;
    std::string control_token_;
    std::unique_ptr<cinatra::coro_http_server> control_server_;

    std::string state_ = "disabled";
    std::string session_dir_;
};

}  // namespace mooncake::traffic
