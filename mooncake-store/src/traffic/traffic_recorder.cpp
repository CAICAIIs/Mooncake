#include "traffic/traffic_recorder.h"

#include <json/json.h>
#include <ylt/coro_http/coro_http_server.hpp>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#endif
#include <sys/stat.h>
#include <sys/uio.h>
#include <limits.h>
#include <new>
#include <thread>
#include <utility>
#include <vector>

#include <glog/logging.h>

#include "traffic/file_sink.h"
#include "traffic/traffic_admin.h"
#include "traffic/traffic_format.h"

namespace mooncake::traffic {
namespace {

constexpr std::uint64_t kAccepting = 1ull << 63;
constexpr std::uint64_t kProducerMask = ~kAccepting;
constexpr std::uint64_t kMinShardBytes = 64ull * 1024;
constexpr std::uint64_t kDefaultStopTimeoutMs = 30000;
constexpr std::uint64_t kDefaultMaxTotalBytes = 10ull * 1024 * 1024 * 1024;
constexpr const char* kControlTokenFile = "control.token";
constexpr std::uint32_t kRingPadMagic = 0x50414431;  // PAD1
constexpr std::size_t kRingPadSize = 8;

std::uint64_t FloorPowerOfTwo(std::uint64_t value) {
    if (value == 0) {
        return 0;
    }
    std::uint64_t result = 1;
    while (result <= value / 2) {
        result <<= 1;
    }
    return result;
}

bool ParseSize(const Json::Value& node, const char* name, std::uint64_t* out) {
    if (node.isNull()) {
        return true;
    }
    if (!node.isUInt64()) {
        LOG(ERROR) << "traffic: config " << name << " must be an integer";
        return false;
    }
    *out = node.asUInt64();
    return true;
}

}  // namespace

struct alignas(64) TrafficRecorder::Shard {
    std::unique_ptr<char[]> ring;
    std::uint64_t capacity = 0;
    std::uint64_t mask = 0;
    alignas(64) std::atomic<std::uint64_t> head{0};
    std::atomic<std::uint64_t> state{1};
    std::atomic<std::uint64_t> observed{0};
    std::atomic<std::uint64_t> recorded{0};
    alignas(64) std::atomic<std::uint64_t> tail{0};
    alignas(64) std::atomic<std::uint64_t> dropped_ring{0};
    std::atomic<std::uint64_t> dropped_filtered{0};
    std::atomic<std::uint64_t> dropped_oversize{0};
    std::atomic<std::uint64_t> dropped_unredacted{0};
};

TrafficRecorder& TrafficRecorder::Instance() {
    static TrafficRecorder instance;
    return instance;
}

TrafficRecorder::TrafficRecorder() { ResetConfig(); }

TrafficRecorder::~TrafficRecorder() { Shutdown(); }

void TrafficRecorder::ResetConfig() {
    output_dir_.clear();
    block_bytes_ = 4ull * 1024 * 1024;
    segment_bytes_ = 256ull * 1024 * 1024;
    segment_seconds_ = 300;
    ring_bytes_ = 1024ull * 1024 * 1024;
    max_record_bytes_ = 4ull * 1024 * 1024;
    max_total_bytes_ = kDefaultMaxTotalBytes;
    ring_shards_ = 0;
    stop_timeout_ms_ = kDefaultStopTimeoutMs;
    control_host_ = "127.0.0.1";
    control_port_ = 37901;
    control_token_.clear();
    allowed_routes_.clear();
    redaction_mode_ = RedactionMode::kNone;
}

bool TrafficRecorder::ApplyOptions(const Json::Value& root,
                                   std::size_t rpc_threads) {
    if (!root["output_dir"].isString() ||
        root["output_dir"].asString().empty()) {
        LOG(ERROR) << "traffic: output_dir is required";
        SetState("error");
        return false;
    }
    output_dir_ = root["output_dir"].asString();
    if (root["redaction"].isString()) {
        const auto mode = root["redaction"].asString();
        if (mode == "none") {
            redaction_mode_ = RedactionMode::kNone;
        } else if (mode == "metadata") {
            redaction_mode_ = RedactionMode::kMetadata;
        } else if (mode == "redacted") {
            redaction_mode_ = RedactionMode::kRedacted;
        } else {
            LOG(ERROR) << "traffic: redaction must be none, metadata, or "
                          "redacted";
            SetState("error");
            return false;
        }
    }
    if (!ParseSize(root["ring_bytes"], "ring_bytes", &ring_bytes_) ||
        !ParseSize(root["max_record_bytes"], "max_record_bytes",
                   &max_record_bytes_) ||
        !ParseSize(root["segment_bytes"], "segment_bytes", &segment_bytes_) ||
        !ParseSize(root["segment_seconds"], "segment_seconds",
                   &segment_seconds_) ||
        !ParseSize(root["block_bytes"], "block_bytes", &block_bytes_) ||
        !ParseSize(root["max_total_bytes"], "max_total_bytes",
                   &max_total_bytes_) ||
        !ParseSize(root["stop_timeout_ms"], "stop_timeout_ms",
                   &stop_timeout_ms_)) {
        SetState("error");
        return false;
    }
    if (root["ring_shards"].isUInt()) {
        ring_shards_ = root["ring_shards"].asUInt();
    }
    if (ring_shards_ == 0) {
        const std::uint32_t fallback =
            std::max(1u, std::thread::hardware_concurrency());
        ring_shards_ =
            rpc_threads > 0
                ? static_cast<std::uint32_t>(std::min<std::size_t>(
                      rpc_threads, std::numeric_limits<std::uint32_t>::max()))
                : fallback;
    } else if (rpc_threads > 0 && ring_shards_ < rpc_threads) {
        LOG(ERROR) << "traffic: ring_shards " << ring_shards_
                   << " is smaller than rpc_threads " << rpc_threads;
        SetState("error");
        return false;
    }
    if (block_bytes_ == 0 || block_bytes_ > segment_bytes_) {
        LOG(ERROR) << "traffic: block_bytes must be in (0, segment_bytes]";
        SetState("error");
        return false;
    }
    if (max_record_bytes_ == 0 || max_record_bytes_ > block_bytes_) {
        LOG(ERROR) << "traffic: max_record_bytes must be in (0, block_bytes]";
        SetState("error");
        return false;
    }
    if (max_total_bytes_ > 0 && max_total_bytes_ < segment_bytes_) {
        LOG(ERROR) << "traffic: max_total_bytes must be >= segment_bytes";
        SetState("error");
        return false;
    }
    return true;
}

bool TrafficRecorder::ApplyControl(const Json::Value& root) {
    if (const char* port = std::getenv("MOONCAKE_TRAFFIC_CONTROL_PORT");
        port != nullptr && *port != '\0') {
        const int parsed = std::atoi(port);
        if (parsed > 0 && parsed < 65536) {
            control_port_ = static_cast<std::uint16_t>(parsed);
        }
    }
    std::string token_file;
    if (root["control"].isObject()) {
        const Json::Value& control = root["control"];
        if (control["host"].isString()) {
            control_host_ = control["host"].asString();
        }
        if (control["port"].isUInt()) {
            const auto port = control["port"].asUInt();
            if (port > 0 && port < 65536) {
                control_port_ = static_cast<std::uint16_t>(port);
            }
        }
        if (control["token"].isString()) {
            control_token_ = control["token"].asString();
        }
        if (control["token_file"].isString()) {
            token_file = control["token_file"].asString();
        }
    }
    if (control_host_ != "127.0.0.1" && control_host_ != "localhost" &&
        control_host_ != "::1") {
        LOG(ERROR) << "traffic: control.host must be a loopback address";
        SetState("error");
        return false;
    }
    if (control_token_.empty() && !token_file.empty()) {
        if (!ReadControlToken(token_file, &control_token_)) {
            LOG(ERROR) << "traffic: cannot read control.token_file "
                       << token_file;
            SetState("error");
            return false;
        }
    }
    if (!control_token_.empty()) {
        return true;
    }
    std::error_code ec;
    const bool created = std::filesystem::create_directories(output_dir_, ec);
    if (ec || (!created && !std::filesystem::is_directory(output_dir_))) {
        LOG(ERROR) << "traffic: cannot create output_dir " << output_dir_;
        SetState("error");
        return false;
    }
    if (created) {
        ::chmod(output_dir_.c_str(), 0700);
    }
    const std::filesystem::path token_path =
        std::filesystem::path(output_dir_) / kControlTokenFile;
    if (!ReadControlToken(token_path, &control_token_, true)) {
        std::error_code token_ec;
        const bool token_exists = std::filesystem::exists(token_path, token_ec);
        if (token_ec || token_exists ||
            !GenerateControlToken(&control_token_) ||
            !WriteControlToken(token_path, &control_token_)) {
            LOG(ERROR) << "traffic: cannot create a secure control token at "
                       << token_path;
            SetState("error");
            return false;
        }
    }
    return true;
}

bool TrafficRecorder::ApplyRoutes(const Json::Value& root) {
    if (root.isMember("routes") && !root["routes"].isArray()) {
        LOG(ERROR) << "traffic: routes must be an array";
        SetState("error");
        return false;
    }
    if (root["routes"].isArray()) {
        for (const auto& item : root["routes"]) {
            if (!item.isString()) {
                LOG(ERROR) << "traffic: routes[] must contain strings";
                SetState("error");
                return false;
            }
            const std::string route = item.asString();
            if (route == "*") {
                if (root["routes"].size() != 1) {
                    LOG(ERROR) << "traffic: wildcard route cannot be combined "
                                  "with numeric routes";
                    SetState("error");
                    return false;
                }
                allowed_routes_.clear();
                continue;
            }
            char* end = nullptr;
            errno = 0;
            const unsigned long key = std::strtoul(route.c_str(), &end, 10);
            if (end == route.c_str() || *end != '\0' || errno == ERANGE ||
                key > std::numeric_limits<std::uint32_t>::max()) {
                LOG(ERROR) << "traffic: invalid route key " << route;
                SetState("error");
                return false;
            }
            allowed_routes_.insert(static_cast<std::uint32_t>(key));
        }
    }
    return true;
}

void TrafficRecorder::Install(std::size_t rpc_threads,
                              RedactionRegistry redaction_registry) {
    if (config_loaded_ || shutdown_.load(std::memory_order_acquire)) {
        return;
    }
    const char* path = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    if (path == nullptr || *path == '\0') {
        return;
    }
    redaction_registry_ = std::move(redaction_registry);
    ResetConfig();
    const std::string config_path(path);
    std::ifstream input(config_path);
    if (!input) {
        LOG(ERROR) << "traffic: cannot open config " << config_path;
        SetState("error");
        return;
    }
    Json::Value root;
    Json::CharReaderBuilder builder;
    std::string errors;
    if (!Json::parseFromStream(builder, input, &root, &errors)) {
        LOG(ERROR) << "traffic: invalid config: " << errors;
        SetState("error");
        return;
    }
    if (!root.isObject()) {
        LOG(ERROR) << "traffic: config root must be an object";
        SetState("error");
        return;
    }
    if (!ApplyOptions(root, rpc_threads)) {
        return;
    }
    if (!ApplyControl(root)) {
        return;
    }
    if (!ApplyRoutes(root)) {
        return;
    }
    if (redaction_mode_ == RedactionMode::kRedacted) {
        if (allowed_routes_.empty()) {
            LOG(ERROR) << "traffic: redacted mode requires an explicit routes "
                          "list";
            SetState("error");
            return;
        }
        for (const auto route_key : allowed_routes_) {
            if (!redaction_registry_.Contains(route_key)) {
                LOG(ERROR) << "traffic: no redactor for route " << route_key;
                SetState("error");
                return;
            }
        }
    }
    config_loaded_ = true;
    SetState("idle");
    static std::once_flag atexit_once;
    std::call_once(atexit_once, [] {
        std::atexit([] { TrafficRecorder::Instance().Shutdown(); });
    });
    StartControlServer();
    {
        std::lock_guard<std::mutex> status_lock(status_mu_);
        if (state_ == "error") {
            return;
        }
    }
    const char* auto_start = std::getenv("MOONCAKE_TRAFFIC_AUTOSTART");
    if (auto_start == nullptr || std::strcmp(auto_start, "0") != 0) {
        if (!Start()) {
            LOG(ERROR) << "traffic: autostart failed";
        }
    }
}

bool TrafficRecorder::Start() {
    std::unique_lock<std::mutex> lifecycle(lifecycle_mu_);
    if (!config_loaded_ || shutdown_.load(std::memory_order_acquire)) {
        return false;
    }
    if (stop_thread_.joinable()) {
        stop_thread_.join();
    }
    if (active_.load(std::memory_order_acquire)) {
        return false;
    }
    if (writer_thread_.joinable()) {
        lifecycle.unlock();
        Stop();
        lifecycle.lock();
    }
    writer_failed_.store(false, std::memory_order_relaxed);
    stop_timed_out_.store(false, std::memory_order_relaxed);
    attachment_dropped_.store(false, std::memory_order_relaxed);
    stop_deadline_ms_.store(0, std::memory_order_relaxed);
    const std::uint64_t shard_bytes =
        FloorPowerOfTwo(ring_bytes_ / std::max(1u, ring_shards_));
    if (shard_bytes < kMinShardBytes) {
        LOG(ERROR) << "traffic: ring is too small";
        SetState("error");
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        shards_.clear();
        shards_.reserve(ring_shards_);
        for (std::uint32_t i = 0; i < ring_shards_; ++i) {
            auto shard = std::make_unique<Shard>();
            shard->ring = std::make_unique<char[]>(shard_bytes);
            shard->capacity = shard_bytes;
            shard->mask = shard_bytes - 1;
            shard->state.store(kAccepting, std::memory_order_relaxed);
            shards_.push_back(std::move(shard));
        }
        next_shard_.store(0, std::memory_order_relaxed);
        dropped_no_shard_.store(0, std::memory_order_relaxed);
    }
    {
        std::lock_guard<std::mutex> status_lock(status_mu_);
        sink_.reset();
        sink_ = std::make_unique<FileSink>(segment_bytes_, segment_seconds_,
                                           max_total_bytes_, redaction_mode_);
        const std::uint64_t start_ms = TrafficNowMs();
        if (!sink_->OpenSession(output_dir_, start_ms)) {
            state_ = "error";
            return false;
        }
        session_dir_ = sink_->session_dir();
    }
    writer_stop_.store(false, std::memory_order_release);
    generation_.fetch_add(1, std::memory_order_acq_rel);
    writer_thread_ = std::thread(&TrafficRecorder::WriterLoop, this);
    SetState("recording");
    active_.store(true, std::memory_order_release);
    return true;
}

void TrafficRecorder::Stop() {
    StopAsync();
    std::lock_guard<std::mutex> lifecycle(lifecycle_mu_);
    if (stop_thread_.joinable()) {
        stop_thread_.join();
    }
}

void TrafficRecorder::StopAsync() {
    std::lock_guard<std::mutex> lifecycle(lifecycle_mu_);
    if (stop_thread_.joinable()) {
        return;
    }
    if (!active_.load(std::memory_order_acquire) &&
        !writer_thread_.joinable()) {
        return;
    }
    SetState("stopping");
    stop_timeout_ms_ =
        stop_timeout_ms_ == 0 ? kDefaultStopTimeoutMs : stop_timeout_ms_;
    stop_deadline_ms_.store(TrafficNowMs() + stop_timeout_ms_,
                            std::memory_order_relaxed);
    stop_thread_ = std::thread(&TrafficRecorder::StopInternal, this);
}

void TrafficRecorder::StopInternal() {
    const std::uint64_t stop_begin_ms = TrafficNowMs();
    active_.store(false, std::memory_order_release);
    while (in_request_.load(std::memory_order_acquire) != 0) {
        std::this_thread::yield();
    }
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (auto& shard : shards_) {
            shard->state.fetch_and(~kAccepting, std::memory_order_acq_rel);
        }
    }
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (auto& shard : shards_) {
            while ((shard->state.load(std::memory_order_acquire) &
                    kProducerMask) != 0) {
                std::this_thread::yield();
            }
        }
    }
    writer_stop_.store(true, std::memory_order_release);
    writer_cv_.notify_all();
    if (writer_thread_.joinable()) {
        writer_thread_.join();
    }
    if (sink_ != nullptr) {
        std::vector<struct iovec> iov;
        iov.reserve(static_cast<std::size_t>(IOV_MAX));
        for (auto& shard : shards_) {
            const bool ok = redaction_mode_ == RedactionMode::kNone
                                ? DrainShardDirect(*shard, iov)
                                : DrainShardRedacted(*shard);
            if (!ok) {
                writer_failed_.store(true, std::memory_order_relaxed);
            }
        }
        sink_->Close();
    }
    const std::uint64_t elapsed_ms = TrafficNowMs() - stop_begin_ms;
    if (elapsed_ms > stop_timeout_ms_) {
        stop_timed_out_.store(true, std::memory_order_relaxed);
    }
    stop_deadline_ms_.store(0, std::memory_order_relaxed);
    const bool failed = writer_failed_.load(std::memory_order_relaxed) ||
                        (sink_ != nullptr && sink_->failed());
    SetState(failed ? "error" : "idle");
    WriteSummaryJson(session_dir_, GetStatus());
}

void TrafficRecorder::SetState(const char* state) {
    std::lock_guard<std::mutex> lock(status_mu_);
    state_ = state;
}

bool TrafficRecorder::Recording() const noexcept {
    return active_.load(std::memory_order_acquire);
}

void TrafficRecorder::OnRequest(const RequestView& view) noexcept {
    in_request_.fetch_add(1, std::memory_order_acq_rel);
    struct InRequestGuard {
        std::atomic<std::uint32_t>& count;
        ~InRequestGuard() { count.fetch_sub(1, std::memory_order_acq_rel); }
    } in_request_guard{in_request_};
    if (!active_.load(std::memory_order_acquire)) {
        return;
    }
    static thread_local Shard* tls_shard = nullptr;
    static thread_local std::uint64_t tls_generation = 0;
    const std::uint64_t generation =
        generation_.load(std::memory_order_relaxed);
    if (tls_shard == nullptr || tls_generation != generation) {
        // shards_ is immutable while active_; binding must not wait on the
        // writer lock on the request path.
        const std::uint64_t index =
            next_shard_.fetch_add(1, std::memory_order_relaxed);
        if (index >= shards_.size()) {
            dropped_no_shard_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        tls_shard = shards_[index].get();
        tls_generation = generation;
    }
    Shard& shard = *tls_shard;
    shard.observed.fetch_add(1, std::memory_order_relaxed);
    if (!allowed_routes_.empty() &&
        allowed_routes_.find(view.route_key) == allowed_routes_.end()) {
        shard.dropped_filtered.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    if (view.attach_length != 0) {
        attachment_dropped_.store(true, std::memory_order_relaxed);
        shard.dropped_filtered.fetch_add(1, std::memory_order_relaxed);
        return;
    }
    const std::uint64_t total = kTrafficRecordHeaderSize +
                                static_cast<std::uint64_t>(view.payload.size());
    if (total > max_record_bytes_ || total > shard.capacity) {
        shard.dropped_oversize.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    std::uint64_t state = shard.state.load(std::memory_order_acquire);
    for (;;) {
        if ((state & kAccepting) == 0) {
            return;
        }
        if (shard.state.compare_exchange_weak(state, state + 1,
                                              std::memory_order_acq_rel,
                                              std::memory_order_acquire)) {
            break;
        }
    }

    std::uint64_t head = shard.head.load(std::memory_order_relaxed);
    const std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    std::uint64_t used = head - tail;
    std::uint64_t index = head & shard.mask;
    if (index + total > shard.capacity) {
        const std::uint64_t remaining = shard.capacity - index;
        if (used + remaining > shard.capacity) {
            shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
            shard.state.fetch_sub(1, std::memory_order_release);
            return;
        }
        if (remaining >= kRingPadSize) {
            const std::uint32_t pad_len = static_cast<std::uint32_t>(remaining);
            const std::uint32_t pad_magic = kRingPadMagic;
            std::memcpy(shard.ring.get() + index, &pad_len, sizeof(pad_len));
            std::memcpy(shard.ring.get() + index + sizeof(pad_len), &pad_magic,
                        sizeof(pad_magic));
        }
        head += remaining;
        used += remaining;
        index = 0;
    }
    if (total > shard.capacity - used) {
        shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
        shard.state.fetch_sub(1, std::memory_order_release);
        return;
    }

    TrafficRecordHeader header;
    header.total_len = static_cast<std::uint32_t>(total);
    header.route_key = view.route_key;
    header.conn_id = view.conn_id;
    header.seq_num = view.seq_num;
    header.payload_len = static_cast<std::uint32_t>(view.payload.size());
    header.timestamp_ms = TrafficNowMs();
    char* base = shard.ring.get() + index;
    std::memcpy(base, &header, sizeof(header));
    if (!view.payload.empty()) {
        std::memcpy(base + sizeof(header), view.payload.data(),
                    view.payload.size());
    }
    shard.head.store(head + total, std::memory_order_release);
    shard.recorded.fetch_add(1, std::memory_order_relaxed);
    shard.state.fetch_sub(1, std::memory_order_release);
}

void TrafficRecorder::WriterLoop() {
#if defined(__linux__)
    if (const char* cpu_env = std::getenv("MOONCAKE_TRAFFIC_WRITER_CPU")) {
        const long cpu = std::strtol(cpu_env, nullptr, 10);
        if (cpu >= 0 && cpu < CPU_SETSIZE) {
            cpu_set_t set;
            CPU_ZERO(&set);
            CPU_SET(static_cast<int>(cpu), &set);
            pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
        }
    }
#endif
    std::vector<struct iovec> iov;
    iov.reserve(static_cast<std::size_t>(IOV_MAX));
    while (true) {
        {
            std::unique_lock<std::mutex> lock(writer_mu_);
            writer_cv_.wait_for(lock, std::chrono::milliseconds(1), [this] {
                return writer_stop_.load(std::memory_order_acquire);
            });
        }
        for (auto& shard : shards_) {
            const bool ok = redaction_mode_ == RedactionMode::kNone
                                ? DrainShardDirect(*shard, iov)
                                : DrainShardRedacted(*shard);
            if (!ok) {
                writer_failed_.store(true, std::memory_order_relaxed);
                active_.store(false, std::memory_order_release);
                SetState("error");
                writer_stop_.store(true, std::memory_order_release);
                return;
            }
        }
        if (sink_ != nullptr && sink_->failed()) {
            writer_failed_.store(true, std::memory_order_relaxed);
            active_.store(false, std::memory_order_release);
            SetState("error");
            writer_stop_.store(true, std::memory_order_release);
            break;
        }
        if (writer_stop_.load(std::memory_order_acquire)) {
            break;
        }
    }
}

bool TrafficRecorder::DrainShardDirect(Shard& shard,
                                       std::vector<struct iovec>& iov) {
    std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    const std::uint64_t head = shard.head.load(std::memory_order_acquire);
    if (tail == head) {
        return true;
    }
    iov.clear();
    std::uint64_t bytes = 0;
    std::uint32_t records = 0;
    std::uint64_t commit_tail = tail;
    while (tail != head) {
        const std::uint64_t index = tail & shard.mask;
        if (index + kRingPadSize > shard.capacity) {
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return false;
            }
            bytes = 0;
            records = 0;
            tail += shard.capacity - index;
            commit_tail = tail;
            shard.tail.store(tail, std::memory_order_release);
            continue;
        }
        std::uint32_t pad_len = 0;
        std::uint32_t pad_magic = 0;
        std::memcpy(&pad_len, shard.ring.get() + index, sizeof(pad_len));
        std::memcpy(&pad_magic, shard.ring.get() + index + sizeof(pad_len),
                    sizeof(pad_magic));
        if (pad_magic == kRingPadMagic) {
            if (pad_len < kRingPadSize || pad_len > shard.capacity - index) {
                shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
                shard.tail.store(head, std::memory_order_release);
                return true;
            }
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return false;
            }
            bytes = 0;
            records = 0;
            tail += pad_len;
            commit_tail = tail;
            shard.tail.store(tail, std::memory_order_release);
            continue;
        }
        TrafficRecordHeader header;
        std::memcpy(&header, shard.ring.get() + index, sizeof(header));
        if (header.total_len < kTrafficRecordHeaderSize ||
            header.total_len > shard.capacity ||
            index + header.total_len > shard.capacity) {
            shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
            shard.tail.store(head, std::memory_order_release);
            return true;
        }
        if (records > 0 && (bytes + header.total_len > block_bytes_ ||
                            iov.size() >= static_cast<std::size_t>(IOV_MAX))) {
            if (!FlushRecords(shard, iov, bytes, records, commit_tail)) {
                return false;
            }
            bytes = 0;
            records = 0;
        }
        iov.push_back({shard.ring.get() + index, header.total_len});
        bytes += header.total_len;
        ++records;
        tail += header.total_len;
        commit_tail = tail;
    }
    return FlushRecords(shard, iov, bytes, records, commit_tail);
}

bool TrafficRecorder::DrainShardRedacted(Shard& shard) {
    std::uint64_t tail = shard.tail.load(std::memory_order_acquire);
    const std::uint64_t head = shard.head.load(std::memory_order_acquire);
    std::string redacted;
    while (tail != head) {
        const std::uint64_t index = tail & shard.mask;
        if (index + kRingPadSize > shard.capacity) {
            tail += shard.capacity - index;
            shard.tail.store(tail, std::memory_order_release);
            continue;
        }
        std::uint32_t pad_len = 0;
        std::uint32_t pad_magic = 0;
        std::memcpy(&pad_len, shard.ring.get() + index, sizeof(pad_len));
        std::memcpy(&pad_magic, shard.ring.get() + index + sizeof(pad_len),
                    sizeof(pad_magic));
        if (pad_magic == kRingPadMagic) {
            if (pad_len < kRingPadSize || pad_len > shard.capacity - index) {
                shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
                shard.tail.store(head, std::memory_order_release);
                return true;
            }
            tail += pad_len;
            shard.tail.store(tail, std::memory_order_release);
            continue;
        }
        TrafficRecordHeader header;
        std::memcpy(&header, shard.ring.get() + index, sizeof(header));
        if (header.total_len < kTrafficRecordHeaderSize ||
            header.total_len > shard.capacity ||
            index + header.total_len > shard.capacity ||
            header.payload_len != header.total_len - kTrafficRecordHeaderSize) {
            shard.dropped_ring.fetch_add(1, std::memory_order_relaxed);
            shard.tail.store(head, std::memory_order_release);
            return true;
        }
        const char* body = shard.ring.get() + index + sizeof(header);
        redacted.clear();
        bool ok = true;
        if (redaction_mode_ == RedactionMode::kRedacted) {
            ok = redaction_registry_.Redact(
                header.route_key, std::string_view(body, header.payload_len),
                redacted);
        }
        if (!ok) {
            shard.dropped_unredacted.fetch_add(1, std::memory_order_relaxed);
            tail += header.total_len;
            shard.tail.store(tail, std::memory_order_release);
            continue;
        }
        TrafficRecordHeader output = header;
        output.payload_len = static_cast<std::uint32_t>(redacted.size());
        output.total_len = static_cast<std::uint32_t>(kTrafficRecordHeaderSize +
                                                      redacted.size());
        struct iovec iov[2] = {
            {&output, sizeof(output)},
            {redacted.empty() ? nullptr : redacted.data(), redacted.size()},
        };
        const int iov_count = redacted.empty() ? 1 : 2;
        if (sink_ == nullptr ||
            !sink_->WriteRecords(iov, iov_count, output.total_len, 1)) {
            return false;
        }
        tail += header.total_len;
        shard.tail.store(tail, std::memory_order_release);
    }
    return true;
}

bool TrafficRecorder::FlushRecords(Shard& shard, std::vector<struct iovec>& iov,
                                   std::uint64_t bytes, std::uint32_t records,
                                   std::uint64_t commit_tail) {
    if (iov.empty()) {
        shard.tail.store(commit_tail, std::memory_order_release);
        return true;
    }
    if (sink_ == nullptr ||
        !sink_->WriteRecords(iov.data(), static_cast<int>(iov.size()), bytes,
                             records)) {
        LOG(ERROR) << "traffic: write records failed";
        return false;
    }
    shard.tail.store(commit_tail, std::memory_order_release);
    iov.clear();
    return true;
}

void TrafficRecorder::StartControlServer() {
    control_server_ = traffic::StartControlServer(
        *this, control_host_, control_port_, control_token_);
    if (control_server_ == nullptr) {
        SetState("error");
    }
}

RecorderStatus TrafficRecorder::GetStatus() const {
    RecorderStatus status;
    {
        std::lock_guard<std::mutex> lock(status_mu_);
        status.state = state_;
        status.session_dir = session_dir_;
    }
    std::uint64_t observed = 0;
    std::uint64_t recorded = 0;
    std::uint64_t dropped_ring = 0;
    std::uint64_t dropped_filtered = 0;
    std::uint64_t dropped_oversize = 0;
    std::uint64_t dropped_unredacted = 0;
    std::uint64_t used = 0;
    std::uint64_t capacity = 0;
    {
        std::lock_guard<std::mutex> lock(shards_mu_);
        for (const auto& shard : shards_) {
            observed += shard->observed.load(std::memory_order_relaxed);
            recorded += shard->recorded.load(std::memory_order_relaxed);
            dropped_ring += shard->dropped_ring.load(std::memory_order_relaxed);
            dropped_filtered +=
                shard->dropped_filtered.load(std::memory_order_relaxed);
            dropped_oversize +=
                shard->dropped_oversize.load(std::memory_order_relaxed);
            dropped_unredacted +=
                shard->dropped_unredacted.load(std::memory_order_relaxed);
            used += shard->head.load(std::memory_order_relaxed) -
                    shard->tail.load(std::memory_order_relaxed);
            capacity += shard->capacity;
        }
    }
    status.observed_requests = observed;
    status.recorded_requests = recorded;
    status.dropped_ring =
        dropped_ring + dropped_no_shard_.load(std::memory_order_relaxed);
    status.dropped_filtered = dropped_filtered;
    status.dropped_oversize = dropped_oversize;
    status.dropped_unredacted = dropped_unredacted;
    status.dropped_requests =
        status.dropped_ring + dropped_oversize + dropped_unredacted;
    status.ring_used_bytes = used;
    status.ring_capacity_bytes = capacity;
    {
        std::lock_guard<std::mutex> status_lock(status_mu_);
        if (sink_ != nullptr) {
            status.bytes_written = sink_->bytes_written();
            status.segments = sink_->segments();
            status.format_complete =
                status.state == "idle" && !sink_->failed() &&
                !writer_failed_.load(std::memory_order_relaxed);
        }
    }
    status.stop_timed_out = stop_timed_out_.load(std::memory_order_relaxed);
    const std::uint64_t deadline =
        stop_deadline_ms_.load(std::memory_order_relaxed);
    if (status.state == "stopping" && deadline != 0 &&
        TrafficNowMs() >= deadline) {
        status.stop_timed_out = true;
    }
    status.capture_complete =
        status.dropped_requests == 0 && status.format_complete &&
        !status.stop_timed_out &&
        !attachment_dropped_.load(std::memory_order_relaxed);
    return status;
}

void TrafficRecorder::Shutdown() {
    if (shutdown_.exchange(true)) {
        return;
    }
    Stop();
    if (control_server_ != nullptr) {
        control_server_->stop();
        control_server_.reset();
    }
    config_loaded_ = false;
    ResetConfig();
    shutdown_.store(false, std::memory_order_release);
}

}  // namespace mooncake::traffic
