#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "client_service.h"
#include "common/client_buffer_allocation.h"
#include "random.h"
#include "types.h"

namespace mooncake::bench {
namespace {

struct Options {
    std::string master = "127.0.0.1:50051";
    std::string protocol = "tcp";
    std::string host = "127.0.0.1";
    std::string key_prefix = "mcbench";
    std::uint64_t keys = 100000000;
    std::size_t value_bytes = 512;
    std::size_t batch_size = 64;
    int fill_threads = 8;
    int read_threads = 16;
    std::uint64_t read_keys = 0;
    std::size_t segment_bytes = 0;
    std::size_t segments = 0;
    int provider_port = 19101;
    int consumer_port = 19102;
    std::uint64_t seed = 1;
    bool random_read = true;
};

void PrintUsage(const char* program) {
    std::fprintf(
        stderr,
        "usage: %s [options]\n"
        "  --master <host:port>       master address (default "
        "127.0.0.1:50051)\n"
        "  --protocol <tcp|rdma>      transfer protocol (default tcp)\n"
        "  --host <address>           local address for the clients "
        "(default 127.0.0.1)\n"
        "  --key-prefix <text>        namespace for every object key "
        "(default mcbench)\n"
        "  --keys <n>                 number of keys to fill (default "
        "100000000)\n"
        "  --value-bytes <n>          payload bytes per key (default 512)\n"
        "  --batch-size <n>           keys per BatchPut/BatchGet (default "
        "64)\n"
        "  --fill-threads <n>         concurrent fill threads (default 8)\n"
        "  --read-threads <n>         concurrent read threads (default 16)\n"
        "  --read-keys <n>            keys to read after fill; 0 means --keys\n"
        "  --segment-bytes <n>        bytes per provider segment; defaults to "
        "the payload plus one GiB, or 64 GiB for payloads above 8 GiB\n"
        "  --segments <n>             provider segments to mount; defaults to "
        "two above 32 GiB of payload, otherwise one\n"
        "  --provider-port <n>        provider client port (default 19101)\n"
        "  --consumer-port <n>        consumer client port (default 19102)\n"
        "  --seed <n>                 random seed for read keys (default 1)\n"
        "  --sequential-read          read consecutive keys instead of random "
        "batches\n",
        program);
}

bool ParseU64(const std::string& text, std::uint64_t* value) {
    if (text.empty() || text[0] == '-' || text[0] == '+') {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || end == text.c_str() || *end != '\0') {
        return false;
    }
    *value = parsed;
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "--help" || argument == "-h") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (argument == "--sequential-read") {
            options->random_read = false;
            continue;
        }
        if (i + 1 >= argc) {
            std::fprintf(stderr, "%s needs a value\n", argument.c_str());
            return false;
        }
        const std::string value = argv[++i];
        std::uint64_t number = 0;
        if (argument == "--master") {
            options->master = value;
        } else if (argument == "--protocol") {
            options->protocol = value;
        } else if (argument == "--host") {
            options->host = value;
        } else if (argument == "--key-prefix") {
            options->key_prefix = value;
        } else if (argument == "--keys") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --keys\n");
                return false;
            }
            options->keys = number;
        } else if (argument == "--value-bytes") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --value-bytes\n");
                return false;
            }
            options->value_bytes = static_cast<std::size_t>(number);
        } else if (argument == "--batch-size") {
            if (!ParseU64(value, &number) || number == 0 || number > 4096) {
                std::fprintf(stderr, "bad --batch-size\n");
                return false;
            }
            options->batch_size = static_cast<std::size_t>(number);
        } else if (argument == "--fill-threads") {
            if (!ParseU64(value, &number) || number == 0 || number > 256) {
                std::fprintf(stderr, "bad --fill-threads\n");
                return false;
            }
            options->fill_threads = static_cast<int>(number);
        } else if (argument == "--read-threads") {
            if (!ParseU64(value, &number) || number == 0 || number > 1024) {
                std::fprintf(stderr, "bad --read-threads\n");
                return false;
            }
            options->read_threads = static_cast<int>(number);
        } else if (argument == "--read-keys") {
            if (!ParseU64(value, &number)) {
                std::fprintf(stderr, "bad --read-keys\n");
                return false;
            }
            options->read_keys = number;
        } else if (argument == "--segment-bytes") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --segment-bytes\n");
                return false;
            }
            options->segment_bytes = static_cast<std::size_t>(number);
        } else if (argument == "--segments") {
            if (!ParseU64(value, &number) || number == 0 || number > 64) {
                std::fprintf(stderr, "bad --segments\n");
                return false;
            }
            options->segments = static_cast<std::size_t>(number);
        } else if (argument == "--provider-port" ||
                   argument == "--consumer-port") {
            if (!ParseU64(value, &number) || number == 0 || number > 65535) {
                std::fprintf(stderr, "bad %s\n", argument.c_str());
                return false;
            }
            if (argument == "--provider-port") {
                options->provider_port = static_cast<int>(number);
            } else {
                options->consumer_port = static_cast<int>(number);
            }
        } else if (argument == "--seed") {
            if (!ParseU64(value, &number)) {
                std::fprintf(stderr, "bad --seed\n");
                return false;
            }
            options->seed = number;
        } else {
            std::fprintf(stderr, "unknown option: %s\n", argument.c_str());
            return false;
        }
    }
    if (options->read_keys == 0) {
        options->read_keys = options->keys;
    }
    if (options->value_bytes >
        std::numeric_limits<std::size_t>::max() / options->keys) {
        std::fprintf(stderr, "keys*value-bytes overflows size_t\n");
        return false;
    }
    if (options->keys * options->value_bytes > (1ull << 40)) {
        std::fprintf(stderr,
                     "keys*value-bytes cannot exceed the one TiB segment "
                     "limit\n");
        return false;
    }
    if (options->batch_size > options->keys) {
        options->batch_size = static_cast<std::size_t>(options->keys);
    }
    return true;
}

std::uint64_t NowNs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

double Percentile(std::vector<std::uint64_t>* values, double percentile) {
    if (values->empty()) {
        return 0.0;
    }
    const std::size_t index = static_cast<std::size_t>(
        std::ceil(percentile / 100.0 * static_cast<double>(values->size())));
    const std::size_t clamped =
        std::min(values->size() - 1, index > 0 ? index - 1 : 0);
    std::nth_element(values->begin(), values->begin() + clamped, values->end());
    return static_cast<double>((*values)[clamped]);
}

std::string KeyFor(const Options& options, std::uint64_t index) {
    char suffix[32];
    std::snprintf(suffix, sizeof(suffix), "/%020llu",
                  static_cast<unsigned long long>(index));
    return options.key_prefix + suffix;
}

std::shared_ptr<Client> CreateClient(const Options& options, int port) {
    const std::string hostname = options.host + ":" + std::to_string(port);
    auto client = Client::Create(hostname, "P2PHANDSHAKE", options.protocol,
                                 std::nullopt, options.master);
    return client.has_value() ? client.value() : nullptr;
}

struct FillResult {
    std::uint64_t completed = 0;
    std::uint64_t errors = 0;
};

void FillWorker(Client& provider, const Options& options, void* source,
                int index, std::atomic<bool>* failed, FillResult* result) {
    const std::uint64_t begin =
        options.keys * static_cast<std::uint64_t>(index) /
        static_cast<std::uint64_t>(options.fill_threads);
    const std::uint64_t end = options.keys *
                              static_cast<std::uint64_t>(index + 1) /
                              static_cast<std::uint64_t>(options.fill_threads);

    ReplicateConfig config;
    config.replica_num = 1;

    std::vector<ObjectKey> keys;
    std::vector<std::vector<Slice>> slices;
    keys.reserve(options.batch_size);
    slices.reserve(options.batch_size);

    for (std::uint64_t base = begin; base < end && !failed->load();) {
        const std::size_t count = static_cast<std::size_t>(
            std::min<std::uint64_t>(options.batch_size, end - base));
        keys.clear();
        slices.clear();
        for (std::size_t i = 0; i < count; ++i) {
            keys.push_back(KeyFor(options, base + i));
            const auto offset =
                static_cast<char*>(source) + i * options.value_bytes;
            slices.push_back({Slice{offset, options.value_bytes}});
        }
        const auto results = provider.BatchPut(keys, slices, config);
        if (results.size() != keys.size()) {
            std::fprintf(stderr, "BatchPut returned %zu results for %zu keys\n",
                         results.size(), keys.size());
            failed->store(true);
            break;
        }
        for (const auto& item : results) {
            if (item.has_value()) {
                ++result->completed;
            } else {
                ++result->errors;
                std::fprintf(stderr, "put failed: %s\n",
                             mooncake::toString(item.error()).c_str());
                failed->store(true);
                break;
            }
        }
        if (failed->load()) {
            break;
        }
        base += count;
    }
}

struct ReadResult {
    std::uint64_t completed = 0;
    std::uint64_t errors = 0;
    std::uint64_t bytes = 0;
    std::vector<std::uint64_t> latency_us;
};

void ReadWorker(Client& consumer, const Options& options, char* buffer,
                std::size_t stride, int index, std::atomic<std::uint64_t>* next,
                std::atomic<bool>* failed, ReadResult* result) {
    std::mt19937_64 engine(options.seed + static_cast<std::uint64_t>(index));
    std::vector<ObjectKey> keys;
    std::unordered_map<std::string, std::vector<Slice>> destinations;
    keys.reserve(options.batch_size);
    destinations.reserve(options.batch_size);
    result->latency_us.reserve(
        static_cast<std::size_t>(options.read_keys / options.batch_size) + 1);

    for (;;) {
        const std::uint64_t sequence =
            next->fetch_add(options.batch_size, std::memory_order_relaxed);
        if (sequence >= options.read_keys || failed->load()) {
            break;
        }
        const std::size_t count =
            static_cast<std::size_t>(std::min<std::uint64_t>(
                options.batch_size, options.read_keys - sequence));
        std::uint64_t first = sequence;
        if (options.random_read) {
            const std::uint64_t span = options.keys - count;
            first = mooncake::randomIndex(span + 1, engine);
        } else {
            first %= options.keys;
        }

        keys.clear();
        destinations.clear();
        for (std::size_t i = 0; i < count; ++i) {
            const std::uint64_t key_id = (first + i) % options.keys;
            keys.push_back(KeyFor(options, key_id));
            auto& slices = destinations[keys.back()];
            slices.clear();
            slices.push_back(
                Slice{buffer + i * options.value_bytes, options.value_bytes});
        }

        const std::uint64_t begin = NowNs();
        const auto results = consumer.BatchGet(keys, destinations);
        const std::uint64_t end = NowNs();
        bool ok = results.size() == keys.size();
        for (const auto& item : results) {
            ok = ok && item.has_value();
        }
        if (!ok) {
            ++result->errors;
            for (const auto& item : results) {
                if (!item.has_value()) {
                    std::fprintf(stderr, "get failed: %s\n",
                                 mooncake::toString(item.error()).c_str());
                    break;
                }
            }
            failed->store(true);
            break;
        }
        ++result->completed;
        result->bytes += count * options.value_bytes;
        result->latency_us.push_back((end - begin) / 1000);
    }
}

std::string JsonEscape(const std::string& text) {
    std::string out;
    for (const char c : text) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out += c;
                break;
        }
    }
    return out;
}

}  // namespace
}  // namespace mooncake::bench

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    FLAGS_minloglevel = google::ERROR;

    mooncake::bench::Options options;
    if (!mooncake::bench::ParseOptions(argc, argv, &options)) {
        mooncake::bench::PrintUsage(argv[0]);
        return 2;
    }

    if (options.segment_bytes == 0) {
        const std::uint64_t payload = options.keys * options.value_bytes;
        options.segment_bytes =
            payload > (8ull << 30)
                ? 64ull << 30
                : static_cast<std::size_t>(payload + (1ull << 30));
    }
    if (options.segments == 0) {
        options.segments =
            options.keys * options.value_bytes > (32ull << 30) ? 2 : 1;
    }
    if (options.segment_bytes > (1ull << 40)) {
        std::fprintf(stderr, "segment size exceeds the one TiB limit\n");
        return 2;
    }
    if (options.segment_bytes >
        std::numeric_limits<std::size_t>::max() / options.segments) {
        std::fprintf(stderr, "segments*segment-bytes overflows size_t\n");
        return 2;
    }

    auto provider =
        mooncake::bench::CreateClient(options, options.provider_port);
    if (provider == nullptr) {
        std::fprintf(stderr, "cannot create the provider client\n");
        return 1;
    }
    std::vector<void*> provider_segments;
    provider_segments.reserve(options.segments);
    for (std::size_t i = 0; i < options.segments; ++i) {
        void* segment = mooncake::allocate_buffer_allocator_memory(
            options.segment_bytes, options.protocol);
        if (segment == nullptr) {
            std::fprintf(stderr,
                         "cannot allocate provider segment %zu of %zu\n", i + 1,
                         options.segments);
            return 1;
        }
        const auto mounted = provider->MountSegment(
            segment, options.segment_bytes, options.protocol);
        if (!mounted.has_value()) {
            std::fprintf(
                stderr, "cannot mount provider segment %zu of %zu: %s\n", i + 1,
                options.segments, mooncake::toString(mounted.error()).c_str());
            return 1;
        }
        provider_segments.push_back(segment);
    }

    const std::size_t source_bytes = std::max<std::size_t>(
        options.batch_size * options.value_bytes, 16u * 1024u * 1024u);
    void* source = mooncake::allocate_buffer_allocator_memory(source_bytes,
                                                              options.protocol);
    if (source == nullptr) {
        std::fprintf(stderr, "cannot allocate the source buffer\n");
        return 1;
    }
    std::memset(source, 0x5a, source_bytes);

    std::atomic<bool> failed{false};
    std::vector<mooncake::bench::FillResult> fill_results(
        static_cast<std::size_t>(options.fill_threads));
    const std::uint64_t fill_begin = mooncake::bench::NowNs();
    std::vector<std::thread> fillers;
    fillers.reserve(fill_results.size());
    for (std::size_t i = 0; i < fill_results.size(); ++i) {
        fillers.emplace_back([&, i] {
            mooncake::bench::FillWorker(*provider, options, source,
                                        static_cast<int>(i), &failed,
                                        &fill_results[i]);
        });
    }
    for (auto& thread : fillers) {
        thread.join();
    }
    const std::uint64_t fill_end = mooncake::bench::NowNs();

    std::uint64_t fill_completed = 0;
    std::uint64_t fill_errors = 0;
    for (const auto& result : fill_results) {
        fill_completed += result.completed;
        fill_errors += result.errors;
    }

    auto consumer =
        mooncake::bench::CreateClient(options, options.consumer_port);
    if (consumer == nullptr) {
        std::fprintf(stderr, "cannot create the consumer client\n");
        return 1;
    }
    const std::size_t read_stride = options.batch_size * options.value_bytes;
    const std::size_t read_bytes = std::max<std::size_t>(
        read_stride * static_cast<std::size_t>(options.read_threads),
        16u * 1024u * 1024u);
    void* reads = mooncake::allocate_buffer_allocator_memory(read_bytes,
                                                             options.protocol);
    if (reads == nullptr) {
        std::fprintf(stderr, "cannot allocate the read buffer\n");
        return 1;
    }
    const auto reads_mounted =
        consumer->MountSegment(reads, read_bytes, options.protocol);
    if (!reads_mounted.has_value()) {
        std::fprintf(stderr, "cannot mount the read segment: %s\n",
                     mooncake::toString(reads_mounted.error()).c_str());
        return 1;
    }

    std::atomic<std::uint64_t> next{0};
    std::vector<mooncake::bench::ReadResult> read_results(
        static_cast<std::size_t>(options.read_threads));
    const std::uint64_t read_begin = mooncake::bench::NowNs();
    std::vector<std::thread> readers;
    readers.reserve(read_results.size());
    for (std::size_t i = 0; i < read_results.size(); ++i) {
        readers.emplace_back([&, i] {
            mooncake::bench::ReadWorker(
                *consumer, options, static_cast<char*>(reads), read_stride,
                static_cast<int>(i), &next, &failed, &read_results[i]);
        });
    }
    for (auto& thread : readers) {
        thread.join();
    }
    const std::uint64_t read_end = mooncake::bench::NowNs();

    std::uint64_t read_completed = 0;
    std::uint64_t read_errors = 0;
    std::uint64_t read_bytes_done = 0;
    std::vector<std::uint64_t> latency_us;
    for (auto& result : read_results) {
        read_completed += result.completed;
        read_errors += result.errors;
        read_bytes_done += result.bytes;
        latency_us.insert(latency_us.end(), result.latency_us.begin(),
                          result.latency_us.end());
    }

    const double fill_seconds =
        static_cast<double>(fill_end - fill_begin) / 1e9;
    const double read_seconds =
        static_cast<double>(read_end - read_begin) / 1e9;
    const double total_gib =
        static_cast<double>(options.keys * options.value_bytes) /
        (1024.0 * 1024.0 * 1024.0);
    const double read_gib =
        static_cast<double>(read_bytes_done) / (1024.0 * 1024.0 * 1024.0);

    std::printf(
        "{\"tool\":\"memory_segment\",\"master\":\"%s\",\"protocol\":\"%s\","
        "\"keys\":%llu,\"value_bytes\":%zu,\"dataset_gib\":%.3f,"
        "\"batch_size\":%zu,\"fill_threads\":%d,\"read_threads\":%d,"
        "\"segments\":%zu,\"segment_bytes\":%zu,\"fill_completed\":%llu,\"fill_"
        "errors\":%llu,"
        "\"fill_seconds\":%.3f,\"fill_rps\":%.1f,\"fill_gib_s\":%.3f,"
        "\"read_keys\":%llu,\"read_completed\":%llu,\"read_errors\":%llu,"
        "\"read_seconds\":%.3f,\"read_rps\":%.1f,\"read_gib_s\":%.3f,"
        "\"read_us\":{\"p50\":%.0f,\"p90\":%.0f,\"p99\":%.0f,\"p999\":%.0f},"
        "\"key_prefix\":\"%s\",\"seed\":%llu,\"random_read\":%s}\n",
        options.master.c_str(), options.protocol.c_str(),
        static_cast<unsigned long long>(options.keys), options.value_bytes,
        total_gib, options.batch_size, options.fill_threads,
        options.read_threads, options.segments, options.segment_bytes,
        static_cast<unsigned long long>(fill_completed),
        static_cast<unsigned long long>(fill_errors), fill_seconds,
        static_cast<double>(fill_completed) / std::max(1e-9, fill_seconds),
        static_cast<double>(fill_completed * options.value_bytes) /
            (1024.0 * 1024.0 * 1024.0) / std::max(1e-9, fill_seconds),
        static_cast<unsigned long long>(options.read_keys),
        static_cast<unsigned long long>(read_completed),
        static_cast<unsigned long long>(read_errors), read_seconds,
        static_cast<double>(read_completed) / std::max(1e-9, read_seconds),
        read_gib / std::max(1e-9, read_seconds),
        mooncake::bench::Percentile(&latency_us, 50.0),
        mooncake::bench::Percentile(&latency_us, 90.0),
        mooncake::bench::Percentile(&latency_us, 99.0),
        mooncake::bench::Percentile(&latency_us, 99.9),
        mooncake::bench::JsonEscape(options.key_prefix).c_str(),
        static_cast<unsigned long long>(options.seed),
        options.random_read ? "true" : "false");

    consumer->UnmountSegment(reads, read_bytes);
    mooncake::free_memory(options.protocol, reads);
    for (void* segment : provider_segments) {
        provider->UnmountSegment(segment, options.segment_bytes);
        mooncake::free_memory(options.protocol, segment);
    }
    mooncake::free_memory(options.protocol, source);
    google::ShutdownGoogleLogging();
    return (fill_errors == 0 && read_errors == 0) ? 0 : 1;
}
