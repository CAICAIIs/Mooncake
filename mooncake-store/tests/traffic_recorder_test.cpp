#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <optional>
#include <string>

#include <async_simple/coro/SyncAwait.h>
#include "crc32c.h"
#include "common/network.h"
#include "rpc_worker_pool.h"
#include "traffic/file_sink.h"
#include "traffic/traffic_format.h"
#include "traffic/traffic_recorder.h"
#include "traffic/ylt_adapter.h"
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace mooncake::traffic {
namespace {

class TempDir {
   public:
    TempDir() {
        setenv("MOONCAKE_TRAFFIC_AUTOSTART", "0", 1);
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake_traffic_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(counter_++));
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    const std::filesystem::path& path() const { return path_; }

   private:
    std::filesystem::path path_;
    static inline int counter_ = 0;
};

class TrafficTestService {
   public:
    int Echo(int value) { return value; }
};

TEST(TrafficFormatTest, HeaderSizesAreStable) {
    EXPECT_EQ(sizeof(TrafficFileHeader), kTrafficFileHeaderSize);
    EXPECT_EQ(sizeof(TrafficBlockHeader), kTrafficBlockHeaderSize);
    EXPECT_EQ(sizeof(TrafficRecordHeader), kTrafficRecordHeaderSize);
    EXPECT_EQ(sizeof(TrafficFooter), kTrafficFooterSize);
}

TEST(FileSinkTest, StopsAtMaxTotalBytes) {
    TempDir dir;
    FileSink sink(4096, 300, 1024);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), TrafficNowMs()));
    std::array<char, 1024> data{};
    struct iovec first{data.data(), 512};
    ASSERT_TRUE(sink.WriteRecords(&first, 1, first.iov_len, 1));
    struct iovec second{data.data(), data.size()};
    EXPECT_FALSE(sink.WriteRecords(&second, 1, second.iov_len, 1));
    EXPECT_TRUE(sink.failed());
    sink.Close();
}

TEST(FileSinkTest, WritesHeaderRecordChecksumAndFooter) {
    TempDir dir;
    FileSink sink(4096, 300);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), TrafficNowMs()));
    const std::string session_dir = sink.session_dir();

    const std::string payload = "traffic-record";
    TrafficRecordHeader record;
    record.total_len =
        static_cast<std::uint32_t>(sizeof(record) + payload.size());
    record.route_key = 17;
    record.conn_id = 23;
    record.seq_num = 5;
    record.payload_len = static_cast<std::uint32_t>(payload.size());
    std::string record_bytes(sizeof(record), '\0');
    std::memcpy(record_bytes.data(), &record, sizeof(record));
    record_bytes.append(payload);
    struct iovec iov{record_bytes.data(), record_bytes.size()};
    ASSERT_TRUE(sink.WriteRecords(&iov, 1, record_bytes.size(), 1));
    sink.Close();

    std::ifstream input(session_dir + "/segment-000000.mcr", std::ios::binary);
    ASSERT_TRUE(input);
    const std::string file((std::istreambuf_iterator<char>(input)),
                           std::istreambuf_iterator<char>());
    ASSERT_EQ(file.size(), kTrafficFileHeaderSize + kTrafficBlockHeaderSize +
                               record_bytes.size() + kTrafficFooterSize);

    TrafficFileHeader file_header;
    std::memcpy(&file_header, file.data(), sizeof(file_header));
    EXPECT_EQ(file_header.magic, kTrafficFileMagic);

    TrafficBlockHeader block_header;
    std::memcpy(&block_header, file.data() + kTrafficFileHeaderSize,
                sizeof(block_header));
    EXPECT_EQ(block_header.magic, kTrafficBlockMagic);
    EXPECT_EQ(block_header.record_count, 1u);
    EXPECT_EQ(block_header.block_bytes, record_bytes.size());
    mooncake::Crc32c crc;
    crc.Extend(file.data() + kTrafficFileHeaderSize + kTrafficBlockHeaderSize,
               record_bytes.size());
    EXPECT_EQ(block_header.crc32, crc.Final());
    EXPECT_EQ(file.compare(kTrafficFileHeaderSize + kTrafficBlockHeaderSize,
                           record_bytes.size(), record_bytes),
              0);

    TrafficFooter footer;
    std::memcpy(&footer,
                file.data() + kTrafficFileHeaderSize + kTrafficBlockHeaderSize +
                    record_bytes.size(),
                sizeof(footer));
    EXPECT_EQ(footer.magic, kTrafficFooterMagic);
    EXPECT_EQ(footer.block_count, 1u);
    EXPECT_EQ(footer.record_count, 1u);
    EXPECT_EQ(footer.payload_bytes, record_bytes.size());
}

TEST(TrafficRecorderTest, RejectsShardCountBelowRpcThreads) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 65536,
          "ring_shards": 2,
          "max_record_bytes": 1024,
          "block_bytes": 2048,
          "segment_bytes": 32768,
          "segment_seconds": 300,
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": 37997,
                      "token": "test-token"}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(4);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();
    unsetenv("MOONCAKE_TRAFFIC_CONFIG");
}

TEST(TrafficRecorderTest, RejectsRedactedWithoutRoutes) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 65536,
          "ring_shards": 1,
          "max_record_bytes": 1024,
          "block_bytes": 2048,
          "segment_bytes": 32768,
          "segment_seconds": 300,
          "redaction": "redacted",
          "control": {"host": "127.0.0.1", "port": 37999,
                      "token": "test-token"}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();
    unsetenv("MOONCAKE_TRAFFIC_CONFIG");
}

TEST(TrafficRecorderTest, RejectsRedactedWithoutRedactor) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 65536,
          "ring_shards": 1,
          "max_record_bytes": 1024,
          "block_bytes": 2048,
          "segment_bytes": 32768,
          "segment_seconds": 300,
          "redaction": "redacted",
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": 37999,
                      "token": "test-token"}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();
    unsetenv("MOONCAKE_TRAFFIC_CONFIG");
}

TEST(TrafficRecorderTest, ReinstallReplacesPreviousRoutes) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](int route, int control_port) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 262144,
          "ring_shards": 2,
          "max_record_bytes": 2048,
          "block_bytes": 4096,
          "segment_bytes": 65536,
          "segment_seconds": 300,
          "routes": [")"
            << route << R"("],
          "control": {"host": "127.0.0.1", "port": )"
            << control_port << R"(,
                      "token": "test-token"}
        })";
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    auto& recorder = TrafficRecorder::Instance();
    write_config(1, 37997);
    recorder.Install();
    ASSERT_TRUE(recorder.Start());
    const std::string payload = "payload";
    recorder.OnRequest(RequestView{1, payload, 1, 1, 0});
    recorder.Stop();
    recorder.Shutdown();

    write_config(2, 37998);
    recorder.Install();
    ASSERT_TRUE(recorder.Start());
    recorder.OnRequest(RequestView{1, payload, 2, 1, 0});
    recorder.OnRequest(RequestView{2, payload, 2, 2, 0});
    recorder.Stop();
    const RecorderStatus status = recorder.GetStatus();
    EXPECT_EQ(status.recorded_requests, 1u);
    EXPECT_EQ(status.dropped_filtered, 1u);
}

TEST(TrafficRecorderTest, ControlTokenUsesSecureFilePermissions) {
    TempDir dir;
    const auto ports = getFreeTcpPorts(3);
    ASSERT_EQ(ports.size(), 3u);
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](int control_port,
                                  const std::string& token_file) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": )"
            << control_port;
        if (!token_file.empty()) {
            out << R"(, "token_file": ")" << token_file << '"';
        }
        out << "}}";
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    write_config(ports[0], {});
    recorder.Install();
    ASSERT_EQ(recorder.GetStatus().state, "idle");
    const auto token_path = dir.path() / "control.token";
    struct stat token_stat{};
    ASSERT_EQ(::stat(token_path.c_str(), &token_stat), 0);
    EXPECT_EQ(token_stat.st_mode & 0777, 0600);
    std::ifstream token_input(token_path);
    std::string token;
    std::getline(token_input, token);
    EXPECT_EQ(token.size(), 32u);
    recorder.Shutdown();

    {
        std::ofstream out(token_path, std::ios::trunc);
        out << "partial-token\n";
    }
    ASSERT_EQ(::chmod(token_path.c_str(), 0600), 0);
    write_config(ports[1], {});
    recorder.Install();
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    const auto insecure_token_path = dir.path() / "insecure.token";
    {
        std::ofstream out(insecure_token_path);
        out << "test-token\n";
    }
    ASSERT_EQ(::chmod(insecure_token_path.c_str(), 0644), 0);
    write_config(ports[2], insecure_token_path.string());
    recorder.Install();
    EXPECT_EQ(recorder.GetStatus().state, "error");
}

TEST(TrafficRecorderTest, CoroutineAdapterFiltersRequestAttachments) {
    TempDir dir;
    const auto ports = getFreeTcpPorts(2);
    ASSERT_EQ(ports.size(), 2u);
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto route_key = coro_rpc::func_id<&TrafficTestService::Echo>();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 65536,
          "ring_shards": 1,
          "max_record_bytes": 4096,
          "block_bytes": 4096,
          "segment_bytes": 32768,
          "routes": [")"
            << route_key << R"("],
          "control": {"host": "127.0.0.1", "port": )"
            << ports[1] << R"(,
                      "token": "test-token"}
        })";
    }
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::filesystem::path path;
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
            std::filesystem::remove_all(path);
        }
    } cleanup{dir.path(), previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_TRUE(recorder.Start());

    TrafficTestService service;
    RpcWorkerPool pool(1);
    RecordingServer server(1, ports[0], "127.0.0.1", std::chrono::seconds(0),
                           true);
    pool.Register<&TrafficTestService::Echo>(server, service);
    auto server_start = server.async_start();
    ASSERT_FALSE(server_start.hasResult());

    coro_rpc::coro_rpc_client client;
    auto ec = async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(ports[0])));
    ASSERT_FALSE(ec) << ec.message();
    auto response = async_simple::coro::syncAwait(
        client.call<&TrafficTestService::Echo>(7));
    ASSERT_TRUE(response.has_value()) << response.error().msg;
    EXPECT_EQ(response.value(), 7);

    client.set_req_attachment("attachment");
    auto attached_response = async_simple::coro::syncAwait(
        client.call<&TrafficTestService::Echo>(8));
    ASSERT_TRUE(attached_response.has_value()) << attached_response.error().msg;
    EXPECT_EQ(attached_response.value(), 8);

    server.stop();
    recorder.Stop();
    const RecorderStatus status = recorder.GetStatus();
    EXPECT_EQ(status.recorded_requests, 1u);
    EXPECT_EQ(status.dropped_filtered, 1u);
    EXPECT_FALSE(status.capture_complete);
}

TEST(TrafficRecorderTest, RecordsAcrossWrapAndRestarts) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 262144,
          "ring_shards": 2,
          "max_record_bytes": 2048,
          "block_bytes": 4096,
          "segment_bytes": 65536,
          "segment_seconds": 300,
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": 37999,
                      "token": "test-token"}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install();
    ASSERT_TRUE(recorder.Start());
    const std::string payload(128, 'x');
    for (int i = 0; i < 5000; ++i) {
        recorder.OnRequest(
            RequestView{1, payload, 1, static_cast<std::uint32_t>(i), 0});
    }
    recorder.Stop();
    const RecorderStatus first = recorder.GetStatus();
    EXPECT_GT(first.recorded_requests, 0u);
    EXPECT_FALSE(first.session_dir.empty());
    EXPECT_TRUE(std::filesystem::exists(first.session_dir + "/summary.json"));

    ASSERT_TRUE(recorder.Start());
    for (int i = 0; i < 100; ++i) {
        recorder.OnRequest(
            RequestView{1, payload, 2, static_cast<std::uint32_t>(i), 0});
    }
    recorder.Stop();
    const RecorderStatus second = recorder.GetStatus();
    EXPECT_GT(second.recorded_requests, 0u);
    EXPECT_NE(second.session_dir, first.session_dir);

    recorder.Shutdown();
}

TEST(TrafficRecorderTest, WriterFailureCanBeStoppedAndRestarted) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 262144,
          "ring_shards": 2,
          "max_record_bytes": 2048,
          "block_bytes": 4096,
          "segment_bytes": 65536,
          "segment_seconds": 300,
          "max_total_bytes": 131072,
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": 38001,
                      "token": "test-token"}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install();
    ASSERT_TRUE(recorder.Start());
    const std::string payload(512, 'y');
    for (int i = 0; i < 2000; ++i) {
        recorder.OnRequest(
            RequestView{1, payload, 3, static_cast<std::uint32_t>(i), 0});
    }
    for (int i = 0; i < 200 && recorder.GetStatus().state != "error"; ++i) {
        usleep(10000);
    }
    EXPECT_EQ(recorder.GetStatus().state, "error");

    recorder.Stop();
    const RecorderStatus failed = recorder.GetStatus();
    EXPECT_FALSE(failed.format_complete);
    EXPECT_FALSE(failed.capture_complete);

    ASSERT_TRUE(recorder.Start());
    recorder.OnRequest(RequestView{1, payload, 4, 0, 0});
    recorder.Stop();
    EXPECT_EQ(recorder.GetStatus().state, "idle");
    recorder.Shutdown();
}

TEST(TrafficRecorderTest, MinimalConfigurationRecordsAllRoutes) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "max_total_bytes": 1073741824
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    unsetenv("MOONCAKE_TRAFFIC_AUTOSTART");
    setenv("MOONCAKE_TRAFFIC_CONTROL_PORT", "37998", 1);
    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_EQ(recorder.GetStatus().state, "recording");

    const std::string payload = "minimal";
    recorder.OnRequest(RequestView{123, payload, 1, 1, 0});
    recorder.Stop();
    const RecorderStatus status = recorder.GetStatus();
    EXPECT_EQ(status.state, "idle");
    EXPECT_EQ(status.observed_requests, 1u);
    EXPECT_EQ(status.recorded_requests, 1u);
    EXPECT_EQ(status.dropped_filtered, 0u);

    const std::filesystem::path token_path = dir.path() / "control.token";
    ASSERT_TRUE(std::filesystem::exists(token_path));
    struct stat token_stat{};
    ASSERT_EQ(::stat(token_path.c_str(), &token_stat), 0);
    EXPECT_EQ(token_stat.st_mode & 0777, 0600);

    recorder.Shutdown();
    unsetenv("MOONCAKE_TRAFFIC_CONFIG");
    unsetenv("MOONCAKE_TRAFFIC_CONTROL_PORT");
}

TEST(TrafficRecorderTest, RejectsMalformedRouteConfiguration) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](const std::string& routes) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string() << R"(","routes":)"
            << routes
            << R"(,"control":{"host":"127.0.0.1","port":38012,"token":"test-token"}})";
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    write_config("\"1\"");
    recorder.Install();
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    write_config("[\"*\", \"1\"]");
    recorder.Install();
    EXPECT_EQ(recorder.GetStatus().state, "error");
}

}  // namespace
}  // namespace mooncake::traffic
