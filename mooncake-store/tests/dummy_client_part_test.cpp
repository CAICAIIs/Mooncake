// dummy_client_part_test.cpp
// Verifies DummyClient::put_part_from / get_part_into over the DummyClient ->
// RealClient RPC boundary: the two part handlers, the int conversion in
// DummyClient, and the round trip down to the master.
//
// The handler list below is local to this test, mirroring
// dummy_client_probe_key_test.cpp. The production list lives in
// RegisterClientRpcService() in real_client_main.cpp, which tests cannot
// link against.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "dummy_client.h"
#include "real_client.h"
#include "test_server_helpers.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

static void RegisterRpcHandlers(coro_rpc::coro_rpc_server &server,
                                RealClient &rc) {
    server.register_handler<&RealClient::service_ready_internal>(&rc);
    server.register_handler<&RealClient::ping>(&rc);
    server.register_handler<&RealClient::map_shm_internal>(&rc);
    server.register_handler<&RealClient::unmap_shm_internal>(&rc);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(&rc);
    server.register_handler<&RealClient::put_part_from_shm_helper>(&rc);
    server.register_handler<&RealClient::get_part_into_shm_helper>(&rc);
    server.register_handler<&RealClient::get_into_range_shm_helper>(&rc);
}

static constexpr size_t kMB = 1024ULL * 1024;
static constexpr size_t kSegmentSize = 128 * kMB;
static constexpr size_t kLocalBufSize = 64 * kMB;
static constexpr size_t kPoolSize = 64 * kMB;

class DummyClientPartTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("DummyClientPartTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
    }

    void TearDown() override {
        easylog::set_min_severity(easylog::Severity::FATAL);
        if (dummy_client_) dummy_client_->tearDownAll();
        if (rpc_server_) rpc_server_->stop();
        if (real_client_) real_client_->tearDownAll();
        master_.Stop();
        easylog::set_min_severity(easylog::Severity::WARN);
    }

    bool SetupStack() {
        if (!master_.Start(InProcMasterConfigBuilder().build())) return false;

        real_client_ = RealClient::create();
        const std::string rdma_devices =
            (FLAGS_protocol == "rdma") ? FLAGS_device_name : "";
        ipc_path_ = "@part_test_" + std::to_string(getpid()) + ".sock";
        if (real_client_->setup_real(
                "localhost:17820", "P2PHANDSHAKE", kSegmentSize, kLocalBufSize,
                FLAGS_protocol, rdma_devices, master_.master_address(), nullptr,
                ipc_path_) != 0) {
            return false;
        }

        rpc_port_ = getFreeTcpPort();
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/2, /*port=*/rpc_port_, /*address=*/"127.0.0.1",
            std::chrono::seconds(0), /*tcp_no_delay=*/true);
        RegisterRpcHandlers(*rpc_server_, *real_client_);
        auto ec = rpc_server_->async_start();
        if (ec.hasResult()) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        dummy_client_ = std::make_shared<DummyClient>();
        const std::string rpc_addr = "127.0.0.1:" + std::to_string(rpc_port_);
        return dummy_client_->setup_dummy(kPoolSize, kLocalBufSize, rpc_addr,
                                          ipc_path_) == 0;
    }

    mooncake::testing::InProcMaster master_;
    std::shared_ptr<RealClient> real_client_;
    std::shared_ptr<DummyClient> dummy_client_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    int rpc_port_ = 0;
    std::string ipc_path_;
};

TEST_F(DummyClientPartTest, PartsCrossTheRpcBoundaryOneAtATime) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    constexpr uint32_t kPartCount = 3;
    constexpr size_t kPartSize = 8192;
    const std::string key = "dummy_part_key";
    ReplicateConfig config;
    config.replica_num = 1;

    // The dummy client only accepts buffers from its own shared memory, so
    // every buffer here comes from its pool and is registered. The last one
    // receives the reads.
    auto take_buffer = [&]() -> void * {
        const uint64_t address = dummy_client_->alloc_from_mem_pool(kPartSize);
        if (address == 0) return nullptr;
        void *buffer = reinterpret_cast<void *>(address);
        if (dummy_client_->register_buffer(buffer, kPartSize) != 0) {
            ShmHelper::getInstance()->free(buffer);
            return nullptr;
        }
        return buffer;
    };
    std::vector<void *> buffers;
    buffers.reserve(2 * kPartCount + 1);

    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        void *source = take_buffer();
        ASSERT_NE(source, nullptr) << "part source " << part_index;
        buffers.push_back(source);
        std::memset(source, static_cast<char>('a' + part_index), kPartSize);

        EXPECT_EQ(dummy_client_->put_part_from(key, source, kPartSize,
                                               part_index, kPartCount, config),
                  0)
            << "part " << part_index;

        // No part reads before the last one lands.
        if (part_index + 1 < kPartCount) {
            void *destination = take_buffer();
            ASSERT_NE(destination, nullptr) << "early destination";
            buffers.push_back(destination);
            EXPECT_EQ(dummy_client_->get_part_into(key, destination, kPartSize,
                                                   part_index),
                      static_cast<int64_t>(
                          toInt(ErrorCode::REPLICA_IS_NOT_READY)));
        }
    }

    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        void *destination = take_buffer();
        ASSERT_NE(destination, nullptr) << "part destination " << part_index;
        buffers.push_back(destination);
        EXPECT_EQ(dummy_client_->get_part_into(key, destination, kPartSize,
                                               part_index),
                  static_cast<int64_t>(kPartSize));
        const std::string expected(kPartSize,
                                   static_cast<char>('a' + part_index));
        EXPECT_EQ(std::memcmp(destination, expected.data(), kPartSize), 0)
            << "part " << part_index;
    }

    // The whole-object read that predates parts has nothing to hand out.
    void *whole = take_buffer();
    ASSERT_NE(whole, nullptr) << "whole-object destination";
    buffers.push_back(whole);
    EXPECT_EQ(dummy_client_->get_into(key, whole, kPartSize),
              static_cast<int64_t>(toInt(ErrorCode::INVALID_PARAMS)));

    for (void *buffer : buffers) {
        EXPECT_EQ(dummy_client_->unregister_buffer(buffer), 0);
        ShmHelper::getInstance()->free(buffer);
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
