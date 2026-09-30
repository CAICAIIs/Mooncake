#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace mooncake {

class MemcpyWorkerPoolTestPeer {
   public:
    static size_t WorkerCount(const MemcpyWorkerPool& pool) {
        return pool.workers_.size();
    }
};

namespace {

class ScopedMemcpyWorkersEnv {
   public:
    explicit ScopedMemcpyWorkersEnv(const char* value) {
        if (const char* old = std::getenv("MC_STORE_MEMCPY_WORKERS")) {
            original_ = old;
        }
        EXPECT_EQ(setenv("MC_STORE_MEMCPY_WORKERS", value, 1), 0);
    }

    ~ScopedMemcpyWorkersEnv() {
        if (original_) {
            EXPECT_EQ(setenv("MC_STORE_MEMCPY_WORKERS", original_->c_str(), 1),
                      0);
        } else {
            EXPECT_EQ(unsetenv("MC_STORE_MEMCPY_WORKERS"), 0);
        }
    }

   private:
    std::optional<std::string> original_;
};

TEST(MemcpyWorkerPoolTest, WorkerCountComesFromTheEnvironment) {
    google::InitGoogleLogging("MemcpyWorkerPoolTest");
    ScopedMemcpyWorkersEnv env("3");
    // The pool reads the worker count when it is constructed, so later changes
    // to the environment do not resize an existing pool.
    {
        MemcpyWorkerPool pool;
        EXPECT_EQ(MemcpyWorkerPoolTestPeer::WorkerCount(pool), 3u);
    }
    ASSERT_EQ(setenv("MC_STORE_MEMCPY_WORKERS", "5", 1), 0);
    {
        MemcpyWorkerPool pool;
        EXPECT_EQ(MemcpyWorkerPoolTestPeer::WorkerCount(pool), 5u);
    }
    google::ShutdownGoogleLogging();
}

TEST(MemcpyWorkerPoolTest, CompletesEveryConcurrentTask) {
    constexpr int kTasks = 128;
    constexpr size_t kBytes = 8192;

    MemcpyWorkerPool pool;
    std::vector<std::vector<char>> src(kTasks, std::vector<char>(kBytes, 's'));
    std::vector<std::vector<char>> dst(kTasks, std::vector<char>(kBytes, '\0'));
    std::vector<std::shared_ptr<MemcpyOperationState>> states;
    states.reserve(kTasks);

    for (int i = 0; i < kTasks; ++i) {
        std::vector<MemcpyOperation> operations;
        operations.emplace_back(dst[i].data(), src[i].data(), kBytes);
        auto state = std::make_shared<MemcpyOperationState>();
        states.emplace_back(state);
        pool.submitTask(MemcpyTask(std::move(operations), state));
    }

    for (auto& state : states) {
        state->wait_for_completion();
        EXPECT_TRUE(state->is_completed());
        EXPECT_EQ(state->get_result(), ErrorCode::OK);
    }
    for (int i = 0; i < kTasks; ++i) {
        EXPECT_EQ(dst[i], src[i]);
    }
}

TEST(MemcpyWorkerPoolTest, SharedStateCompletesAfterEveryChunk) {
    constexpr int kChunks = 4;
    constexpr int kOperationsPerChunk = 8;
    constexpr size_t kBytes = 4096;
    constexpr int kTotal = kChunks * kOperationsPerChunk;

    MemcpyWorkerPool pool;
    std::vector<std::vector<char>> src(kTotal, std::vector<char>(kBytes, 'c'));
    std::vector<std::vector<char>> dst(kTotal, std::vector<char>(kBytes, '\0'));
    auto state = std::make_shared<MemcpyOperationState>(kChunks);

    for (int chunk = 0; chunk < kChunks; ++chunk) {
        std::vector<MemcpyOperation> operations;
        for (int i = 0; i < kOperationsPerChunk; ++i) {
            const int index = chunk * kOperationsPerChunk + i;
            operations.emplace_back(dst[index].data(), src[index].data(),
                                    kBytes);
        }
        pool.submitTask(MemcpyTask(std::move(operations), state));
    }

    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
    for (int i = 0; i < kTotal; ++i) {
        EXPECT_EQ(dst[i], src[i]);
    }
}

TEST(MemcpyWorkerPoolTest, ReportsTheFailureOfAnyChunk) {
    auto state = std::make_shared<MemcpyOperationState>(3);
    state->complete_chunk(ErrorCode::OK);
    state->complete_chunk(ErrorCode::TRANSFER_FAIL);
    EXPECT_FALSE(state->is_completed());
    state->complete_chunk(ErrorCode::OK);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);

    // A repeated report does not change the published result.
    state->complete_chunk(ErrorCode::OK);
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
}

TEST(MemcpyWorkerPoolTest, AConcurrentFailureIsNeverReportedAsSuccess) {
    // The copy that fails and the copy that finishes last race for the
    // publication of the result. The failing report must win regardless of
    // which one arrives last.
    constexpr int kRounds = 20000;
    int reported_success = 0;

    for (int round = 0; round < kRounds; ++round) {
        auto state = std::make_shared<MemcpyOperationState>(2);
        std::atomic<int> ready{0};
        auto report = [&state, &ready](ErrorCode code) {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) < 2) {
                std::this_thread::yield();
            }
            state->complete_chunk(code);
        };

        std::thread failing(report, ErrorCode::TRANSFER_FAIL);
        std::thread succeeding(report, ErrorCode::OK);
        failing.join();
        succeeding.join();

        if (state->get_result() == ErrorCode::OK) {
            ++reported_success;
        }
    }

    EXPECT_EQ(reported_success, 0);
}

}  // namespace
}  // namespace mooncake
