#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "rpc_service.h"
#include "traffic/ylt_adapter.h"

namespace mooncake {

// Selects the plain coro_rpc server unless traffic recording is configured.
// Keeping the selection in one place avoids compiling the recording adapter
// into the default request path.
class MasterRpcServer {
   public:
    template <typename Duration>
    MasterRpcServer(int rpc_thread_num, std::uint16_t rpc_port,
                    const std::string& rpc_address, Duration rpc_timeout,
                    bool enable_tcp_no_delay, bool enable_recording)
        : rpc_thread_num_(rpc_thread_num > 0 ? rpc_thread_num : 1) {
        const auto timeout =
            std::chrono::duration_cast<std::chrono::seconds>(rpc_timeout);
        const char* traffic_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
        if (enable_recording && traffic_config != nullptr &&
            *traffic_config != '\0') {
            recording_ = std::make_unique<traffic::RecordingServer>(
                rpc_thread_num, rpc_port, rpc_address, timeout,
                enable_tcp_no_delay);
        } else {
            plain_ = std::make_unique<coro_rpc::coro_rpc_server>(
                rpc_thread_num, rpc_port, rpc_address, timeout,
                enable_tcp_no_delay);
        }
    }

    MasterRpcServer(const MasterRpcServer&) = delete;
    MasterRpcServer& operator=(const MasterRpcServer&) = delete;

    void init_ibv() {
#if defined(YLT_ENABLE_IBV)
        if (recording_ != nullptr) {
            recording_->init_ibv();
        } else {
            plain_->init_ibv();
        }
#endif
    }

    template <typename Service>
    void Register(Service& service, RpcWorkerPool* worker_pool) {
        if (recording_ != nullptr) {
            RegisterRpcService(*recording_, service, worker_pool,
                               static_cast<std::size_t>(rpc_thread_num_),
                               /*enable_recording=*/true);
        } else {
            RegisterRpcService(*plain_, service, worker_pool,
                               static_cast<std::size_t>(rpc_thread_num_),
                               /*enable_recording=*/false);
        }
    }

    int start() {
        return recording_ != nullptr ? recording_->start() : plain_->start();
    }

    auto async_start() {
        return recording_ != nullptr ? recording_->async_start()
                                     : plain_->async_start();
    }

    void stop() {
        if (recording_ != nullptr) {
            recording_->stop();
            traffic::TrafficRecorder::Instance().Shutdown();
        } else {
            plain_->stop();
        }
    }

    auto port() const {
        return recording_ != nullptr ? recording_->port() : plain_->port();
    }

   private:
    int rpc_thread_num_;
    std::unique_ptr<coro_rpc::coro_rpc_server> plain_;
    std::unique_ptr<traffic::RecordingServer> recording_;
};

}  // namespace mooncake
