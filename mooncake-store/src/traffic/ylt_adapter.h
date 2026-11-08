#pragma once

// The only file that depends on coro_rpc's router internals. Keeping it
// separate lets the recorder, file format, sink, and replay tool stay
// independent of the RPC framework.

#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include <async_simple/coro/Lazy.h>
#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/coro_rpc/impl/default_config/coro_rpc_config.hpp>
#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>
#include <ylt/coro_rpc/impl/router.hpp>

#include "traffic_recorder.h"

namespace mooncake::traffic {
namespace detail {

inline bool RouteDumpEnabled() {
    static const bool enabled = [] {
        const char* value = std::getenv("MOONCAKE_TRAFFIC_DUMP_ROUTES");
        return value != nullptr && std::string_view{value} == "1";
    }();
    return enabled;
}

template <typename Router>
void DumpRouteOnce(Router& router, std::uint32_t route_key) {
    static std::mutex mu;
    static std::unordered_set<std::uint32_t> seen;
    std::lock_guard lock(mu);
    if (!seen.insert(route_key).second) {
        return;
    }
    LOG(INFO) << "traffic route key=" << route_key
              << " name=" << router.get_name(route_key);
}

template <typename Context>
std::uint32_t RequestAttachmentLength(const Context& context) {
    return static_cast<std::uint32_t>(context->get_request_attachment().size());
}

}  // namespace detail

struct RecordingProtocol;
template <typename Protocol>
class RecordingRouter;

struct RecordingProtocol : coro_rpc::protocol::coro_rpc_protocol {
    using router = RecordingRouter<RecordingProtocol>;
};

template <typename Protocol>
class RecordingRouter : public coro_rpc::protocol::router<Protocol> {
   public:
    using Base = coro_rpc::protocol::router<Protocol>;

    template <typename Handler>
    std::pair<coro_rpc::err_code, std::string> route(
        std::uint64_t conn_id, std::uint64_t req_id, Handler handler,
        std::string_view data, coro_rpc::rpc_context<Protocol>& context_info,
        typename Protocol::supported_serialize_protocols protocols,
        const typename Protocol::route_key_t& route_key) {
        if (detail::RouteDumpEnabled()) {
            detail::DumpRouteOnce(*this, static_cast<std::uint32_t>(route_key));
        }
        if (handler != nullptr && TrafficRecorder::Instance().Recording()) {
            Observe(data, conn_id, req_id, route_key,
                    detail::RequestAttachmentLength(context_info));
        }
        return Base::route(conn_id, req_id, handler, data, context_info,
                           protocols, route_key);
    }

    template <typename Handler>
    async_simple::coro::Lazy<std::pair<coro_rpc::err_code, std::string>>
    route_coro(std::uint64_t conn_id, std::uint64_t req_id, Handler handler,
               std::string_view data,
               typename Protocol::supported_serialize_protocols protocols,
               const typename Protocol::route_key_t& route_key) {
        if (detail::RouteDumpEnabled()) {
            detail::DumpRouteOnce(*this, static_cast<std::uint32_t>(route_key));
        }
        if (handler != nullptr && TrafficRecorder::Instance().Recording()) {
            auto* context_info =
                co_await coro_rpc::get_context_in_coro<Protocol>();
            Observe(data, conn_id, req_id, route_key,
                    detail::RequestAttachmentLength(context_info));
        }
        co_return co_await Base::route_coro(conn_id, req_id, handler, data,
                                            protocols, route_key);
    }

   private:
    static void Observe(std::string_view data, std::uint64_t conn_id,
                        std::uint64_t req_id,
                        const typename Protocol::route_key_t& route_key,
                        std::uint32_t attach_length) {
        // YLT request ids are 64-bit; the record format stores 32 bits.
        TrafficRecorder::Instance().OnRequest(
            RequestView{static_cast<std::uint32_t>(route_key), data, conn_id,
                        static_cast<std::uint32_t>(req_id), attach_length});
    }
};

struct RecordingConfig : coro_rpc::config_t {
    using rpc_protocol = RecordingProtocol;
};

using RecordingServer = coro_rpc::coro_rpc_server_base<RecordingConfig>;

}  // namespace mooncake::traffic
