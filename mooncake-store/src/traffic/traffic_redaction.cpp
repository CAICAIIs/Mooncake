#include "traffic/traffic_redaction.h"

#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <ylt/struct_pack.hpp>
#include <ylt/util/utils.hpp>

#include "rpc_service.h"
#include "rpc_types.h"

namespace mooncake::traffic {
namespace {

constexpr std::string_view kMask = "<redacted>";

void Mask(std::string& value) { value.assign(kMask); }

void Mask(std::vector<std::string>& values) {
    for (auto& value : values) {
        Mask(value);
    }
}

template <typename Tuple, typename Fn>
class TupleRedactor final : public Redactor {
   public:
    explicit TupleRedactor(Fn fn) : fn_(std::move(fn)) {}

    bool Redact(std::string_view input, std::string& output) const override {
        Tuple args{};
        if (struct_pack::deserialize_to(args, input) != struct_pack::errc::ok) {
            return false;
        }
        fn_(args);
        const auto bytes = struct_pack::serialize(args);
        output.assign(bytes.data(), bytes.size());
        return true;
    }

   private:
    Fn fn_;
};

template <typename Tuple, typename Fn>
void Add(RedactionRegistry& registry, std::uint32_t route_key, Fn fn) {
    registry.Add(route_key,
                 std::make_shared<TupleRedactor<Tuple, Fn>>(std::move(fn)));
}

template <auto Method>
constexpr std::uint32_t RouteKey() {
    return coro_rpc::func_id<Method>();
}

}  // namespace

void RedactionRegistry::Add(std::uint32_t route_key,
                            std::shared_ptr<const Redactor> redactor) {
    redactors_[route_key] = std::move(redactor);
}

bool RedactionRegistry::Contains(std::uint32_t route_key) const {
    return redactors_.find(route_key) != redactors_.end();
}

bool RedactionRegistry::Redact(std::uint32_t route_key, std::string_view input,
                               std::string& output) const {
    const auto it = redactors_.find(route_key);
    if (it == redactors_.end()) {
        return false;
    }
    return it->second->Redact(input, output);
}

void RegisterMasterRedactors(RedactionRegistry& registry) {
    using Service = WrappedMasterService;

    Add<std::tuple<std::string, std::string>>(
        registry, RouteKey<&Service::GetReplicaList>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::string, std::string>>(
        registry, RouteKey<&Service::GetReplicaListForAdmin>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::vector<std::string>, std::string>>(
        registry, RouteKey<&Service::BatchGetReplicaList>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::vector<std::string>, std::string>>(
        registry, RouteKey<&Service::BatchGetReplicaListForAdmin>(),
        [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::string, std::string>>(
        registry, RouteKey<&Service::ExistKey>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::string, std::string>>(
        registry, RouteKey<&Service::ProbeKey>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::vector<std::string>, std::string>>(
        registry, RouteKey<&Service::BatchExistKey>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::vector<std::string>, std::string>>(
        registry, RouteKey<&Service::BatchProbeKey>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<1>(args));
        });
    Add<std::tuple<std::string, bool, std::string>>(
        registry, RouteKey<&Service::Remove>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<2>(args));
        });
    Add<std::tuple<std::vector<std::string>, bool, std::string>>(
        registry, RouteKey<&Service::BatchRemove>(), [](auto& args) {
            Mask(std::get<0>(args));
            Mask(std::get<2>(args));
        });
    Add<std::tuple<UUID, std::string, std::uint64_t, ReplicateConfig,
                   std::string>>(registry, RouteKey<&Service::PutStart>(),
                                 [](auto& args) {
                                     Mask(std::get<1>(args));
                                     Mask(std::get<4>(args));
                                 });
    Add<std::tuple<UUID, std::vector<std::string>, std::vector<std::uint64_t>,
                   ReplicateConfig, std::string>>(
        registry, RouteKey<&Service::BatchPutStart>(), [](auto& args) {
            Mask(std::get<1>(args));
            Mask(std::get<4>(args));
        });
}

}  // namespace mooncake::traffic
