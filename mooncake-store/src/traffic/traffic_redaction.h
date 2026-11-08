#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

namespace mooncake::traffic {

// 脱敏在写入线程执行；录制热路径仍然只复制原始 payload。
class Redactor {
   public:
    virtual ~Redactor() = default;
    virtual bool Redact(std::string_view input, std::string& output) const = 0;
};

class RedactionRegistry {
   public:
    void Add(std::uint32_t route_key, std::shared_ptr<const Redactor> redactor);
    bool Contains(std::uint32_t route_key) const;
    bool Redact(std::uint32_t route_key, std::string_view input,
                std::string& output) const;

   private:
    std::unordered_map<std::uint32_t, std::shared_ptr<const Redactor>>
        redactors_;
};

// 注册 Mooncake master 路由的脱敏器；调用方在录制启动前执行一次。
void RegisterMasterRedactors(RedactionRegistry& registry);

}  // namespace mooncake::traffic
