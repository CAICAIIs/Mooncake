#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

namespace cinatra {
class coro_http_server;
}

namespace mooncake::traffic {

class TrafficRecorder;
struct RecorderStatus;

bool GenerateControlToken(std::string* token);
bool ReadControlToken(const std::filesystem::path& path, std::string* token,
                      bool require_generated_format = false);
bool WriteControlToken(const std::filesystem::path& path, std::string* token);

bool WriteSummaryJson(const std::filesystem::path& session_dir,
                      const RecorderStatus& status);

std::unique_ptr<cinatra::coro_http_server> StartControlServer(
    TrafficRecorder& recorder, const std::string& host, std::uint16_t port,
    const std::string& token);

}  // namespace mooncake::traffic
