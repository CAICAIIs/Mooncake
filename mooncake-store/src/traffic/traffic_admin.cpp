#include "traffic_admin.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <vector>

#include <json/json.h>
#include <openssl/rand.h>
#include <ylt/coro_http/coro_http_server.hpp>

#include <exception>

#include <glog/logging.h>

#include "traffic_recorder.h"

namespace mooncake::traffic {
namespace {

Json::Value BuildStatusJson(const RecorderStatus& status) {
    Json::Value root;
    root["state"] = status.state;
    root["observed_requests"] =
        static_cast<Json::UInt64>(status.observed_requests);
    root["recorded_requests"] =
        static_cast<Json::UInt64>(status.recorded_requests);
    root["filtered_requests"] =
        static_cast<Json::UInt64>(status.filtered_requests);
    root["dropped_requests"] =
        static_cast<Json::UInt64>(status.dropped_requests);
    root["bytes_written"] = static_cast<Json::UInt64>(status.bytes_written);
    root["segments"] = static_cast<Json::UInt64>(status.segments);
    root["ring_used_bytes"] = static_cast<Json::UInt64>(status.ring_used_bytes);
    root["ring_capacity_bytes"] =
        static_cast<Json::UInt64>(status.ring_capacity_bytes);
    return root;
}

}  // namespace

bool GenerateControlToken(std::string* token) {
    std::array<unsigned char, 16> random_bytes{};
    if (RAND_bytes(random_bytes.data(),
                   static_cast<int>(random_bytes.size())) != 1) {
        return false;
    }
    static constexpr char kHex[] = "0123456789abcdef";
    token->clear();
    token->reserve(random_bytes.size() * 2);
    for (unsigned char byte : random_bytes) {
        token->push_back(kHex[byte >> 4]);
        token->push_back(kHex[byte & 0x0f]);
    }
    return true;
}

bool ReadControlToken(const std::filesystem::path& path, std::string* token,
                      bool require_generated_format) {
    int flags = O_RDONLY | O_CLOEXEC;
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    const int fd = ::open(path.c_str(), flags);
    if (fd < 0) {
        return false;
    }
    struct stat metadata{};
    if (::fstat(fd, &metadata) != 0 || !S_ISREG(metadata.st_mode) ||
        metadata.st_uid != ::geteuid() || (metadata.st_mode & 07777) != 0600) {
        ::close(fd);
        return false;
    }
    token->clear();
    char chunk[128];
    bool done = false;
    while (!done && token->size() <= 4096) {
        const ssize_t bytes = ::read(fd, chunk, sizeof(chunk));
        if (bytes < 0 && errno == EINTR) {
            continue;
        }
        if (bytes < 0) {
            ::close(fd);
            return false;
        }
        if (bytes == 0) {
            break;
        }
        const char* newline =
            static_cast<const char*>(std::memchr(chunk, '\n', bytes));
        const std::size_t length =
            newline == nullptr ? static_cast<std::size_t>(bytes)
                               : static_cast<std::size_t>(newline - chunk);
        token->append(chunk, length);
        done = newline != nullptr;
    }
    const bool closed = ::close(fd) == 0;
    if (!closed || token->size() > 4096) {
        token->clear();
        return false;
    }
    while (!token->empty() && (token->back() == '\n' || token->back() == '\r' ||
                               token->back() == ' ' || token->back() == '\t')) {
        token->pop_back();
    }
    if (token->empty()) {
        return false;
    }
    if (require_generated_format &&
        (token->size() != 32 ||
         !std::all_of(token->begin(), token->end(), [](unsigned char ch) {
             return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f');
         }))) {
        return false;
    }
    return true;
}

bool WriteControlToken(const std::filesystem::path& path, std::string* token) {
    std::string temp_path = path.string() + ".tmp.XXXXXX";
    std::vector<char> temp_path_buffer(temp_path.begin(), temp_path.end());
    temp_path_buffer.push_back('\0');
    const int fd = ::mkstemp(temp_path_buffer.data());
    if (fd < 0) {
        return false;
    }

    const std::string published_temp_path(temp_path_buffer.data());
    bool success =
        ::fcntl(fd, F_SETFD, FD_CLOEXEC) == 0 && ::fchmod(fd, 0600) == 0;
    const std::string contents = *token + '\n';
    std::size_t written_total = 0;
    while (success && written_total < contents.size()) {
        const ssize_t written = ::write(fd, contents.data() + written_total,
                                        contents.size() - written_total);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            success = false;
            break;
        }
        written_total += static_cast<std::size_t>(written);
    }
    if (success && ::fsync(fd) != 0) {
        success = false;
    }
    if (::close(fd) != 0) {
        success = false;
    }
    if (success && ::link(published_temp_path.c_str(), path.c_str()) == 0) {
        ::unlink(published_temp_path.c_str());
        return true;
    }
    const int link_error = errno;
    ::unlink(published_temp_path.c_str());
    if (success && link_error == EEXIST) {
        std::string published_token;
        if (ReadControlToken(path, &published_token, true)) {
            *token = std::move(published_token);
            return true;
        }
    }
    return false;
}

static std::string StatusJson(const RecorderStatus& status) {
    Json::Value root = BuildStatusJson(status);
    root["stop_timed_out"] = status.stop_timed_out;
    root["format_complete"] = status.format_complete;
    root["capture_complete"] = status.capture_complete;
    root["session_dir"] = status.session_dir;
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    return Json::writeString(builder, root);
}

bool WriteSummaryJson(const std::filesystem::path& session_dir,
                      const RecorderStatus& status) {
    if (session_dir.empty()) {
        return false;
    }
    Json::Value root = BuildStatusJson(status);
    root["format_complete"] = status.format_complete;
    root["capture_complete"] = status.capture_complete;
    root["stop_timed_out"] = status.stop_timed_out;
    Json::StreamWriterBuilder builder;
    builder["indentation"] = "";
    std::ofstream out(session_dir / "summary.json", std::ios::trunc);
    if (!out) {
        LOG(ERROR) << "traffic: cannot write summary.json in " << session_dir;
        return false;
    }
    out << Json::writeString(builder, root);
    out.close();
    if (::chmod((session_dir / "summary.json").c_str(), 0600) != 0) {
        LOG(WARNING) << "traffic: cannot chmod summary.json";
    }
    return true;
}

std::unique_ptr<cinatra::coro_http_server> StartControlServer(
    TrafficRecorder& recorder, const std::string& host, std::uint16_t port,
    const std::string& token) {
    using namespace coro_http;
    try {
        auto server = std::make_unique<coro_http_server>(2, port, host);
        server->set_http_handler<GET>(
            "/v1/recording/health",
            [](coro_http_request&, coro_http_response& resp) {
                resp.set_status_and_content(status_type::ok, "ok");
            });
        server->set_http_handler<GET>(
            "/v1/recording/status",
            [&recorder, token](coro_http_request& req,
                               coro_http_response& resp) {
                if (!token.empty() && req.get_header_value("Authorization") !=
                                          "Bearer " + token) {
                    resp.set_status_and_content(status_type::unauthorized,
                                                "{\"error\":\"unauthorized\"}");
                    return;
                }
                resp.add_header("Content-Type", "application/json");
                resp.set_status_and_content(status_type::ok,
                                            StatusJson(recorder.GetStatus()));
            });
        server->set_http_handler<POST>(
            "/v1/recording/start",
            [&recorder, token](coro_http_request& req,
                               coro_http_response& resp) {
                if (!token.empty() && req.get_header_value("Authorization") !=
                                          "Bearer " + token) {
                    resp.set_status_and_content(status_type::unauthorized,
                                                "{\"error\":\"unauthorized\"}");
                    return;
                }
                if (!recorder.Start()) {
                    resp.set_status_and_content(status_type::conflict,
                                                "{\"error\":\"busy\"}");
                    return;
                }
                resp.set_status_and_content(status_type::ok,
                                            StatusJson(recorder.GetStatus()));
            });
        server->set_http_handler<POST>(
            "/v1/recording/stop", [&recorder, token](coro_http_request& req,
                                                     coro_http_response& resp) {
                if (!token.empty() && req.get_header_value("Authorization") !=
                                          "Bearer " + token) {
                    resp.set_status_and_content(status_type::unauthorized,
                                                "{\"error\":\"unauthorized\"}");
                    return;
                }
                recorder.StopAsync();
                resp.set_status_and_content(status_type::accepted,
                                            "{\"state\":\"stopping\"}");
            });
        auto ec = server->async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "traffic: control server failed on " << host << ":"
                       << port << ": " << ec.value().message();
            return nullptr;
        }
        LOG(INFO) << "traffic: control server on " << host << ":" << port;
        return server;
    } catch (const std::exception& e) {
        LOG(ERROR) << "traffic: control server exception: " << e.what();
        return nullptr;
    }
}

}  // namespace mooncake::traffic
