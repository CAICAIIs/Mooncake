#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <thread>
#include <vector>

#include <json/json.h>

#include "crc32c.h"
#include "traffic_format.h"

namespace {

using mooncake::traffic::TrafficBlockHeader;
using mooncake::traffic::TrafficFileHeader;
using mooncake::traffic::TrafficFooter;
using mooncake::traffic::TrafficRecordHeader;

constexpr std::uint32_t kRequestMagic = 21;
constexpr std::uint32_t kMaxBlockBytes = 64U * 1024U * 1024U;
constexpr std::uint32_t kMaxResponseBytes = 512U * 1024U * 1024U;

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

#pragma pack(push, 1)
struct RequestHeader {
    std::uint8_t magic;
    std::uint8_t version;
    std::uint8_t serialize_type;
    std::uint8_t msg_type;
    std::uint32_t seq_num;
    std::uint32_t function_id;
    std::uint32_t length;
    std::uint32_t attach_length;
};
static_assert(sizeof(RequestHeader) == 20);

struct ResponseHeader {
    std::uint8_t magic;
    std::uint8_t version;
    std::uint8_t err_code;
    std::uint8_t msg_type;
    std::uint32_t seq_num;
    std::uint32_t length;
    std::uint32_t attach_length;
};
static_assert(sizeof(ResponseHeader) == 16);
#pragma pack(pop)

struct Record {
    TrafficRecordHeader header{};
    std::vector<char> payload;
};

const char* RedactionName(std::uint8_t value) {
    switch (value) {
        case 0:
            return "none";
        case 1:
            return "metadata";
        case 2:
            return "redacted";
        default:
            return "unknown";
    }
}

class SessionReader {
   public:
    bool Open(const std::string& dir, std::string* error) {
        std::error_code ec;
        auto collect = [&](const std::filesystem::path& root) {
            files_.clear();
            session_dir_ = root;
            for (const auto& entry :
                 std::filesystem::directory_iterator(root, ec)) {
                if (!entry.is_regular_file(ec)) {
                    continue;
                }
                const std::string name = entry.path().filename().string();
                if (name.rfind("segment-", 0) == 0 &&
                    entry.path().extension() == ".mcr") {
                    files_.push_back(entry.path());
                }
            }
        };
        collect(dir);
        if (ec) {
            *error = "cannot read " + dir + ": " + ec.message();
            return false;
        }
        if (files_.empty()) {
            std::vector<std::filesystem::path> sessions;
            for (const auto& entry :
                 std::filesystem::directory_iterator(dir, ec)) {
                if (!entry.is_directory(ec)) {
                    continue;
                }
                const std::string name = entry.path().filename().string();
                if (name.rfind("session-", 0) == 0) {
                    sessions.push_back(entry.path());
                }
            }
            if (!ec && sessions.size() == 1) {
                collect(sessions.front());
            }
            if (files_.empty()) {
                *error = "no segment-*.mcr files in " + dir;
                return false;
            }
        }
        std::sort(files_.begin(), files_.end());
        return OpenNextFile(error);
    }

    enum class NextResult { kRecord, kDone, kError };

    NextResult Next(Record* record, std::string* error) {
        while (true) {
            if (!input_.is_open()) {
                if (!OpenNextFile(error)) {
                    return error->empty() ? NextResult::kDone
                                          : NextResult::kError;
                }
            }
            if (block_records_left_ > 0) {
                return ReadRecord(record, error);
            }
            if (footer_seen_) {
                input_.close();
                continue;
            }

            std::uint32_t magic = 0;
            input_.read(reinterpret_cast<char*>(&magic), sizeof(magic));
            if (input_.gcount() !=
                static_cast<std::streamsize>(sizeof(magic))) {
                *error = "truncated segment before footer: " +
                         current_path_.string();
                return NextResult::kError;
            }
            if (magic == mooncake::traffic::kTrafficFooterMagic) {
                if (!ReadFooter(magic, error)) {
                    return NextResult::kError;
                }
                continue;
            }
            if (magic != mooncake::traffic::kTrafficBlockMagic) {
                *error = "bad block magic in " + current_path_.string();
                return NextResult::kError;
            }
            if (!ReadBlock(magic, error)) {
                return NextResult::kError;
            }
        }
    }

    std::uint64_t records() const { return records_; }
    std::uint64_t payload_bytes() const { return payload_bytes_; }
    std::uint64_t block_count() const { return block_count_; }
    std::size_t segment_count() const { return segment_count_; }
    std::uint8_t redaction() const { return redaction_; }
    const std::filesystem::path& session_dir() const { return session_dir_; }
    const std::map<std::uint32_t, std::uint64_t>& routes() const {
        return routes_;
    }

   private:
    bool OpenNextFile(std::string* error) {
        if (next_file_ >= files_.size()) {
            error->clear();
            return false;
        }
        current_path_ = files_[next_file_++];
        input_.open(current_path_, std::ios::binary);
        if (!input_) {
            *error = "cannot open " + current_path_.string();
            return false;
        }
        TrafficFileHeader header{};
        input_.read(reinterpret_cast<char*>(&header), sizeof(header));
        if (input_.gcount() != static_cast<std::streamsize>(sizeof(header))) {
            *error = "truncated file header: " + current_path_.string();
            return false;
        }
        if (header.magic != mooncake::traffic::kTrafficFileMagic ||
            header.header_size != sizeof(TrafficFileHeader) ||
            header.version_major != mooncake::traffic::kTrafficVersionMajor ||
            header.version_minor != mooncake::traffic::kTrafficVersionMinor ||
            header.redaction > 2) {
            *error = "unsupported file header: " + current_path_.string();
            return false;
        }
        if (segment_count_ == 0) {
            std::memcpy(uuid_.data(), header.session_uuid, uuid_.size());
            redaction_ = header.redaction;
        } else if (redaction_ != header.redaction ||
                   !std::equal(uuid_.begin(), uuid_.end(),
                               header.session_uuid)) {
            *error = "segment header does not match session: " +
                     current_path_.string();
            return false;
        }
        file_block_count_ = 0;
        file_record_count_ = 0;
        file_record_bytes_ = 0;
        footer_seen_ = false;
        block_records_left_ = 0;
        block_offset_ = 0;
        block_.clear();
        ++segment_count_;
        return true;
    }

    bool ReadBlock(std::uint32_t magic, std::string* error) {
        TrafficBlockHeader header{};
        header.magic = magic;
        input_.read(reinterpret_cast<char*>(&header) + sizeof(magic),
                    sizeof(header) - sizeof(magic));
        if (input_.gcount() !=
            static_cast<std::streamsize>(sizeof(header) - sizeof(magic))) {
            *error = "truncated block header: " + current_path_.string();
            return false;
        }
        if (header.block_bytes > kMaxBlockBytes) {
            *error = "block too large: " + current_path_.string();
            return false;
        }
        block_.resize(header.block_bytes);
        if (header.block_bytes > 0) {
            input_.read(block_.data(), header.block_bytes);
            if (input_.gcount() !=
                static_cast<std::streamsize>(header.block_bytes)) {
                *error = "truncated block payload: " + current_path_.string();
                return false;
            }
        }
        mooncake::Crc32c crc;
        crc.Extend(block_.data(), block_.size());
        if (crc.Final() != header.crc32) {
            *error = "block crc mismatch: " + current_path_.string();
            return false;
        }
        block_offset_ = 0;
        block_records_left_ = header.record_count;
        ++block_count_;
        ++file_block_count_;
        if (block_records_left_ == 0 && !block_.empty()) {
            *error =
                "block has bytes but no records: " + current_path_.string();
            return false;
        }
        return true;
    }

    bool ReadFooter(std::uint32_t magic, std::string* error) {
        TrafficFooter footer{};
        footer.magic = magic;
        input_.read(reinterpret_cast<char*>(&footer) + sizeof(magic),
                    sizeof(footer) - sizeof(magic));
        if (input_.gcount() !=
            static_cast<std::streamsize>(sizeof(footer) - sizeof(magic))) {
            *error = "truncated footer: " + current_path_.string();
            return false;
        }
        if (footer.block_count != file_block_count_ ||
            footer.record_count != file_record_count_ ||
            footer.payload_bytes != file_record_bytes_) {
            *error = "footer counters mismatch: " + current_path_.string();
            return false;
        }
        footer_seen_ = true;
        return true;
    }

    NextResult ReadRecord(Record* record, std::string* error) {
        if (block_offset_ + sizeof(TrafficRecordHeader) > block_.size()) {
            *error = "truncated record header: " + current_path_.string();
            return NextResult::kError;
        }
        std::memcpy(&record->header, block_.data() + block_offset_,
                    sizeof(TrafficRecordHeader));
        const auto& header = record->header;
        if (header.total_len < sizeof(TrafficRecordHeader) ||
            header.payload_len !=
                header.total_len - sizeof(TrafficRecordHeader) ||
            block_offset_ + header.total_len > block_.size()) {
            *error = "bad record length: " + current_path_.string();
            return NextResult::kError;
        }
        record->payload.assign(
            block_.data() + block_offset_ + sizeof(TrafficRecordHeader),
            block_.data() + block_offset_ + header.total_len);
        block_offset_ += header.total_len;
        --block_records_left_;
        if (block_records_left_ == 0 && block_offset_ != block_.size()) {
            *error = "mismatched block record count: " + current_path_.string();
            return NextResult::kError;
        }
        ++records_;
        payload_bytes_ += header.payload_len;
        ++file_record_count_;
        file_record_bytes_ += header.total_len;
        ++routes_[header.route_key];
        return NextResult::kRecord;
    }

    std::vector<std::filesystem::path> files_;
    std::size_t next_file_ = 0;
    std::filesystem::path current_path_;
    std::ifstream input_;
    std::array<std::uint8_t, 16> uuid_{};
    std::uint8_t redaction_ = 0;
    bool footer_seen_ = false;
    std::vector<char> block_;
    std::size_t block_offset_ = 0;
    std::uint32_t block_records_left_ = 0;
    std::uint64_t file_block_count_ = 0;
    std::uint64_t file_record_count_ = 0;
    // FileSink stores record bytes (header plus payload) in
    // footer.payload_bytes.
    std::uint64_t file_record_bytes_ = 0;
    std::uint64_t records_ = 0;
    std::uint64_t payload_bytes_ = 0;
    std::uint64_t block_count_ = 0;
    std::size_t segment_count_ = 0;
    std::map<std::uint32_t, std::uint64_t> routes_;
    std::filesystem::path session_dir_;
};

bool WriteAll(int fd, const void* data, std::size_t len) {
    const auto* p = static_cast<const char*>(data);
    while (len > 0) {
        const ssize_t n = ::send(fd, p, len, MSG_NOSIGNAL);
        if (n <= 0) {
            return false;
        }
        p += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

bool ReadAll(int fd, void* data, std::size_t len) {
    auto* p = static_cast<char*>(data);
    while (len > 0) {
        const ssize_t n = ::recv(fd, p, len, 0);
        if (n <= 0) {
            return false;
        }
        p += n;
        len -= static_cast<std::size_t>(n);
    }
    return true;
}

class SocketClient {
   public:
    ~SocketClient() { Close(); }

    bool EnsureConnected(const std::string& target, int timeout_ms,
                         std::string* error) {
        if (fd_ >= 0) {
            return true;
        }
        const std::size_t colon = target.rfind(':');
        if (colon == std::string::npos || colon == 0 ||
            colon + 1 == target.size()) {
            *error = "target must be host:port";
            return false;
        }
        std::string host = target.substr(0, colon);
        const std::string port = target.substr(colon + 1);
        if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
            host = host.substr(1, host.size() - 2);
        }

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* result = nullptr;
        const int rc =
            ::getaddrinfo(host.c_str(), port.c_str(), &hints, &result);
        if (rc != 0) {
            *error = std::string("getaddrinfo: ") + ::gai_strerror(rc);
            return false;
        }

        timeval tv{};
        tv.tv_sec = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        int one = 1;
        for (addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
            const int fd =
                ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (fd < 0) {
                continue;
            }
            ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
            ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#ifdef SO_NOSIGPIPE
            ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif
            if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0) {
                fd_ = fd;
                break;
            }
            ::close(fd);
        }
        ::freeaddrinfo(result);
        if (fd_ < 0) {
            *error = "connect " + target + ": " + std::strerror(errno);
            return false;
        }
        return true;
    }

    bool Replay(const Record& record, bool* rpc_ok, std::string* error) {
        RequestHeader request{};
        request.magic = static_cast<std::uint8_t>(kRequestMagic);
        request.version = 0;
        request.serialize_type = 0;
        request.msg_type = 0;
        request.seq_num = record.header.seq_num;
        request.function_id = record.header.route_key;
        request.length = static_cast<std::uint32_t>(record.payload.size());
        request.attach_length = 0;
        if (!WriteAll(fd_, &request, sizeof(request)) ||
            (!record.payload.empty() &&
             !WriteAll(fd_, record.payload.data(), record.payload.size()))) {
            Close();
            *error = "send failed";
            return false;
        }

        ResponseHeader response{};
        if (!ReadAll(fd_, &response, sizeof(response)) ||
            response.magic != kRequestMagic || response.version != 0 ||
            response.length > kMaxResponseBytes) {
            Close();
            *error = "bad response header";
            return false;
        }
        if (response.length > 0) {
            std::vector<char> body(response.length);
            if (!ReadAll(fd_, body.data(), body.size())) {
                Close();
                *error = "truncated response body";
                return false;
            }
        }
        *rpc_ok = response.err_code == 0;
        return true;
    }

   private:
    void Close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    int fd_ = -1;
};

struct Options {
    std::string command;
    std::string dir;
    std::string target;
    std::uint64_t limit = 0;
    double rate = 0.0;
    double speed = 0.0;
    int timeout_ms = 5000;
    bool allow_all = false;
    bool dry_run = false;
    bool verbose = false;
    std::vector<std::string> allow_route_args;
};

bool ParseU64(const std::string& text, std::uint64_t* value) {
    char* end = nullptr;
    errno = 0;
    const unsigned long long v = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || end == text.c_str() || *end != '\0') {
        return false;
    }
    *value = v;
    return true;
}

bool ParseU32(const std::string& text, std::uint32_t* value) {
    std::uint64_t v = 0;
    if (!ParseU64(text, &v) || v > UINT32_MAX) {
        return false;
    }
    *value = static_cast<std::uint32_t>(v);
    return true;
}

bool LoadRouteNames(const std::filesystem::path& session_dir,
                    std::map<std::string, std::uint32_t>* names,
                    std::string* error) {
    names->clear();
    const std::filesystem::path path = session_dir / "routes.json";
    std::error_code ec;
    if (!std::filesystem::exists(path, ec)) {
        return true;
    }
    std::ifstream in(path);
    if (!in) {
        *error = "cannot read " + path.string();
        return false;
    }
    Json::CharReaderBuilder builder;
    Json::Value root;
    std::string parse_error;
    if (!Json::parseFromStream(builder, in, &root, &parse_error)) {
        *error = "cannot parse " + path.string() + ": " + parse_error;
        return false;
    }
    if (!root.isObject()) {
        *error = path.string() + " must be a JSON object";
        return false;
    }
    for (const auto& key : root.getMemberNames()) {
        std::uint32_t route = 0;
        if (!ParseU32(key, &route) || !root[key].isString()) {
            *error = "invalid route entry in " + path.string();
            return false;
        }
        (*names)[root[key].asString()] = route;
    }
    return true;
}

bool ParseDouble(const std::string& text, double* value) {
    char* end = nullptr;
    errno = 0;
    const double v = std::strtod(text.c_str(), &end);
    if (errno != 0 || end == text.c_str() || *end != '\0' || v < 0) {
        return false;
    }
    *value = v;
    return true;
}

void PrintUsage() {
    std::cout
        << "usage:\n"
        << "  traffic_replay inspect --dir SESSION\n"
        << "  traffic_replay run --dir SESSION --target HOST:PORT "
           "[options]\n"
        << "\noptions:\n"
        << "  --limit N             replay at most N records\n"
        << "  --allow-route ROUTE   allow a route key or name (repeatable)\n"
        << "  --allow-all           allow every route\n"
        << "  --dry-run             parse and count without sending\n"
        << "  --rate QPS            fixed send rate\n"
        << "  --speed N             replay at N times the recorded speed\n"
        << "  --timeout-ms N        socket timeout (default 5000)\n"
        << "  --verbose             print progress\n";
}

bool NextValue(int argc, char** argv, int* index, std::string* value) {
    if (*index + 1 >= argc) {
        return false;
    }
    *value = argv[++(*index)];
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options, std::string* error) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        std::string inline_value;
        const auto equals = arg.find('=');
        if (arg.rfind("--", 0) == 0 && equals != std::string::npos) {
            inline_value = arg.substr(equals + 1);
            arg.resize(equals);
        }
        const auto take = [&](std::string* out) {
            if (!inline_value.empty()) {
                *out = inline_value;
                return true;
            }
            return NextValue(argc, argv, &i, out);
        };
        std::string value;
        if (arg == "--dir") {
            if (!take(&options->dir)) {
                *error = "--dir needs a value";
                return false;
            }
        } else if (arg == "--target") {
            if (!take(&options->target)) {
                *error = "--target needs a value";
                return false;
            }
        } else if (arg == "--limit") {
            if (!take(&value) || !ParseU64(value, &options->limit)) {
                *error = "bad --limit";
                return false;
            }
        } else if (arg == "--rate") {
            if (!take(&value) || !ParseDouble(value, &options->rate)) {
                *error = "bad --rate";
                return false;
            }
        } else if (arg == "--speed") {
            if (!take(&value) || !ParseDouble(value, &options->speed)) {
                *error = "bad --speed";
                return false;
            }
        } else if (arg == "--timeout-ms") {
            std::uint64_t timeout = 0;
            if (!take(&value) || !ParseU64(value, &timeout) || timeout == 0 ||
                timeout > 600000) {
                *error = "bad --timeout-ms";
                return false;
            }
            options->timeout_ms = static_cast<int>(timeout);
        } else if (arg == "--allow-route") {
            if (!take(&value) || value.empty()) {
                *error = "--allow-route needs a route key or name";
                return false;
            }
            options->allow_route_args.push_back(value);
        } else if (arg == "--allow-all") {
            options->allow_all = true;
        } else if (arg == "--dry-run") {
            options->dry_run = true;
        } else if (arg == "--verbose") {
            options->verbose = true;
        } else if (arg == "--help") {
            PrintUsage();
            std::exit(0);
        } else if (arg.rfind("--", 0) == 0) {
            *error = "unknown option: " + arg;
            return false;
        } else if (options->command.empty()) {
            options->command = arg;
        } else {
            *error = "unexpected argument: " + arg;
            return false;
        }
    }
    if (options->rate > 0 && options->speed > 0) {
        *error = "--rate and --speed are mutually exclusive";
        return false;
    }
    return true;
}

bool Allowed(const Options& options,
             const std::vector<std::uint32_t>& allow_routes,
             std::uint32_t route) {
    if (options.allow_all) {
        return true;
    }
    return std::find(allow_routes.begin(), allow_routes.end(), route) !=
           allow_routes.end();
}

class Pacer {
   public:
    explicit Pacer(const Options& options)
        : rate_(options.rate), speed_(options.speed) {}

    void Wait(std::uint64_t timestamp_ms) {
        if (start_ == std::chrono::steady_clock::time_point{}) {
            start_ = std::chrono::steady_clock::now();
            first_timestamp_ms_ = timestamp_ms;
        }
        std::chrono::steady_clock::time_point due = start_;
        if (speed_ > 0) {
            const std::uint64_t delta = timestamp_ms > first_timestamp_ms_
                                            ? timestamp_ms - first_timestamp_ms_
                                            : 0;
            const auto ns = static_cast<std::int64_t>(
                static_cast<double>(delta) * 1000000.0 / speed_);
            due += std::chrono::nanoseconds(ns);
        } else if (rate_ > 0) {
            const auto ns = static_cast<std::int64_t>(
                static_cast<double>(count_) * 1000000000.0 / rate_);
            due += std::chrono::nanoseconds(ns);
        } else {
            return;
        }
        ++count_;
        std::this_thread::sleep_until(due);
    }

   private:
    double rate_ = 0;
    double speed_ = 0;
    std::chrono::steady_clock::time_point start_{};
    std::uint64_t first_timestamp_ms_ = 0;
    std::uint64_t count_ = 0;
};

bool ResolveAllowRoutes(const Options& options,
                        const std::map<std::string, std::uint32_t>& names,
                        std::vector<std::uint32_t>* routes,
                        std::string* error) {
    routes->clear();
    for (const std::string& arg : options.allow_route_args) {
        std::uint32_t key = 0;
        if (ParseU32(arg, &key)) {
            routes->push_back(key);
            continue;
        }
        const auto it = names.find(arg);
        if (it != names.end()) {
            routes->push_back(it->second);
            continue;
        }
        std::vector<std::uint32_t> matches;
        const std::string suffix = "::" + arg;
        for (const auto& [name, key] : names) {
            if (name.size() >= suffix.size() &&
                name.compare(name.size() - suffix.size(), suffix.size(),
                             suffix) == 0) {
                matches.push_back(key);
            }
        }
        if (matches.size() != 1) {
            *error = matches.empty()
                         ? "unknown route name: " + arg +
                               "; use `inspect` to list names"
                         : "ambiguous route name: " + arg;
            return false;
        }
        routes->push_back(matches.front());
    }
    return true;
}

int Inspect(const Options& options) {
    SessionReader reader;
    std::string error;
    if (!reader.Open(options.dir, &error)) {
        std::cerr << error << "\n";
        return 1;
    }
    Record record;
    while (true) {
        const auto result = reader.Next(&record, &error);
        if (result == SessionReader::NextResult::kDone) {
            break;
        }
        if (result == SessionReader::NextResult::kError) {
            std::cerr << error << "\n";
            return 1;
        }
    }
    std::map<std::string, std::uint32_t> names;
    if (!LoadRouteNames(reader.session_dir(), &names, &error)) {
        std::cerr << error << "\n";
        return 1;
    }
    std::map<std::uint32_t, std::string> by_key;
    for (const auto& [name, key] : names) {
        by_key[key] = name;
    }
    std::cout << "dir=" << options.dir << "\n"
              << "segments=" << reader.segment_count() << "\n"
              << "blocks=" << reader.block_count() << "\n"
              << "records=" << reader.records() << "\n"
              << "payload_bytes=" << reader.payload_bytes() << "\n"
              << "redaction=" << RedactionName(reader.redaction()) << "\n"
              << "routes:\n";
    for (const auto& [route, count] : reader.routes()) {
        std::cout << "  " << route;
        const auto it = by_key.find(route);
        if (it != by_key.end()) {
            std::cout << " " << it->second;
        }
        std::cout << " " << count << "\n";
    }
    return 0;
}

int Run(const Options& options) {
    SessionReader reader;
    std::string error;
    if (!reader.Open(options.dir, &error)) {
        std::cerr << error << "\n";
        return 1;
    }
    if (reader.redaction() == 1) {
        std::cerr << "metadata sessions contain no request payload and cannot "
                     "be replayed\n";
        return 1;
    }
    std::map<std::string, std::uint32_t> names;
    if (!LoadRouteNames(reader.session_dir(), &names, &error)) {
        std::cerr << error << "\n";
        return 1;
    }
    std::vector<std::uint32_t> allow_routes;
    if (!ResolveAllowRoutes(options, names, &allow_routes, &error)) {
        std::cerr << error << "\n";
        return 1;
    }
    if (!options.dry_run && !options.allow_all && allow_routes.empty()) {
        std::cerr << "refusing to replay every route without --allow-all or "
                     "--allow-route\n";
        return 1;
    }

    SocketClient client;
    Pacer pacer(options);
    std::uint64_t selected = 0;
    std::uint64_t sent = 0;
    std::uint64_t ok = 0;
    std::uint64_t rpc_error = 0;
    std::uint64_t transport_error = 0;
    std::uint64_t bytes = 0;
    std::uint64_t records_read = 0;
    Record record;
    const auto start = std::chrono::steady_clock::now();

    while (true) {
        const auto result = reader.Next(&record, &error);
        if (result == SessionReader::NextResult::kDone) {
            break;
        }
        if (result == SessionReader::NextResult::kError) {
            std::cerr << error << "\n";
            return 1;
        }
        ++records_read;
        if (!options.dry_run && !options.allow_all &&
            !Allowed(options, allow_routes, record.header.route_key)) {
            continue;
        }
        if (options.limit > 0 && selected >= options.limit) {
            break;
        }
        ++selected;
        if (options.dry_run) {
            continue;
        }
        if (!client.EnsureConnected(options.target, options.timeout_ms,
                                    &error)) {
            std::cerr << error << "\n";
            return 2;
        }
        pacer.Wait(record.header.timestamp_ms);
        bool rpc_ok = false;
        if (!client.Replay(record, &rpc_ok, &error)) {
            ++transport_error;
            std::cerr << "record " << records_read << ": " << error << "\n";
            return 2;
        }
        ++sent;
        bytes += record.payload.size();
        if (rpc_ok) {
            ++ok;
        } else {
            ++rpc_error;
        }
        if (options.verbose && sent % 10000 == 0) {
            std::cerr << "sent=" << sent << "\n";
        }
    }

    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start);
    const double seconds = std::max(0.001, elapsed.count() / 1000.0);
    std::cout << "records_read=" << records_read << " selected=" << selected
              << " sent=" << sent << " ok=" << ok << " rpc_error=" << rpc_error
              << " transport_error=" << transport_error << " bytes=" << bytes
              << " elapsed_ms=" << elapsed.count()
              << " qps=" << static_cast<double>(sent) / seconds << "\n";
    return transport_error == 0 ? 0 : 2;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << error << "\n";
        PrintUsage();
        return 1;
    }
    if (options.command.empty() || options.command == "help") {
        PrintUsage();
        return options.command.empty() ? 1 : 0;
    }
    if (options.dir.empty()) {
        std::cerr << "--dir is required\n";
        return 1;
    }
    if (options.command == "inspect") {
        return Inspect(options);
    }
    if (options.command == "run") {
        if (!options.dry_run && options.target.empty()) {
            std::cerr << "--target is required for run\n";
            return 1;
        }
        return Run(options);
    }
    std::cerr << "unknown command: " << options.command << "\n";
    PrintUsage();
    return 1;
}
