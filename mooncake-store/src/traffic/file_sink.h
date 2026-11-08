#pragma once

#include <sys/uio.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <string>
#include <vector>

#include "traffic_format.h"

namespace mooncake::traffic {

class TrafficSink {
   public:
    virtual ~TrafficSink() = default;

    virtual bool OpenSession(const std::string& output_dir,
                             std::uint64_t start_ms) = 0;
    virtual bool WriteRecords(const struct iovec* iov, int iov_count,
                              std::uint64_t bytes, std::uint32_t records) = 0;
    virtual void Close() = 0;

    virtual const std::string& session_dir() const = 0;
    virtual std::uint64_t bytes_written() const = 0;
    virtual std::uint64_t segments() const = 0;
    virtual bool failed() const = 0;
};

class FileSink final : public TrafficSink {
   public:
    FileSink(std::uint64_t segment_bytes, std::uint64_t segment_seconds,
             std::uint64_t max_total_bytes = 0,
             RedactionMode redaction = RedactionMode::kNone);
    ~FileSink() override;

    bool OpenSession(const std::string& output_dir,
                     std::uint64_t start_ms) override;
    bool WriteRecords(const struct iovec* iov, int iov_count,
                      std::uint64_t bytes, std::uint32_t records) override;
    void Close() override;

    const std::string& session_dir() const override { return session_dir_; }
    std::uint64_t bytes_written() const override {
        return bytes_written_.load(std::memory_order_relaxed);
    }
    std::uint64_t segments() const override {
        return segments_.load(std::memory_order_relaxed);
    }
    bool failed() const override {
        return failed_.load(std::memory_order_relaxed);
    }

   private:
    bool OpenNextSegment();
    bool CloseSegment(bool sync);

    mutable std::mutex mu_;

    std::uint64_t segment_bytes_;
    std::uint64_t segment_seconds_;
    std::uint64_t max_total_bytes_;
    RedactionMode redaction_;
    std::string session_dir_;
    std::string segment_path_;
    std::FILE* file_ = nullptr;
    std::uint64_t segment_seq_ = 0;
    std::uint64_t segment_bytes_written_ = 0;
    std::uint64_t segment_start_ms_ = 0;
    std::array<std::uint8_t, 16> session_uuid_{};
    std::vector<struct iovec> iovec_scratch_;
    std::uint64_t block_count_ = 0;
    std::uint64_t record_count_ = 0;
    std::uint64_t payload_bytes_ = 0;
    std::atomic<std::uint64_t> bytes_written_{0};
    std::atomic<std::uint64_t> segments_{0};
    std::atomic<bool> failed_{false};
};

}  // namespace mooncake::traffic
