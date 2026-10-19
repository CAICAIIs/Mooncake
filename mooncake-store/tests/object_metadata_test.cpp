// object_metadata_test.cpp
//
// The envelope's answer to "is this object readable" and the per-slot
// bookkeeping behind it: which parts hold content, what the object's total
// length is once part 0 is the only part written, and what popping replicas
// does to a slot.

#include "object_metadata.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "master_metric_manager.h"
#include "object_part.h"
#include "replica.h"
#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace {

constexpr size_t kPartLength = 4096;
constexpr uint32_t kPartCount = 3;
constexpr auto kWriteTime =
    std::chrono::system_clock::time_point(std::chrono::seconds(1700000000));

// A disk replica names a file and needs no allocator, so these cases build one
// without a segment.
Replica MakeDiskReplica(ReplicaStatus status) {
    return Replica(std::string("file://object-metadata-test"), kPartLength,
                   status);
}

std::vector<Replica> MakeDiskReplicas(
    const std::vector<ReplicaStatus>& statuses) {
    std::vector<Replica> replicas;
    replicas.reserve(statuses.size());
    for (const auto status : statuses) {
        replicas.push_back(MakeDiskReplica(status));
    }
    return replicas;
}

// The content goes into the part the constructor is told about; the other
// parts start empty.
std::unique_ptr<ObjectMetadata> MakeEnvelope(uint32_t part_count,
                                             uint32_t part_index,
                                             std::vector<Replica>&& replicas) {
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, kWriteTime, kPartLength, std::move(replicas), std::nullopt,
        false, ObjectDataType::UNKNOWN, std::string(), TenantId(),
        "object-metadata-test-key", part_count, part_index);
}

bool IsCompleted(const Replica& replica) { return replica.is_completed(); }

}  // namespace

// An object with parts 1 and 2 still empty is not readable, and its length is
// only what the written parts hold.
TEST(ObjectMetadataPartTest, OneWrittenPartOfThreeIsNotReadable) {
    auto metadata = MakeEnvelope(kPartCount, 0,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));

    ASSERT_EQ(metadata->part_count, kPartCount);
    EXPECT_NE(metadata->GetPart(0), nullptr);
    EXPECT_EQ(metadata->GetPart(1), nullptr);
    EXPECT_EQ(metadata->GetPart(2), nullptr);

    EXPECT_EQ(metadata->TotalLength(), kPartLength);
    EXPECT_TRUE(metadata->IsValid());
    EXPECT_FALSE(metadata->IsReadable(IsCompleted));
}

// Readability is computed from the replicas each time it is asked for, so a
// part going complete is visible without any state to refresh.
TEST(ObjectMetadataPartTest, ReadabilityFollowsTheReplicaStatus) {
    auto metadata =
        MakeEnvelope(1, 0, MakeDiskReplicas({ReplicaStatus::PROCESSING}));
    EXPECT_FALSE(metadata->IsReadable(IsCompleted));

    metadata->MutablePart(0)->replicas().front().mark_complete();
    EXPECT_TRUE(metadata->IsReadable(IsCompleted));
}

// A part whose only replica the predicate refuses leaves the object
// unreadable, even though the replica itself is complete.
TEST(ObjectMetadataPartTest, ARefusedReplicaLeavesTheObjectUnreadable) {
    auto metadata =
        MakeEnvelope(1, 0, MakeDiskReplicas({ReplicaStatus::COMPLETE}));

    const auto refuses_everything = [](const Replica&) { return false; };
    EXPECT_FALSE(metadata->IsReadable(refuses_everything));
    EXPECT_TRUE(metadata->IsReadable(IsCompleted));

    const auto accepts_only_memory = [](const Replica& replica) {
        return replica.is_memory_replica();
    };
    EXPECT_FALSE(metadata->IsReadable(accepts_only_memory));
}

// Emptying a part drops its length with it, so nothing of it is left to read
// or to measure, and the object's metric is decremented once, by destruction.
TEST(ObjectMetadataPartTest, EmptyingTheOnlyPartLeavesNothingBehind) {
    auto metadata = MakeEnvelope(kPartCount, 0,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));

    const int64_t key_count_with_object =
        MasterMetricManager::instance().get_key_count();
    EXPECT_EQ(metadata->EraseReplicas([](const Replica&) { return true; }), 1u);

    EXPECT_EQ(metadata->GetPart(0), nullptr);
    EXPECT_EQ(metadata->TotalLength(), 0u);
    EXPECT_EQ(metadata->CountReplicas(), 0u);
    EXPECT_FALSE(metadata->IsReadable(IsCompleted));
    EXPECT_FALSE(metadata->IsValid());
    EXPECT_EQ(MasterMetricManager::instance().get_key_count(),
              key_count_with_object);
}

// Popping and counting reach every slot, not just the first one that holds
// content.
TEST(ObjectMetadataPartTest, PoppingAndCountingReachEveryPart) {
    auto metadata = MakeEnvelope(kPartCount, 0,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    metadata->EmplacePart(
        1, kPartLength,
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING}));
    metadata->EmplacePart(2, kPartLength,
                          MakeDiskReplicas({ReplicaStatus::COMPLETE}));

    EXPECT_EQ(metadata->CountReplicas(), 4u);
    EXPECT_EQ(metadata->CountReplicas(&Replica::fn_is_completed), 3u);
    EXPECT_TRUE(metadata->IsReadable(IsCompleted));

    auto processing = metadata->PopAllReplicas(&Replica::fn_is_processing);
    EXPECT_EQ(processing.size(), 1u);
    EXPECT_EQ(metadata->CountReplicas(), 3u);
    // A part that lost only some of its replicas keeps its content.
    EXPECT_NE(metadata->GetPart(1), nullptr);
    EXPECT_EQ(metadata->TotalLength(), 3 * kPartLength);
    EXPECT_TRUE(metadata->IsReadable(IsCompleted));

    // A part that lost all of them drops its content, and with it the object's
    // readability.
    auto last_part = metadata->PopAllReplicas(
        [](const Replica& replica) { return replica.is_disk_replica(); });
    EXPECT_EQ(last_part.size(), 3u);
    EXPECT_EQ(metadata->GetPart(2), nullptr);
    EXPECT_EQ(metadata->GetPart(1), nullptr);
    EXPECT_EQ(metadata->TotalLength(), 0u);
    EXPECT_FALSE(metadata->IsReadable(IsCompleted));
}

}  // namespace mooncake
