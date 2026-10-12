// part_test.cpp
//
// The content the envelope now holds in a part: the operations on the part
// itself, the envelope's forwarding to its single part, and the payload
// encodings that carry the fields.

#include "object_metadata.h"
#include "part.h"

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <msgpack.hpp>

#include "master_config.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "metadata_store.h"
#include "replica.h"

namespace mooncake {
namespace {

constexpr size_t kObjectLength = 4096;
constexpr uint64_t kChecksum = 77;
constexpr const char* kGroupId = "group-1";
constexpr auto kWriteTime =
    std::chrono::system_clock::time_point(std::chrono::seconds(1700000000));
constexpr int64_t kWriteTimeMs = 1700000000000;
constexpr int64_t kLeaseTimeMs = 1700000060000;

// A disk replica names a file and needs no allocator, so a suite about the
// part's own bookkeeping builds one without a segment.
Replica MakeDiskReplica(ReplicaStatus status) {
    return Replica(std::string("file://part-test"), kObjectLength, status);
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

std::unique_ptr<ObjectMetadata> MakeEnvelope(size_t length,
                                             std::vector<Replica>&& replicas) {
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, kWriteTime, length, std::move(replicas), std::nullopt,
        false, ObjectDataType::UNKNOWN, std::string(kGroupId), TenantId(),
        "part-test-key");
}

// The array the payload writer emits for one object, written out by hand so a
// reader is checked against the format rather than against the writer.
std::vector<uint8_t> MakePayloadArray(uint64_t length,
                                      std::optional<uint64_t> checksum,
                                      bool with_optional_tail) {
    msgpack::sbuffer buffer;
    msgpack::packer<msgpack::sbuffer> packer(&buffer);
    packer.pack_array(with_optional_tail ? 11 : 7);
    packer.pack(std::string("1-2"));
    packer.pack(kWriteTimeMs);
    packer.pack(length);
    packer.pack(kLeaseTimeMs);
    // Soft pin is runtime-only state and the format keeps its two slots.
    packer.pack(false);
    packer.pack(uint64_t{0});
    packer.pack(uint32_t{0});  // replicas_count
    if (with_optional_tail) {
        packer.pack(static_cast<uint8_t>(ObjectDataType::KVCACHE));
        packer.pack(false);  // hard_pinned
        packer.pack(std::string(kGroupId));
        if (checksum.has_value()) {
            packer.pack(*checksum);
        }
    }
    return std::vector<uint8_t>(
        reinterpret_cast<const uint8_t*>(buffer.data()),
        reinterpret_cast<const uint8_t*>(buffer.data()) + buffer.size());
}

}  // namespace

// The payload encodings need a service for the segment view its replica
// entries resolve through, so these cases carry the logging setup the other
// service-backed suites use.
class PartSerializationTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("PartSerializationTest");
        FLAGS_logtostderr = true;
    }

    // glog rejects a second InitGoogleLogging with the same process, so each
    // case releases the one it took.
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST(PartTest, HoldsItsContentAndItsReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});

    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_FALSE(part.checksum().has_value());
    EXPECT_EQ(part.CountReplicas(), 0u);

    part.SetChecksum(kChecksum);
    ASSERT_TRUE(part.checksum().has_value());
    EXPECT_EQ(*part.checksum(), kChecksum);
}

TEST(PartTest, AppendsAndFindsReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});
    auto replicas =
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING});
    const ReplicaID processing_id = replicas.back().id();
    part.AddReplicas(std::move(replicas));

    EXPECT_EQ(part.CountReplicas(), 2u);
    EXPECT_EQ(part.CountReplicas(&Replica::fn_is_completed), 1u);
    EXPECT_EQ(part.GetAllReplicas().size(), 2u);
    EXPECT_TRUE(part.HasReplica(&Replica::fn_is_completed));
    EXPECT_TRUE(part.HasReplica(&Replica::fn_is_processing));
    EXPECT_FALSE(part.AllReplicas(&Replica::fn_is_completed));

    ASSERT_NE(part.GetReplicaByID(processing_id), nullptr);
    EXPECT_EQ(part.GetReplicaByID(processing_id)->status(),
              ReplicaStatus::PROCESSING);
    EXPECT_EQ(part.GetReplicaByID(0), nullptr);

    const auto first_other = part.HasDiffRepStatus(ReplicaStatus::COMPLETE);
    ASSERT_TRUE(first_other.has_value());
    EXPECT_EQ(*first_other, ReplicaStatus::PROCESSING);

    // Nothing here is a memory or NoF replica, so the medium counts are zero
    // and the handle checks answer for the medium each replica is on.
    EXPECT_FALSE(part.HasMemReplica());
    EXPECT_FALSE(part.HasNoFReplica());
    EXPECT_EQ(part.GetMemReplicaCount(), 0u);
    EXPECT_EQ(part.GetNoFReplicaCount(), 0u);
}

TEST(PartTest, VisitsOnlyTheMatchingReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    size_t visited = 0;
    const size_t matched =
        part.VisitReplicas(&Replica::fn_is_completed, [&](Replica& replica) {
            ++visited;
            EXPECT_EQ(replica.status(), ReplicaStatus::COMPLETE);
        });

    EXPECT_EQ(matched, 2u);
    EXPECT_EQ(visited, 2u);
}

TEST(PartTest, PopsAndErasesOnlyTheMatchingReplicas) {
    Part part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    auto popped = part.PopReplicas(&Replica::fn_is_processing);
    ASSERT_EQ(popped.size(), 1u);
    EXPECT_EQ(popped.front().status(), ReplicaStatus::PROCESSING);
    EXPECT_EQ(part.CountReplicas(), 2u);
    EXPECT_TRUE(part.AllReplicas(&Replica::fn_is_completed));

    const ReplicaID kept_id = part.GetAllReplicas().front().id();
    EXPECT_FALSE(part.EraseReplicaByID(0));
    EXPECT_TRUE(part.EraseReplicaByID(kept_id));
    EXPECT_EQ(part.CountReplicas(), 1u);

    auto remaining = part.PopReplicas();
    EXPECT_EQ(remaining.size(), 1u);
    EXPECT_EQ(part.CountReplicas(), 0u);
    EXPECT_TRUE(part.GetAllReplicas().empty());
}

TEST(PartTest, EraseReplicasReportsTheNumberOfMatches) {
    Part part(kObjectLength, std::vector<Replica>{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::COMPLETE}));

    EXPECT_EQ(part.EraseReplicas(&Replica::fn_is_completed), 2u);
    EXPECT_EQ(part.CountReplicas(), 0u);
}

TEST(EnvelopePartTest, TheEnvelopeForwardsToItsSinglePart) {
    auto metadata = MakeEnvelope(kObjectLength,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    metadata->MutablePart(0).SetChecksum(kChecksum);

    const Part& part = metadata->GetPart(0);
    EXPECT_EQ(&metadata->MutablePart(0), &part);
    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_EQ(*part.checksum(), kChecksum);
    EXPECT_EQ(metadata->CountReplicas(), part.CountReplicas());
    EXPECT_EQ(&metadata->GetAllReplicas(), &part.GetAllReplicas());
    EXPECT_TRUE(metadata->HasReplica(&Replica::fn_is_completed));
    EXPECT_EQ(metadata->GetPart(0).length(), kObjectLength);
    // The aggregate reads the same content the one part holds.
    EXPECT_EQ(metadata->TotalLength(), kObjectLength);
}

TEST(EnvelopePartTest, ValidityFollowsThePartsLengthAndReplicas) {
    auto with_replica = MakeEnvelope(
        kObjectLength, MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    EXPECT_TRUE(with_replica->IsValid());

    auto without_replica = MakeEnvelope(kObjectLength, {});
    EXPECT_FALSE(without_replica->IsValid());

    auto without_content =
        MakeEnvelope(0, MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    EXPECT_FALSE(without_content->IsValid());
}

TEST_F(PartSerializationTest, ThePayloadKeepsTheFieldsInTheirSlots) {
    MasterService service{MasterServiceConfig{}};
    auto metadata = MakeEnvelope(kObjectLength, {});
    metadata->MutablePart(0).SetChecksum(kChecksum);

    auto bytes = test::MasterServiceTestPeer::SerializeObjectMetadataForTesting(
        service, *metadata);
    ASSERT_TRUE(bytes.has_value()) << bytes.error().message;

    auto handle = msgpack::unpack(reinterpret_cast<const char*>(bytes->data()),
                                  bytes->size());
    const msgpack::object& array = handle.get();
    ASSERT_EQ(array.type, msgpack::type::ARRAY);
    // [client_id, put_start_time, size, lease_timeout, has_soft_pin_timeout,
    //  soft_pin_timeout, replicas_count, data_type, hard_pinned, group_id,
    //  object_checksum]
    ASSERT_EQ(array.via.array.size, 11u);
    EXPECT_EQ(array.via.array.ptr[0].as<std::string>(), "1-2");
    EXPECT_EQ(array.via.array.ptr[1].as<uint64_t>(), kWriteTimeMs);
    EXPECT_EQ(array.via.array.ptr[2].as<uint64_t>(), kObjectLength);
    EXPECT_EQ(array.via.array.ptr[6].as<uint32_t>(), 0u);
    EXPECT_EQ(array.via.array.ptr[7].as<uint8_t>(),
              static_cast<uint8_t>(ObjectDataType::UNKNOWN));
    EXPECT_EQ(array.via.array.ptr[8].as<bool>(), false);
    EXPECT_EQ(array.via.array.ptr[9].as<std::string>(), kGroupId);
    EXPECT_EQ(array.via.array.ptr[10].as<uint64_t>(), kChecksum);
}

TEST_F(PartSerializationTest, ReadsAPayloadWrittenBeforeThePartsSplit) {
    MasterService service{MasterServiceConfig{}};
    const auto bytes = MakePayloadArray(kObjectLength, kChecksum, true);

    auto restored =
        test::MasterServiceTestPeer::DeserializeObjectMetadataForTesting(
            service, bytes);
    ASSERT_TRUE(restored.has_value()) << restored.error().message;

    const auto& metadata = *restored.value();
    EXPECT_EQ(metadata.client_id, (UUID{1, 2}));
    EXPECT_EQ(metadata.GetPart(0).length(), kObjectLength);
    ASSERT_TRUE(metadata.GetPart(0).checksum().has_value());
    EXPECT_EQ(*metadata.GetPart(0).checksum(), kChecksum);
    EXPECT_EQ(metadata.group_id, kGroupId);
    EXPECT_EQ(metadata.data_type, ObjectDataType::KVCACHE);
    EXPECT_FALSE(metadata.IsHardPinned());
    EXPECT_EQ(metadata.EvictionDeadline(),
              std::chrono::system_clock::time_point(
                  std::chrono::milliseconds(kLeaseTimeMs)));
}

TEST_F(PartSerializationTest, ReadsTheOldestPayloadWithoutTheOptionalFields) {
    MasterService service{MasterServiceConfig{}};
    const auto bytes = MakePayloadArray(kObjectLength, std::nullopt, false);

    auto restored =
        test::MasterServiceTestPeer::DeserializeObjectMetadataForTesting(
            service, bytes);
    ASSERT_TRUE(restored.has_value()) << restored.error().message;

    const auto& metadata = *restored.value();
    EXPECT_EQ(metadata.GetPart(0).length(), kObjectLength);
    EXPECT_FALSE(metadata.GetPart(0).checksum().has_value());
    EXPECT_TRUE(metadata.group_id.empty());
    EXPECT_EQ(metadata.data_type, ObjectDataType::UNKNOWN);
}

TEST_F(PartSerializationTest, TheOplogPayloadCarriesThePartsContent) {
    MasterService service{MasterServiceConfig{}};
    auto metadata = MakeEnvelope(kObjectLength, {});
    metadata->MutablePart(0).SetChecksum(kChecksum);

    const std::string bytes =
        test::MasterServiceTestPeer::SerializeMetadataForOpLogForTesting(
            service, *metadata);

    MetadataPayload payload;
    ASSERT_EQ(struct_pack::deserialize_to(payload, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(payload.client_id, (UUID{1, 2}));
    EXPECT_EQ(payload.size, kObjectLength);
    EXPECT_TRUE(payload.replicas.empty());
    EXPECT_EQ(payload.group_id.value_or(""), kGroupId);
    EXPECT_EQ(payload.data_type.value_or(ObjectDataType::UNKNOWN),
              ObjectDataType::UNKNOWN);
    EXPECT_FALSE(payload.hard_pinned.value_or(true));
}

}  // namespace mooncake
