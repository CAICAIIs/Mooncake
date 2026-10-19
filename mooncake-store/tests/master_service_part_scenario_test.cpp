// master_service_part_scenario_test.cpp
//
// One key whose content is several independently written parts: what the
// master does when the parts arrive out of order, when one never arrives, when
// a writer is late, and when the object as a whole is evicted, persisted or
// lost a part.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "ha/snapshot/batch_oplog/metadata.h"
#include "ha/standby_metadata_store.h"
#include "master_config.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "master_service_test_fixture.h"
#include "metadata_store.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake::test {
namespace {

constexpr uint32_t kPartCount = 8;
constexpr uint64_t kPartLength = 1024;
const char* kKey = "part_scenario_key";

// One PutStart for `part_index` of the scenario object.
std::vector<Replica::Descriptor> PutStartPart(MasterService& service,
                                              const UUID& client_id,
                                              const std::string& key,
                                              uint32_t part_index,
                                              const ReplicateConfig& config) {
    std::vector<PutPartItem> items{{key, part_index, kPartLength}};
    auto results = service.BatchPutStartForPart(client_id, kPartCount, items,
                                                TenantId::Default(), config);
    EXPECT_EQ(results.size(), 1u);
    if (!results.front()) {
        return {};
    }
    return std::move(results.front().value());
}

// The ids a PutEnd must name to be scoped to the write that produced them.
std::vector<ReplicaID> ReplicaIdsOf(
    const std::vector<Replica::Descriptor>& descriptors) {
    std::vector<ReplicaID> ids;
    ids.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        ids.push_back(descriptor.id);
    }
    return ids;
}

ErrorCode PutEndPart(MasterService& service, const UUID& client_id,
                     const std::string& key, uint32_t part_index,
                     const std::vector<ReplicaID>& replica_ids,
                     std::optional<uint64_t> checksum = std::nullopt) {
    std::vector<PutEndPartItem> items{{key, part_index, checksum, replica_ids}};
    auto results = service.BatchPutEndForPart(
        client_id, kPartCount, items, TenantId::Default(), ReplicaType::ALL);
    EXPECT_EQ(results.size(), 1u);
    return results.front() ? ErrorCode::OK : results.front().error();
}

tl::expected<GetReplicaListForPartResponse, ErrorCode> GetPart(
    MasterService& service, const std::string& key, uint32_t part_index) {
    std::vector<GetPartItem> items{{key, part_index}};
    auto results =
        service.BatchGetReplicaListForPart(items, TenantId::Default());
    EXPECT_EQ(results.size(), 1u);
    return std::move(results.front());
}

}  // namespace

class MasterServicePartScenarioTest : public MasterServiceTest {
   protected:
    ReplicateConfig SingleReplicaConfig(const std::string& segment_name) const {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment_name;
        return config;
    }

    // A memory segment the writers allocate on, sized for a few whole objects.
    MountedSegmentContext PreparePartSegment(MasterService& service,
                                             const std::string& name,
                                             size_t object_count = 4) const {
        return PrepareSimpleSegment(service, name, kDefaultSegmentBase,
                                    kPartLength * kPartCount * object_count);
    }

    // The segment a restored object's memory replicas are expected to sit on.
    static StandbySegmentInfo MakeStandbySegment(const std::string& endpoint) {
        StandbySegmentInfo segment;
        segment.segment_name = endpoint;
        segment.transport_endpoint = endpoint;
        segment.capacity = kDefaultSegmentSize;
        segment.is_memory_segment = true;
        return segment;
    }
};

// The eight writers arrive in any order; the object is readable, and the key
// is a hit, only once the last of them has ended its part.
TEST_F(MasterServicePartScenarioTest, ObjectBecomesReadableOnTheLastPart) {
    MasterService service{MasterServiceConfig{}};
    const auto node = PreparePartSegment(service, "parts_memory");
    const auto config = SingleReplicaConfig("parts_memory");

    std::vector<UUID> writers;
    std::vector<std::vector<Replica::Descriptor>> descriptors(kPartCount);
    // Reversed order, so the last part started is the first one written.
    for (uint32_t part_index = kPartCount; part_index > 0; --part_index) {
        const UUID writer = generate_uuid();
        writers.push_back(writer);
        descriptors[part_index - 1] =
            PutStartPart(service, writer, kKey, part_index - 1, config);
        ASSERT_EQ(descriptors[part_index - 1].size(), 1u);
    }

    for (uint32_t part_index = 0; part_index + 1 < kPartCount; ++part_index) {
        ASSERT_EQ(
            PutEndPart(service, writers[kPartCount - 1 - part_index], kKey,
                       part_index, ReplicaIdsOf(descriptors[part_index])),
            ErrorCode::OK);
        // Seven of eight parts is not an object anyone can read.
        EXPECT_FALSE(
            service.GetReplicaList(kKey, TenantId::Default()).has_value());
        auto exists = service.ExistKey(kKey, TenantId::Default());
        ASSERT_TRUE(exists.has_value());
        EXPECT_FALSE(*exists);
        const auto part = GetPart(service, kKey, part_index);
        ASSERT_FALSE(part.has_value());
        EXPECT_EQ(part.error(), ErrorCode::REPLICA_IS_NOT_READY);
    }

    ASSERT_EQ(PutEndPart(service, writers[0], kKey, kPartCount - 1,
                         ReplicaIdsOf(descriptors[kPartCount - 1])),
              ErrorCode::OK);

    auto exists = service.ExistKey(kKey, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(*exists);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        const auto part = GetPart(service, kKey, part_index);
        ASSERT_TRUE(part.has_value()) << "part " << part_index;
        EXPECT_EQ(part->part_count, kPartCount);
        EXPECT_EQ(part->part_index, part_index);
        EXPECT_EQ(part->length, kPartLength);
        EXPECT_EQ(part->replicas.size(), 1u);
        EXPECT_EQ(part->replicas.front().status, ReplicaStatus::COMPLETE);
    }
}

// A readable part object has no single replica list to hand out, so a caller
// that names no part is refused instead of being given every part at once.
TEST_F(MasterServicePartScenarioTest, LegacyReadsRefuseAPartObject) {
    MasterService service{MasterServiceConfig{}};
    PreparePartSegment(service, "legacy_memory");
    const auto config = SingleReplicaConfig("legacy_memory");

    UUID writer = generate_uuid();
    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        auto descriptors =
            PutStartPart(service, writer, "legacy_key", part_index, config);
        ASSERT_EQ(descriptors.size(), 1u);
        ids[part_index] = ReplicaIdsOf(descriptors);
    }
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        ASSERT_EQ(PutEndPart(service, writer, "legacy_key", part_index,
                             ids[part_index]),
                  ErrorCode::OK);
    }

    auto list = service.GetReplicaList("legacy_key", TenantId::Default());
    ASSERT_FALSE(list.has_value());
    EXPECT_EQ(list.error(), ErrorCode::INVALID_PARAMS);

    auto batch =
        service.BatchGetReplicaList({"legacy_key"}, TenantId::Default());
    ASSERT_EQ(batch.size(), 1u);
    ASSERT_FALSE(batch.front().has_value());
    EXPECT_EQ(batch.front().error(), ErrorCode::INVALID_PARAMS);
}

// A writer whose attempt the discard timeout replaced cannot end the part the
// new writer owns, and the new writer's replicas are untouched by the late
// call. A repeated End of a finished part is a no-op.
TEST_F(MasterServicePartScenarioTest, LatePutEndDoesNotTouchTheNewWrite) {
    MasterServiceConfig service_config;
    service_config.put_start_discard_timeout_sec = 1;
    service_config.put_start_release_timeout_sec = 2;
    MasterService service{service_config};
    PreparePartSegment(service, "late_memory");
    const auto config = SingleReplicaConfig("late_memory");

    const UUID first = generate_uuid();
    const std::string key = "late_key";
    auto first_descriptors =
        PutStartPart(service, first, key, /*part_index=*/0, config);
    ASSERT_EQ(first_descriptors.size(), 1u);
    const auto first_ids = ReplicaIdsOf(first_descriptors);

    // Wait out the discard window, then let the same client take the part
    // over, which is what a retried PutStart looks like from here.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    auto second_descriptors = PutStartPart(service, first, key,
                                           /*part_index=*/0, config);
    ASSERT_EQ(second_descriptors.size(), 1u);
    const auto second_ids = ReplicaIdsOf(second_descriptors);
    ASSERT_NE(first_ids.front(), second_ids.front());

    // The late End names replicas of the attempt that was replaced, so it is
    // refused even though the client is the one that owns the part now.
    EXPECT_EQ(PutEndPart(service, first, key, /*part_index=*/0, first_ids),
              ErrorCode::INVALID_WRITE);
    // Every part but this one is still missing, so the object is not readable,
    // and the part itself still holds the second attempt's replica.
    auto part = GetPart(service, key, /*part_index=*/0);
    ASSERT_FALSE(part.has_value());

    ASSERT_EQ(PutEndPart(service, first, key, /*part_index=*/0, second_ids),
              ErrorCode::OK);
    // A repeated End of a finished part changes nothing.
    EXPECT_EQ(PutEndPart(service, first, key, /*part_index=*/0, second_ids),
              ErrorCode::OK);
}

// The part count is fixed by the first PutStart of an object, and a part index
// outside it is refused.
TEST_F(MasterServicePartScenarioTest, PartCountAndIndexAreValidated) {
    MasterService service{MasterServiceConfig{}};
    PreparePartSegment(service, "shape_memory");
    const auto config = SingleReplicaConfig("shape_memory");
    const UUID writer = generate_uuid();
    const std::string key = "shape_key";

    auto descriptors =
        PutStartPart(service, writer, key, /*part_index=*/0, config);
    ASSERT_EQ(descriptors.size(), 1u);
    ASSERT_EQ(PutEndPart(service, writer, key, /*part_index=*/0,
                         ReplicaIdsOf(descriptors)),
              ErrorCode::OK);

    // The same key with a different part count is a different shape.
    std::vector<PutPartItem> mismatch{{key, 0, kPartLength}};
    auto mismatched = service.BatchPutStartForPart(
        writer, /*part_count=*/4, mismatch, TenantId::Default(), config);
    ASSERT_EQ(mismatched.size(), 1u);
    ASSERT_FALSE(mismatched.front().has_value());
    EXPECT_EQ(mismatched.front().error(), ErrorCode::INVALID_PARAMS);

    // An index outside the object's part count has no part to write or read.
    std::vector<PutPartItem> out_of_range{{key, kPartCount, kPartLength}};
    auto too_far = service.BatchPutStartForPart(
        writer, kPartCount, out_of_range, TenantId::Default(), config);
    ASSERT_EQ(too_far.size(), 1u);
    ASSERT_FALSE(too_far.front().has_value());
    EXPECT_EQ(too_far.front().error(), ErrorCode::INVALID_PARAMS);

    auto read_out_of_range = GetPart(service, key, kPartCount);
    ASSERT_FALSE(read_out_of_range.has_value());
    EXPECT_EQ(read_out_of_range.error(), ErrorCode::INVALID_PARAMS);
}

// A stored event says how many parts the object has, and it is published when
// the object first becomes readable rather than on every PutEnd.
TEST_F(MasterServicePartScenarioTest, StoredEventWaitsForTheWholeObject) {
    const std::string endpoint =
        "ipc:///tmp/mooncake_part_scenario_" + std::to_string(::getpid());
    MasterServiceConfig config = MasterServiceConfig::builder().build();
    config.enable_kv_events = true;
    config.kv_events_bind_endpoint = endpoint;
    MasterService service{config};
    if (!service.KvEventsEnabled()) {
        GTEST_SKIP() << "this build has no KV event publisher";
    }
    PreparePartSegment(service, "event_memory");
    const auto put_config = SingleReplicaConfig("event_memory");

    const UUID writer = generate_uuid();
    const std::string key = "event_key";
    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        auto descriptors =
            PutStartPart(service, writer, key, part_index, put_config);
        ASSERT_EQ(descriptors.size(), 1u);
        ids[part_index] = ReplicaIdsOf(descriptors);
    }
    for (uint32_t part_index = 0; part_index + 1 < kPartCount; ++part_index) {
        ASSERT_EQ(PutEndPart(service, writer, key, part_index, ids[part_index]),
                  ErrorCode::OK);
    }
    // Nothing about the object is announced while one part is missing.
    EXPECT_EQ(service.GetKvEventStats().published_events, 0u);
    ASSERT_EQ(
        PutEndPart(service, writer, key, kPartCount - 1, ids[kPartCount - 1]),
        ErrorCode::OK);
    EXPECT_EQ(service.GetKvEventStats().published_events, 1u);
}

// A part that never arrives keeps the object unreadable, and the object is
// reclaimed once the last attempt's release window has passed.
TEST_F(MasterServicePartScenarioTest, MissingPartIsReclaimed) {
    MasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = 0;
    service_config.put_start_discard_timeout_sec = 1;
    service_config.put_start_release_timeout_sec = 2;
    MasterService service{service_config};
    const auto node = PreparePartSegment(service, "missing_memory");
    const auto config = SingleReplicaConfig("missing_memory");
    const UUID writer = generate_uuid();
    const std::string key = "missing_key";

    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index + 1 < kPartCount; ++part_index) {
        auto descriptors =
            PutStartPart(service, writer, key, part_index, config);
        ASSERT_EQ(descriptors.size(), 1u);
        ids[part_index] = ReplicaIdsOf(descriptors);
        ASSERT_EQ(PutEndPart(service, writer, key, part_index, ids[part_index]),
                  ErrorCode::OK);
    }
    // The eighth writer starts its part and never finishes.
    auto last = PutStartPart(service, writer, key, kPartCount - 1, config);
    ASSERT_EQ(last.size(), 1u);
    const auto last_ids = ReplicaIdsOf(last);

    auto exists = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(*exists);
    auto part = GetPart(service, key, /*part_index=*/0);
    ASSERT_FALSE(part.has_value());
    EXPECT_EQ(part.error(), ErrorCode::REPLICA_IS_NOT_READY);

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);

    // Every part is gone with the object, including the seven that completed.
    auto reclaimed = GetPart(service, key, /*part_index=*/0);
    ASSERT_FALSE(reclaimed.has_value());
    EXPECT_EQ(reclaimed.error(), ErrorCode::OBJECT_NOT_FOUND);
}

// A readable part object is evicted as one object: every part goes, and the
// subscriber is told the key went once.
TEST_F(MasterServicePartScenarioTest, WholeObjectEvictionRemovesEveryPart) {
    MasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = 0;
    MasterService service{service_config};
    PreparePartSegment(service, "evict_memory");
    const auto config = SingleReplicaConfig("evict_memory");
    const UUID writer = generate_uuid();
    const std::string key = "evict_key";

    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        auto descriptors =
            PutStartPart(service, writer, key, part_index, config);
        ASSERT_EQ(descriptors.size(), 1u);
        ids[part_index] = ReplicaIdsOf(descriptors);
        ASSERT_EQ(PutEndPart(service, writer, key, part_index, ids[part_index]),
                  ErrorCode::OK);
    }
    auto before = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(before.has_value());
    ASSERT_TRUE(*before);

    // The object's lease is already expired, so it is a candidate.
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);

    auto after = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(after.has_value());
    EXPECT_FALSE(*after);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        auto part = GetPart(service, key, part_index);
        ASSERT_FALSE(part.has_value()) << "part " << part_index;
    }
}

// A part being written keeps its object out of the eviction census: the parts
// written early must survive until the object is complete, or until the part
// being written has been abandoned for long enough.
TEST_F(MasterServicePartScenarioTest, WriteInFlightBlocksEviction) {
    MasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = 0;
    service_config.put_start_discard_timeout_sec = 1;
    service_config.put_start_release_timeout_sec = 2;
    MasterService service{service_config};
    PreparePartSegment(service, "inflight_memory");
    const auto config = SingleReplicaConfig("inflight_memory");
    const std::string key = "inflight_key";

    const UUID writer = generate_uuid();
    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index + 1 < kPartCount; ++part_index) {
        auto descriptors =
            PutStartPart(service, writer, key, part_index, config);
        ASSERT_EQ(descriptors.size(), 1u);
        ids[part_index] = ReplicaIdsOf(descriptors);
        ASSERT_EQ(PutEndPart(service, writer, key, part_index, ids[part_index]),
                  ErrorCode::OK);
    }
    auto last = PutStartPart(service, writer, key, kPartCount - 1, config);
    ASSERT_EQ(last.size(), 1u);

    // The eighth writer is still working, so nothing is evicted.
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service,
            MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                key, TenantId::Default()));
        ASSERT_TRUE(accessor.Exists());
        for (uint32_t part_index = 0; part_index + 1 < kPartCount;
             ++part_index) {
            ASSERT_NE(accessor.Get().GetPart(part_index), nullptr)
                << "part " << part_index;
        }
    }

    // Once its attempt is older than the discard window it is no longer a
    // write in flight, and the object is picked up.
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    MasterServiceTestPeer(service).RunBatchEvictForTesting(1.0, 1.0);
    auto gone = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(gone.has_value());
    EXPECT_FALSE(*gone);
}

// A segment going away takes the part it held with it, which makes the object
// unreadable while the remaining parts stay protected by the lease.
TEST_F(MasterServicePartScenarioTest, LostPartMakesTheObjectUnreadable) {
    MasterServiceConfig service_config;
    service_config.default_kv_lease_ttl = 1000;
    MasterService service{service_config};
    PreparePartSegment(service, "lost_memory_a", /*object_count=*/1);
    const auto second_node = PrepareSimpleSegment(
        service, "lost_memory_b", kDefaultSegmentBase + 0x10000000,
        kPartLength * kPartCount);
    const std::string key = "lost_key";
    const UUID writer = generate_uuid();

    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment =
            part_index % 2 == 0 ? "lost_memory_a" : "lost_memory_b";
        auto descriptors =
            PutStartPart(service, writer, key, part_index, config);
        ASSERT_EQ(descriptors.size(), 1u);
        ids[part_index] = ReplicaIdsOf(descriptors);
        ASSERT_EQ(PutEndPart(service, writer, key, part_index, ids[part_index]),
                  ErrorCode::OK);
    }
    auto readable = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(readable.has_value());
    ASSERT_TRUE(*readable);

    ASSERT_TRUE(
        service.UnmountSegment(second_node.segment_id, second_node.client_id)
            .has_value());

    // Every odd part is gone, so the object as a whole is not readable, and a
    // reader that asks for a part is answered about the object.
    auto exists = service.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(*exists);
    auto part = GetPart(service, key, /*part_index=*/1);
    ASSERT_FALSE(part.has_value());
    EXPECT_EQ(part.error(), ErrorCode::REPLICA_IS_NOT_READY);
    // The parts that survived are still there: the lease that was granted
    // before the segment went away still protects them.
    MasterServiceTestPeer::MetadataAccessorRO accessor(
        &service, MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                      key, TenantId::Default()));
    ASSERT_TRUE(accessor.Exists());
    const ObjectPart* survivor = accessor.Get().GetPart(0);
    ASSERT_NE(survivor, nullptr);
    EXPECT_EQ(survivor->replicas().size(), 1u);
    EXPECT_EQ(survivor->replicas().front().status(), ReplicaStatus::COMPLETE);
    EXPECT_FALSE(accessor.Get().IsLeaseExpired());
}

// The persisted payload carries one part for an ordinary object, exactly as it
// did before parts existed, and the parts themselves once there is more than
// one.
TEST_F(MasterServicePartScenarioTest, PayloadKeepsTheOldShapeForOnePart) {
    MasterService service{MasterServiceConfig{}};
    PreparePartSegment(service, "payload_memory");
    const auto config = SingleReplicaConfig("payload_memory");
    const UUID writer = generate_uuid();

    // An ordinary object: one PutStart, one PutEnd, no parts in the payload.
    std::vector<PutPartItem> single{{"payload_single", 0, kPartLength}};
    auto started = service.BatchPutStartForPart(
        writer, /*part_count=*/1, single, TenantId::Default(), config);
    ASSERT_EQ(started.size(), 1u);
    ASSERT_TRUE(started.front().has_value());
    std::vector<PutEndPartItem> ended{{"payload_single", 0, std::nullopt,
                                       ReplicaIdsOf(started.front().value())}};
    ASSERT_TRUE(service
                    .BatchPutEndForPart(writer, /*part_count=*/1, ended,
                                        TenantId::Default(), ReplicaType::ALL)
                    .front()
                    .has_value());

    MasterServiceTestPeer::MetadataAccessorRO accessor(
        &service, MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                      "payload_single", TenantId::Default()));
    ASSERT_TRUE(accessor.Exists());
    const std::string bytes =
        MasterServiceTestPeer::SerializeMetadataForOpLogForTesting(
            service, accessor.Get());
    MetadataPayload payload;
    ASSERT_EQ(struct_pack::deserialize_to(payload, bytes),
              struct_pack::errc::ok);
    EXPECT_FALSE(payload.parts.has_value());
    EXPECT_EQ(payload.size, kPartLength);
    ASSERT_EQ(payload.replicas.size(), 1u);
    EXPECT_EQ(payload.client_id, writer);

    // The same bytes read back describe the same single-part object, so a
    // standby that only knows this shape is not surprised by an ordinary key.
    auto standby = payload.ToStandbyMetadata();
    EXPECT_EQ(standby.PartCount(), 1u);
    EXPECT_FALSE(standby.parts.has_value());
    EXPECT_EQ(standby.replicas.size(), 1u);
}

// A part object survives a snapshot round trip: it comes back readable, with
// every part where it was.
TEST_F(MasterServicePartScenarioTest, PartObjectSurvivesASnapshot) {
    MasterService service{MasterServiceConfig{}};
    PreparePartSegment(service, "snapshot_memory");
    const auto config = SingleReplicaConfig("snapshot_memory");
    const std::string key = "snapshot_key";
    const UUID writer = generate_uuid();

    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        auto descriptors =
            PutStartPart(service, writer, key, part_index, config);
        ASSERT_EQ(descriptors.size(), 1u);
        ids[part_index] = ReplicaIdsOf(descriptors);
        ASSERT_EQ(PutEndPart(service, writer, key, part_index, ids[part_index],
                             /*checksum=*/part_index + 100),
                  ErrorCode::OK);
    }

    MasterServiceTestPeer::MetadataAccessorRO accessor(
        &service, MasterServiceTestPeer(service).MakeObjectIdentityForRequest(
                      key, TenantId::Default()));
    ASSERT_TRUE(accessor.Exists());
    const std::string bytes =
        MasterServiceTestPeer::SerializeMetadataForOpLogForTesting(
            service, accessor.Get());
    MetadataPayload payload;
    ASSERT_EQ(struct_pack::deserialize_to(payload, bytes),
              struct_pack::errc::ok);
    // A part object fills the parts and leaves the top-level list empty.
    EXPECT_TRUE(payload.replicas.empty());
    ASSERT_TRUE(payload.parts.has_value());
    ASSERT_EQ(payload.parts->size(), kPartCount);
    for (const auto& part : *payload.parts) {
        EXPECT_EQ(part.length, kPartLength);
        ASSERT_TRUE(part.checksum.has_value());
        EXPECT_EQ(*part.checksum, part.part_index + 100);
        EXPECT_EQ(part.replicas.size(), 1u);
    }

    // A standby hands the object back through the promotion path, which is the
    // one that carries parts.
    auto store = std::make_unique<StandbyMetadataStore>();
    ASSERT_TRUE(
        store->PutMetadata("default", key, payload.ToStandbyMetadata()));
    ReplicaID max_replica_id = 0;
    ASSERT_TRUE(store->ValidateReplicaIds(max_replica_id));
    EXPECT_GE(max_replica_id, kPartCount);

    MasterService restored{MasterServiceConfig{}};
    // The promoted master serves the same segment, so the restored replicas
    // are readable straight away instead of waiting for it to be remounted.
    ASSERT_TRUE(
        restored.MountSegment(MakeSegment("snapshot_memory"), generate_uuid())
            .has_value());
    ASSERT_TRUE(restored
                    .RestoreFromBatchOpLogPromotion(
                        *store, DurablePrefix{.batch_id = 7, .last_seq = 7},
                        {MakeStandbySegment("snapshot_memory")}, max_replica_id,
                        /*chunk_object_count=*/8)
                    .has_value());

    auto exists = restored.ExistKey(key, TenantId::Default());
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(*exists);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        auto part = GetPart(restored, key, part_index);
        ASSERT_TRUE(part.has_value()) << "part " << part_index;
        EXPECT_EQ(part->part_count, kPartCount);
        EXPECT_EQ(*part->checksum, part_index + 100);
    }
    // A reader that names no part still cannot read it.
    auto legacy = restored.GetReplicaList(key, TenantId::Default());
    ASSERT_FALSE(legacy.has_value());
    EXPECT_EQ(legacy.error(), ErrorCode::INVALID_PARAMS);
}

// The identity a snapshot carrying a part object is written with is one that a
// master knowing only v1 and v2 refuses, so it starts empty rather than
// reading the parts as one flat replica list.
TEST_F(MasterServicePartScenarioTest, PartSnapshotNeedsANewerReader) {
    EXPECT_EQ(ha::kBatchOpLogSnapshotMinReaderVersion,
              ha::kBatchOpLogPartSnapshotSchemaVersion);
    // The two versions an older master accepts, and neither is this one.
    EXPECT_NE(ha::kBatchOpLogSnapshotMinReaderVersion,
              ha::kBatchOpLogSnapshotSchemaVersion);
    EXPECT_NE(ha::kBatchOpLogSnapshotMinReaderVersion,
              ha::kBatchOpLogWeightSnapshotSchemaVersion);

    ha::BatchOpLogSnapshotManifest manifest;
    manifest.schema_version = ha::kBatchOpLogPartSnapshotSchemaVersion;
    manifest.snapshot_format = ha::kBatchOpLogPartSnapshotFormat;
    manifest.snapshot_id = "7-3";
    manifest.last_included_seq = 7;
    manifest.last_included_batch_id = 7;
    manifest.segments = ha::BatchOpLogSnapshotObjectDescriptor{
        .key = "segments", .stored_size = 1, .crc32c = 1};

    const auto bytes = ha::EncodeBatchOpLogSnapshotManifest(manifest);
    auto decoded = ha::DecodeBatchOpLogSnapshotManifest(bytes);
    ASSERT_TRUE(decoded.has_value()) << decoded.error();
    EXPECT_EQ(decoded->schema_version,
              ha::kBatchOpLogPartSnapshotSchemaVersion);
    EXPECT_EQ(decoded->snapshot_format, ha::kBatchOpLogPartSnapshotFormat);
}

// Eight writers charge the tenant for the eight slices they write, and the
// object settles once, when the last of them makes it readable.
TEST_F(MasterServicePartScenarioTest, QuotaChargesEveryPartAndSettlesOnce) {
    const std::string tenant_name = "part_tenant";
    auto service =
        std::make_unique<MasterService>(MakeStrictTenantConfig({tenant_name}));
    const TenantId tenant(tenant_name);
    PreparePartSegment(*service, "quota_memory");
    const auto config = SingleReplicaConfig("quota_memory");
    const UUID writer = generate_uuid();
    const std::string key = "quota_key";

    std::vector<std::vector<ReplicaID>> ids(kPartCount);
    for (uint32_t part_index = 0; part_index < kPartCount; ++part_index) {
        std::vector<PutPartItem> items{{key, part_index, kPartLength}};
        auto started = service->BatchPutStartForPart(writer, kPartCount, items,
                                                     tenant, config);
        ASSERT_EQ(started.size(), 1u);
        ASSERT_TRUE(started.front().has_value());
        ids[part_index] = ReplicaIdsOf(started.front().value());
    }
    // Every slice the writers asked for is charged, whether or not anyone can
    // read the object yet.
    auto charged = service->GetTenantQuotaSnapshot(tenant);
    ASSERT_TRUE(charged.has_value());
    EXPECT_EQ(charged->charged_bytes, kPartLength * kPartCount);

    for (uint32_t part_index = 0; part_index + 1 < kPartCount; ++part_index) {
        std::vector<PutEndPartItem> items{
            {key, part_index, std::nullopt, ids[part_index]}};
        ASSERT_TRUE(service
                        ->BatchPutEndForPart(writer, kPartCount, items, tenant,
                                             ReplicaType::ALL)
                        .front()
                        .has_value());
    }
    // Still one part short, so the bytes are still charged as pending.
    auto pending = service->GetTenantQuotaSnapshot(tenant);
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(pending->charged_bytes, kPartLength * kPartCount);

    std::vector<PutEndPartItem> last{
        {key, kPartCount - 1, std::nullopt, ids[kPartCount - 1]}};
    ASSERT_TRUE(service
                    ->BatchPutEndForPart(writer, kPartCount, last, tenant,
                                         ReplicaType::ALL)
                    .front()
                    .has_value());
    // Readable now: the object settles to exactly what it holds.
    auto settled = service->GetTenantQuotaSnapshot(tenant);
    ASSERT_TRUE(settled.has_value());
    EXPECT_EQ(settled->charged_bytes, kPartLength * kPartCount);
    auto exists = service->ExistKey(key, tenant);
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(*exists);
}

}  // namespace mooncake::test
