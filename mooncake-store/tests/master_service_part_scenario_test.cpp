// master_service_part_scenario_test.cpp
//
// The scenarios RFC #4366 puts on the part layer, driven through MasterService
// directly with the feature flag on: parts that arrive out of order, a part
// read that answers only for its own slice, a late PutEnd arriving after its
// writer lost the part, one part revoked while another keeps its replicas, and
// one part lost while another is leased.

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "master_service_test_fixture.h"
#include "types.h"

namespace mooncake::test {
namespace {

constexpr uint64_t kPartLength = 512;
constexpr const char* kSecondSegment = "part_scenario_second_segment";

MasterServiceConfig PartScenarioConfig() {
    MasterServiceConfig config;
    config.enable_multi_part_objects = true;
    return config;
}

ReplicateConfig PartConfig(uint32_t part_index, uint32_t part_count,
                           const std::string& preferred_segment = {}) {
    ReplicateConfig config;
    config.part_index = part_index;
    config.part_count = part_count;
    config.preferred_segment = preferred_segment;
    return config;
}

ObjectMeta PartEndMeta(const std::string& key, uint32_t part_index,
                       std::vector<ReplicaID> replica_ids,
                       std::optional<uint64_t> checksum = std::nullopt) {
    ObjectMeta meta{key, checksum};
    meta.part_index = part_index;
    meta.replica_ids = std::move(replica_ids);
    return meta;
}

std::vector<ReplicaID> ReplicaIdsOf(
    const std::vector<Replica::Descriptor>& descriptors) {
    std::vector<ReplicaID> ids;
    ids.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        ids.push_back(descriptor.id);
    }
    return ids;
}

// The error a call returned. A call that unexpectedly succeeded fails the case
// here instead of tripping the library's own assertion on error().
template <typename T>
ErrorCode ErrorOf(const tl::expected<T, ErrorCode>& result) {
    EXPECT_FALSE(result.has_value());
    return result.has_value() ? ErrorCode::OK : result.error();
}

}  // namespace

class MasterServicePartScenarioTest : public MasterServiceTest {
   protected:
    void SetUp() override {
        service_ = std::make_unique<MasterService>(PartScenarioConfig());
        first_ = PrepareSimpleSegment(*service_);
        // The second segment gets its own address range, so a part written on
        // it really is a replica of another segment.
        second_ =
            PrepareSimpleSegment(*service_, kSecondSegment,
                                 kDefaultSegmentBase + kDefaultSegmentSize);
    }

    void TearDown() override { service_.reset(); }

    std::unique_ptr<MasterService> service_;
    MountedSegmentContext first_;
    MountedSegmentContext second_;
};

TEST_F(MasterServicePartScenarioTest,
       PartsArrivingOutOfOrderAreNotReadableUntilTheLastEnd) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "out_of_order_key";
    const UUID third_writer = generate_uuid();

    // The writers arrive in the order 2, 0, 1.
    auto part_two = service_->PutStart(third_writer, key, tenant, kPartLength,
                                       PartConfig(2, 3, kSecondSegment));
    ASSERT_TRUE(part_two.has_value());
    auto part_zero = service_->PutStart(first_.client_id, key, tenant,
                                        kPartLength, PartConfig(0, 3));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_.client_id, key, tenant,
                                       kPartLength, PartConfig(1, 3));
    ASSERT_TRUE(part_one.has_value());

    ASSERT_TRUE(
        service_
            ->PutEnd(third_writer,
                     PartEndMeta(key, 2, ReplicaIdsOf(part_two.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(first_.client_id,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());

    // Two of three parts are written: no hit, and no part of it answers.
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);

    ASSERT_TRUE(
        service_
            ->PutEnd(second_.client_id,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));
}

TEST_F(MasterServicePartScenarioTest, APartReadAnswersOnlyForThatPart) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "part_read_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());

    ASSERT_TRUE(
        service_
            ->PutEnd(first_writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value()), 40),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(second_writer,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value()), 41),
                     tenant, ReplicaType::ALL)
            .has_value());

    auto first_read = service_->GetReplicaListForPart(key, tenant, 0);
    ASSERT_TRUE(first_read.has_value());
    EXPECT_EQ(first_read->part_count.value_or(0), 2u);
    EXPECT_EQ(first_read->object_checksum.value_or(0), 40u);
    EXPECT_EQ(ReplicaIdsOf(first_read->replicas),
              ReplicaIdsOf(part_zero.value()));

    auto second_read = service_->GetReplicaListForPart(key, tenant, 1);
    ASSERT_TRUE(second_read.has_value());
    EXPECT_EQ(second_read->part_count.value_or(0), 2u);
    EXPECT_EQ(second_read->object_checksum.value_or(0), 41u);
    EXPECT_EQ(ReplicaIdsOf(second_read->replicas),
              ReplicaIdsOf(part_one.value()));

    // A read that names no part cannot say which slice it means, and a part
    // outside the object does not exist.
    EXPECT_EQ(ErrorOf(service_->GetReplicaList(key, tenant)),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 2)),
              ErrorCode::INVALID_PARAMS);
}

TEST_F(MasterServicePartScenarioTest,
       ALateEndAfterThePartWasReallocatedIsRejected) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "late_end_key";
    const UUID writer = generate_uuid();

    auto first_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(first_attempt.has_value());
    const std::vector<ReplicaID> stale_ids =
        ReplicaIdsOf(first_attempt.value());

    // The writer goes quiet past its discard timeout: the part keeps its
    // PROCESSING replicas but the attempt is old enough to be replaced.
    const auto stale =
        std::chrono::system_clock::now() -
        std::chrono::seconds(DEFAULT_PUT_START_DISCARD_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(
            0, writer,
            std::unordered_set<ReplicaID>(stale_ids.begin(), stale_ids.end()),
            stale);
    }

    auto second_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(second_attempt.has_value());
    const std::vector<ReplicaID> current_ids =
        ReplicaIdsOf(second_attempt.value());
    EXPECT_NE(current_ids.front(), stale_ids.front());

    // The abandoned attempt's End names replicas it no longer owns, so it
    // completes nothing and the part's replicas stay as they are.
    EXPECT_EQ(ErrorOf(service_->PutEnd(writer, PartEndMeta(key, 0, stale_ids),
                                       tenant, ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(accessor.Get().GetPart(0)->IsWriting());
    }

    EXPECT_TRUE(service_
                    ->PutEnd(writer, PartEndMeta(key, 0, current_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                        &Replica::fn_is_completed));
    }
    // The object still waits for its other part, so it is not readable yet.
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
}

TEST_F(MasterServicePartScenarioTest, RevokingOnePartLeavesTheOthers) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "revoke_one_part_key";
    const UUID writer = generate_uuid();

    auto part_zero =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    auto part_one = service_->PutStart(writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());

    ASSERT_TRUE(service_
                    ->PutRevokeForPart(writer, key, tenant, 1,
                                       ReplicaIdsOf(part_one.value()),
                                       ReplicaType::ALL)
                    .has_value());

    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        // The revoked part is empty; part 0 is untouched.
        EXPECT_EQ(accessor.Get().GetPart(1), nullptr);
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                        &Replica::fn_is_completed));
    }

    // The part can be written again, and that makes the object readable.
    auto retry = service_->PutStart(writer, key, tenant, kPartLength,
                                    PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(retry.has_value());
    ASSERT_TRUE(service_
                    ->PutEnd(writer,
                             PartEndMeta(key, 1, ReplicaIdsOf(retry.value())),
                             tenant, ReplicaType::ALL)
                    .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));
}

TEST_F(MasterServicePartScenarioTest,
       LosingOnePartMakesTheObjectUnreadableWhileItsLeaseHolds) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "lost_part_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(first_writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(second_writer,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));

    // A reader takes the object, which grants the object's lease.
    ASSERT_TRUE(service_->GetReplicaListForPart(key, tenant, 0).has_value());

    // The segment holding part 1 goes away, and its replicas with it. The case
    // drops them the way the master's segment-offline path does, so it does not
    // depend on where the fixture put the segment's buffers.
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().MutablePart(1), nullptr);
        accessor.Get().MutablePart(1)->PopReplicas();
    }

    // No partial hit: the object is unreadable at once, and every part read
    // says so.
    EXPECT_FALSE(service_->ExistKey(key, tenant).value_or(true));
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 0)),
              ErrorCode::REPLICA_IS_NOT_READY);

    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        const ObjectMetadata& metadata = accessor.Get();
        // The lost part holds nothing, the other part's replica is still there,
        // and the lease that was granted still covers it.
        ASSERT_NE(metadata.GetPart(1), nullptr);
        EXPECT_TRUE(metadata.GetPart(1)->IsEmpty());
        ASSERT_NE(metadata.GetPart(0), nullptr);
        EXPECT_GT(metadata.EvictionDeadline(),
                  std::chrono::system_clock::now());
    }
}

TEST_F(MasterServicePartScenarioTest,
       AReadOfAPartNothingWasWrittenToIsNotReady) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "unwritten_part_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    // Only part 0 is started, so part 1 holds no content at all.
    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);

    // And a part whose writer revoked its attempt holds nothing either.
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(service_
                    ->PutRevokeForPart(second_writer, key, tenant, 1,
                                       ReplicaIdsOf(part_one.value()),
                                       ReplicaType::ALL)
                    .has_value());
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);

    // So does a part whose attempt the release timeout reclaimed.
    ASSERT_TRUE(service_
                    ->PutStart(second_writer, key, tenant, kPartLength,
                               PartConfig(1, 2, kSecondSegment))
                    .has_value());
    const auto stale =
        std::chrono::system_clock::now() -
        std::chrono::seconds(DEFAULT_PUT_START_RELEASE_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(1, second_writer, {}, stale);
    }
    {
        const auto now = std::chrono::system_clock::now();
        MasterServiceTestPeer peer(*service_);
        for (size_t shard_index = 0;
             shard_index <
             MasterServiceTestPeer::MetadataShards(*service_).size();
             ++shard_index) {
            MasterServiceTestPeer::MetadataShardAccessorRW shard(service_.get(),
                                                                 shard_index);
            peer.DiscardExpiredProcessingReplicas(shard, now);
        }
    }
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 1)),
              ErrorCode::REPLICA_IS_NOT_READY);
    EXPECT_EQ(ErrorOf(service_->GetReplicaList(key, tenant)),
              ErrorCode::INVALID_PARAMS);
}

TEST_F(MasterServicePartScenarioTest,
       ALegacyWriteCannotTakeOverAnObjectWrittenInSeveralParts) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "layout_conflict_key";
    const UUID writer = generate_uuid();

    // The object is created by a writer of part 1, so part 0 does not exist
    // yet.
    auto part_one = service_->PutStart(writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());

    // An ordinary write names no part and cannot join it.
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, key, tenant, kPartLength,
                                         ReplicateConfig{})),
              ErrorCode::INVALID_PARAMS);
    // A scope of one part against an object of two is a different object too.
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, key, tenant, kPartLength,
                                         PartConfig(0, 1))),
              ErrorCode::INVALID_PARAMS);

    // The reverse direction is refused as well: an ordinary object keeps one
    // part.
    const std::string legacy_key = "layout_conflict_legacy_key";
    ASSERT_TRUE(service_
                    ->PutStart(writer, legacy_key, tenant, kPartLength,
                               ReplicateConfig{})
                    .has_value());
    EXPECT_EQ(ErrorOf(service_->PutStart(writer, legacy_key, tenant,
                                         kPartLength, PartConfig(0, 2))),
              ErrorCode::INVALID_PARAMS);
}

TEST_F(MasterServicePartScenarioTest,
       ARevokeFromAnAttemptThatLostThePartChangesNothing) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "stale_revoke_key";
    const UUID writer = generate_uuid();

    auto first_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(first_attempt.has_value());
    const std::vector<ReplicaID> stale_ids =
        ReplicaIdsOf(first_attempt.value());

    // The attempt goes quiet past its discard timeout, and the same writer
    // takes the part again with new replicas.
    const auto stale =
        std::chrono::system_clock::now() -
        std::chrono::seconds(DEFAULT_PUT_START_DISCARD_TIMEOUT + 60);
    {
        MasterServiceTestPeer::MetadataAccessorRW accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        accessor.Get().BeginPartWrite(
            0, writer,
            std::unordered_set<ReplicaID>(stale_ids.begin(), stale_ids.end()),
            stale);
    }
    auto second_attempt =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(second_attempt.has_value());
    const std::vector<ReplicaID> current_ids =
        ReplicaIdsOf(second_attempt.value());
    EXPECT_NE(current_ids.front(), stale_ids.front());

    // The old attempt's revoke names replicas the part no longer holds, so it
    // changes nothing: the current attempt keeps its replicas and its writer.
    EXPECT_EQ(ErrorOf(service_->PutRevokeForPart(writer, key, tenant, 0,
                                                 stale_ids, ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        const ObjectMetadata& metadata = accessor.Get();
        ASSERT_NE(metadata.GetPart(0), nullptr);
        std::vector<ReplicaID> held_ids;
        for (const auto& replica : metadata.GetPart(0)->replicas()) {
            held_ids.push_back(replica.id());
        }
        EXPECT_EQ(held_ids, current_ids);
        EXPECT_TRUE(metadata.GetPart(0)->IsWriting());
    }
    EXPECT_TRUE(service_
                    ->PutEnd(writer, PartEndMeta(key, 0, current_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
}

TEST_F(MasterServicePartScenarioTest, ARevokeAfterThePartCompletedIsRefused) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "revoke_completed_key";
    const UUID writer = generate_uuid();

    auto part_zero =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    const std::vector<ReplicaID> completed_ids =
        ReplicaIdsOf(part_zero.value());
    ASSERT_TRUE(service_
                    ->PutEnd(writer, PartEndMeta(key, 0, completed_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());

    // There is no attempt left to abandon, so the part keeps its replicas.
    EXPECT_EQ(ErrorOf(service_->PutRevokeForPart(
                  writer, key, tenant, 0, completed_ids, ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                        &Replica::fn_is_completed));
    }

    // A part-scoped revoke has to say which attempt it abandons.
    EXPECT_EQ(ErrorOf(service_->PutRevokeForPart(writer, key, tenant, 0, {},
                                                 ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
}

TEST_F(MasterServicePartScenarioTest, APartEndNamesItsOwnAttempt) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "end_scope_key";
    const UUID writer = generate_uuid();

    auto part =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(part.has_value());
    const std::vector<ReplicaID> ids = ReplicaIdsOf(part.value());

    // An End without a replica scope would complete whatever is PROCESSING in
    // the part, which after a timeout reallocation belongs to another writer.
    EXPECT_EQ(ErrorOf(service_->PutEnd(writer, ObjectMeta{key, std::nullopt},
                                       tenant, ReplicaType::ALL)),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ErrorOf(service_->PutEnd(writer, PartEndMeta(key, 0, {}), tenant,
                                       ReplicaType::ALL)),
              ErrorCode::INVALID_PARAMS);

    // An End that names another attempt's replica changes nothing, and a set
    // that names only part of the attempt would leave the rest of it behind.
    EXPECT_EQ(ErrorOf(service_->PutEnd(
                  writer, PartEndMeta(key, 0, {ids.front() + 1000}), tenant,
                  ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(accessor.Get().GetPart(0)->IsWriting());
    }

    // The attempt's own set completes the part, and its retry is a no-op.
    ASSERT_TRUE(
        service_
            ->PutEnd(writer, PartEndMeta(key, 0, ids), tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(
        service_
            ->PutEnd(writer, PartEndMeta(key, 0, ids), tenant, ReplicaType::ALL)
            .has_value());
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        ASSERT_NE(accessor.Get().GetPart(0), nullptr);
        EXPECT_TRUE(std::ranges::all_of(accessor.Get().GetPart(0)->replicas(),
                                        &Replica::fn_is_completed));
    }
}

TEST_F(MasterServicePartScenarioTest,
       ALegacyUpsertCannotRewriteAnObjectWrittenInSeveralParts) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "upsert_multipart_key";
    const UUID writer = generate_uuid();

    auto part_zero =
        service_->PutStart(writer, key, tenant, kPartLength, PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());

    // The request names no part, and the object it hits has several: rewriting
    // its buffers in place would take the layout away from its writers.
    EXPECT_EQ(ErrorOf(service_->UpsertStart(writer, key, tenant, kPartLength,
                                            ReplicateConfig{})),
              ErrorCode::INVALID_PARAMS);
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            service_.get(), MasterServiceTestPeer::ObjectIdentity{tenant, key});
        ASSERT_TRUE(accessor.Exists());
        EXPECT_EQ(accessor.Get().PartCount(), 2u);
    }
}

TEST_F(MasterServicePartScenarioTest,
       CopyAndMoveLeaveAnObjectWrittenInSeveralPartsAlone) {
    const TenantId tenant = TenantId::Default();
    const std::string key = "copy_multipart_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    auto part_zero = service_->PutStart(first_writer, key, tenant, kPartLength,
                                        PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service_->PutStart(second_writer, key, tenant, kPartLength,
                                       PartConfig(1, 2, kSecondSegment));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(first_writer,
                     PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(
        service_
            ->PutEnd(second_writer,
                     PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                     tenant, ReplicaType::ALL)
            .has_value());
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));

    // One part of the object lives on each segment, so a copy or a move that
    // took a part for the whole object would drop the other one. The caller is
    // the client that mounted the source segment, so the request gets past the
    // client checks and meets the object.
    EXPECT_EQ(ErrorOf(service_->CopyStart(second_.client_id, key, tenant,
                                          kSecondSegment, {kSecondSegment})),
              ErrorCode::INVALID_PARAMS);
    EXPECT_EQ(ErrorOf(service_->MoveStart(second_.client_id, key, tenant,
                                          kSecondSegment, "test_segment")),
              ErrorCode::INVALID_PARAMS);
    EXPECT_TRUE(service_->ExistKey(key, tenant).value_or(false));
}

TEST_F(MasterServicePartScenarioTest,
       AnEvictionPassReleasesEveryPartOfAnObject) {
    MasterServiceConfig config = PartScenarioConfig();
    config.enable_offload = true;
    config.offload_on_evict = true;
    config.default_kv_lease_ttl = 50;
    MasterService service{config};
    const auto context = PrepareSimpleSegment(service);
    const TenantId tenant = TenantId::Default();
    const std::string key = "evicted_multipart_key";
    const UUID first_writer = generate_uuid();
    const UUID second_writer = generate_uuid();

    auto part_zero = service.PutStart(first_writer, key, tenant, kPartLength,
                                      PartConfig(0, 2));
    ASSERT_TRUE(part_zero.has_value());
    auto part_one = service.PutStart(second_writer, key, tenant, kPartLength,
                                     PartConfig(1, 2));
    ASSERT_TRUE(part_one.has_value());
    ASSERT_TRUE(
        service
            .PutEnd(first_writer,
                    PartEndMeta(key, 0, ReplicaIdsOf(part_zero.value())),
                    tenant, ReplicaType::ALL)
            .has_value());
    ASSERT_TRUE(service
                    .PutEnd(second_writer,
                            PartEndMeta(key, 1, ReplicaIdsOf(part_one.value())),
                            tenant, ReplicaType::ALL)
                    .has_value());
    EXPECT_TRUE(service.ExistKey(key, tenant).value_or(false));

    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    // An offload would mirror one part and leave the others to be released, so
    // the object is released whole instead.
    MasterServiceTestPeer peer(service);
    peer.RunBatchEvictForTesting(1.0, 1.0);
    EXPECT_EQ(ErrorOf(service.GetReplicaListForPart(key, tenant, 0)),
              ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(MasterServicePartScenarioTest,
       AGroupEvictionTakesTheCompleteMemberAndLeavesTheIncompleteOne) {
    constexpr size_t kSegmentSize = 4 * 1024 * 1024;
    constexpr uint64_t kObjectLength = 2 * 1024 * 1024;
    MasterServiceConfig config = PartScenarioConfig();
    // Both members have to be eviction candidates, so their leases have to be
    // over by the time the pass runs.
    config.default_kv_lease_ttl = 50;
    MasterService service{config};
    [[maybe_unused]] const auto context =
        PrepareSimpleSegment(service, "part_group_eviction_segment",
                             kDefaultSegmentBase, kSegmentSize);
    const TenantId tenant = TenantId::Default();
    const UUID writer = generate_uuid();

    const std::string complete_key = "group_evict_complete_key";
    const std::string incomplete_key = "group_evict_incomplete_key";
    const std::string group_id = FindGroupIdOnDifferentShard(complete_key);

    ReplicateConfig complete_config;
    complete_config.replica_num = 1;
    complete_config.group_ids = std::vector<std::string>{group_id};
    PutCompletedObject(service, writer, complete_key, complete_config,
                       kObjectLength);

    // The same group holds an object whose second part never arrives.
    ReplicateConfig part_config = complete_config;
    part_config.part_index = 0;
    part_config.part_count = 2;
    auto partial = service.PutStart(writer, incomplete_key, tenant,
                                    kObjectLength / 2, part_config);
    ASSERT_TRUE(partial.has_value());
    ASSERT_TRUE(service
                    .PutEnd(writer,
                            PartEndMeta(incomplete_key, 0,
                                        ReplicaIdsOf(partial.value())),
                            tenant, ReplicaType::ALL)
                    .has_value());

    // Fill the segment so the pool eviction pass has to free memory, and let
    // both members' leases run out first.
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    ReplicateConfig trigger_config;
    trigger_config.replica_num = 1;
    EXPECT_FALSE(service
                     .PutStart(writer, "group_evict_trigger_key", tenant,
                               kObjectLength, trigger_config)
                     .has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    EXPECT_FALSE(service.ExistKey(complete_key, tenant).value_or(true));
    {
        MasterServiceTestPeer::MetadataAccessorRO accessor(
            &service,
            MasterServiceTestPeer::ObjectIdentity{tenant, incomplete_key});
        EXPECT_TRUE(accessor.Exists());
    }
}

}  // namespace mooncake::test
