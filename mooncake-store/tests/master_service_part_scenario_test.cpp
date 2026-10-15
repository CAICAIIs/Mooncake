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
#include <thread>
#include <utility>
#include <vector>

#include "master_service_test_fixture.h"
#include "types.h"

namespace mooncake::test {
namespace {

constexpr uint64_t kPartLength = 512;
constexpr const char* kSecondSegment = "part_scenario_second_segment";
// Short enough that a lease taken by a read runs out inside a case.
constexpr uint64_t kLeaseTtlMs = 100;

MasterServiceConfig PartScenarioConfig() {
    MasterServiceConfig config;
    config.enable_multi_part_objects = true;
    config.default_kv_lease_ttl = kLeaseTtlMs;
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
    // completes nothing and the part stays unreadable.
    EXPECT_EQ(ErrorOf(service_->PutEnd(writer, PartEndMeta(key, 0, stale_ids),
                                       tenant, ReplicaType::ALL)),
              ErrorCode::INVALID_WRITE);
    EXPECT_EQ(ErrorOf(service_->GetReplicaListForPart(key, tenant, 0)),
              ErrorCode::REPLICA_IS_NOT_READY);

    EXPECT_TRUE(service_
                    ->PutEnd(writer, PartEndMeta(key, 0, current_ids), tenant,
                             ReplicaType::ALL)
                    .has_value());
    EXPECT_TRUE(service_->GetReplicaListForPart(key, tenant, 0).has_value());
    // The object still waits for its other part.
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
        // The lost part is empty, the other part's replica is still there, and
        // the lease that was granted still covers it.
        EXPECT_EQ(metadata.GetPart(1), nullptr);
        ASSERT_NE(metadata.GetPart(0), nullptr);
        EXPECT_GT(metadata.EvictionDeadline(),
                  std::chrono::system_clock::now());
    }
}

}  // namespace mooncake::test
