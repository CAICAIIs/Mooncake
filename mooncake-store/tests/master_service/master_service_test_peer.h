#pragma once

#include <utility>

#include "master_service.h"

namespace mooncake::test {
namespace detail {

// A result the entry helpers below refuse, because their own empty optional is
// what reports "the callback did not run": a callback that also carries an
// optional would be wrapped a second time and every caller would have to unwrap
// twice.
template <typename T>
struct IsOptionalResult : std::false_type {};
template <typename T>
struct IsOptionalResult<std::optional<T>> : std::true_type {};

}  // namespace detail

// The single test access boundary for MasterService. Keep test-only inspection,
// mutation and synchronous drivers here, not in the production service API.
// Raw state accessors do not acquire locks. Callers must preserve the service's
// lock order; synchronous drivers must not race their background workers.
class MasterServiceTestPeer {
   public:
    explicit MasterServiceTestPeer(MasterService& service)
        : service_(service) {}

    using MetadataSerializer = MasterService::MetadataSerializer;
    using ObjectIdentity = MasterService::ObjectIdentity;
    using ObjectMetadata = mooncake::ObjectMetadata;
    using PromotionQueueResult = mooncake::PromotionQueueResult;
    using PromotionTask = mooncake::PromotionTask;
    using QuotaEraseMode = MasterService::QuotaEraseMode;
    using TenantRegistry = metadata::TenantRegistry;
    using TenantQuotaEvictionResult = MasterService::TenantQuotaEvictionResult;

    static constexpr auto kDynamicReplicationWindowEntryLimit =
        MasterService::kDynamicReplicationWindowEntryLimit;
    static constexpr auto kMaxPromotionExecutionFailures =
        MasterService::kMaxPromotionExecutionFailures;
    static constexpr auto kObjectOperationLockStripes =
        MasterService::kObjectOperationLockStripes;
    static constexpr auto kPromotionCandidateMaxRetries =
        MasterService::kPromotionCandidateMaxRetries;

    ErrorCode SetBatchOpLogBackendForTesting(
        std::shared_ptr<HaKvBackend> backend);

    void SetBatchOpLogWriterFactoryForTesting(
        MasterService::BatchOpLogWriterFactory factory);

    // Drive one eviction cycle synchronously, without the periodic worker.
    void RunBatchEvictForTesting(double evict_ratio_target,
                                 double evict_ratio_lowerbound);

    void RunNoFBatchEvictForTesting(double evict_ratio_target,
                                    double evict_ratio_lowerbound);

    void RunDfsEvictionForTesting();

    // Drive one tenant watermark eviction pass without the periodic worker.
    void RunTenantEvictForTesting();

    // Enable epoch bookkeeping without starting a live KV event publisher.
    void SetKvTenantEpochTrackingForTesting(bool enabled);

    // Publish-side entry point of PutStart's snapshot section: fires after
    // client_mutex_ is released and while snapshot_mutex_ is held shared. Lets
    // a lock-order test park PutStart there deterministically.
    void SetSnapshotArriveHookForTesting(std::function<void()> hook);

    // The owning ObjectIndex's route lock, held EXCLUSIVE. PutStart takes it at
    // its first Pin inside the snapshot barrier, so holding it parks a PutStart
    // that has already entered that barrier.
    std::unique_lock<std::shared_mutex> LockRouteForTesting(
        const TenantId& tenant_id);

    // Counts of published clears and clears suppressed by a concurrent commit.
    uint64_t GetKvClearedPublishedForTesting() const;

    uint64_t GetKvClearedSuppressedForTesting() const;

    void SetNoFProbeFnForTesting(MasterService::NoFProbeFn fn);

    size_t GetMountedNoFSegmentCountForTesting();

    bool IsNoFSegmentMountedForTesting(const UUID& segment_id);

    std::optional<uint32_t> GetNoFHeartbeatFailureCountForTesting(
        const UUID& segment_id);

    size_t RunPromotionCandidateRetryForTesting();

    size_t CountCandidatesForTesting(const TenantId& tenant_id);

    // Seeds an in-flight PromotionTask on (tenant, key), publishing the entry
    // when the key is not routed yet, so a test can drive
    // NotifyPromotionSuccess without the on-hit admission gate.
    void SeedPromotionTaskForTesting(const TenantId& tenant_id,
                                     const std::string& key,
                                     const UUID& holder_id, ReplicaID alloc_id,
                                     uint64_t object_size);

    void ResetCandidateBackoffsForTesting();

    size_t SoftPinHeapSize() const;

    size_t SoftPinRegistrationCount() const;

    static auto& AllocationStrategy(MasterService& service) {
        return service.allocation_strategy_;
    }
    static const auto& AllocationStrategy(const MasterService& service) {
        return service.allocation_strategy_;
    }

    static auto& BatchOplogStorage(MasterService& service) {
        return service.batch_oplog_storage_;
    }
    static const auto& BatchOplogStorage(const MasterService& service) {
        return service.batch_oplog_storage_;
    }

    static auto& ClientLivenessRecords(MasterService& service) {
        return service.client_liveness_records_;
    }
    static const auto& ClientLivenessRecords(const MasterService& service) {
        return service.client_liveness_records_;
    }

    static auto& ClientMutex(MasterService& service) {
        return service.client_mutex_;
    }
    static const auto& ClientMutex(const MasterService& service) {
        return service.client_mutex_;
    }

    static auto& DynamicReplicationWindows(MasterService& service) {
        return service.dynamic_replication_windows_;
    }
    static const auto& DynamicReplicationWindows(const MasterService& service) {
        return service.dynamic_replication_windows_;
    }

    static auto& EnableDfs(MasterService& service) {
        return service.enable_dfs_;
    }
    static const auto& EnableDfs(const MasterService& service) {
        return service.enable_dfs_;
    }

    static auto& EnableOplog(MasterService& service) {
        return service.enable_oplog_;
    }
    static const auto& EnableOplog(const MasterService& service) {
        return service.enable_oplog_;
    }

    static auto& EvictionRunning(MasterService& service) {
        return service.eviction_running_;
    }
    static const auto& EvictionRunning(const MasterService& service) {
        return service.eviction_running_;
    }

    static auto& EvictionThread(MasterService& service) {
        return service.eviction_thread_;
    }
    static const auto& EvictionThread(const MasterService& service) {
        return service.eviction_thread_;
    }

    static auto& LocalSsdManager(MasterService& service) {
        return service.local_ssd_manager_;
    }
    static const auto& LocalSsdManager(const MasterService& service) {
        return service.local_ssd_manager_;
    }

    static auto& Tenants(MasterService& service) { return service.tenants_; }
    static const auto& Tenants(const MasterService& service) {
        return service.tenants_;
    }

    // A replica-action lease is recorded per tenant and keyed by proposal id,
    // so a caller names both.
    static std::optional<ReplicaActionLease> FindDynamicReplicationLease(
        MasterService& service, const TenantId& tenant_id,
        const UUID& proposal_id) {
        return service.FindDynamicReplicationLease(tenant_id, proposal_id);
    }

    static std::vector<std::string> PromotionCandidateKeys(
        MasterService& service, const TenantId& tenant_id) {
        return service.PromotionCandidateKeys(tenant_id);
    }

    static auto& NeedMemEviction(MasterService& service) {
        return service.need_mem_eviction_;
    }
    static const auto& NeedMemEviction(const MasterService& service) {
        return service.need_mem_eviction_;
    }

    static auto& NofSegmentManager(MasterService& service) {
        return service.nof_segment_manager_;
    }
    static const auto& NofSegmentManager(const MasterService& service) {
        return service.nof_segment_manager_;
    }

    static auto& ObjectOperationLocks(MasterService& service) {
        return service.object_operation_locks_;
    }
    static const auto& ObjectOperationLocks(const MasterService& service) {
        return service.object_operation_locks_;
    }

    static auto& OrderedOplogWriter(MasterService& service) {
        return service.ordered_oplog_writer_;
    }
    static const auto& OrderedOplogWriter(const MasterService& service) {
        return service.ordered_oplog_writer_;
    }

    static auto& PromotionAdmissionThreshold(MasterService& service) {
        return service.promotion_admission_threshold_;
    }
    static const auto& PromotionAdmissionThreshold(
        const MasterService& service) {
        return service.promotion_admission_threshold_;
    }

    static auto& PromotionCandidateCount(MasterService& service) {
        return service.promotion_candidate_count_;
    }
    static const auto& PromotionCandidateCount(const MasterService& service) {
        return service.promotion_candidate_count_;
    }

    static auto& PromotionInFlight(MasterService& service) {
        return service.promotion_in_flight_;
    }
    static const auto& PromotionInFlight(const MasterService& service) {
        return service.promotion_in_flight_;
    }

    static auto& ReplicaCleanupWorker(MasterService& service) {
        return service.replica_cleanup_worker_;
    }
    static const auto& ReplicaCleanupWorker(const MasterService& service) {
        return service.replica_cleanup_worker_;
    }

    static auto& RootFsDir(MasterService& service) {
        return service.root_fs_dir_;
    }
    static const auto& RootFsDir(const MasterService& service) {
        return service.root_fs_dir_;
    }

    static auto& SegmentManager(MasterService& service) {
        return service.segment_manager_;
    }
    static const auto& SegmentManager(const MasterService& service) {
        return service.segment_manager_;
    }

    static auto& SnapshotCatalogStore(MasterService& service) {
        return service.snapshot_catalog_store_;
    }
    static const auto& SnapshotCatalogStore(const MasterService& service) {
        return service.snapshot_catalog_store_;
    }

    static auto& SnapshotManager(MasterService& service) {
        return service.snapshot_manager_;
    }
    static const auto& SnapshotManager(const MasterService& service) {
        return service.snapshot_manager_;
    }

    static auto& SnapshotMutex(MasterService& service) {
        return service.snapshot_mutex_;
    }
    static const auto& SnapshotMutex(const MasterService& service) {
        return service.snapshot_mutex_;
    }

    static auto& SnapshotObjectStore(MasterService& service) {
        return service.snapshot_object_store_;
    }
    static const auto& SnapshotObjectStore(const MasterService& service) {
        return service.snapshot_object_store_;
    }

    static auto& SoftPinDeadlineIndex(MasterService& service) {
        return service.soft_pin_deadline_index_;
    }
    static const auto& SoftPinDeadlineIndex(const MasterService& service) {
        return service.soft_pin_deadline_index_;
    }

    static auto& TaskManager(MasterService& service) {
        return service.task_manager_;
    }
    static const auto& TaskManager(const MasterService& service) {
        return service.task_manager_;
    }

    static auto& TenantQuotaPolicyMutex(MasterService& service) {
        return service.tenant_quota_policy_mutex_;
    }
    static const auto& TenantQuotaPolicyMutex(const MasterService& service) {
        return service.tenant_quota_policy_mutex_;
    }

    static auto& TenantQuotaPolicyStore(MasterService& service) {
        return service.tenant_quota_policy_store_;
    }
    static const auto& TenantQuotaPolicyStore(const MasterService& service) {
        return service.tenant_quota_policy_store_;
    }

    static auto& TenantQuotaRecomputeMutex(MasterService& service) {
        return service.tenant_quota_recompute_mutex_;
    }
    static const auto& TenantQuotaRecomputeMutex(const MasterService& service) {
        return service.tenant_quota_recompute_mutex_;
    }

    static auto& TenantQuotaTable(MasterService& service) {
        return service.tenant_quota_table_;
    }
    static const auto& TenantQuotaTable(const MasterService& service) {
        return service.tenant_quota_table_;
    }

    auto AddReplicaForRetainedClient(const UUID& client_id,
                                     const std::string& key,
                                     const TenantId& tenant_id,
                                     Replica& replica)
        -> tl::expected<bool, ErrorCode> {
        return service_.AddReplicaForRetainedClient(client_id, key, tenant_id,
                                                    replica);
    }

    tl::expected<uint64_t, ErrorCode> AppendOpLogVisibleBeforeDurable(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload) {
        return service_.AppendOpLogVisibleBeforeDurable(type, tenant_id, key,
                                                        payload);
    }

    tl::expected<OpLogEntry, ErrorCode> AppendOpLogWithDurableFinalize(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload,
        MasterService::DurableFinalizeCallback callback) {
        return service_.AppendOpLogWithDurableFinalize(
            type, tenant_id, key, payload, std::move(callback));
    }

    tl::expected<void, ErrorCode> ChargeTenantQuota(TenantQuotaHandle account,
                                                    uint64_t bytes) {
        return service_.ChargeTenantQuota(std::move(account), bytes);
    }

    void CleanupExpiredSoftPins(
        const std::chrono::system_clock::time_point& now) {
        service_.CleanupExpiredSoftPins(now);
    }

    void ClearCandidatesForReload() { service_.ClearCandidatesForReload(); }

    // Drops the pending dynamic-replication task state for `key` under the
    // entry's own lock, the way the sweep paths do.
    void ClearDynamicReplicationStateForKey(const TenantId& tenant_id,
                                            metadata::Tenant& tenant,
                                            const std::string& key) {
        auto entry = tenant.Get(key);
        if (entry == nullptr) {
            return;
        }
        entry->WithExclusiveAccess(
            [&](ObjectMetadata&, ObjectEntry::State& state) {
                service_.ClearDynamicReplicationStateLocked(tenant_id, entry,
                                                            state);
            });
    }

    void ClearLocalDiskHandlesOwnedBy(const UUID& owner) {
        service_.ClearLocalDiskHandlesOwnedBy(owner);
    }

    void ClearInvalidHandles() { service_.ClearInvalidHandles(); }

    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients) {
        service_.ClearInvalidHandles(retaining_clients);
    }

    std::unique_ptr<ha::SnapshotCatalogStore> CreateSnapshotCatalogStore(
        const MasterServiceConfig& config);

    void DiscardExpiredProcessingReplicas(
        metadata::Tenant& tenant,
        const std::chrono::system_clock::time_point& now) {
        service_.DiscardExpiredProcessingReplicas(tenant, now);
    }

    uint32_t DynamicReplicationAdmissionMinHits() const {
        return service_.DynamicReplicationAdmissionMinHits();
    }

    static int64_t DynamicReplicationNowMs() {
        return MasterService::DynamicReplicationNowMs();
    }

    uint64_t DynamicReplicationVersionEpoch(
        const ObjectMetadata& metadata) const {
        return service_.DynamicReplicationVersionEpoch(metadata);
    }

    size_t EraseReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn,
        std::vector<ReplicaID>* erased_replica_ids = nullptr) {
        return service_.EraseReplicasWithCacheTotalAccounting(
            metadata, pred_fn, erased_replica_ids);
    }

    TenantQuotaEvictionResult EvictTenantMemoryForQuota(
        const TenantId& tenant_id, uint64_t target_bytes) {
        return service_.EvictTenantMemoryForQuota(tenant_id, target_bytes);
    }

    void FinalizeExpiredProcessingReplicasAfterDurable(
        std::shared_ptr<ObjectEntry> entry, const OpLogEntry& durable_entry,
        const std::chrono::system_clock::time_point& ttl) {
        service_.FinalizeExpiredProcessingReplicasAfterDurable(
            std::move(entry), durable_entry, ttl);
    }

    void FinalizeRemovedReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids, QuotaEraseMode quota_mode,
        const std::vector<std::string>& previous_media_hint = {}) {
        service_.FinalizeRemovedReplicasAfterDurable(
            durable_entry, replica_ids, quota_mode, previous_media_hint);
    }

    std::shared_ptr<ClientLivenessRecord> FindClientRecord(
        const UUID& client_id) const {
        return service_.FindClientRecord(client_id);
    }

    TenantQuotaHandle GetBoundTenantQuotaHandle(
        const metadata::Tenant& tenant) const {
        return service_.GetBoundTenantQuotaHandle(tenant);
    }

    std::shared_ptr<metadata::Tenant> GetOrCreateTenantHandle(
        const TenantId& tenant_id) {
        return service_.GetOrCreateTenantHandle(tenant_id);
    }

    // The entry currently routed for (tenant, key), or nullptr when the tenant
    // or the key is absent. Lets a test hold one entry across a concurrent
    // operation.
    std::shared_ptr<ObjectEntry> GetEntryForTesting(const TenantId& tenant_id,
                                                    const std::string& key) {
        auto tenant_handle =
            service_.tenants_.Lookup(ResolveRequestTenantId(tenant_id));
        return tenant_handle == nullptr ? nullptr : tenant_handle->Get(key);
    }

    // Tears one object down the way a remove path does, with the same
    // accounting: the entry's own lock is held while the teardown runs and the
    // route slot is dropped. Lets a test release an object's allocation
    // without going through the public remove entry points.
    bool EraseObjectForTesting(const TenantId& tenant_id,
                               const std::string& key) {
        const TenantId normalized = ResolveRequestTenantId(tenant_id);
        auto tenant_handle = service_.tenants_.Lookup(normalized);
        if (tenant_handle == nullptr) {
            return false;
        }
        auto entry = tenant_handle->Get(key);
        if (entry == nullptr) {
            return false;
        }
        return entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata, ObjectEntry::State& state) {
                return service_.EraseMetadata(*tenant_handle, entry, metadata,
                                              state, normalized);
            });
    }

    // Runs `fn` while the entry's own lock is held, the way a mutating path
    // holds it. A test parks a background sweep by blocking inside `fn`: the
    // sweep blocks on the same entry at its next step, which is where an
    // operation that first pins the entry and only then decides about it can be
    // interrupted deterministically.
    template <typename Fn>
    void WithEntryLockedForTesting(const TenantId& tenant_id,
                                   const std::string& key, Fn&& fn) {
        auto entry = GetEntryForTesting(tenant_id, key);
        assert(entry != nullptr);
        entry->WithExclusiveAccess(
            [&](ObjectMetadata&, ObjectEntry::State&) { fn(); });
    }

    // Runs `fn(tenant, entry, metadata, state)` on the object under its entry's
    // own shared lock, the way a read path sees it, with the callback contract
    // of MasterService::WithObjectMetadataForRead: the tenant first, then the
    // entry, then the object's two guarded halves, and the callback returns its
    // own result rather than an optional, because this helper's own empty
    // optional already carries "the callback did not run".
    //
    // Nothing when the tenant or the key is absent, and nothing for an entry
    // whose accounting is already released: a torn-down entry keeps its
    // envelope wired until the route slot goes, and an object left with no
    // valid replica is no longer readable, which is how the retired accessors'
    // Exists() read it.
    template <typename Fn>
    [[nodiscard]] auto WithPublishedObjectForRead(const TenantId& tenant_id,
                                                  const std::string& key,
                                                  Fn&& fn) const
        -> std::optional<std::invoke_result_t<
            Fn, const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata&, const ObjectEntry::State&>> {
        using Result = std::invoke_result_t<
            Fn, const metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            const ObjectMetadata&, const ObjectEntry::State&>;
        static_assert(!std::is_void_v<Result>,
                      "the helper reports absence through its own optional, so "
                      "the callback must return a result");
        static_assert(!std::is_reference_v<Result>,
                      "the result is carried by value: nothing a callback "
                      "returns may outlive the lock it ran under");
        static_assert(
            !detail::IsOptionalResult<std::remove_cv_t<Result>>::value,
            "the callback must return its result itself; an optional "
            "here would be wrapped a second time");
        auto tenant_handle =
            service_.tenants_.Lookup(ResolveRequestTenantId(tenant_id));
        if (tenant_handle == nullptr) {
            return std::nullopt;
        }
        auto entry = tenant_handle->Get(key);
        if (entry == nullptr) {
            return std::nullopt;
        }
        return entry->WithSharedAccess(
            [&](const ObjectMetadata& metadata,
                const ObjectEntry::State& state) -> std::optional<Result> {
                if (state.is_torn_down || !metadata.IsValid()) {
                    return std::nullopt;
                }
                return std::optional<Result>{std::forward<Fn>(fn)(
                    *tenant_handle, entry, metadata, state)};
            });
    }

    // The same under the entry's own write lock, for a test that has to stage
    // state a production path would have produced. `fn(tenant, entry, metadata,
    // state)` may mutate the object, the callback contract is the write gate's,
    // and the same readability predicate gates it. Not const: it hands the
    // callback a mutable object.
    template <typename Fn>
    [[nodiscard]] auto WithPublishedObjectForWrite(const TenantId& tenant_id,
                                                   const std::string& key,
                                                   Fn&& fn)
        -> std::optional<std::invoke_result_t<
            Fn, metadata::Tenant&, const std::shared_ptr<ObjectEntry>&,
            ObjectMetadata&, ObjectEntry::State&>> {
        using Result =
            std::invoke_result_t<Fn, metadata::Tenant&,
                                 const std::shared_ptr<ObjectEntry>&,
                                 ObjectMetadata&, ObjectEntry::State&>;
        static_assert(!std::is_void_v<Result>,
                      "the helper reports absence through its own optional, so "
                      "the callback must return a result");
        static_assert(!std::is_reference_v<Result>,
                      "the result is carried by value: nothing a callback "
                      "returns may outlive the lock it ran under");
        static_assert(
            !detail::IsOptionalResult<std::remove_cv_t<Result>>::value,
            "the callback must return its result itself; an optional "
            "here would be wrapped a second time");
        auto tenant_handle =
            service_.tenants_.Lookup(ResolveRequestTenantId(tenant_id));
        if (tenant_handle == nullptr) {
            return std::nullopt;
        }
        auto entry = tenant_handle->Get(key);
        if (entry == nullptr) {
            return std::nullopt;
        }
        return entry->WithExclusiveAccess(
            [&](ObjectMetadata& metadata,
                ObjectEntry::State& state) -> std::optional<Result> {
                if (state.is_torn_down || !metadata.IsValid()) {
                    return std::nullopt;
                }
                return std::optional<Result>{std::forward<Fn>(fn)(
                    *tenant_handle, entry, metadata, state)};
            });
    }

    // Runs `fn(entry, metadata, state)` on the object the route of `tenant_id`
    // publishes for `key`, under the entry's own shared lock, and hands the
    // envelope over exactly as stored. Unlike the published-object accessors
    // this resolves the tenant as given and applies no readability predicate,
    // so a test can inspect state a request path would refuse to serve.
    // Nothing when the tenant has no route or the key is not routed.
    template <typename Fn>
    [[nodiscard]] auto WithStoredObjectForRead(const TenantId& tenant_id,
                                               const std::string& key,
                                               Fn&& fn) const
        -> std::optional<std::invoke_result_t<
            Fn, const std::shared_ptr<ObjectEntry>&, const ObjectMetadata&,
            const ObjectEntry::State&>> {
        using Result =
            std::invoke_result_t<Fn, const std::shared_ptr<ObjectEntry>&,
                                 const ObjectMetadata&,
                                 const ObjectEntry::State&>;
        static_assert(!std::is_void_v<Result>,
                      "the helper reports absence through its own optional, so "
                      "the callback must return a result");
        static_assert(!std::is_reference_v<Result>,
                      "the result is carried by value: nothing a callback "
                      "returns may outlive the lock it ran under");
        auto tenant_handle = service_.tenants_.Lookup(tenant_id);
        if (tenant_handle == nullptr) {
            return std::nullopt;
        }
        auto entry = tenant_handle->Get(key);
        if (entry == nullptr) {
            return std::nullopt;
        }
        return entry->WithSharedAccess(
            [&](const ObjectMetadata& metadata,
                const ObjectEntry::State& state) -> std::optional<Result> {
                return std::optional<Result>{
                    std::forward<Fn>(fn)(entry, metadata, state)};
            });
    }

    // The tenant's `cleared`-ordering epoch, as RemoveAll reads it before its
    // walk. Exposed so a test can pin the epoch-changed decision without
    // racing threads.
    uint64_t KvTenantEpochForTesting(const TenantId& tenant_id) {
        return service_.ReadKvTenantEpoch(
            ResolveRequestTenantId(tenant_id).value());
    }

    void PublishKvClearedIfEpochUnchangedForTesting(const TenantId& tenant_id,
                                                    uint64_t expected_epoch) {
        service_.PublishKvClearedIfEpochUnchanged(
            ResolveRequestTenantId(tenant_id), expected_epoch);
    }

    bool IsReplicaReadable(const Replica& replica) const {
        return service_.IsReplicaReadable(replica);
    }

    static std::vector<std::string> KvMediaForMetadata(
        const ObjectMetadata& metadata) {
        return MasterService::KvMediaForMetadata(metadata);
    }

    static std::vector<std::string> KvMediaForRemoval(
        const ObjectMetadata& metadata) {
        return MasterService::KvMediaForRemoval(metadata);
    }

    void LoadTenantQuotaPoliciesFromStoreOrThrow() {
        service_.LoadTenantQuotaPoliciesFromStoreOrThrow();
    }

    ObjectIdentity MakeObjectIdentityForRequest(
        const std::string& user_key, const TenantId& tenant_id) const {
        return service_.MakeObjectIdentityForRequest(user_key, tenant_id);
    }

    bool ObserveDynamicReplicationAccess(const ObjectIdentity& object_id) {
        return service_.ObserveDynamicReplicationAccess(object_id);
    }

    bool ProcessClientOffboardingJob(ClientOffboardingJob& job) {
        return service_.ProcessClientOffboardingJob(job);
    }

    tl::expected<void, ErrorCode> PushOffloadingQueue(
        const ObjectIdentity& object_id, Replica& replica,
        std::vector<UUID>* mirror_clients = nullptr) {
        return service_.PushOffloadingQueue(object_id, replica, mirror_clients);
    }

    void RebuildGroupState() { service_.RebuildGroupState(); }

    void RebuildTenantQuotaUsageFromMetadata() {
        service_.RebuildTenantQuotaUsageFromMetadata();
    }

    void RecomputeTenantEffectiveQuotas() {
        service_.RecomputeTenantEffectiveQuotas();
    }

    void ReleaseTenantQuota(TenantQuotaHandle account, uint64_t bytes) {
        service_.ReleaseTenantQuota(std::move(account), bytes);
    }

    const TenantId& ResolveRequestTenantId(const TenantId& tenant_id) const {
        return service_.ResolveRequestTenantId(tenant_id);
    }

    size_t RunPromotionCandidateRetry() {
        return service_.RunPromotionCandidateRetry();
    }

    PromotionQueueResult TryPushPromotionQueue(const ObjectIdentity& object_id,
                                               bool record_candidate = true) {
        return service_.TryPushPromotionQueue(object_id, record_candidate);
    }

   private:
    MasterService& service_;
};

}  // namespace mooncake::test
