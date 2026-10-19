#pragma once

// ObjectMetadata: the per-object metadata envelope.
//
// Moved out of MasterService so the service and the per-object metadata
// module share one definition. The envelope owns the object identity, the
// authoritative read lease, the soft-pin state and the tenant quota ledger.
// The object's content and the replicas holding it live in the envelope's
// slots: one slot per part, fixed in number when the object is created.
//
// It has no ownership lock: whoever owns the envelope decides how the
// envelope as a whole is guarded. `slot_locks_` and `writers_lock_` below are
// internal and narrower — a part's writer holds its own slot's lock, and
// `writers_` has its own leaf lock, so two parts of one object can be written
// at the same time.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lease.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "object_part.h"
#include "replica.h"
#include "tenant_id.h"
#include "tenant_quota_ledger.h"
#include "types.h"

namespace mooncake {

struct ResolvedSoftPinRequest {
    SoftPinAction action{SoftPinAction::PRESERVE};
    std::chrono::milliseconds ttl{0};
};

// The write in flight on one part: who started it, when, and which replicas
// that PutStart handed out. PutEnd and PutRevoke are scoped to it, so a late
// or repeated call cannot touch a later writer's replicas.
struct WriteAttempt {
    UUID client_id;
    std::chrono::system_clock::time_point started;
    std::vector<ReplicaID> replica_ids;
};

// One part of the object. `part` is that part's content, absent until the part
// is first written and again once no replica of it is left; `attempt` is the
// write in flight, present only while one is; `writer` indexes `writers_`.
struct PartSlot {
    std::optional<ObjectPart> part;
    std::unique_ptr<WriteAttempt> attempt;
    uint16_t writer{0};
    // When this part's most recent PutStart happened. Retained after the write
    // finishes: the object-level incompleteness timeout and the dynamic
    // replication version epoch are both read from it.
    std::chrono::system_clock::time_point last_started{};
};

struct ObjectMetadata {
    struct SoftPinEvaluation {
        bool active{false};
        int metric_delta{0};
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
    };

    struct PendingSoftPinAction {
        SoftPinAction action{SoftPinAction::PRESERVE};
        std::chrono::milliseconds ttl{0};
        std::unordered_set<ReplicaID> eligible_replica_ids;
    };

    // RAII-style metric management
    ~ObjectMetadata() {
        MasterMetricManager::instance().dec_key_count(1);
        if (soft_pin_timeout) {
            MasterMetricManager::instance().dec_soft_pin_key_count(1);
        }
    }

    ObjectMetadata() = delete;

    // The content given here becomes part `part_index` of an object with
    // `part_count` parts; every other part starts empty. The write this call
    // starts is recorded as the attempt on that same part.
    ObjectMetadata(const UUID& client_id_,
                   const std::chrono::system_clock::time_point put_start_time_,
                   size_t value_length, std::vector<Replica>&& reps,
                   std::optional<std::chrono::system_clock::time_point>
                       committed_soft_pin_timeout = std::nullopt,
                   bool enable_hard_pin = false,
                   ObjectDataType data_type_ = ObjectDataType::UNKNOWN,
                   std::string group_id_ = "", TenantId tenant_id_ = TenantId(),
                   std::string user_key_ = {}, uint32_t part_count = 1,
                   uint32_t part_index = 0)
        : part_count(part_count),
          data_type(data_type_),
          group_id(std::move(group_id_)),
          tenant_id(std::move(tenant_id_)),
          user_key(std::move(user_key_)),
          soft_pin_timeout(std::move(committed_soft_pin_timeout)),
          hard_pinned(enable_hard_pin) {
        if (part_count == 0 || part_index >= part_count) {
            throw std::invalid_argument("part index outside the part count");
        }
        slots_.resize(part_count);
        slot_locks_ = std::make_unique<SpinLock[]>(part_count);
        std::vector<ReplicaID> replica_ids;
        replica_ids.reserve(reps.size());
        for (const auto& replica : reps) {
            replica_ids.push_back(replica.id());
        }
        slots_.at(part_index).part.emplace(value_length, std::move(reps));
        BeginWrite(part_index, client_id_, put_start_time_,
                   std::move(replica_ids));
        MasterMetricManager::instance().inc_key_count(1);
        if (soft_pin_timeout) {
            MasterMetricManager::instance().inc_soft_pin_key_count(1);
        }
        MasterMetricManager::instance().observe_value_size(value_length);
    }

    ObjectMetadata(const ObjectMetadata&) = delete;
    ObjectMetadata& operator=(const ObjectMetadata&) = delete;
    ObjectMetadata(ObjectMetadata&&) = delete;
    ObjectMetadata& operator=(ObjectMetadata&&) = delete;

    // Fixed when the object is created; never grows or shrinks after that.
    const uint32_t part_count;
    const ObjectDataType data_type{ObjectDataType::UNKNOWN};
    const std::string group_id;
    const TenantId tenant_id;
    const std::string user_key;

    // Committed attribution only; protected by the owning metadata shard lock.
    std::string session_id;
    struct PendingSessionTag {
        std::string session_id;
        std::unordered_set<ReplicaID> eligible_replica_ids;
    };
    std::optional<PendingSessionTag> pending_session_tag;

    void BeginSessionTag(std::string id,
                         const std::unordered_set<ReplicaID>& replicas) {
        pending_session_tag.reset();
        if (!id.empty()) {
            pending_session_tag = PendingSessionTag{std::move(id), replicas};
        }
    }

    void CommitSessionTag(ReplicaID replica_id) {
        if (pending_session_tag &&
            pending_session_tag->eligible_replica_ids.contains(replica_id)) {
            session_id = std::move(pending_session_tag->session_id);
            pending_session_tag.reset();
        }
    }

    void ClearSessionTagIfNoViableReplica() {
        if (pending_session_tag && !HasReplica([this](const Replica& replica) {
                return pending_session_tag->eligible_replica_ids.contains(
                           replica.id()) &&
                       replica.is_processing() &&
                       !replica.has_invalid_mem_handle() &&
                       !replica.has_invalid_nof_handle();
            })) {
            pending_session_tag.reset();
        }
    }

    mutable SpinLock lock;
    // Authoritative lease: ungrouped objects own one; grouped objects share
    // the group's. Never null after construction.
    mutable std::shared_ptr<Lease> lease_ GUARDED_BY(lock) =
        std::make_shared<Lease>();
    mutable std::optional<std::chrono::system_clock::time_point>
        soft_pin_timeout GUARDED_BY(lock);  // committed object soft-pin
                                            // deadline
    // Replica IDs scope this action to the current write. PutEnd does not
    // carry a generation token, so a stale End from the same client cannot
    // otherwise be distinguished from the current write.
    std::optional<PendingSoftPinAction> pending_soft_pin_action;
    const bool hard_pinned{false};  // immutable, set at creation
    bool memory_cache_total_accounted{false};
    bool disk_cache_total_accounted{false};
    TenantQuotaLedger quota_ledger;

    struct DynamicReplicaRecord {
        std::chrono::system_clock::time_point created_at;
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
        bool complete{false};
    };

    std::unordered_map<ReplicaID, DynamicReplicaRecord> dynamic_replicas;
    std::chrono::steady_clock::time_point dynamic_replication_recreate_after{};

    void MarkDynamicReplica(ReplicaID replica_id, DynamicReplicaRecord record) {
        dynamic_replicas[replica_id] = std::move(record);
    }

    void MarkDynamicReplicasComplete(
        const std::vector<ReplicaID>& replica_ids) {
        for (const auto& replica_id : replica_ids) {
            auto it = dynamic_replicas.find(replica_id);
            if (it != dynamic_replicas.end()) {
                it->second.complete = true;
            }
        }
    }

    size_t ForgetDynamicReplicas(const std::vector<ReplicaID>& replica_ids) {
        size_t forgotten = 0;
        for (const auto& replica_id : replica_ids) {
            forgotten += dynamic_replicas.erase(replica_id);
        }
        return forgotten;
    }

    size_t DynamicReplicaCount() const { return dynamic_replicas.size(); }

    bool DynamicReplicationRecreateBlocked(
        std::chrono::steady_clock::time_point now) const {
        return now < dynamic_replication_recreate_after;
    }

    void SetDynamicReplicationRecreateAfter(
        std::chrono::steady_clock::time_point deadline) {
        dynamic_replication_recreate_after =
            std::max(dynamic_replication_recreate_after, deadline);
    }

    // The three invariants the slots hold: there are `part_count` of them and
    // that never changes; an empty slot means that part has no replica at all,
    // so its length is gone with them; a non-empty slot keeps its length.

    // Read under the owner's shared lock, change under its exclusive lock. A
    // replica pointer taken from a part is invalidated by any add or pop on it.
    const PartSlot& Slot(size_t index) const { return slots_.at(index); }

    PartSlot& MutableSlot(size_t index) { return slots_.at(index); }

    const ObjectPart* GetPart(size_t index) const {
        const auto& part = slots_.at(index).part;
        return part.has_value() ? &*part : nullptr;
    }

    ObjectPart* MutablePart(size_t index) {
        auto& part = slots_.at(index).part;
        return part.has_value() ? &*part : nullptr;
    }

    // The part's own lock. A part's writer holds it while it allocates,
    // completes or revokes that part, under the owner's shared lock.
    SpinLock& GetSlotLock(size_t index) { return slot_locks_[index]; }

    const WriteAttempt* GetAttempt(size_t index) const {
        return slots_.at(index).attempt.get();
    }

    // The client that wrote part `index`, as far back as the object remembers.
    const UUID& WriterOf(size_t index) const {
        return writers_.at(slots_.at(index).writer);
    }

    // Whether this client wrote any part of the object. Ownership questions
    // that used to compare against one object-level writer ask this instead.
    bool WasWrittenBy(const UUID& client_id) const {
        SpinLocker locker(&writers_lock_);
        return std::ranges::find(writers_, client_id) != writers_.end();
    }

    // Record `client_id` as part `index`'s writer and open the write attempt
    // whose replicas the matching PutEnd or PutRevoke is scoped to.
    void BeginWrite(size_t index, const UUID& client_id,
                    const std::chrono::system_clock::time_point started,
                    std::vector<ReplicaID> replica_ids) {
        auto& slot = slots_.at(index);
        {
            SpinLocker locker(&writers_lock_);
            auto writer = std::ranges::find(writers_, client_id);
            if (writer == writers_.end()) {
                if (writers_.size() > std::numeric_limits<uint16_t>::max()) {
                    throw std::length_error("too many writers for one object");
                }
                writers_.push_back(client_id);
                writer = std::prev(writers_.end());
            }
            slot.writer =
                static_cast<uint16_t>(std::distance(writers_.begin(), writer));
        }
        slot.last_started = started;
        slot.attempt = std::make_unique<WriteAttempt>(
            WriteAttempt{client_id, started, std::move(replica_ids)});
    }

    // The write on part `index` finished or was revoked; what it was scoped to
    // no longer applies to anything.
    void EndWrite(size_t index) { slots_.at(index).attempt.reset(); }

    // No write is in flight on any part. A restored object is not a write in
    // flight: its replicas arrived complete.
    void ForgetWriteAttempts() {
        for (auto& slot : slots_) {
            slot.attempt.reset();
        }
    }

    // Whether a PutStart is in flight on any part. An attempt older than
    // `not_before` is not: PutStart reallocates over it once
    // put_start_discard_timeout has passed, so callers pass now minus that.
    bool HasWriteInFlight(
        const std::chrono::system_clock::time_point& not_before) const {
        for (const auto& slot : slots_) {
            if (slot.attempt != nullptr && slot.attempt->started > not_before) {
                return true;
            }
        }
        return false;
    }

    size_t TotalLength() const {
        size_t total = 0;
        for (const auto& slot : slots_) {
            if (slot.part.has_value()) {
                total += slot.part->length();
            }
        }
        return total;
    }

    // Give a part its content. A part that still holds replicas does not take
    // new content: it is either being written or already written.
    void EmplacePart(size_t index, size_t length,
                     std::vector<Replica>&& replicas) {
        auto& part = slots_.at(index).part;
        if (part.has_value()) {
            throw std::logic_error("part already holds content");
        }
        part.emplace(length, std::move(replicas));
    }

    void AddReplicasToPart(size_t index, std::vector<Replica>&& replicas) {
        slots_.at(index).part.value().AddReplicas(std::move(replicas));
    }

    // A part's last replica leaving also drops its length: an empty part keeps
    // no content at all, so no other slot state needs rewinding.
    std::vector<Replica> PopReplicasOfPart(
        size_t index, const std::function<bool(const Replica&)>& pred_fn) {
        auto& slot = slots_.at(index);
        if (!slot.part.has_value()) {
            return {};
        }
        auto popped_replicas = slot.part->PopReplicas(pred_fn);
        if (slot.part->replicas().empty()) {
            slot.part.reset();
        }
        return popped_replicas;
    }

    std::vector<Replica> PopAllReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        std::vector<Replica> popped_replicas;
        for (size_t index = 0; index < slots_.size(); ++index) {
            auto from_part = PopReplicasOfPart(index, pred_fn);
            popped_replicas.insert(popped_replicas.end(),
                                   std::make_move_iterator(from_part.begin()),
                                   std::make_move_iterator(from_part.end()));
        }
        return popped_replicas;
    }

    std::vector<Replica> PopAllReplicas() {
        std::vector<Replica> popped_replicas;
        for (size_t index = 0; index < slots_.size(); ++index) {
            auto from_part =
                PopReplicasOfPart(index, [](const Replica&) { return true; });
            popped_replicas.insert(popped_replicas.end(),
                                   std::make_move_iterator(from_part.begin()),
                                   std::make_move_iterator(from_part.end()));
        }
        return popped_replicas;
    }

    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        return PopAllReplicas(pred_fn).size();
    }

    // visit_fn may modify the replica but must not add or pop replicas.
    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        size_t num_visited = 0;

        for (auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            for (auto& replica : slot.part->replicas()) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    num_visited++;
                }
            }
        }

        return num_visited;
    }

    size_t VisitReplicas(
        const std::function<bool(const Replica&)>& pred_fn,
        const std::function<void(const Replica&)>& visit_fn) const {
        size_t num_visited = 0;

        for (const auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            for (const auto& replica : slot.part->replicas()) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    num_visited++;
                }
            }
        }

        return num_visited;
    }

    // Walks every replica of every part. An object's replicas are no longer
    // one contiguous range, so a caller that only wants to look at them all
    // says so through this rather than through a span.
    template <typename Fn>
    void ForEachReplica(Fn&& fn) const {
        for (const auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            for (const auto& replica : slot.part->replicas()) {
                fn(replica);
            }
        }
    }

    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        for (const auto& slot : slots_) {
            if (slot.part.has_value() &&
                std::ranges::any_of(slot.part->replicas(), pred_fn)) {
                return true;
            }
        }
        return false;
    }

    // Every part holds content and every replica of it matches. An empty part
    // is content this object is missing, so it answers false.
    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        for (const auto& slot : slots_) {
            if (!slot.part.has_value() ||
                !std::ranges::all_of(slot.part->replicas(), pred_fn)) {
                return false;
            }
        }
        return true;
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        size_t count = 0;
        for (const auto& slot : slots_) {
            if (slot.part.has_value()) {
                count += static_cast<size_t>(
                    std::ranges::count_if(slot.part->replicas(), pred_fn));
            }
        }
        return count;
    }

    size_t CountReplicas() const {
        size_t count = 0;
        for (const auto& slot : slots_) {
            if (slot.part.has_value()) {
                count += slot.part->replicas().size();
            }
        }
        return count;
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        for (auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            auto replicas = slot.part->replicas();
            const auto it = std::ranges::find_if(replicas, pred_fn);
            if (it != replicas.end()) {
                return &(*it);
            }
        }
        return nullptr;
    }

    Replica* GetReplicaByID(const ReplicaID& id) {
        return GetFirstReplica(
            [&id](const Replica& replica) { return replica.id() == id; });
    }

    Replica* GetReplicaBySegmentName(const std::string& segment_name) {
        return GetFirstReplica([&segment_name](const Replica& replica) {
            const auto names = replica.get_segment_names();
            return std::ranges::any_of(names,
                                       [&segment_name](const auto& name_opt) {
                                           return name_opt == segment_name;
                                       });
        });
    }

    // Grant a read lease at now() + ttl. Grouped objects extend the shared
    // group TTL; a zero ttl is a no-op on a live lease (PutEnd cannot
    // expire a live group).
    void GrantReadLease(std::chrono::milliseconds ttl) const {
        SpinLocker locker(&lock);
        lease_->GrantReadLease(ttl);
    }

    // Whether the authoritative lease is expired.
    bool IsLeaseExpired() const {
        SpinLocker locker(&lock);
        return lease_->IsExpired(std::chrono::system_clock::now());
    }

    bool IsLeaseExpired(std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        return lease_->IsExpired(now);
    }

    // Lease deadline for the eviction census.
    std::chrono::system_clock::time_point EvictionDeadline() const {
        return lease_->ExpiresAt();
    }

    SoftPinEvaluation EvaluateSoftPin(
        const std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        if (soft_pin_timeout && now >= *soft_pin_timeout) {
            const auto removed_deadline = *soft_pin_timeout;
            soft_pin_timeout.reset();
            return {.active = false,
                    .metric_delta = -1,
                    .removed_deadline = removed_deadline,
                    .deadline_to_index = std::nullopt};
        }
        return {.active = soft_pin_timeout.has_value(),
                .metric_delta = 0,
                .removed_deadline = std::nullopt,
                .deadline_to_index = std::nullopt};
    }

    bool ExpireSoftPinIfDeadlineMatches(
        const std::chrono::system_clock::time_point& expected_deadline,
        const std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        if (!soft_pin_timeout || *soft_pin_timeout != expected_deadline ||
            now < expected_deadline) {
            return false;
        }
        soft_pin_timeout.reset();
        return true;
    }

    // Saturates at the clock's maximum rather than overflowing when the TTL
    // reaches past it. The comparison is done in milliseconds because
    // converting a huge TTL up to the clock's finer period would overflow.
    static std::chrono::system_clock::time_point ComputeSoftPinDeadline(
        const std::chrono::system_clock::time_point& now,
        std::chrono::milliseconds ttl) {
        const auto max_time = std::chrono::system_clock::time_point::max();
        const auto headroom = max_time - now;
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(headroom);
        return ttl > remaining ? max_time : now + ttl;
    }

    std::optional<std::chrono::system_clock::time_point>
    GetCommittedSoftPinTimeout() const {
        SpinLocker locker(&lock);
        return soft_pin_timeout;
    }

    void BeginSoftPinAction(
        const ResolvedSoftPinRequest& request,
        std::unordered_set<ReplicaID> eligible_replica_ids) {
        pending_soft_pin_action = PendingSoftPinAction{
            request.action, request.ttl, std::move(eligible_replica_ids)};
    }

    bool PendingSoftPinOwnsReplica(ReplicaID replica_id) const {
        if (!pending_soft_pin_action) {
            return false;
        }
        return pending_soft_pin_action->eligible_replica_ids.contains(
            replica_id);
    }

    void ClearPendingSoftPinAction() { pending_soft_pin_action.reset(); }

    SoftPinEvaluation CommitPendingSoftPin(
        const std::chrono::system_clock::time_point& now) {
        if (!pending_soft_pin_action) {
            return EvaluateSoftPin(now);
        }

        const PendingSoftPinAction pending =
            std::move(*pending_soft_pin_action);
        pending_soft_pin_action.reset();

        SpinLocker locker(&lock);
        int metric_delta = 0;
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
        if (soft_pin_timeout && now >= *soft_pin_timeout) {
            removed_deadline = *soft_pin_timeout;
            soft_pin_timeout.reset();
            --metric_delta;
        }

        switch (pending.action) {
            case SoftPinAction::PRESERVE:
                break;
            case SoftPinAction::ENABLE:
                if (pending.ttl == std::chrono::milliseconds::zero()) {
                    if (soft_pin_timeout) {
                        removed_deadline = *soft_pin_timeout;
                        soft_pin_timeout.reset();
                        --metric_delta;
                    }
                } else {
                    if (!soft_pin_timeout) {
                        ++metric_delta;
                    }
                    soft_pin_timeout = ComputeSoftPinDeadline(now, pending.ttl);
                    deadline_to_index = soft_pin_timeout;
                    // Upserting the latest registration supersedes any
                    // expired or previously active deadline.
                    removed_deadline.reset();
                }
                break;
            case SoftPinAction::DISABLE:
                if (soft_pin_timeout) {
                    removed_deadline = *soft_pin_timeout;
                    soft_pin_timeout.reset();
                    --metric_delta;
                }
                break;
        }
        return {.active = soft_pin_timeout.has_value(),
                .metric_delta = metric_delta,
                .removed_deadline = removed_deadline,
                .deadline_to_index = deadline_to_index};
    }

    void ClearPendingSoftPinIfNoViableReplica() {
        if (!pending_soft_pin_action) {
            return;
        }
        const auto& eligible = pending_soft_pin_action->eligible_replica_ids;
        const bool has_viable_replica =
            HasReplica([&eligible](const Replica& replica) {
                const bool belongs_to_write = eligible.contains(replica.id());
                const bool valid_handle = !replica.has_invalid_mem_handle() &&
                                          !replica.has_invalid_nof_handle();
                return belongs_to_write && replica.is_processing() &&
                       valid_handle;
            });
        if (!has_viable_replica) {
            pending_soft_pin_action.reset();
        }
    }

    bool IsHardPinned() const { return hard_pinned; }

    bool IsGrouped() const { return !group_id.empty(); }

    // Every part holds content and every part has a replica this predicate
    // accepts. Readability is asked for, never stored: a part loses it the
    // moment its last readable replica goes away, whether through eviction, a
    // segment going offline or a handle going stale.
    template <typename IsReplicaReadable>
    bool IsReadable(IsReplicaReadable&& is_readable) const {
        if (slots_.empty()) {
            return false;
        }
        for (const auto& slot : slots_) {
            if (!slot.part.has_value() ||
                !std::ranges::any_of(slot.part->replicas(), is_readable)) {
                return false;
            }
        }
        return true;
    }

    // Check if the metadata is valid
    // Valid means it has at least one valid replica and size is greater
    // than 0
    bool IsValid() const {
        return TotalLength() > 0 && HasReplica([](const Replica& replica) {
                   return !replica.is_memory_replica() ||
                          !replica.has_invalid_mem_handle();
               });
    }

    std::vector<std::string> GetReplicaSegmentNames() const {
        std::vector<std::string> segment_names;
        for (const auto& slot : slots_) {
            if (!slot.part.has_value()) {
                continue;
            }
            for (const auto& replica : slot.part->replicas()) {
                const auto& segment_name_options = replica.get_segment_names();
                for (const auto& segment_name_opt : segment_name_options) {
                    if (segment_name_opt.has_value()) {
                        segment_names.push_back(segment_name_opt.value());
                    }
                }
            }
        }
        return segment_names;
    }

   private:
    std::vector<PartSlot> slots_;
    // The clients that have written a part of this object, in the order they
    // first did. A slot's `writer` indexes this, which is how "who wrote this
    // part" survives after the write attempt itself is gone.
    std::vector<UUID> writers_;
    mutable SpinLock writers_lock_;
    std::unique_ptr<SpinLock[]> slot_locks_;
};

}  // namespace mooncake
