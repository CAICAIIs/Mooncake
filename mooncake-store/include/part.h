#pragma once

// Part: the content of one object, and the replicas that hold it.
//
// `length` and `checksum` describe the content, and every replica of that
// content shares them. The replicas themselves live here, so ownership of a
// replica is expressed by the part that contains it rather than by a flag on
// the replica.
//
// The operations below are the ones ObjectMetadata used to expose. The
// envelope currently holds exactly one part and forwards to it, so a call site
// keeps the meaning it had before the split.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "replica.h"

namespace mooncake {

class Part {
   public:
    Part() = delete;

    Part(size_t length, std::vector<Replica>&& replicas)
        : length_(length), replicas_(std::move(replicas)) {}

    size_t length() const { return length_; }

    const std::optional<uint64_t>& checksum() const { return checksum_; }

    void SetChecksum(std::optional<uint64_t> checksum) {
        checksum_ = std::move(checksum);
    }

    void AddReplicas(std::vector<Replica>&& replicas) {
        replicas_.insert(replicas_.end(), std::move_iterator(replicas.begin()),
                         std::move_iterator(replicas.end()));
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        auto partition_point = std::partition(
            replicas_.begin(), replicas_.end(),
            [pred_fn](const Replica& replica) { return !pred_fn(replica); });

        std::vector<Replica> popped_replicas;
        if (partition_point != replicas_.end()) {
            popped_replicas.reserve(
                std::distance(partition_point, replicas_.end()));
            std::move(partition_point, replicas_.end(),
                      std::back_inserter(popped_replicas));
            replicas_.erase(partition_point, replicas_.end());
        }

        return popped_replicas;
    }

    std::vector<Replica> PopReplicas() { return std::move(replicas_); }

    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        auto erased_replicas = PopReplicas(pred_fn);
        return erased_replicas.size();
    }

    size_t EraseReplicas() {
        auto erased_replicas = PopReplicas();
        return erased_replicas.size();
    }

    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        size_t num_visited = 0;

        for (auto& replica : replicas_) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    size_t VisitReplicas(
        const std::function<bool(const Replica&)>& pred_fn,
        const std::function<void(const Replica&)>& visit_fn) const {
        size_t num_visited = 0;

        for (auto& replica : replicas_) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::all_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas() const { return replicas_.size(); }

    const std::vector<Replica>& GetAllReplicas() const { return replicas_; }

    std::optional<ReplicaStatus> HasDiffRepStatus(ReplicaStatus status) const {
        for (const auto& replica : replicas_) {
            if (replica.status() != status) {
                return replica.status();
            }
        }
        return {};
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        const auto it =
            std::find_if(replicas_.begin(), replicas_.end(), pred_fn);
        return it != replicas_.end() ? &(*it) : nullptr;
    }

    Replica* GetReplicaByID(const ReplicaID& id) {
        return GetFirstReplica(
            [&id](const Replica& replica) { return replica.id() == id; });
    }

    bool EraseReplicaByID(const ReplicaID& id) {
        auto num_erased = EraseReplicas(
            [&id](const Replica& replica) { return replica.id() == id; });
        return num_erased > 0;
    }

    size_t EraseReplica(ReplicaType replica_type) {
        return EraseReplicas([replica_type](const Replica& replica) {
            if (replica_type == ReplicaType::ALL) {
                return replica.is_memory_replica() ||
                       replica.is_nof_replica() || replica.is_dfs_replica();
            }
            return replica.type() == replica_type;
        });
    }

    bool HasMemReplica() const {
        return HasReplica(&Replica::fn_is_memory_replica);
    }

    bool HasNoFReplica() const {
        return HasReplica(&Replica::fn_is_nof_replica);
    }

    size_t GetMemReplicaCount() const {
        return CountReplicas(&Replica::fn_is_memory_replica);
    }

    size_t GetNoFReplicaCount() const {
        return CountReplicas(&Replica::fn_is_nof_replica);
    }

    Replica* GetReplicaBySegmentName(const std::string& segment_name) {
        return GetFirstReplica([&segment_name](const Replica& replica) {
            auto names = replica.get_segment_names();
            for (auto& name_opt : names) {
                if (name_opt == segment_name) {
                    return true;
                }
            }
            return false;
        });
    }

    std::vector<std::string> GetReplicaSegmentNames() const {
        std::vector<std::string> segment_names;
        for (const auto& replica : replicas_) {
            const auto& segment_name_options = replica.get_segment_names();
            for (const auto& segment_name_opt : segment_name_options) {
                if (segment_name_opt.has_value()) {
                    segment_names.push_back(segment_name_opt.value());
                }
            }
        }
        return segment_names;
    }

   private:
    // Immutable for the part's lifetime, as the object's size was before the
    // content moved here.
    const size_t length_;
    std::optional<uint64_t> checksum_;
    // Use the accessors above to visit and modify the replicas.
    std::vector<Replica> replicas_;
};

}  // namespace mooncake
