#pragma once

#include <optional>

#include "master_client.h"

namespace mooncake {
namespace test {

// Reaches the state no call can set: what the client holds as its master's
// answer about parts.
class MasterClientTestPeer {
   public:
    // Stands in for that answer. `std::nullopt` forgets it, so the client asks
    // the master again.
    static void SetMasterTakesParts(MasterClient& client,
                                    std::optional<bool> takes_parts) {
        MutexLocker lock(&client.part_support_mutex_);
        client.master_takes_parts_ = takes_parts;
    }

    // The answer the client holds, `std::nullopt` before it asked.
    static std::optional<bool> MasterTakesParts(const MasterClient& client) {
        MutexLocker lock(&client.part_support_mutex_);
        return client.master_takes_parts_;
    }
};

}  // namespace test
}  // namespace mooncake
