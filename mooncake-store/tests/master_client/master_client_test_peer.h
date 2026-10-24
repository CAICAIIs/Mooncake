// master_client_test_peer.h
//
// Reaches the one piece of MasterClient's state no call can set: what its
// master said about parts. A suite that needs a client whose master predates
// parts says so here, because the answer otherwise comes from the master's
// Ping.

#pragma once

#include <optional>

#include "master_client.h"

namespace mooncake {
namespace test {

class MasterClientTestPeer {
   public:
    // The answer the client takes as its master's, as of the next call that
    // names a part. `std::nullopt` forgets it, so the client asks again.
    static void SetMasterTakesParts(MasterClient& client,
                                    std::optional<bool> takes_parts) {
        MutexLocker lock(&client.part_support_mutex_);
        client.master_takes_parts_ = takes_parts;
    }
};

}  // namespace test
}  // namespace mooncake
