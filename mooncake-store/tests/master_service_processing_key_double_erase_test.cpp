// Regression test for the teardown an access triggers on an object whose only
// replicas went invalid behind the service's back.
//
// The shape: the head of a read-write access drops the object's invalid memory
// replicas under the entry's own lock, and that cleanup can find the object
// unreadable, tear it down and drop its route slot. A teardown must run at
// most once per publication — ObjectEntry::State::is_torn_down claims it in
// EraseMetadata, and a second eraser of the same entry bails out rather than
// releasing the refcounts, quota charges and KV removal events a second time.
// A path that instead erased per-key bookkeeping twice, or that acted on an
// entry an earlier teardown had already released, died here with SIGSEGV
// inside a container erase.
//
// Production trigger chain reproduced here (public API + test peer):
//   1. MountSegment                     (a "ghost" client mounts a segment)
//   2. PutStart without PutEnd          (key stays routed with is_processing
//                                        set and an incomplete replica on that
//                                        segment)
//   3. PrepareUnmountSegment            (ghost client expires; the replica's
//                                        allocator weak_ptr expires, so
//                                        has_invalid_mem_handle() == true)
//   4. PutEnd                           (the access cleanup drops the invalid
//                                        replica and tears the object down
//                                        once)
//
// Two scenario details matter for a deterministic repro:
//   * Step 3 must NOT use MasterService::UnmountSegment: it internally runs
//     ClearInvalidHandles(), which would erase the crafted object through the
//     SAFE path (EraseMetadata) before step 4 can hit the buggy accessor path.
//     Production had the same window: the expiry thread unmounts the ghost
//     segment and only THEN slowly sweeps 23M keys in ClearInvalidHandles —
//     any RPC landing in that window hits the buggy accessor cleanup first.
//   * A second live key in the SAME tenant must keep the tenant non-empty.
//     Otherwise the tenant is dropped and the recorded handle goes away,
//     masking the bug (the buggy branch is guarded by a non-null tenant).
//     Production tenants hold millions of keys, so the buggy branch always
//     executed.
//
// On the buggy code step 4 segfaults; the forked-child assertion below turns
// that into a clean test failure. After the fix the child exits 0 (PutEnd
// simply reports OBJECT_NOT_FOUND) and the test passes.

#include "master_service.h"
#include "master_service/master_service_test_peer.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cerrno>
#include <csignal>
#include <cstring>
#include <sys/wait.h>
#include <unistd.h>

namespace mooncake::test {

class MasterServiceProcessingKeyDoubleEraseTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterServiceProcessingKeyDoubleEraseTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static constexpr size_t kSegmentBase = 0x300000000;
    static constexpr size_t kSegmentSize = 16 * 1024 * 1024;

    // Exit codes used by the child to report how far it got.
    static constexpr int kExitOk = 0;              // reached past the trigger
    static constexpr int kExitMountFailed = 2;     // scenario setup broken
    static constexpr int kExitPutStartFailed = 3;  // scenario setup broken
    static constexpr int kExitUnmountFailed = 4;   // scenario setup broken

    // All of a tenant's keys live in that tenant's one container, so any
    // second live key keeps the tenant non-empty.
    std::string MakeKeepaliveKey(const std::string& key) const {
        return key + "_keepalive";
    }

    // Builds the incident state and fires the trigger. Only returns on
    // fixed code; a path that tears the object down twice dies with SIGSEGV
    // during the write access PutEnd performs.
    void RunIncidentScenario() {
        MasterService service(MasterServiceConfig::builder().build());

        // 1. Ghost client mounts a segment.
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "ghost_segment";
        segment.base = kSegmentBase;
        segment.size = kSegmentSize;
        segment.te_endpoint = segment.name;
        const UUID client_id = generate_uuid();
        if (!service.MountSegment(segment, client_id).has_value()) {
            ::_exit(kExitMountFailed);
        }

        // 2. PutStart a key onto the segment and never complete it — the key
        //    stays routed to the tenant with is_processing set (client "died"
        //    mid-put).
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segment = segment.name;
        const std::string key = "orphan_processing_key";
        if (!service.PutStart(client_id, key, TenantId::Default(), 1024, config)
                 .has_value()) {
            ::_exit(kExitPutStartFailed);
        }

        // 2b. A second, completed key in the same tenant keeps the tenant
        //     non-empty in step 4 (see file header for why this is required).
        const std::string keepalive_key = MakeKeepaliveKey(key);
        if (!service
                 .PutStart(client_id, keepalive_key, TenantId::Default(), 1024,
                           config)
                 .has_value() ||
            !service
                 .PutEnd(client_id, keepalive_key, TenantId::Default(),
                         ReplicaType::MEMORY)
                 .has_value()) {
            ::_exit(kExitPutStartFailed);
        }

        // 3. Ghost client expires: the segment allocator is destroyed,
        //    invalidating the replica's memory handle (weak_ptr expires).
        //    No ClearInvalidHandles sweep here (see file header).
        size_t metrics_dec_capacity = 0;
        {
            auto segment_access = MasterServiceTestPeer::SegmentManager(service)
                                      .getSegmentAccess();
            if (segment_access.PrepareUnmountSegment(
                    segment.id, metrics_dec_capacity) != ErrorCode::OK) {
                ::_exit(kExitUnmountFailed);
            }
        }

        // 4. Trigger: PutEnd takes the write access on the key. Its cleanup
        //    drops the invalid replica, the object is left unreadable, and the
        //    teardown must claim it exactly once and report OBJECT_NOT_FOUND.
        (void)service.PutEnd(client_id, key, TenantId::Default(),
                             ReplicaType::MEMORY);
        ::_exit(kExitOk);  // only reachable on fixed code
    }
};

// Regression assertion: the incident scenario must complete without crashing.
// A teardown that runs twice or acts on an already-released entry segfaults the
// forked child; a correct one lets it exit kExitOk and the test passes.
TEST_F(MasterServiceProcessingKeyDoubleEraseTest,
       AccessCleanupAfterSegmentUnmountDoesNotCrash) {
    ::fflush(nullptr);
    pid_t pid = ::fork();
    ASSERT_NE(pid, -1) << "fork failed: " << strerror(errno);
    if (pid == 0) {
        RunIncidentScenario();
        ::_exit(kExitOk);  // unreachable (RunIncidentScenario exits itself)
    }

    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);

    if (WIFSIGNALED(status)) {
        FAIL() << "entry teardown reproduced a double erase: child died "
                  "with signal "
               << WTERMSIG(status)
               << (WTERMSIG(status) == SIGSEGV ? " (SIGSEGV)" : "")
               << ". A teardown must claim the entry once through "
                  "ObjectEntry::State::is_torn_down and bail out on any "
                  "second attempt.";
    }
    ASSERT_TRUE(WIFEXITED(status)) << "child did not exit normally";
    EXPECT_EQ(WEXITSTATUS(status), kExitOk)
        << "scenario setup failed (exit " << WEXITSTATUS(status)
        << ": 2=MountSegment, 3=PutStart, 4=UnmountSegment)";
}

}  // namespace mooncake::test
