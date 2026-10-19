#pragma once

#include <cstdint>

namespace mooncake::ha {

// The snapshot wire format. A reader accepts every version below the one it
// knows, and refuses anything newer, so a master that only understands v1 and
// v2 starts empty rather than reading state it cannot represent.
//
// v1: objects as one flat replica list.
// v2: adds weight metadata.
// v3: adds objects whose content is several independently written parts. A
//     reader that does not know parts would read such an object as one part
//     holding every replica, so the version it needs is raised instead.
inline constexpr uint32_t kBatchOpLogSnapshotSchemaVersion = 1;
inline constexpr char kBatchOpLogSnapshotFormat[] =
    "standby-oplog-materialized/v1";
inline constexpr uint32_t kBatchOpLogWeightSnapshotSchemaVersion = 2;
inline constexpr char kBatchOpLogWeightSnapshotFormat[] =
    "standby-oplog-materialized/v2";
inline constexpr uint32_t kBatchOpLogPartSnapshotSchemaVersion = 3;
inline constexpr char kBatchOpLogPartSnapshotFormat[] =
    "standby-oplog-materialized/v3";

// The lowest schema version a snapshot holding multi-part objects is written
// with. It is the reader version such a snapshot requires.
inline constexpr uint32_t kBatchOpLogSnapshotMinReaderVersion =
    kBatchOpLogPartSnapshotSchemaVersion;

}  // namespace mooncake::ha
