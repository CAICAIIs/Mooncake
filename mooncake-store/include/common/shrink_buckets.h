#pragma once

#include <cstddef>

namespace mooncake {

// Erasing from a standard unordered container never returns bucket memory, so
// a container that once held millions of entries keeps its high-water bucket
// array (8 bytes per bucket) for the rest of its life. This rehashes it down to
// roughly twice its live size, once the array is both large enough to be worth
// shrinking and less than a quarter full: the floor avoids rehash churn on
// small containers, and the 2x headroom keeps a freshly shrunk container from
// growing again right away.
//
// Rehashing invalidates iterators, so a caller must hold the lock guarding the
// container and must not be iterating it.
inline constexpr size_t kShrinkMinBucketCount = 1024;

template <typename UnorderedContainer>
void ShrinkBucketsIfSparse(UnorderedContainer& container) {
    if (container.bucket_count() > kShrinkMinBucketCount &&
        container.size() < container.bucket_count() / 4) {
        container.rehash(container.size() * 2);
    }
}

}  // namespace mooncake
