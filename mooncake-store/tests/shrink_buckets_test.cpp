#include "common/shrink_buckets.h"

#include <cstddef>
#include <unordered_map>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

using Container = std::unordered_map<size_t, size_t>;

Container GrownContainer() {
    Container container;
    for (size_t i = 0; i < 100'000; ++i) {
        container.emplace(i, i);
    }
    return container;
}

TEST(ShrinkBucketsIfSparseTest, RehashesAContainerThatEmptiedOut) {
    auto container = GrownContainer();
    const size_t grown = container.bucket_count();
    ASSERT_GT(grown, kShrinkMinBucketCount);

    container.clear();
    ShrinkBucketsIfSparse(container);

    EXPECT_LT(container.bucket_count(), grown);
    EXPECT_EQ(container.size(), 0u);
}

TEST(ShrinkBucketsIfSparseTest, LeavesAContainerThatIsStillDense) {
    auto container = GrownContainer();
    const size_t grown = container.bucket_count();

    ShrinkBucketsIfSparse(container);

    EXPECT_EQ(container.bucket_count(), grown);
}

TEST(ShrinkBucketsIfSparseTest, LeavesASmallContainerAlone) {
    Container container;
    for (size_t i = 0; i < 8; ++i) {
        container.emplace(i, i);
    }
    const size_t buckets = container.bucket_count();
    ASSERT_LT(buckets, kShrinkMinBucketCount);

    container.clear();
    ShrinkBucketsIfSparse(container);

    EXPECT_EQ(container.bucket_count(), buckets);
}

}  // namespace
}  // namespace mooncake
