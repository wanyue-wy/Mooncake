#include <gtest/gtest.h>
#include <glog/logging.h>

#include <atomic>
#include <thread>
#include <vector>

#include "p2p_segment_manager.h"

namespace mooncake {

class P2PSegmentManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("P2PSegmentManagerTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    Segment MakeSegment(UUID id = {1, 1},
                        const std::string& name = "seg1",
                        size_t size = 1024 * 1024,
                        int priority = 1) {
        Segment seg;
        seg.id = id;
        seg.name = name;
        seg.size = size;
        seg.extra = P2PSegmentExtraData{
            .priority = priority,
            .tags = {},
            .memory_type = MemoryType::DRAM,
            .usage = 0,
        };
        return seg;
    }
};

// ============================================================
// Mount / Unmount (base SegmentManager interface)
// ============================================================

TEST_F(P2PSegmentManagerTest, MountSegmentSuccess) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment();
    auto res = mgr.MountSegment(seg);
    EXPECT_TRUE(res.has_value());
}

TEST_F(P2PSegmentManagerTest, MountDuplicateSegment) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment();
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.MountSegment(seg);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_ALREADY_EXISTS);
}

TEST_F(P2PSegmentManagerTest, UnmountSegmentSuccess) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment();
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.UnmountSegment(seg.id);
    EXPECT_TRUE(res.has_value());
}

TEST_F(P2PSegmentManagerTest, UnmountNonexistentSegment) {
    P2PSegmentManager mgr;
    UUID id = {999, 999};
    auto res = mgr.UnmountSegment(id);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(P2PSegmentManagerTest, UnmountDuplicate) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment();
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    ASSERT_TRUE(mgr.UnmountSegment(seg.id).has_value());

    auto res = mgr.UnmountSegment(seg.id);
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

// ============================================================
// GetSegments / QuerySegment
// ============================================================

TEST_F(P2PSegmentManagerTest, GetSegmentsAfterMount) {
    P2PSegmentManager mgr;
    auto seg1 = MakeSegment({1, 1}, "seg1", 1000);
    auto seg2 = MakeSegment({2, 2}, "seg2", 2000);
    ASSERT_TRUE(mgr.MountSegment(seg1).has_value());
    ASSERT_TRUE(mgr.MountSegment(seg2).has_value());

    auto res = mgr.GetSegments();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().size(), 2);
}

TEST_F(P2PSegmentManagerTest, QuerySegmentByName) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "my_seg", 4096);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.QuerySegments("my_seg");
    ASSERT_TRUE(res.has_value());
    auto [used, capacity] = res.value();
    EXPECT_EQ(capacity, 4096);
}

TEST_F(P2PSegmentManagerTest, QueryNonexistentSegment) {
    P2PSegmentManager mgr;
    auto res = mgr.QuerySegments("nonexistent");
    EXPECT_FALSE(res.has_value());
    EXPECT_EQ(res.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

// ============================================================
// UpdateSegmentUsage
// ============================================================

TEST_F(P2PSegmentManagerTest, UpdateSegmentUsage) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "seg1", 10000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    auto res = mgr.UpdateSegmentUsage({1, 1}, 5000);
    ASSERT_TRUE(res.has_value());
    // Returns old usage
    EXPECT_EQ(res.value(), 0);

    // Verify new usage
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 5000);
}

TEST_F(P2PSegmentManagerTest, UpdateSegmentUsageMultipleTimes) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "seg1", 10000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    mgr.UpdateSegmentUsage({1, 1}, 1000);
    auto res = mgr.UpdateSegmentUsage({1, 1}, 3000);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), 1000);  // old usage
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 3000);
}

TEST_F(P2PSegmentManagerTest, GetSegmentUsageDefault) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "seg1", 10000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    // Default usage should be 0
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 0);
}

// ============================================================
// ForEachSegment
// ============================================================

TEST_F(P2PSegmentManagerTest, ForEachSegmentVisitsAll) {
    P2PSegmentManager mgr;
    for (int i = 0; i < 5; i++) {
        auto seg = MakeSegment(
            {static_cast<uint64_t>(i), 0},
            "seg_" + std::to_string(i), 1024);
        ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    }

    int count = 0;
    mgr.ForEachSegment([&count](const Segment& seg) -> bool {
        count++;
        return false;  // continue
    });
    EXPECT_EQ(count, 5);
}

TEST_F(P2PSegmentManagerTest, ForEachSegmentEmpty) {
    P2PSegmentManager mgr;

    int count = 0;
    mgr.ForEachSegment([&count](const Segment& seg) -> bool {
        count++;
        return false;
    });
    EXPECT_EQ(count, 0);
}

TEST_F(P2PSegmentManagerTest, ForEachSegmentEarlyStop) {
    P2PSegmentManager mgr;
    for (int i = 0; i < 10; i++) {
        auto seg = MakeSegment(
            {static_cast<uint64_t>(i), 0},
            "seg_" + std::to_string(i), 1024);
        ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    }

    int count = 0;
    mgr.ForEachSegment([&count](const Segment& seg) -> bool {
        count++;
        return count >= 3;  // stop after 3
    });
    EXPECT_EQ(count, 3);
}

// ============================================================
// Callbacks
// ============================================================

TEST_F(P2PSegmentManagerTest, MountCallbackTriggered) {
    P2PSegmentManager mgr;

    int add_count = 0;
    int remove_count = 0;
    mgr.SetSegmentChangeCallbacks(
        [&add_count](const Segment& seg) { add_count++; },
        [&remove_count](const Segment& seg) { remove_count++; });

    auto seg = MakeSegment();
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    EXPECT_EQ(add_count, 1);
    EXPECT_EQ(remove_count, 0);
}

TEST_F(P2PSegmentManagerTest, UnmountCallbackTriggered) {
    P2PSegmentManager mgr;

    int add_count = 0;
    int remove_count = 0;
    mgr.SetSegmentChangeCallbacks(
        [&add_count](const Segment& seg) { add_count++; },
        [&remove_count](const Segment& seg) { remove_count++; });

    auto seg = MakeSegment();
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());
    ASSERT_TRUE(mgr.UnmountSegment(seg.id).has_value());

    EXPECT_EQ(add_count, 1);
    EXPECT_EQ(remove_count, 1);
}

// ============================================================
// Boundary values
// ============================================================

TEST_F(P2PSegmentManagerTest, UsageZeroAndMax) {
    P2PSegmentManager mgr;
    auto seg = MakeSegment({1, 1}, "seg1", SIZE_MAX);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    mgr.UpdateSegmentUsage({1, 1}, 0);
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), 0);

    mgr.UpdateSegmentUsage({1, 1}, SIZE_MAX);
    EXPECT_EQ(mgr.GetSegmentUsage({1, 1}), SIZE_MAX);
}

// ============================================================
// Concurrency Tests
// ============================================================

TEST_F(P2PSegmentManagerTest, ConcurrentMountAndUnmount) {
    P2PSegmentManager mgr;
    constexpr int kNumThreads = 16;
    std::vector<std::thread> threads;
    std::atomic<int> success_count{0};

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&mgr, &success_count, i]() {
            auto seg = Segment();
            seg.id = {static_cast<uint64_t>(i + 100), 0};
            seg.name = "concurrent_seg_" + std::to_string(i);
            seg.size = 1024 * 1024;
            seg.extra = P2PSegmentExtraData{
                .priority = 1,
                .tags = {},
                .memory_type = MemoryType::DRAM,
                .usage = 0,
            };

            auto mount_result = mgr.MountSegment(seg);
            ASSERT_TRUE(mount_result.has_value());
            success_count.fetch_add(1);

            auto unmount_result = mgr.UnmountSegment(seg.id);
            ASSERT_TRUE(unmount_result.has_value());
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count, kNumThreads);

    // All segments should be unmounted
    auto res = mgr.GetSegments();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value().size(), 0);
}

TEST_F(P2PSegmentManagerTest, ConcurrentUpdateSegmentUsage) {
    P2PSegmentManager mgr;

    // Mount a single segment
    auto seg = MakeSegment({1, 1}, "shared_seg", 1000000);
    ASSERT_TRUE(mgr.MountSegment(seg).has_value());

    constexpr int kNumThreads = 8;
    constexpr int kIterations = 100;
    std::vector<std::thread> threads;

    for (int i = 0; i < kNumThreads; i++) {
        threads.emplace_back([&mgr, i]() {
            for (int j = 0; j < kIterations; j++) {
                size_t new_usage = static_cast<size_t>(i * 1000 + j);
                auto res = mgr.UpdateSegmentUsage({1, 1}, new_usage);
                // Should always succeed (segment exists)
                ASSERT_TRUE(res.has_value());
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Final usage should be a valid value (no corruption)
    size_t final_usage = mgr.GetSegmentUsage({1, 1});
    EXPECT_GE(final_usage, 0);
    EXPECT_LE(final_usage,
              static_cast<size_t>((kNumThreads - 1) * 1000 + kIterations - 1));
}

}  // namespace mooncake
