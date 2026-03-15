#include <gtest/gtest.h>
#include "tiered_cache/scheduler/lru_policy.h"

namespace mooncake {

class LRUPolicyTest : public ::testing::Test {
   protected:
    UUID fast_tier_id_ = {100, 1};
    UUID slow_tier_id_ = {200, 2};

    std::unordered_map<UUID, TierStats> MakeTierStats(
        size_t fast_used, size_t fast_total,
        size_t slow_used = 0, size_t slow_total = 100 * 1024 * 1024) {
        std::unordered_map<UUID, TierStats> stats;
        stats[fast_tier_id_] = {fast_total, fast_used};
        stats[slow_tier_id_] = {slow_total, slow_used};
        return stats;
    }

    KeyContext MakeKey(const std::string& key, double heat,
                       std::vector<UUID> locations, size_t size = 1024) {
        return KeyContext{key, heat, std::move(locations), size};
    }
};

// ============================================================
// Basic edge cases
// ============================================================

// No fast tier set → empty actions
TEST_F(LRUPolicyTest, NoFastTierReturnsEmpty) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    auto stats = MakeTierStats(9000, 10000);
    std::vector<KeyContext> keys = {MakeKey("k1", 10.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Empty active_keys → empty actions
TEST_F(LRUPolicyTest, EmptyActiveKeysReturnsEmpty) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats(9500, 10000);
    std::vector<KeyContext> keys;

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Fast tier stats missing → empty actions
TEST_F(LRUPolicyTest, MissingFastTierStatsReturnsEmpty) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // Only slow tier stats provided
    std::unordered_map<UUID, TierStats> stats;
    stats[slow_tier_id_] = {100 * 1024 * 1024, 0};

    std::vector<KeyContext> keys = {MakeKey("k1", 10.0, {slow_tier_id_})};
    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Zero capacity → empty actions
TEST_F(LRUPolicyTest, ZeroCapacityReturnsEmpty) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats(0, 0);
    std::vector<KeyContext> keys = {MakeKey("k1", 10.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// ============================================================
// Within watermark range → no scheduling
// ============================================================

TEST_F(LRUPolicyTest, WithinWatermarkRangeNoAction) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // Usage at 80% (between 70% and 90%)
    auto stats = MakeTierStats(8000, 10000);
    std::vector<KeyContext> keys = {
        MakeKey("k1", 10.0, {fast_tier_id_}, 4000),
        MakeKey("k2", 5.0, {fast_tier_id_}, 4000),
    };

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Exactly at high watermark → no scheduling
TEST_F(LRUPolicyTest, ExactlyAtHighWatermarkNoAction) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // Usage exactly at 90%
    auto stats = MakeTierStats(9000, 10000);
    std::vector<KeyContext> keys = {
        MakeKey("k1", 10.0, {fast_tier_id_}, 9000),
    };

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// ============================================================
// Above high watermark → eviction
// ============================================================

TEST_F(LRUPolicyTest, AboveHighWatermarkEvictsColdKeys) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // Usage at 95% (10000 total, 9500 used)
    // Target: 70% = 7000 bytes
    // Keys sorted by heat (hottest first assumed by caller):
    auto stats = MakeTierStats(9500, 10000);
    std::vector<KeyContext> keys = {
        MakeKey("hot1", 100.0, {fast_tier_id_}, 3000),   // should stay
        MakeKey("hot2", 50.0, {fast_tier_id_}, 3000),    // should stay (6000)
        MakeKey("warm", 20.0, {fast_tier_id_}, 1000),    // fits (7000 = target)
        MakeKey("cold1", 5.0, {fast_tier_id_}, 1000),    // should be evicted
        MakeKey("cold2", 1.0, {fast_tier_id_}, 1500),    // should be evicted
    };

    auto actions = policy.Decide(stats, keys);
    ASSERT_GE(actions.size(), 2);

    // cold1 and cold2 should be evicted/migrated
    bool cold1_acted = false, cold2_acted = false;
    for (const auto& action : actions) {
        if (action.key == "cold1") cold1_acted = true;
        if (action.key == "cold2") cold2_acted = true;
        // All eviction sources should be from fast tier
        EXPECT_EQ(action.source_tier_id, fast_tier_id_);
    }
    EXPECT_TRUE(cold1_acted);
    EXPECT_TRUE(cold2_acted);
}

// Eviction with multiple replicas → EVICT (not MIGRATE)
TEST_F(LRUPolicyTest, MultiReplicaEviction) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // 95% usage
    auto stats = MakeTierStats(9500, 10000);
    std::vector<KeyContext> keys = {
        MakeKey("hot", 100.0, {fast_tier_id_}, 7000),
        // cold key has copies in both tiers
        MakeKey("cold", 1.0, {fast_tier_id_, slow_tier_id_}, 2500),
    };

    auto actions = policy.Decide(stats, keys);
    ASSERT_GE(actions.size(), 1);

    // Find the cold key action - should be EVICT since it has other copy
    for (const auto& action : actions) {
        if (action.key == "cold") {
            EXPECT_EQ(action.type, SchedAction::Type::EVICT);
            EXPECT_EQ(action.source_tier_id, fast_tier_id_);
        }
    }
}

// ============================================================
// Below low watermark → promotion
// ============================================================

TEST_F(LRUPolicyTest, BelowLowWatermarkPromotesHotKeys) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // Usage at 50% (below low_watermark 70%)
    // Target: 70% = 7000 bytes
    auto stats = MakeTierStats(5000, 10000);
    std::vector<KeyContext> keys = {
        MakeKey("hot1", 100.0, {fast_tier_id_}, 3000),   // already in fast
        MakeKey("hot2", 50.0, {fast_tier_id_}, 2000),    // already in fast
        MakeKey("promote1", 30.0, {slow_tier_id_}, 1000),  // should promote
        MakeKey("promote2", 20.0, {slow_tier_id_}, 1000),  // should promote
        MakeKey("stay_slow", 1.0, {slow_tier_id_}, 5000),  // too big
    };

    auto actions = policy.Decide(stats, keys);

    // At least promote1 and promote2 should have MIGRATE actions
    bool promote1_found = false, promote2_found = false;
    for (const auto& action : actions) {
        if (action.key == "promote1") {
            EXPECT_EQ(action.type, SchedAction::Type::MIGRATE);
            EXPECT_EQ(action.target_tier_id, fast_tier_id_);
            EXPECT_EQ(action.source_tier_id, slow_tier_id_);
            promote1_found = true;
        }
        if (action.key == "promote2") {
            EXPECT_EQ(action.type, SchedAction::Type::MIGRATE);
            EXPECT_EQ(action.target_tier_id, fast_tier_id_);
            promote2_found = true;
        }
    }
    EXPECT_TRUE(promote1_found);
    EXPECT_TRUE(promote2_found);
}

// ============================================================
// Single tier configuration
// ============================================================

TEST_F(LRUPolicyTest, SingleTierEvictsWhenFull) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // Only fast tier stats, no slow tier
    std::unordered_map<UUID, TierStats> stats;
    stats[fast_tier_id_] = {10000, 9500};  // 95% used

    std::vector<KeyContext> keys = {
        MakeKey("hot", 100.0, {fast_tier_id_}, 7000),
        MakeKey("cold", 1.0, {fast_tier_id_}, 2500),  // single copy
    };

    auto actions = policy.Decide(stats, keys);
    // Cold key should be EVICT (no slow tier for MIGRATE)
    bool cold_evicted = false;
    for (const auto& action : actions) {
        if (action.key == "cold") {
            EXPECT_EQ(action.type, SchedAction::Type::EVICT);
            cold_evicted = true;
        }
    }
    EXPECT_TRUE(cold_evicted);
}

// ============================================================
// Different watermark configurations
// ============================================================

TEST_F(LRUPolicyTest, TightWatermarks) {
    // Very tight watermarks: 0.5 high, 0.3 low
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.5,
                                        .low_watermark = 0.3});
    policy.SetFastTier(fast_tier_id_);

    // 60% usage (above 50% high watermark)
    auto stats = MakeTierStats(6000, 10000);
    // Target: 30% = 3000 bytes
    std::vector<KeyContext> keys = {
        MakeKey("hot", 100.0, {fast_tier_id_}, 3000),
        MakeKey("cold1", 5.0, {fast_tier_id_}, 1500),
        MakeKey("cold2", 2.0, {fast_tier_id_}, 1500),
    };

    auto actions = policy.Decide(stats, keys);
    // cold1 and cold2 should be evicted/migrated
    ASSERT_GE(actions.size(), 2);
}

TEST_F(LRUPolicyTest, WideWatermarks) {
    // Wide watermarks: 0.95 high, 0.1 low
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.95,
                                        .low_watermark = 0.1});
    policy.SetFastTier(fast_tier_id_);

    // 90% usage (between 10% and 95%) — no action
    auto stats = MakeTierStats(9000, 10000);
    std::vector<KeyContext> keys = {
        MakeKey("k1", 100.0, {fast_tier_id_}, 9000),
    };

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// ============================================================
// Mixed scenario: evict cold + promote hot
// ============================================================

TEST_F(LRUPolicyTest, EvictAndPromoteSimultaneously) {
    LRUPolicy policy(LRUPolicy::Config{.high_watermark = 0.9,
                                        .low_watermark = 0.7});
    policy.SetFastTier(fast_tier_id_);

    // 95% usage, above high watermark
    // Target: 70% = 7000 bytes
    auto stats = MakeTierStats(9500, 10000);
    std::vector<KeyContext> keys = {
        // Hottest: in slow, should be promoted
        MakeKey("super_hot", 200.0, {slow_tier_id_}, 3000),
        // In fast, should stay
        MakeKey("warm_in_fast", 50.0, {fast_tier_id_}, 3000),
        // In fast, fits in target
        MakeKey("ok_in_fast", 30.0, {fast_tier_id_}, 1000),
        // In fast, cold — should be evicted
        MakeKey("cold_in_fast", 1.0, {fast_tier_id_}, 2500),
    };

    auto actions = policy.Decide(stats, keys);
    ASSERT_GE(actions.size(), 2);

    bool promoted = false, evicted = false;
    for (const auto& action : actions) {
        if (action.key == "super_hot" &&
            action.type == SchedAction::Type::MIGRATE &&
            action.target_tier_id == fast_tier_id_) {
            promoted = true;
        }
        if (action.key == "cold_in_fast") {
            evicted = true;
        }
    }
    EXPECT_TRUE(promoted) << "super_hot should be promoted to fast tier";
    EXPECT_TRUE(evicted) << "cold_in_fast should be evicted";
}

}  // namespace mooncake
