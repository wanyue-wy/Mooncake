#include <gtest/gtest.h>
#include "tiered_cache/scheduler/simple_policy.h"

namespace mooncake {

class SimplePolicyTest : public ::testing::Test {
   protected:
    UUID fast_tier_id_ = {100, 1};
    UUID slow_tier_id_ = {200, 2};

    std::unordered_map<UUID, TierStats> MakeTierStats(
        size_t fast_used = 0, size_t fast_total = 1024 * 1024,
        size_t slow_used = 0, size_t slow_total = 10 * 1024 * 1024) {
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

// No fast tier set → empty actions
TEST_F(SimplePolicyTest, NoFastTierReturnsEmpty) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    // Do NOT call SetFastTier

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {MakeKey("k1", 20.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Empty active_keys → empty actions
TEST_F(SimplePolicyTest, EmptyActiveKeysReturnsEmpty) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys;

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Key heat below threshold → no promotion
TEST_F(SimplePolicyTest, BelowThresholdNoPromotion) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 10.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {MakeKey("k1", 5.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Key heat above threshold → MIGRATE action generated
TEST_F(SimplePolicyTest, AboveThresholdPromotes) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {MakeKey("k1", 15.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    ASSERT_EQ(actions.size(), 1);
    EXPECT_EQ(actions[0].type, SchedAction::Type::MIGRATE);
    EXPECT_EQ(actions[0].key, "k1");
    EXPECT_EQ(actions[0].source_tier_id, slow_tier_id_);
    EXPECT_EQ(actions[0].target_tier_id, fast_tier_id_);
}

// Key already in fast tier → no action
TEST_F(SimplePolicyTest, AlreadyInTargetTierNoAction) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {MakeKey("k1", 20.0, {fast_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Key in both tiers → already in target, no action
TEST_F(SimplePolicyTest, InBothTiersNoAction) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {
        MakeKey("k1", 20.0, {slow_tier_id_, fast_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Key with no current locations → no action
TEST_F(SimplePolicyTest, NoLocationsNoAction) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {MakeKey("k1", 20.0, {})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Fast tier stats missing → empty actions
TEST_F(SimplePolicyTest, MissingFastTierStatsReturnsEmpty) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    policy.SetFastTier(fast_tier_id_);

    // Only provide slow tier stats, no fast tier stats
    std::unordered_map<UUID, TierStats> stats;
    stats[slow_tier_id_] = {10 * 1024 * 1024, 0};

    std::vector<KeyContext> keys = {MakeKey("k1", 20.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

// Multiple keys: only hot keys get promoted
TEST_F(SimplePolicyTest, MultipleKeysMixedHeat) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 10.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {
        MakeKey("cold1", 3.0, {slow_tier_id_}),
        MakeKey("hot1", 15.0, {slow_tier_id_}),
        MakeKey("cold2", 8.0, {slow_tier_id_}),
        MakeKey("hot2", 25.0, {slow_tier_id_}),
        MakeKey("warm", 10.0, {slow_tier_id_}),  // exactly at threshold
    };

    auto actions = policy.Decide(stats, keys);
    ASSERT_EQ(actions.size(), 3);  // hot1, hot2, warm (>= threshold)

    // Verify all actions are MIGRATE to fast tier
    for (const auto& action : actions) {
        EXPECT_EQ(action.type, SchedAction::Type::MIGRATE);
        EXPECT_EQ(action.target_tier_id, fast_tier_id_);
    }
}

// Different threshold configurations
TEST_F(SimplePolicyTest, VeryHighThreshold) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 1000.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {MakeKey("k1", 999.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    EXPECT_TRUE(actions.empty());
}

TEST_F(SimplePolicyTest, ZeroThreshold) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 0.0});
    policy.SetFastTier(fast_tier_id_);

    auto stats = MakeTierStats();
    std::vector<KeyContext> keys = {MakeKey("k1", 0.0, {slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    // heat_score 0.0 < threshold 0.0 is false, so it should be promoted
    // Actually 0.0 < 0.0 is false so the >= check passes
    ASSERT_EQ(actions.size(), 1);
}

// Source tier is picked from first location
TEST_F(SimplePolicyTest, SourceTierIsFirstLocation) {
    SimplePolicy policy(SimplePolicy::Config{.promotion_threshold = 5.0});
    policy.SetFastTier(fast_tier_id_);

    UUID another_tier = {300, 3};
    auto stats = MakeTierStats();
    stats[another_tier] = {10 * 1024 * 1024, 0};

    std::vector<KeyContext> keys = {
        MakeKey("k1", 20.0, {another_tier, slow_tier_id_})};

    auto actions = policy.Decide(stats, keys);
    ASSERT_EQ(actions.size(), 1);
    EXPECT_EQ(actions[0].source_tier_id, another_tier);
}

}  // namespace mooncake
