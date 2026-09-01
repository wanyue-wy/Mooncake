#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#define private public
#include "p2p/master/p2p_client_meta.h"
#undef private

namespace mooncake {
namespace {

P2PSegment Segment(UUID id = {1, 1}, std::string name = "segment",
                   size_t size = 4096, int priority = 1,
                   std::vector<std::string> tags = {}, size_t usage = 0) {
    return P2PSegment{.id = id,
                      .name = std::move(name),
                      .size = size,
                      .priority = priority,
                      .tags = std::move(tags),
                      .memory_type = MemoryType::DRAM,
                      .usage = usage};
}

std::shared_ptr<P2PClientMeta> Client(
    UUID id = {10, 10}, int64_t disconnect_timeout_sec = 2,
    int64_t crash_timeout_sec = 5) {
    return std::make_shared<P2PClientMeta>(
        id, "127.0.0.1", 50051, disconnect_timeout_sec, crash_timeout_sec);
}

TEST(P2PClientMetaTest, OwnsSegmentSnapshots) {
    auto client = Client();
    auto segment = Segment();
    ASSERT_TRUE(client->MountSegment(segment).has_value());

    segment.name = "caller-change";
    auto stored = client->QuerySegment({1, 1});
    ASSERT_TRUE(stored.has_value());
    EXPECT_EQ(stored->name, "segment");
    ASSERT_EQ(client->GetSegments().size(), 1);

    ASSERT_TRUE(client->UnmountSegment({1, 1}).has_value());
    EXPECT_TRUE(client->GetSegments().empty());
}

TEST(P2PClientMetaTest, TimeoutsArePerClient) {
    auto fast = Client({1, 1}, 1, 2);
    auto slow = Client({2, 2}, 10, 20);
    const auto old_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(3);
    fast->health_state_.last_heartbeat = old_heartbeat;
    slow->health_state_.last_heartbeat = old_heartbeat;

    EXPECT_EQ(fast->CheckHealth().second, P2PClientStatus::CRASHED);
    EXPECT_EQ(slow->CheckHealth().second, P2PClientStatus::HEALTHY);
}

TEST(P2PClientMetaTest, HeartbeatRecoversDisconnectedClient) {
    auto client = Client({1, 1}, 1, 5);
    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(2);
    EXPECT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTED);

    auto transition = client->Heartbeat();
    EXPECT_EQ(transition.first, P2PClientStatus::DISCONNECTED);
    EXPECT_EQ(transition.second, P2PClientStatus::HEALTHY);
}

TEST(P2PClientMetaTest, CrashedClientDoesNotRecover) {
    auto client = Client({1, 1}, 1, 2);
    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(3);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::CRASHED);
    EXPECT_EQ(client->Heartbeat().second, P2PClientStatus::CRASHED);
}

TEST(P2PClientMetaTest, RecycleReturnsLocationsAfterSegmentLocksAreReleased) {
    auto client = Client({7, 7});
    ASSERT_TRUE(client->MountSegment(Segment({1, 1}, "a")).has_value());
    ASSERT_TRUE(client->MountSegment(Segment({2, 2}, "b")).has_value());

    auto locations = client->RecycleSegments();
    ASSERT_EQ(locations.size(), 2);
    for (const auto& location : locations) {
        EXPECT_EQ(location.client_id, (UUID{7, 7}));
    }
    EXPECT_TRUE(client->GetSegments().empty());
    EXPECT_TRUE(client->RecycleSegments().empty());
}

TEST(P2PClientMetaTest, ConcurrentRecycleOnlyRemovesSegmentsOnce) {
    auto client = Client({7, 7});
    for (uint64_t i = 0; i < 32; ++i) {
        ASSERT_TRUE(client->MountSegment(
                              Segment({i + 1, i + 1}, std::to_string(i)))
                        .has_value());
    }
    std::atomic<size_t> removed{0};
    std::vector<std::thread> workers;
    for (size_t i = 0; i < 8; ++i) {
        workers.emplace_back([&]() {
            removed.fetch_add(client->RecycleSegments().size(),
                              std::memory_order_relaxed);
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    EXPECT_EQ(removed.load(), 32);
    EXPECT_TRUE(client->GetSegments().empty());
}

TEST(P2PClientMetaTest, DerivesAvailableCapacityFromSegmentManager) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment({1, 1}, "a", 4096, 1, {}, 100))
                    .has_value());
    ASSERT_TRUE(client->MountSegment(Segment({2, 2}, "b", 8192, 1, {}, 200))
                    .has_value());
    EXPECT_EQ(client->GetAvailableCapacity(), 11988);

    TierUsageInfo update;
    update.segment_id = {1, 1};
    update.usage = 1000;
    auto result = client->UpdateSegmentUsages({update});
    ASSERT_EQ(result.sub_results.size(), 1);
    EXPECT_EQ(result.sub_results.front().error, ErrorCode::OK);
    EXPECT_EQ(client->GetAvailableCapacity(), 11088);
}

TEST(P2PClientMetaTest, UpdateUsageReportsPerSegmentErrors) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(Segment()).has_value());
    TierUsageInfo good{.segment_id = {1, 1}, .usage = 100};
    TierUsageInfo missing{.segment_id = {9, 9}, .usage = 100};
    auto result = client->UpdateSegmentUsages({good, missing});
    ASSERT_EQ(result.sub_results.size(), 2);
    EXPECT_EQ(result.sub_results[0].error, ErrorCode::OK);
    EXPECT_EQ(result.sub_results[1].error, ErrorCode::SEGMENT_NOT_FOUND);
}

TEST(P2PClientMetaTest, ScoresOnlyEligibleSegments) {
    auto client = Client();
    ASSERT_TRUE(client->MountSegment(
                          Segment({1, 1}, "slow", 1000, 1, {"cold"}, 100))
                    .has_value());
    ASSERT_TRUE(client->MountSegment(
                          Segment({2, 2}, "fast", 2000, 10, {"hot"}, 500))
                    .has_value());

    WriteRouteRequest request;
    request.client_id = client->get_client_id();
    request.size = 1000;
    request.config.top_tier_only = true;
    request.config.priority_limit = 5;
    auto candidate = client->GetWriteRouteCandidate(request);
    ASSERT_TRUE(candidate.has_value());
    EXPECT_EQ(candidate->available_capacity, 1500);
    EXPECT_DOUBLE_EQ(candidate->score, 0.75);

    request.config.tag_filters = {"hot"};
    EXPECT_FALSE(client->GetWriteRouteCandidate(request).has_value());
}

TEST(P2PClientMetaTest, UnhealthyClientIsNotWriteCandidate) {
    auto client = Client({1, 1}, 1, 5);
    ASSERT_TRUE(client->MountSegment(Segment()).has_value());
    client->health_state_.last_heartbeat =
        std::chrono::steady_clock::now() - std::chrono::seconds(2);
    ASSERT_EQ(client->CheckHealth().second, P2PClientStatus::DISCONNECTED);

    WriteRouteRequest request;
    request.size = 1;
    EXPECT_FALSE(client->GetWriteRouteCandidate(request).has_value());
}

}  // namespace
}  // namespace mooncake
