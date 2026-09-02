#include <gtest/gtest.h>

#include <functional>
#include <string>
#include <vector>

#include "p2p/master/p2p_route_table.h"

namespace mooncake {
namespace {

const UUID kClientA{1, 1};
const UUID kClientB{2, 2};
const UUID kSharedSegment{9, 9};

P2PRouteLocation Location(const UUID& client, const UUID& segment) {
    return P2PRouteLocation{.client_id = client, .segment_id = segment};
}

class TestBatchSyncHandler final : public P2PRouteTable::BatchSyncHandler {
   public:
    ErrorCode BeforePublish(
        const P2PPublishRouteOperation& operation) override {
        return before_publish ? before_publish(operation) : ErrorCode::OK;
    }

    void AfterPublish(size_t index,
                      const P2PPublishRouteOperation& operation,
                      const P2PRouteTable::Mutation& result) override {
        if (after_publish) {
            after_publish(index, operation, result);
        }
    }

    ErrorCode BeforeWithdraw(
        const P2PWithdrawRouteOperation& operation) override {
        return before_withdraw ? before_withdraw(operation) : ErrorCode::OK;
    }

    void AfterWithdraw(size_t index,
                       const P2PWithdrawRouteOperation& operation,
                       const P2PRouteTable::Mutation& result) override {
        if (after_withdraw) {
            after_withdraw(index, operation, result);
        }
    }

    std::function<ErrorCode(const P2PPublishRouteOperation&)> before_publish;
    std::function<void(size_t, const P2PPublishRouteOperation&,
                       const P2PRouteTable::Mutation&)>
        after_publish;
    std::function<ErrorCode(const P2PWithdrawRouteOperation&)>
        before_withdraw;
    std::function<void(size_t, const P2PWithdrawRouteOperation&,
                       const P2PRouteTable::Mutation&)>
        after_withdraw;
};

TEST(P2PRouteTableTest, PublishAppendAndWithdrawLastRoute) {
    P2PRouteTable table;
    const auto location_a = Location(kClientA, UUID{11, 11});
    const auto location_b = Location(kClientB, UUID{12, 12});

    auto first = table.Publish("key", 1024, location_a);
    ASSERT_TRUE(first.has_value());
    EXPECT_TRUE(first->created_key);
    EXPECT_EQ(table.GetRouteKeyCount(), 1);

    auto second = table.Publish("key", 1024, location_b);
    ASSERT_TRUE(second.has_value());
    EXPECT_FALSE(second->created_key);
    auto route = table.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->object_size, 1024);
    EXPECT_EQ(route->locations.size(), 2);

    auto remove_first = table.Withdraw("key", location_a);
    ASSERT_TRUE(remove_first.has_value());
    EXPECT_FALSE(remove_first->removed_key);
    auto remove_last = table.Withdraw("key", location_b);
    ASSERT_TRUE(remove_last.has_value());
    EXPECT_TRUE(remove_last->removed_key);
    EXPECT_FALSE(table.RouteExists("key"));
    EXPECT_EQ(table.GetRouteKeyCount(), 0);
}

TEST(P2PRouteTableTest, RejectsInvalidSizeAndDuplicateLocation) {
    P2PRouteTable table;
    const auto location = Location(kClientA, UUID{11, 11});

    auto zero_size = table.Publish("key", 0, location);
    ASSERT_FALSE(zero_size.has_value());
    EXPECT_EQ(zero_size.error(), ErrorCode::INVALID_PARAMS);

    ASSERT_TRUE(table.Publish("key", 1024, location).has_value());
    auto duplicate = table.Publish("key", 1024, location);
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::REPLICA_ALREADY_EXISTS);

    auto size_mismatch =
        table.Publish("key", 2048, Location(kClientB, UUID{12, 12}));
    ASSERT_FALSE(size_mismatch.has_value());
    EXPECT_EQ(size_mismatch.error(), ErrorCode::INVALID_PARAMS);
}

TEST(P2PRouteTableTest, CountsUniqueClientsForRouteLimit) {
    P2PRouteTable table(/*max_client_per_key=*/1);
    ASSERT_TRUE(
        table.Publish("key", 1024, Location(kClientA, UUID{11, 11}))
            .has_value());
    EXPECT_TRUE(
        table.Publish("key", 1024, Location(kClientA, UUID{12, 12}))
            .has_value());

    auto second_client =
        table.Publish("key", 1024, Location(kClientB, UUID{13, 13}));
    ASSERT_FALSE(second_client.has_value());
    EXPECT_EQ(second_client.error(), ErrorCode::REPLICA_NUM_EXCEEDED);
}

TEST(P2PRouteTableTest, BatchSyncPreservesResultsAndMutationOrder) {
    P2PRouteTable table;
    const UUID segment_id{11, 11};
    const std::vector<P2PPublishRouteOperation> publishes{
        {.key = "key", .object_size = 1024, .segment_id = segment_id},
        {.key = "key", .object_size = 1024, .segment_id = segment_id},
    };
    const std::vector<P2PWithdrawRouteOperation> withdrawals{
        {.key = "key", .segment_id = segment_id},
        {.key = "missing", .segment_id = segment_id},
    };
    std::vector<ErrorCode> publish_results(publishes.size(), ErrorCode::OK);
    std::vector<ErrorCode> withdraw_results(withdrawals.size(), ErrorCode::OK);
    std::vector<std::string> hooks;

    TestBatchSyncHandler handler;
    handler.after_publish =
        [&](size_t index, const P2PPublishRouteOperation& operation,
            const P2PRouteTable::Mutation& result) {
            publish_results[index] =
                result.has_value() ? ErrorCode::OK : result.error();
            if (result.has_value()) {
                hooks.push_back("publish:" + operation.key);
            }
        };
    handler.before_withdraw =
        [&](const P2PWithdrawRouteOperation& operation) {
            hooks.push_back("withdraw:" + operation.key);
            return ErrorCode::OK;
        };
    handler.after_withdraw =
        [&](size_t index, const P2PWithdrawRouteOperation&,
            const P2PRouteTable::Mutation& result) {
            withdraw_results[index] =
                result.has_value() ? ErrorCode::OK : result.error();
        };

    table.BatchSync(kClientA, publishes, withdrawals, handler);

    EXPECT_EQ(publish_results,
              (std::vector<ErrorCode>{ErrorCode::OK,
                                      ErrorCode::REPLICA_ALREADY_EXISTS}));
    EXPECT_EQ(withdraw_results,
              (std::vector<ErrorCode>{ErrorCode::OK,
                                      ErrorCode::OBJECT_NOT_FOUND}));
    EXPECT_EQ(hooks,
              (std::vector<std::string>{"publish:key", "withdraw:key"}));
    EXPECT_FALSE(table.RouteExists("key"));
}

TEST(P2PRouteTableTest, BatchWithdrawCallbackFailureKeepsRoute) {
    P2PRouteTable table;
    const UUID segment_id{11, 11};
    const auto location = Location(kClientA, segment_id);
    ASSERT_TRUE(table.Publish("key", 1024, location).has_value());
    const std::vector<P2PWithdrawRouteOperation> withdrawals{
        {.key = "key", .segment_id = segment_id},
    };
    ErrorCode result = ErrorCode::OK;

    TestBatchSyncHandler handler;
    handler.before_withdraw = [](const P2PWithdrawRouteOperation&) {
        return ErrorCode::INTERNAL_ERROR;
    };
    handler.after_withdraw =
        [&](size_t, const P2PWithdrawRouteOperation&,
            const P2PRouteTable::Mutation& mutation) {
            ASSERT_FALSE(mutation.has_value());
            result = mutation.error();
        };

    table.BatchWithdraw(kClientA, withdrawals, handler);

    EXPECT_EQ(result, ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(table.RouteExists("key"));
}

TEST(P2PRouteTableTest, BatchPublishCallbackFailureDoesNotCreateRoute) {
    P2PRouteTable table;
    const std::vector<P2PPublishRouteOperation> publishes{
        {.key = "key", .object_size = 1024, .segment_id = UUID{11, 11}},
    };
    ErrorCode result = ErrorCode::OK;

    TestBatchSyncHandler handler;
    handler.before_publish = [](const P2PPublishRouteOperation&) {
        return ErrorCode::SEGMENT_NOT_FOUND;
    };
    handler.after_publish =
        [&](size_t, const P2PPublishRouteOperation&,
            const P2PRouteTable::Mutation& mutation) {
            ASSERT_FALSE(mutation.has_value());
            result = mutation.error();
        };

    table.BatchPublish(kClientA, publishes, handler);

    EXPECT_EQ(result, ErrorCode::SEGMENT_NOT_FOUND);
    EXPECT_FALSE(table.RouteExists("key"));
}

TEST(P2PRouteTableTest, CleanupUsesClientAndSegmentIdentity) {
    P2PRouteTable table;
    const auto location_a = Location(kClientA, kSharedSegment);
    const auto location_b = Location(kClientB, kSharedSegment);
    ASSERT_TRUE(table.Publish("shared", 1024, location_a).has_value());
    ASSERT_TRUE(table.Publish("shared", 1024, location_b).has_value());
    ASSERT_TRUE(table.Publish("only-a", 1024, location_a).has_value());

    auto cleanup = table.RemoveLocation(location_a);
    EXPECT_EQ(cleanup.removed_routes, 2);
    ASSERT_EQ(cleanup.removed_keys.size(), 1);
    EXPECT_EQ(cleanup.removed_keys.front(), "only-a");

    auto shared = table.GetRoute("shared");
    ASSERT_TRUE(shared.has_value());
    ASSERT_EQ(shared->locations.size(), 1);
    EXPECT_EQ(shared->locations.front(), location_b);
}

TEST(P2PRouteTableTest, RepeatedCleanupLeavesNoDanglingReverseKeys) {
    P2PRouteTable table;
    const auto location = Location(kClientA, UUID{11, 11});

    for (size_t i = 0; i < 2000; ++i) {
        std::string key = "route-" + std::to_string(i);
        ASSERT_TRUE(table.Publish(key, i + 1, location).has_value());
    }
    EXPECT_EQ(table.GetRouteKeyCount(), 2000);

    auto cleanup = table.RemoveLocation(location);
    EXPECT_EQ(cleanup.removed_routes, 2000);
    EXPECT_EQ(cleanup.removed_keys.size(), 2000);
    EXPECT_EQ(table.GetRouteKeyCount(), 0);

    auto second_cleanup = table.RemoveLocation(location);
    EXPECT_EQ(second_cleanup.removed_routes, 0);
    EXPECT_TRUE(second_cleanup.removed_keys.empty());
}

}  // namespace
}  // namespace mooncake
