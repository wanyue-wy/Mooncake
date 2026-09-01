#include <gtest/gtest.h>

#include <type_traits>
#include <ylt/struct_pack.hpp>
#include <ylt/util/utils.hpp>

#include "p2p/master/p2p_rpc_service.h"

namespace mooncake {
namespace {

static_assert(coro_rpc::func_id<&P2PMasterRpcService::RegisterClient>() ==
              554672570u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::UnregisterClient>() ==
              3195950100u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::Heartbeat>() ==
              1220160476u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::QueryClientStatus>() ==
              2240222209u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::MountSegment>() ==
              1836414514u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::UnmountSegment>() ==
              1378916896u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::RouteExists>() ==
              2171247821u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchRouteExists>() ==
              352322762u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::GetReadRoute>() ==
              4226816141u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchGetReadRoute>() ==
              1029360511u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::GetWriteRoute>() ==
              3979753808u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchGetWriteRoute>() ==
              3935710653u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::PublishRoute>() ==
              4114711953u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::WithdrawRoute>() ==
              1498064915u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchWithdrawRoute>() ==
              1805314735u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::BatchSyncRoutes>() ==
              3376879306u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::CompleteRouteSync>() ==
              1587901567u);
static_assert(coro_rpc::func_id<&P2PMasterRpcService::ServiceReady>() ==
              1460324940u);
static_assert(
    coro_rpc::func_id<&P2PMasterRpcService::HeartbeatServiceReady>() ==
    1980024971u);

template <typename T>
T RoundTrip(const T& input) {
    const auto bytes = struct_pack::serialize<std::string>(input);
    auto result = struct_pack::deserialize<T>(bytes);
    EXPECT_TRUE(result.has_value());
    return result.has_value() ? std::move(*result) : T{};
}

TEST(P2PRpcProtocolTest, ReadRouteDtoRoundTrips) {
    P2PBatchGetReadRouteResponse input;
    input.responses = {{.routes = {{.client_id = {1, 2},
                                    .segment_id = {3, 4},
                                    .ip_address = "10.0.0.1",
                                    .rpc_port = 5001,
                                    .object_size = 4096}}}};
    input.error_codes = {ErrorCode::OK};
    auto output = RoundTrip(input);
    ASSERT_EQ(output.responses.size(), 1);
    ASSERT_EQ(output.responses[0].routes.size(), 1);
    EXPECT_EQ(output.responses[0].routes[0].client_id, (UUID{1, 2}));
    EXPECT_EQ(output.responses[0].routes[0].segment_id, (UUID{3, 4}));
    EXPECT_EQ(output.responses[0].routes[0].object_size, 4096);
    EXPECT_EQ(output.error_codes, input.error_codes);
}

TEST(P2PRpcProtocolTest, BatchMutationDtoOwnsKeysAndRoundTrips) {
    static_assert(std::is_same_v<
                  typename decltype(P2PBatchSyncRoutesRequest::publish_keys)::
                      value_type,
                  std::string>);
    P2PBatchSyncRoutesRequest input;
    input.client_id = {1, 2};
    input.publish_keys = {"publish"};
    input.publish_sizes = {1024};
    input.publish_segment_ids = {{3, 4}};
    input.withdraw_keys = {"withdraw"};
    input.withdraw_segment_ids = {{5, 6}};
    auto output = RoundTrip(input);
    EXPECT_EQ(output.client_id, input.client_id);
    EXPECT_EQ(output.publish_keys, input.publish_keys);
    EXPECT_EQ(output.publish_sizes, input.publish_sizes);
    EXPECT_EQ(output.publish_segment_ids, input.publish_segment_ids);
    EXPECT_EQ(output.withdraw_keys, input.withdraw_keys);
    EXPECT_EQ(output.withdraw_segment_ids, input.withdraw_segment_ids);
}

}  // namespace
}  // namespace mooncake
