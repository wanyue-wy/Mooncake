#include <gtest/gtest.h>

#define private public
#include "p2p/master/p2p_master_service.h"
#undef private

namespace mooncake {
namespace {

P2PSegment Segment(UUID id, std::string name = "segment",
                   size_t size = 16 * 1024 * 1024, int priority = 1,
                   std::vector<std::string> tags = {}) {
    return P2PSegment{.id = id,
                      .name = std::move(name),
                      .size = size,
                      .priority = priority,
                      .tags = std::move(tags),
                      .memory_type = MemoryType::DRAM,
                      .usage = 0};
}

std::unique_ptr<P2PMasterService> Service(uint64_t max_clients = 0) {
    P2PMasterConfig config;
    config.routes.max_clients_per_key = max_clients;
    config.metrics.enable_reporting = false;
    return std::make_unique<P2PMasterService>(config);
}

void Register(P2PMasterService& service, UUID client,
              std::vector<P2PSegment> segments,
              std::string ip = "127.0.0.1", uint16_t port = 5001) {
    auto result = service.RegisterClient(P2PRegisterClientRequest{
        .client_id = client,
        .segments = std::move(segments),
        .ip_address = std::move(ip),
        .rpc_port = port});
    ASSERT_TRUE(result.has_value()) << toString(result.error());
}

void Publish(P2PMasterService& service, std::string key, uint64_t size,
             UUID client, UUID segment) {
    auto result = service.PublishRoute(P2PPublishRouteRequest{
        .key = std::move(key),
        .object_size = size,
        .client_id = client,
        .segment_id = segment});
    ASSERT_TRUE(result.has_value()) << toString(result.error());
}

TEST(P2PMasterServiceTest, RegisterPublishReadWithdraw) {
    auto service = Service();
    const UUID client{1, 1};
    const UUID segment{2, 2};
    Register(*service, client, {Segment(segment)}, "10.0.0.1", 5001);
    Publish(*service, "key", 1024, client, segment);

    auto route = service->GetReadRoute(P2PGetReadRouteRequest{.key = "key"});
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->routes.size(), 1);
    EXPECT_EQ(route->routes[0].client_id, client);
    EXPECT_EQ(route->routes[0].segment_id, segment);
    EXPECT_EQ(route->routes[0].ip_address, "10.0.0.1");
    EXPECT_EQ(route->routes[0].rpc_port, 5001);
    EXPECT_EQ(route->routes[0].object_size, 1024);

    ASSERT_TRUE(service
                    ->WithdrawRoute(P2PWithdrawRouteRequest{
                        .key = "key",
                        .client_id = client,
                        .segment_id = segment})
                    .has_value());
    EXPECT_FALSE(service->RouteExists({.key = "key"})->exists);
}

TEST(P2PMasterServiceTest, ReadRouteFiltersAndAggregatesByClient) {
    auto service = Service();
    const UUID client1{1, 1};
    const UUID client2{2, 2};
    const UUID slow{10, 10};
    const UUID fast{11, 11};
    const UUID cold{12, 12};
    Register(*service, client1,
             {Segment(slow, "slow", 4096, 1, {"cold"}),
              Segment(fast, "fast", 4096, 10, {"hot"})});
    Register(*service, client2,
             {Segment(cold, "other", 4096, 5, {"cold"})}, "10.0.0.2",
             5002);
    Publish(*service, "key", 1024, client1, slow);
    Publish(*service, "key", 1024, client1, fast);
    Publish(*service, "key", 1024, client2, cold);

    P2PGetReadRouteRequest request;
    request.key = "key";
    request.config.tag_filters = {"cold"};
    auto result = service->GetReadRoute(request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->routes.size(), 1);
    EXPECT_EQ(result->routes[0].client_id, client1);
    EXPECT_EQ(result->routes[0].segment_id, fast);
}

TEST(P2PMasterServiceTest, PublishEnforcesSizeAndUniqueClientLimit) {
    auto service = Service(/*max_clients=*/1);
    const UUID client1{1, 1};
    const UUID client2{2, 2};
    Register(*service, client1, {Segment({10, 10}), Segment({11, 11})});
    Register(*service, client2, {Segment({12, 12})});
    Publish(*service, "key", 1024, client1, {10, 10});
    Publish(*service, "key", 1024, client1, {11, 11});

    auto too_many = service->PublishRoute(P2PPublishRouteRequest{
        .key = "key",
        .object_size = 1024,
        .client_id = client2,
        .segment_id = {12, 12}});
    ASSERT_FALSE(too_many.has_value());
    EXPECT_EQ(too_many.error(), ErrorCode::REPLICA_NUM_EXCEEDED);

    auto size_mismatch = service->PublishRoute(P2PPublishRouteRequest{
        .key = "key",
        .object_size = 2048,
        .client_id = client2,
        .segment_id = {12, 12}});
    ASSERT_FALSE(size_mismatch.has_value());
    EXPECT_EQ(size_mismatch.error(), ErrorCode::INVALID_PARAMS);
}

TEST(P2PMasterServiceTest, UnmountUsesFullRouteLocation) {
    auto service = Service();
    const UUID shared_segment{10, 10};
    const UUID client1{1, 1};
    const UUID client2{2, 2};
    Register(*service, client1, {Segment(shared_segment, "first")});
    Register(*service, client2, {Segment(shared_segment, "second")});
    Publish(*service, "key", 1024, client1, shared_segment);
    Publish(*service, "key", 1024, client2, shared_segment);

    ASSERT_TRUE(service
                    ->UnmountSegment(P2PUnmountSegmentRequest{
                        .client_id = client1,
                        .segment_id = shared_segment})
                    .has_value());
    auto route = service->GetReadRoute({.key = "key"});
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->routes.size(), 1);
    EXPECT_EQ(route->routes[0].client_id, client2);
}

TEST(P2PMasterServiceTest, BatchResponsesStayAligned) {
    auto service = Service();
    const UUID client{1, 1};
    const UUID segment{2, 2};
    Register(*service, client, {Segment(segment)});
    Publish(*service, "present", 1024, client, segment);

    auto exists = service->BatchRouteExists(
        {.keys = {"present", "missing"}});
    ASSERT_EQ(exists.responses.size(), 2);
    ASSERT_EQ(exists.error_codes.size(), 2);
    EXPECT_TRUE(exists.responses[0].exists);
    EXPECT_FALSE(exists.responses[1].exists);

    auto reads = service->BatchGetReadRoute(
        {.keys = {"present", "missing"}});
    ASSERT_EQ(reads.responses.size(), 2);
    ASSERT_EQ(reads.error_codes.size(), 2);
    EXPECT_EQ(reads.error_codes[0], ErrorCode::OK);
    EXPECT_EQ(reads.error_codes[1], ErrorCode::OBJECT_NOT_FOUND);
}

TEST(P2PMasterServiceTest, WriteRouteScoresCandidates) {
    auto service = Service();
    const UUID local{1, 1};
    const UUID remote{2, 2};
    Register(*service, local, {Segment({10, 10}, "local", 4096)});
    Register(*service, remote, {Segment({20, 20}, "remote", 8192)},
             "10.0.0.2", 5002);

    P2PGetWriteRouteRequest request;
    request.key = "new";
    request.client_id = local;
    request.object_size = 1024;
    request.config.max_candidates = 1;
    request.config.remote_weight = 1.0;
    request.config.local_write_waterline = 0.5;
    auto result = service->GetWriteRoute(request);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->candidates.size(), 1);
    EXPECT_EQ(result->candidates[0].client_id, remote);
}

TEST(P2PMasterServiceTest, BatchSyncAndCompleteRouteSync) {
    auto service = Service();
    const UUID client{1, 1};
    const UUID segment{2, 2};
    Register(*service, client, {Segment(segment)});
    ASSERT_TRUE(service->GetClientManager().GetClient(client)->IsSyncing());

    auto sync = service->BatchSyncRoutes(P2PBatchSyncRoutesRequest{
        .client_id = client,
        .publish_keys = {"key"},
        .publish_sizes = {1024},
        .publish_segment_ids = {segment}});
    ASSERT_EQ(sync.publish_results.size(), 1);
    EXPECT_EQ(sync.publish_results[0], ErrorCode::OK);
    ASSERT_TRUE(service
                    ->CompleteRouteSync(
                        P2PCompleteRouteSyncRequest{.client_id = client})
                    .has_value());
    EXPECT_FALSE(service->GetClientManager().GetClient(client)->IsSyncing());
}

TEST(P2PMasterServiceTest, RestoresRouteSchemaFromStandby) {
    auto service = Service();
    const UUID client{1, 1};
    const UUID segment{2, 2};
    P2PStandbyMetadataStore::ExportedMetadata metadata;
    metadata.clients.emplace(
        client, P2PStandbyClientInfo{.client_id = client,
                                     .ip_address = "10.0.0.1",
                                     .rpc_port = 5001,
                                     .segments = {Segment(segment)}});
    metadata.routes.emplace(
        "key", P2PStandbyRouteEntry{
                   .object_size = 1024,
                   .locations = {{.client_id = client,
                                  .segment_id = segment}},
                   .last_sequence_id = 7});

    ASSERT_EQ(service->RestoreFromStandbyMetadata(metadata, 7), ErrorCode::OK);
    auto route = service->GetReadRoute({.key = "key"});
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->routes[0].client_id, client);
}

}  // namespace
}  // namespace mooncake
