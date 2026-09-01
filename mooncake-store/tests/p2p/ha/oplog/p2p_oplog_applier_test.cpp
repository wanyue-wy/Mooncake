#include <gtest/gtest.h>

#include <xxhash.h>
#include <ylt/struct_pack.hpp>

#include "p2p/ha/oplog/p2p_oplog_applier.h"

namespace mooncake {
namespace {

OpLogEntry Entry(uint64_t sequence, OpType type, std::string payload = {}) {
    OpLogEntry entry;
    entry.sequence_id = sequence;
    entry.op_type = type;
    entry.payload = std::move(payload);
    entry.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                             std::chrono::steady_clock::now().time_since_epoch())
                             .count();
    entry.checksum = static_cast<uint32_t>(
        XXH32(entry.payload.data(), entry.payload.size(), 0));
    return entry;
}

P2PSegment Segment(UUID id) {
    return P2PSegment{.id = id,
                      .name = "segment",
                      .size = 4096,
                      .priority = 1,
                      .tags = {},
                      .memory_type = MemoryType::DRAM,
                      .usage = 0};
}

P2PRouteLocation Location(UUID client, UUID segment) {
    return P2PRouteLocation{.client_id = client, .segment_id = segment};
}

TEST(P2POpLogApplierTest, AppliesRouteSchemaV2Lifecycle) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store);
    const UUID client{1, 1};
    const UUID segment{2, 2};

    RegisterClientPayload registration;
    registration.client_id = client;
    registration.ip_address = "10.0.0.1";
    registration.rpc_port = 5001;
    registration.segments = {Segment(segment)};
    ASSERT_TRUE(applier.ApplyOpLogEntry(Entry(
        1, OpType_REGISTER_CLIENT, SerializeP2PPayload(registration))));

    PublishRoutePayload publish;
    publish.object_key = "key";
    publish.client_id = client;
    publish.segment_id = segment;
    publish.size = 1024;
    ASSERT_TRUE(applier.ApplyOpLogEntry(Entry(
        2, OpType_PUBLISH_ROUTE, SerializeP2PPayload(publish))));
    auto route = store.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    EXPECT_EQ(route->object_size, 1024);
    EXPECT_EQ(route->locations.front(),
              (P2PRouteLocation{.client_id = client,
                                .segment_id = segment}));

    WithdrawRoutePayload withdraw;
    withdraw.object_key = "key";
    withdraw.client_id = client;
    withdraw.segment_id = segment;
    ASSERT_TRUE(applier.ApplyOpLogEntry(Entry(
        3, OpType_WITHDRAW_ROUTE, SerializeP2PPayload(withdraw))));
    EXPECT_FALSE(store.RouteExists("key"));
}

TEST(P2POpLogApplierTest, RejectsOldOrUnknownPayloadSchema) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store);

    PublishRoutePayload old;
    old.schema_version = 1;
    old.object_key = "old";
    old.client_id = {1, 1};
    old.segment_id = {2, 2};
    old.size = 1024;
    const auto raw_old_payload = struct_pack::serialize<std::string>(old);

    // Publish is best effort: the invalid payload is skipped, but never enters
    // standby state and does not degrade the applier.
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        Entry(1, OpType_PUBLISH_ROUTE, raw_old_payload)));
    EXPECT_FALSE(store.RouteExists("old"));
    EXPECT_TRUE(applier.IsHealthy());

    RegisterClientPayload unsupported;
    unsupported.schema_version = 99;
    unsupported.client_id = {3, 3};
    EXPECT_FALSE(applier.ApplyOpLogEntry(Entry(
        2, OpType_REGISTER_CLIENT,
        struct_pack::serialize<std::string>(unsupported))));
    EXPECT_FALSE(applier.IsHealthy());
}

TEST(P2POpLogApplierTest, RejectsCentralizedOpTypes) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store);
    EXPECT_FALSE(applier.ApplyOpLogEntry(Entry(1, OpType::REMOVE)));
    EXPECT_EQ(applier.GetExpectedSequenceId(), 1);
}

TEST(P2POpLogApplierTest, UnmountUsesClientAndSegmentLocation) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store);
    const UUID shared_segment{9, 9};
    const UUID first_client{1, 1};
    const UUID second_client{2, 2};
    store.RegisterClient(first_client, "10.0.0.1", 5001,
                         {Segment(shared_segment)});
    store.RegisterClient(second_client, "10.0.0.2", 5002,
                         {Segment(shared_segment)});
    ASSERT_TRUE(store.PublishRoute(
        "key", Location(first_client, shared_segment), 1024, 1));
    ASSERT_TRUE(store.PublishRoute(
        "key", Location(second_client, shared_segment), 1024, 1));

    UnmountSegmentPayload unmount;
    unmount.client_id = first_client;
    unmount.segment_id = shared_segment;
    ASSERT_TRUE(applier.ApplyOpLogEntry(Entry(
        1, OpType_UNMOUNT_SEGMENT, SerializeP2PPayload(unmount))));
    auto route = store.GetRoute("key");
    ASSERT_TRUE(route.has_value());
    ASSERT_EQ(route->locations.size(), 1);
    EXPECT_EQ(route->locations.front().client_id, second_client);
}

TEST(P2POpLogApplierTest, BuffersOutOfOrderP2POperations) {
    P2PStandbyMetadataStore store;
    P2POpLogApplier applier(&store);
    RegisterClientPayload first;
    first.client_id = {1, 1};
    RegisterClientPayload second;
    second.client_id = {2, 2};

    EXPECT_FALSE(applier.ApplyOpLogEntry(
        Entry(2, OpType_REGISTER_CLIENT, SerializeP2PPayload(second))));
    EXPECT_TRUE(applier.ApplyOpLogEntry(
        Entry(1, OpType_REGISTER_CLIENT, SerializeP2PPayload(first))));
    EXPECT_TRUE(store.GetClientInfo(first.client_id).has_value());
    EXPECT_TRUE(store.GetClientInfo(second.client_id).has_value());
    EXPECT_EQ(applier.GetExpectedSequenceId(), 3);
}

}  // namespace
}  // namespace mooncake
