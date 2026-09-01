#pragma once

#include <string>
#include <vector>

#include "p2p/client/heartbeat_type.h"
#include "p2p/common/p2p_types.h"
#include "types.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

/**
 * @brief Registration data sent by a P2P client to the P2P master.
 */
struct P2PRegisterClientRequest {
    UUID client_id;
    std::vector<P2PSegment> segments;
    std::string ip_address;
    uint16_t rpc_port{0};
};
YLT_REFL(P2PRegisterClientRequest, client_id, segments, ip_address, rpc_port);

struct P2PRegisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(P2PRegisterClientResponse, view_version);

struct P2PUnregisterClientRequest {
    UUID client_id;
};
YLT_REFL(P2PUnregisterClientRequest, client_id);

struct P2PUnregisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(P2PUnregisterClientResponse, view_version);

struct P2PHeartbeatRequest {
    UUID client_id;
    std::vector<HeartbeatTask> tasks;
};
YLT_REFL(P2PHeartbeatRequest, client_id, tasks);

struct P2PHeartbeatResponse {
    P2PClientStatus status = P2PClientStatus::UNREGISTERED;
    ViewVersionId view_version = 0;
    std::vector<HeartbeatTaskResult> task_results;
};
YLT_REFL(P2PHeartbeatResponse, status, view_version, task_results);

struct P2PHeartbeatServiceReadyResponse {
    uint32_t heartbeat_rpc_port = 0;
};
YLT_REFL(P2PHeartbeatServiceReadyResponse, heartbeat_rpc_port);

struct P2PQueryClientStatusRequest {
    UUID client_id;
};
YLT_REFL(P2PQueryClientStatusRequest, client_id);

struct P2PQueryClientStatusResponse {
    P2PClientStatus status = P2PClientStatus::UNREGISTERED;
};
YLT_REFL(P2PQueryClientStatusResponse, status);

// -------------------------------------------------------------------------
// P2P master protocol v2. Every complex RPC owns one request value; no wire
// object contains string_view, expected, Replica, or centralized DTO state.
// -------------------------------------------------------------------------

struct P2PMountSegmentRequest {
    UUID client_id;
    P2PSegment segment;
};
YLT_REFL(P2PMountSegmentRequest, client_id, segment);

struct P2PUnmountSegmentRequest {
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(P2PUnmountSegmentRequest, client_id, segment_id);

struct P2PRouteExistsRequest {
    std::string key;
};
YLT_REFL(P2PRouteExistsRequest, key);

struct P2PRouteExistsResponse {
    bool exists{false};
};
YLT_REFL(P2PRouteExistsResponse, exists);

struct P2PBatchRouteExistsRequest {
    std::vector<std::string> keys;
};
YLT_REFL(P2PBatchRouteExistsRequest, keys);

struct P2PBatchRouteExistsResponse {
    std::vector<P2PRouteExistsResponse> responses;
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(P2PBatchRouteExistsResponse, responses, error_codes);

struct P2PGetReadRouteRequest {
    std::string key;
    P2PReadRouteConfig config;
};
YLT_REFL(P2PGetReadRouteRequest, key, config);

struct P2PGetReadRouteResponse {
    std::vector<P2PRouteDescriptor> routes;
};
YLT_REFL(P2PGetReadRouteResponse, routes);

struct P2PBatchGetReadRouteRequest {
    std::vector<std::string> keys;
    P2PReadRouteConfig config;
};
YLT_REFL(P2PBatchGetReadRouteRequest, keys, config);

struct P2PBatchGetReadRouteResponse {
    std::vector<P2PGetReadRouteResponse> responses;
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(P2PBatchGetReadRouteResponse, responses, error_codes);

struct P2PWriteRouteConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;

    size_t max_candidates{2};
    P2PClientSelectionStrategy strategy{
        P2PClientSelectionStrategy::CAPACITY_PRIORITY};
    double remote_weight{0.5};
    double local_write_waterline{0.5};
    bool top_tier_only{true};
    bool early_return{true};
    std::vector<std::string> tag_filters;
    int priority_limit{0};

    bool IsValid() const {
        const bool no_local_write = local_write_waterline <= 0.0;
        const bool no_remote_write = local_write_waterline >= 1.0;
        const bool no_remote_route = remote_weight <= 0.0;
        const bool no_local_route = remote_weight >= 1.0;
        return !(no_local_write && no_remote_route) &&
               !(no_remote_write && no_local_route);
    }
};
YLT_REFL(P2PWriteRouteConfig, max_candidates, strategy, remote_weight,
         local_write_waterline, top_tier_only, early_return, tag_filters,
         priority_limit);

inline std::ostream& operator<<(std::ostream& output,
                                const P2PWriteRouteConfig& config) {
    return output << "P2PWriteRouteConfig: { max_candidates: "
                  << config.max_candidates << ", strategy: " << config.strategy
                  << ", remote_weight: " << config.remote_weight
                  << ", local_write_waterline: "
                  << config.local_write_waterline
                  << ", top_tier_only: " << config.top_tier_only
                  << ", early_return: " << config.early_return
                  << ", priority_limit: " << config.priority_limit << " }";
}

struct P2PWriteCandidate {
    UUID client_id;
    std::string ip_address;
    uint16_t rpc_port{0};
    size_t available_capacity{0};
    double score{0.0};
};
YLT_REFL(P2PWriteCandidate, client_id, ip_address, rpc_port,
         available_capacity, score);

struct P2PGetWriteRouteRequest {
    std::string key;
    UUID client_id;
    uint64_t object_size{0};
    P2PWriteRouteConfig config;
};
YLT_REFL(P2PGetWriteRouteRequest, key, client_id, object_size, config);

struct P2PGetWriteRouteResponse {
    std::vector<P2PWriteCandidate> candidates;
};
YLT_REFL(P2PGetWriteRouteResponse, candidates);

struct P2PBatchGetWriteRouteRequest {
    UUID client_id;
    std::vector<std::string> keys;
    std::vector<uint64_t> object_sizes;
    P2PWriteRouteConfig config;
};
YLT_REFL(P2PBatchGetWriteRouteRequest, client_id, keys, object_sizes, config);

struct P2PBatchGetWriteRouteResponse {
    std::vector<P2PGetWriteRouteResponse> responses;
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(P2PBatchGetWriteRouteResponse, responses, error_codes);

struct P2PPublishRouteRequest {
    std::string key;
    uint64_t object_size{0};
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(P2PPublishRouteRequest, key, object_size, client_id, segment_id);

struct P2PWithdrawRouteRequest {
    std::string key;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(P2PWithdrawRouteRequest, key, client_id, segment_id);

struct P2PBatchWithdrawRouteRequest {
    std::string key;
    UUID client_id;
    std::vector<UUID> segment_ids;
};
YLT_REFL(P2PBatchWithdrawRouteRequest, key, client_id, segment_ids);

struct P2PBatchWithdrawRouteResponse {
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(P2PBatchWithdrawRouteResponse, error_codes);

struct P2PBatchSyncRoutesRequest {
    UUID client_id;
    std::vector<std::string> publish_keys;
    std::vector<uint64_t> publish_sizes;
    std::vector<UUID> publish_segment_ids;
    std::vector<std::string> withdraw_keys;
    std::vector<UUID> withdraw_segment_ids;
};
YLT_REFL(P2PBatchSyncRoutesRequest, client_id, publish_keys, publish_sizes,
         publish_segment_ids, withdraw_keys, withdraw_segment_ids);

struct P2PBatchSyncRoutesResponse {
    std::vector<ErrorCode> publish_results;
    std::vector<ErrorCode> withdraw_results;
};
YLT_REFL(P2PBatchSyncRoutesResponse, publish_results, withdraw_results);

struct P2PCompleteRouteSyncRequest {
    UUID client_id;
};
YLT_REFL(P2PCompleteRouteSyncRequest, client_id);

}  // namespace mooncake
