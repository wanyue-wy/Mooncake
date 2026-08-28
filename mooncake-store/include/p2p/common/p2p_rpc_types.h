#pragma once

#include <string>
#include <string_view>
#include <optional>
#include <vector>

#include "replica.h"
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

// TODO(M8): Replace the lifecycle DTOs below with the final owning P2P
// protocol types. The current field order is retained during M6 so this change
// only establishes architecture ownership.
struct P2PRegisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(P2PRegisterClientResponse, view_version);

struct P2PUnregisterClientRequest {
    UUID client_id;
    DeploymentMode deployment_mode = DeploymentMode::P2P;
};
YLT_REFL(P2PUnregisterClientRequest, client_id, deployment_mode);

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
    P2PClientStatus status = P2PClientStatus::UNDEFINED;
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
    P2PClientStatus status = P2PClientStatus::UNDEFINED;
};
YLT_REFL(P2PQueryClientStatusResponse, status);

/**
 * @brief Current P2P read-route filter.
 *
 * TODO(M8): Replace with P2PReadRouteConfig and owning request DTOs.
 */
struct P2PGetReplicaListRequestConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = RETURN_ALL_CANDIDATES;
    std::optional<P2PReadRouteConfigExtra> p2p_config;
};
YLT_REFL(P2PGetReplicaListRequestConfig, max_candidates, p2p_config);

struct P2PGetReplicaListResponse {
    std::vector<Replica::Descriptor> replicas;
};
YLT_REFL(P2PGetReplicaListResponse, replicas);

/**
 * @brief Request config for write route.
 *
 * TODO(M8): Replace with P2PWriteRouteConfig as part of the owning P2P wire
 * protocol.
 */
struct WriteRouteRequestConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;
    size_t max_candidates = 2;
    ObjectIterateStrategy strategy = ObjectIterateStrategy::CAPACITY_PRIORITY;
    // Remote-write weight in [0, 1]. Controls local-vs-remote routing via
    // multiplicative scoring on the master side:
    //   score = free_ratio * (is_local ? (1 - remote_weight) : remote_weight)
    //   0   -> local only  (client writes locally);
    //   0.5 -> pure capacity order (local and remote weighted equally);
    //   1   -> remote only (master never returns the local client).
    double remote_weight = 0.5;

    // Local-write waterline in [0, 1]. When the client's local utilization
    // (1 - free/total over eligible tiers) is below this threshold, the client
    // writes locally without asking the master. 0 = disabled.
    double local_write_waterline = 0.5;

    // Capacity metric used when scoring a client:
    //   false = sum free/total over all tiers;
    //   true  = only account the highest-priority eligible tier's free/total
    bool top_tier_only = true;
    bool early_return = true;  // whether to return immediately once candidates
                               // meet conditions of config

    // segment level (TODO)
    // filter the segment with tag
    std::vector<std::string> tag_filters;
    // filter the segments whose priority is lower than priority_limit
    int priority_limit = 0;

    bool IsValid() const {
        // waterline extremes:
        //   <= 0  -> local-write bypass disabled (forbid local write)
        //   >= 1  -> always bypass to local when free (forbid remote write)
        // remote_weight extremes:
        //   <= 0  -> master only returns local routes (forbid remote routing)
        //   >= 1  -> master only returns remote routes (forbid local routing)
        // Two combinations are contradictory (dead end):
        //   forbid local write  + forbid remote routing
        //   forbid remote write + forbid local routing (defensive)
        const bool no_local_write = (local_write_waterline <= 0.0);
        const bool no_remote_write = (local_write_waterline >= 1.0);
        const bool no_remote_route = (remote_weight <= 0.0);
        const bool no_local_route = (remote_weight >= 1.0);
        return !(no_local_write && no_remote_route) &&
               !(no_remote_write && no_local_route);
    }
};
YLT_REFL(WriteRouteRequestConfig, max_candidates, strategy, remote_weight,
         local_write_waterline, top_tier_only, early_return, tag_filters,
         priority_limit);

inline std::ostream& operator<<(std::ostream& os,
                                const WriteRouteRequestConfig& config) {
    os << "WriteRouteRequestConfig: { max_candidates: " << config.max_candidates
       << ", strategy: " << config.strategy
       << ", remote_weight: " << config.remote_weight
       << ", local_write_waterline: " << config.local_write_waterline
       << ", top_tier_only: " << (config.top_tier_only ? "true" : "false")
       << ", early_return: " << (config.early_return ? "true" : "false")
       << ", priority_limit: " << config.priority_limit << " }";
    return os;
}

/**
 * @brief Request structure for getting write route.
 *
 * TODO(M8): Rename to P2PGetWriteRouteRequest
 */
struct WriteRouteRequest {
    // used for pre-filter with limitation of replica number
    std::string_view key;
    UUID client_id;
    size_t size = 0;
    WriteRouteRequestConfig config;
};
YLT_REFL(WriteRouteRequest, key, client_id, size, config);

/**
 * @brief Candidate node for writing route
 */
struct WriteCandidate {
    UUID client_id;
    std::string ip_address;
    uint16_t rpc_port = 0;
    size_t available_capacity = 0;
    double score = 0.0;
};
YLT_REFL(WriteCandidate, client_id, ip_address, rpc_port, available_capacity,
         score);

/**
 * @brief Response structure for getting write route.
 */
struct WriteRouteResponse {
    std::vector<WriteCandidate> candidates;
};
YLT_REFL(WriteRouteResponse, candidates);

/**
 * @brief Request for batch write route lookup.
 *
 * TODO(M8): Rename to P2PBatchGetWriteRouteRequest and use owning keys.
 */
struct BatchGetWriteRouteRequest {
    UUID client_id;
    std::vector<std::string_view> keys;
    std::vector<size_t> sizes;
    WriteRouteRequestConfig config;  // shared config for all keys
};
YLT_REFL(BatchGetWriteRouteRequest, client_id, keys, sizes, config);

/**
 * @brief Response for batch write route lookup.
 *        responses[i] and error_codes[i] correspond to keys[i] in the request.
 */
struct BatchGetWriteRouteResponse {
    std::vector<WriteRouteResponse> responses;  // valid when error_codes[i]==OK
    std::vector<ErrorCode> error_codes;
};
YLT_REFL(BatchGetWriteRouteResponse, responses, error_codes);

/**
 * @brief Request to add a replica.
 *        Master resolves ip_address/rpc_port from registered client info.
 *
 * TODO(M8): Replace with P2PPublishRouteRequest.
 */
struct AddReplicaRequest {
    std::string_view key;
    size_t size;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(AddReplicaRequest, key, size, client_id, segment_id);

/**
 * @brief Request to remove a replica.
 *
 * TODO(M8): Replace with P2PWithdrawRouteRequest.
 */
struct RemoveReplicaRequest {
    std::string_view key;
    UUID client_id;
    UUID segment_id;
};
YLT_REFL(RemoveReplicaRequest, key, client_id, segment_id);

/**
 * @brief Request to remove replicas from multiple segments in one call.
 *
 * TODO(M8): Replace with P2PBatchWithdrawRouteRequest.
 */
struct BatchRemoveReplicaRequest {
    std::string_view key;
    UUID client_id;
    std::vector<UUID> segment_ids;
};
YLT_REFL(BatchRemoveReplicaRequest, key, client_id, segment_ids);

/**
 * @brief Request to batch sync replicas (mixed ADD and REMOVE ops).
 *        Master only needs client_id + segment_id to identify replicas
 *
 * TODO(M8): Replace with P2PBatchSyncRoutesRequest and owning keys.
 */
struct BatchSyncReplicaRequest {
    UUID client_id;
    // ADD operations
    std::vector<std::string_view> add_keys;
    std::vector<size_t> add_sizes;
    std::vector<UUID> add_segment_ids;
    // REMOVE operations
    std::vector<std::string_view> remove_keys;
    std::vector<UUID> remove_segment_ids;
};
YLT_REFL(BatchSyncReplicaRequest, client_id, add_keys, add_sizes,
         add_segment_ids, remove_keys, remove_segment_ids);

/**
 * @brief Response for batch sync replicas.
 */
struct BatchSyncReplicaResponse {
    std::vector<ErrorCode> add_results;
    std::vector<ErrorCode> remove_results;
};
YLT_REFL(BatchSyncReplicaResponse, add_results, remove_results);

}  // namespace mooncake
