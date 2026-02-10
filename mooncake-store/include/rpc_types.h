#pragma once

#include "types.h"
#include "replica.h"
#include "heartbeat_type.h"

namespace mooncake {

/**
 * @brief Response structure for GetReplicaList operation
 */
struct GetReplicaListResponse {
    std::vector<Replica::Descriptor> replicas;
    uint64_t lease_ttl_ms;

    GetReplicaListResponse() : lease_ttl_ms(0) {}
    GetReplicaListResponse(std::vector<Replica::Descriptor>&& replicas_param,
                           uint64_t lease_ttl_ms_param)
        : replicas(std::move(replicas_param)),
          lease_ttl_ms(lease_ttl_ms_param) {}
};
YLT_REFL(GetReplicaListResponse, replicas, lease_ttl_ms);

/**
 * @brief Response structure for GetStorageConfig operation
 */
struct GetStorageConfigResponse {
    std::string fsdir;
    bool enable_disk_eviction;
    uint64_t quota_bytes;

    GetStorageConfigResponse() : enable_disk_eviction(true), quota_bytes(0) {}
    GetStorageConfigResponse(const std::string& fsdir_param,
                             bool enable_eviction, uint64_t quota)
        : fsdir(fsdir_param),
          enable_disk_eviction(enable_eviction),
          quota_bytes(quota) {}
};
YLT_REFL(GetStorageConfigResponse, fsdir, enable_disk_eviction, quota_bytes);

/**
 * @brief Request structure for Heartbeat operation.
 * Client could set HeartbeatTasks for Master to run
 */
struct HeartbeatRequest {
    UUID client_id;
    std::vector<HeartbeatTask> tasks;
};
YLT_REFL(HeartbeatRequest, client_id, tasks);

/**
 * @brief Response structure for Heartbeat operation.
 * Always returns view_version; client uses it under UNDEFINED status
 * for crash-recovery decisions, other statuses for defensive checks.
 */
struct HeartbeatResponse {
    ClientStatus status;
    ViewVersionId view_version = 0;
    std::vector<HeartbeatTaskResult> task_results;
};
YLT_REFL(HeartbeatResponse, status, view_version, task_results);

/**
 * @brief Request structure for RegisterClient operation.
 * Client calls this on startup to register its UUID and local segments.
 * P2P clients additionally provide ip_address and rpc_port.
 */
struct RegisterClientRequest {
    UUID client_id;
    std::vector<Segment> segments;
    // P2P only: network endpoint info
    std::optional<std::string> ip_address;
    std::optional<uint16_t> rpc_port;
};
YLT_REFL(RegisterClientRequest, client_id, segments, ip_address, rpc_port);

/**
 * @brief Response structure for RegisterClient operation.
 * Returns the master's view_version to client for crash checking.
 */
struct RegisterClientResponse {
    ViewVersionId view_version = 0;
};
YLT_REFL(RegisterClientResponse, view_version);

}  // namespace mooncake