#pragma once

#include <cstdint>
#include <string>

#include "types.h"

namespace mooncake {

struct P2PMasterServiceConfig {
    uint64_t max_client_per_key = 1;
    int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC;
    int64_t client_crashed_ttl_sec = DEFAULT_CLIENT_CRASHED_TTL_SEC;
    ViewVersionId view_version = 0;

    bool enable_oplog = false;
    std::string oplog_store_type = "localfs";
    std::string oplog_data_dir = "/tmp/mooncake_oplog";
    uint64_t oplog_async_queue_max_entries = 100000;
    std::string oplog_async_queue_overflow_mode = "reject";
    uint64_t oplog_best_effort_max_retries = 3;

    std::string cluster_id = DEFAULT_CLUSTER_ID;
    std::string redis_endpoint;
    std::string redis_username;
    std::string redis_password;
    int redis_db_index = 0;
};

struct P2PMasterRpcConfig {
    P2PMasterServiceConfig service;
    bool enable_metric_reporting = true;
    uint16_t http_port = 9003;
    uint32_t heartbeat_rpc_port = 0;
};

struct P2PMasterConfig {
    static constexpr int kDefaultRedisPort = 6379;

    P2PMasterServiceConfig service;

    bool enable_metric_reporting = true;
    uint32_t metrics_port = 9003;
    uint32_t rpc_port = 50051;
    uint32_t rpc_thread_num = 4;
    std::string rpc_address = "0.0.0.0";
    int32_t rpc_conn_timeout_seconds = 0;
    bool rpc_enable_tcp_no_delay = true;
    uint32_t heartbeat_rpc_port = 0;
    uint32_t heartbeat_rpc_thread_num = 1;

    bool enable_ha = false;
    ElectionBackend election_backend = ElectionBackend::ETCD;
    std::string etcd_endpoints;
    int redis_master_view_ttl_sec = 4;
    int redis_heartbeat_interval_sec = 1;

    uint32_t standby_snapshot_service_port = 0;
    std::string standby_snapshot_service_endpoint;
    std::string standby_snapshot_sources;
    uint32_t standby_snapshot_chunk_size = 256;

    void ApplyRedisEndpointDefaults() {
        auto& endpoint = service.redis_endpoint;
        if (endpoint.empty()) {
            return;
        }
        if (endpoint.front() == '[') {
            if (endpoint.back() == ']') {
                endpoint += ":" + std::to_string(kDefaultRedisPort);
            }
            return;
        }
        if (endpoint.find(':') == std::string::npos) {
            endpoint += ":" + std::to_string(kDefaultRedisPort);
        }
    }

    P2PMasterRpcConfig BuildRpcConfig(ViewVersionId view_version) const {
        P2PMasterRpcConfig config;
        config.service = service;
        config.service.view_version = view_version;
        config.enable_metric_reporting = enable_metric_reporting;
        config.http_port = static_cast<uint16_t>(metrics_port);
        config.heartbeat_rpc_port = heartbeat_rpc_port;
        return config;
    }
};

}  // namespace mooncake
