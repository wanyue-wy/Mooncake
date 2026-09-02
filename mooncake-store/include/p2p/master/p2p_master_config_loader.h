#pragma once

#include <optional>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_master_config.h"

namespace mooncake {

struct P2PMasterConfigOverrides {
    std::optional<bool> enable_metric_reporting;
    std::optional<uint32_t> metrics_port;
    std::optional<uint32_t> rpc_port;
    std::optional<uint32_t> rpc_thread_num;
    std::optional<std::string> rpc_address;
    std::optional<int32_t> rpc_conn_timeout_seconds;
    std::optional<bool> rpc_enable_tcp_no_delay;
    std::optional<uint32_t> heartbeat_rpc_port;
    std::optional<uint32_t> heartbeat_rpc_thread_num;
    std::optional<int64_t> client_ttl;
    std::optional<int64_t> client_crashed_ttl;
    std::optional<uint64_t> max_client_per_key;
    std::optional<std::string> cluster_id;
    std::optional<std::string> redis_endpoint;
    std::optional<std::string> redis_username;
    std::optional<std::string> redis_password;
    std::optional<int32_t> redis_db_index;
    std::optional<int32_t> redis_master_view_ttl_sec;
    std::optional<int32_t> redis_heartbeat_interval_sec;
    std::optional<bool> enable_oplog;
    std::optional<std::string> oplog_store_type;
    std::optional<std::string> oplog_data_dir;
    std::optional<uint64_t> oplog_async_queue_max_entries;
    std::optional<std::string> oplog_async_queue_overflow_mode;
    std::optional<uint64_t> oplog_best_effort_max_retries;
    std::optional<bool> enable_ha;
    std::optional<std::string> election_backend;
    std::optional<std::string> etcd_endpoints;
    std::optional<uint32_t> standby_snapshot_service_port;
    std::optional<std::string> standby_snapshot_service_endpoint;
    std::optional<std::string> standby_snapshot_sources;
    std::optional<uint32_t> standby_snapshot_chunk_size;
};

auto LoadP2PMasterConfig(
    const std::string& config_path = {},
    const P2PMasterConfigOverrides& overrides = {})
    -> tl::expected<P2PMasterConfig, std::string>;

auto ValidateAndNormalizeP2PMasterConfig(
    P2PMasterConfig& config, const std::string& election_backend)
    -> tl::expected<void, std::string>;

}  // namespace mooncake
