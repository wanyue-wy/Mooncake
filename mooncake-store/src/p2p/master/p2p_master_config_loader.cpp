#include "p2p/master/p2p_master_config_loader.h"

#include <exception>
#include <limits>

#include "default_config.h"
#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"

namespace mooncake {
namespace {

constexpr int kDefaultRedisPort = 6379;

void LoadFile(const DefaultConfig& source, P2PMasterConfig& config,
              std::string& election_backend) {
    source.GetBool("enable_metric_reporting", &config.metrics.enable_reporting,
                   config.metrics.enable_reporting);
    source.GetUInt32("metrics_port", &config.metrics.http_port,
                     config.metrics.http_port);
    source.GetUInt32("rpc_port", &config.rpc.port, config.rpc.port);
    source.GetUInt32("rpc_thread_num", &config.rpc.thread_num,
                     config.rpc.thread_num);
    source.GetString("rpc_address", &config.rpc.address, config.rpc.address);
    source.GetInt32("rpc_conn_timeout_seconds",
                    &config.rpc.connection_timeout_seconds,
                    config.rpc.connection_timeout_seconds);
    source.GetBool("rpc_enable_tcp_no_delay", &config.rpc.enable_tcp_no_delay,
                   config.rpc.enable_tcp_no_delay);
    source.GetUInt32("heartbeat_rpc_port", &config.rpc.heartbeat_port,
                     config.rpc.heartbeat_port);
    source.GetUInt32("heartbeat_rpc_thread_num",
                     &config.rpc.heartbeat_thread_num,
                     config.rpc.heartbeat_thread_num);
    source.GetInt64("client_live_ttl_sec",
                    &config.client_lifecycle.live_ttl_seconds,
                    config.client_lifecycle.live_ttl_seconds);
    source.GetInt64("client_crashed_ttl_sec",
                    &config.client_lifecycle.crashed_ttl_seconds, -1);
    source.GetUInt64("max_client_per_key", &config.routes.max_clients_per_key,
                     config.routes.max_clients_per_key);
    source.GetString("cluster_id", &config.cluster_id, config.cluster_id);
    source.GetString("redis_endpoint", &config.redis.endpoint,
                     config.redis.endpoint);
    source.GetString("redis_username", &config.redis.username,
                     config.redis.username);
    source.GetString("redis_password", &config.redis.password,
                     config.redis.password);
    source.GetInt32("redis_db_index", &config.redis.db_index,
                    config.redis.db_index);
    source.GetInt32("redis_master_view_ttl_sec",
                    &config.redis.master_view_ttl_seconds,
                    config.redis.master_view_ttl_seconds);
    source.GetInt32("redis_heartbeat_interval_sec",
                    &config.redis.heartbeat_interval_seconds,
                    config.redis.heartbeat_interval_seconds);
    source.GetBool("enable_oplog", &config.oplog.enabled, config.oplog.enabled);
    source.GetString("oplog_store_type", &config.oplog.store_type,
                     config.oplog.store_type);
    source.GetString("oplog_data_dir", &config.oplog.data_dir,
                     config.oplog.data_dir);
    source.GetUInt64("oplog_async_queue_max_entries",
                     &config.oplog.async_queue_max_entries,
                     config.oplog.async_queue_max_entries);
    source.GetString("oplog_async_queue_overflow_mode",
                     &config.oplog.async_queue_overflow_mode,
                     config.oplog.async_queue_overflow_mode);
    source.GetUInt64("oplog_best_effort_max_retries",
                     &config.oplog.best_effort_max_retries,
                     config.oplog.best_effort_max_retries);
    source.GetBool("enable_ha", &config.ha.enabled, config.ha.enabled);
    source.GetString("election_backend", &election_backend, election_backend);
    source.GetString("etcd_endpoints", &config.ha.etcd_endpoints,
                     config.ha.etcd_endpoints);
    source.GetUInt32("standby_snapshot_service_port",
                     &config.ha.snapshot_service_port,
                     config.ha.snapshot_service_port);
    source.GetString("standby_snapshot_service_endpoint",
                     &config.ha.snapshot_service_endpoint,
                     config.ha.snapshot_service_endpoint);
    source.GetString("standby_snapshot_sources", &config.ha.snapshot_sources,
                     config.ha.snapshot_sources);
    source.GetUInt32("standby_snapshot_chunk_size",
                     &config.ha.snapshot_chunk_size,
                     config.ha.snapshot_chunk_size);
}

template <typename T>
void Apply(const std::optional<T>& value, T& target) {
    if (value.has_value()) {
        target = *value;
    }
}

void ApplyOverrides(const P2PMasterConfigOverrides& values,
                    P2PMasterConfig& config, std::string& election_backend) {
    Apply(values.enable_metric_reporting, config.metrics.enable_reporting);
    Apply(values.metrics_port, config.metrics.http_port);
    Apply(values.rpc_port, config.rpc.port);
    Apply(values.rpc_thread_num, config.rpc.thread_num);
    Apply(values.rpc_address, config.rpc.address);
    Apply(values.rpc_conn_timeout_seconds,
          config.rpc.connection_timeout_seconds);
    Apply(values.rpc_enable_tcp_no_delay, config.rpc.enable_tcp_no_delay);
    Apply(values.heartbeat_rpc_port, config.rpc.heartbeat_port);
    Apply(values.heartbeat_rpc_thread_num, config.rpc.heartbeat_thread_num);
    Apply(values.client_ttl, config.client_lifecycle.live_ttl_seconds);
    Apply(values.client_crashed_ttl,
          config.client_lifecycle.crashed_ttl_seconds);
    Apply(values.max_client_per_key, config.routes.max_clients_per_key);
    Apply(values.cluster_id, config.cluster_id);
    Apply(values.redis_endpoint, config.redis.endpoint);
    Apply(values.redis_username, config.redis.username);
    Apply(values.redis_password, config.redis.password);
    Apply(values.redis_db_index, config.redis.db_index);
    Apply(values.redis_master_view_ttl_sec,
          config.redis.master_view_ttl_seconds);
    Apply(values.redis_heartbeat_interval_sec,
          config.redis.heartbeat_interval_seconds);
    Apply(values.enable_oplog, config.oplog.enabled);
    Apply(values.oplog_store_type, config.oplog.store_type);
    Apply(values.oplog_data_dir, config.oplog.data_dir);
    Apply(values.oplog_async_queue_max_entries,
          config.oplog.async_queue_max_entries);
    Apply(values.oplog_async_queue_overflow_mode,
          config.oplog.async_queue_overflow_mode);
    Apply(values.oplog_best_effort_max_retries,
          config.oplog.best_effort_max_retries);
    Apply(values.enable_ha, config.ha.enabled);
    Apply(values.election_backend, election_backend);
    Apply(values.etcd_endpoints, config.ha.etcd_endpoints);
    Apply(values.standby_snapshot_service_port,
          config.ha.snapshot_service_port);
    Apply(values.standby_snapshot_service_endpoint,
          config.ha.snapshot_service_endpoint);
    Apply(values.standby_snapshot_sources, config.ha.snapshot_sources);
    Apply(values.standby_snapshot_chunk_size, config.ha.snapshot_chunk_size);
}

void NormalizeRedisEndpoint(std::string& endpoint) {
    if (endpoint.empty()) {
        return;
    }
    if (endpoint.front() == '[') {
        if (endpoint.back() == ']') {
            endpoint += ":" + std::to_string(kDefaultRedisPort);
        }
    } else if (endpoint.find(':') == std::string::npos) {
        endpoint += ":" + std::to_string(kDefaultRedisPort);
    }
}

}  // namespace

auto ValidateAndNormalizeP2PMasterConfig(
    P2PMasterConfig& config, const std::string& election_backend)
    -> tl::expected<void, std::string> {
    NormalizeRedisEndpoint(config.redis.endpoint);
    if (config.rpc.port > std::numeric_limits<uint16_t>::max() ||
        config.rpc.heartbeat_port > std::numeric_limits<uint16_t>::max() ||
        config.metrics.http_port > std::numeric_limits<uint16_t>::max() ||
        config.rpc.thread_num == 0 || config.rpc.heartbeat_thread_num == 0) {
        return tl::make_unexpected("invalid P2P RPC endpoint configuration");
    }
    if (config.client_lifecycle.live_ttl_seconds <= 0) {
        return tl::make_unexpected("client_ttl must be positive");
    }
    if (config.client_lifecycle.crashed_ttl_seconds == -1) {
        config.client_lifecycle.crashed_ttl_seconds =
            config.client_lifecycle.live_ttl_seconds * 3;
    }
    if (config.client_lifecycle.crashed_ttl_seconds <
        config.client_lifecycle.live_ttl_seconds) {
        return tl::make_unexpected("client_crashed_ttl must be >= client_ttl");
    }
    if (election_backend == "etcd") {
        config.ha.election_backend = ElectionBackend::ETCD;
    } else if (election_backend == "redis") {
        config.ha.election_backend = ElectionBackend::REDIS;
    } else if (config.ha.enabled) {
        return tl::make_unexpected(
            "election_backend must be 'etcd' or 'redis'");
    }
    if (config.oplog.store_type != "localfs" &&
        config.oplog.store_type != "redis") {
        return tl::make_unexpected("oplog_store_type is invalid");
    }
    if (config.oplog.async_queue_overflow_mode != "reject" &&
        config.oplog.async_queue_overflow_mode != "bypass") {
        return tl::make_unexpected("oplog overflow mode is invalid");
    }
    if (config.ha.snapshot_service_port >
            std::numeric_limits<uint16_t>::max() ||
        config.ha.snapshot_chunk_size == 0 ||
        config.ha.snapshot_chunk_size > kMaxStandbySnapshotChunkSize) {
        return tl::make_unexpected("invalid standby snapshot configuration");
    }
    if (!config.ha.enabled) {
        return {};
    }
    if (config.ha.election_backend == ElectionBackend::ETCD) {
        return config.ha.etcd_endpoints.empty()
                   ? tl::expected<void, std::string>(tl::make_unexpected(
                         "etcd_endpoints is required for etcd HA"))
                   : tl::expected<void, std::string>{};
    }
#ifndef STORE_USE_REDIS
    return tl::make_unexpected("Redis election requires STORE_USE_REDIS");
#else
    if (config.redis.endpoint.empty()) {
        return tl::make_unexpected("redis_endpoint is required for Redis HA");
    }
    if (config.redis.master_view_ttl_seconds <= 0 ||
        config.redis.heartbeat_interval_seconds <= 0 ||
        config.redis.heartbeat_interval_seconds >=
            config.redis.master_view_ttl_seconds) {
        return tl::make_unexpected(
            "Redis heartbeat interval must be positive and smaller than TTL");
    }
    return {};
#endif
}

auto LoadP2PMasterConfig(const std::string& config_path,
                         const P2PMasterConfigOverrides& overrides)
    -> tl::expected<P2PMasterConfig, std::string> {
    P2PMasterConfig config;
    config.client_lifecycle.crashed_ttl_seconds = -1;
    std::string election_backend = "etcd";
    if (!config_path.empty()) {
        DefaultConfig source;
        source.SetPath(config_path);
        try {
            source.Load();
        } catch (const std::exception& error) {
            return tl::make_unexpected("failed to load P2P master config: " +
                                       std::string(error.what()));
        }
        LoadFile(source, config, election_backend);
    }
    ApplyOverrides(overrides, config, election_backend);
    auto result =
        ValidateAndNormalizeP2PMasterConfig(config, election_backend);
    return result.has_value()
               ? tl::expected<P2PMasterConfig, std::string>(std::move(config))
               : tl::expected<P2PMasterConfig, std::string>(
                     tl::make_unexpected(result.error()));
}

}  // namespace mooncake
