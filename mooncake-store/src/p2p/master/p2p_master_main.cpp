#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>
#include <thread>

#include "default_config.h"
#include "p2p/common/p2p_master_config.h"
#include "p2p/ha/p2p_master_ha_runner.h"
#include "p2p/master/p2p_master_server.h"
#include "types.h"

DEFINE_string(config_path, "", "P2P master config file path");
DEFINE_int32(port, 50051,
             "RPC port (deprecated, use rpc_port instead)");
DEFINE_int32(max_threads, 4,
             "RPC threads (deprecated, use rpc_thread_num instead)");
DEFINE_bool(enable_metric_reporting, true,
            "Enable periodic P2P master metric reporting");
DEFINE_int32(metrics_port, 9003, "P2P master HTTP metrics port");
DEFINE_int32(rpc_thread_num, 0,
             "RPC server threads (0 = use max_threads)");
DEFINE_int32(rpc_port, 0, "RPC server port (0 = use port)");
DEFINE_string(rpc_address, "0.0.0.0", "RPC server bind address");
DEFINE_int32(rpc_conn_timeout_seconds, 0,
             "RPC connection timeout in seconds (0 = no timeout)");
DEFINE_bool(rpc_enable_tcp_no_delay, true,
            "Enable TCP_NODELAY for RPC connections");
DEFINE_uint32(heartbeat_rpc_port, 0,
              "Dedicated P2P heartbeat RPC port (0 = use main RPC port)");
DEFINE_uint32(heartbeat_rpc_thread_num, 1,
              "Dedicated heartbeat RPC server threads");

DEFINE_bool(enable_ha, false, "Enable P2P master HA");
DEFINE_string(election_backend, "etcd",
              "P2P HA election backend: etcd or redis");
DEFINE_string(etcd_endpoints, "", "Etcd endpoints separated by semicolons");
DEFINE_int64(client_ttl, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
             "Client live timeout in seconds");
DEFINE_int64(client_crashed_ttl, mooncake::DEFAULT_CLIENT_CRASHED_TTL_SEC,
             "Client crashed timeout in seconds");
DEFINE_string(cluster_id, mooncake::DEFAULT_CLUSTER_ID,
              "P2P master cluster ID");
DEFINE_uint64(max_client_per_key, 1,
              "Maximum client owners per key (0 = unlimited)");

DEFINE_bool(enable_oplog, false, "Enable P2P metadata OpLog");
DEFINE_string(oplog_store_type, "localfs",
              "P2P OpLog backend: localfs or redis");
DEFINE_string(oplog_data_dir, "/tmp/mooncake_oplog",
              "Local filesystem OpLog root directory");
DEFINE_uint64(oplog_async_queue_max_entries, 100000,
              "Maximum queued asynchronous OpLog entries");
DEFINE_string(oplog_async_queue_overflow_mode, "reject",
              "Asynchronous OpLog overflow mode: reject or bypass");
DEFINE_uint64(oplog_best_effort_max_retries, 3,
              "Maximum best-effort Redis OpLog retries");
DEFINE_uint32(standby_snapshot_service_port, 0,
              "Standby snapshot RPC port (0 = disabled)");
DEFINE_string(standby_snapshot_service_endpoint, "",
              "Advertised standby snapshot endpoint override");
DEFINE_string(standby_snapshot_sources, "",
              "Comma-separated standby snapshot source overrides");
DEFINE_uint32(standby_snapshot_chunk_size, 256,
              "Maximum records in a standby snapshot chunk");

DEFINE_string(redis_endpoint, "", "Redis endpoint for P2P HA and OpLog");
DEFINE_string(redis_username, "", "Redis ACL username");
DEFINE_string(redis_password, "", "Redis AUTH password");
DEFINE_int32(redis_db_index, 0, "Redis database index");
DEFINE_int32(redis_master_view_ttl_sec, 4,
             "Redis leader key TTL in seconds");
DEFINE_int32(redis_heartbeat_interval_sec, 1,
             "Redis leader heartbeat interval in seconds");

namespace {

void InitP2PMasterConfig(const mooncake::DefaultConfig& source,
                         mooncake::P2PMasterConfig& config,
                         std::string& election_backend) {
    source.GetBool("enable_metric_reporting",
                   &config.enable_metric_reporting,
                   FLAGS_enable_metric_reporting);
    source.GetUInt32("metrics_port", &config.metrics_port,
                     FLAGS_metrics_port);
    source.GetUInt32("rpc_port", &config.rpc_port, FLAGS_rpc_port);
    source.GetUInt32("rpc_thread_num", &config.rpc_thread_num,
                     FLAGS_rpc_thread_num);
    source.GetString("rpc_address", &config.rpc_address, FLAGS_rpc_address);
    source.GetInt32("rpc_conn_timeout_seconds",
                    &config.rpc_conn_timeout_seconds,
                    FLAGS_rpc_conn_timeout_seconds);
    source.GetBool("rpc_enable_tcp_no_delay",
                   &config.rpc_enable_tcp_no_delay,
                   FLAGS_rpc_enable_tcp_no_delay);
    source.GetUInt32("heartbeat_rpc_port", &config.heartbeat_rpc_port,
                     FLAGS_heartbeat_rpc_port);
    source.GetUInt32("heartbeat_rpc_thread_num",
                     &config.heartbeat_rpc_thread_num,
                     FLAGS_heartbeat_rpc_thread_num);

    source.GetBool("enable_ha", &config.enable_ha, FLAGS_enable_ha);
    source.GetString("election_backend", &election_backend,
                     FLAGS_election_backend);
    source.GetString("etcd_endpoints", &config.etcd_endpoints,
                     FLAGS_etcd_endpoints);
    source.GetInt64("client_live_ttl_sec",
                    &config.service.client_live_ttl_sec, FLAGS_client_ttl);
    source.GetInt64("client_crashed_ttl_sec",
                    &config.service.client_crashed_ttl_sec, -1);
    source.GetString("cluster_id", &config.service.cluster_id,
                     FLAGS_cluster_id);
    source.GetUInt64("max_client_per_key",
                     &config.service.max_client_per_key,
                     FLAGS_max_client_per_key);

    source.GetBool("enable_oplog", &config.service.enable_oplog,
                   FLAGS_enable_oplog);
    source.GetString("oplog_store_type", &config.service.oplog_store_type,
                     FLAGS_oplog_store_type);
    source.GetString("oplog_data_dir", &config.service.oplog_data_dir,
                     FLAGS_oplog_data_dir);
    source.GetUInt64("oplog_async_queue_max_entries",
                     &config.service.oplog_async_queue_max_entries,
                     FLAGS_oplog_async_queue_max_entries);
    source.GetString("oplog_async_queue_overflow_mode",
                     &config.service.oplog_async_queue_overflow_mode,
                     FLAGS_oplog_async_queue_overflow_mode);
    source.GetUInt64("oplog_best_effort_max_retries",
                     &config.service.oplog_best_effort_max_retries,
                     FLAGS_oplog_best_effort_max_retries);
    source.GetUInt32("standby_snapshot_service_port",
                     &config.standby_snapshot_service_port,
                     FLAGS_standby_snapshot_service_port);
    source.GetString("standby_snapshot_service_endpoint",
                     &config.standby_snapshot_service_endpoint,
                     FLAGS_standby_snapshot_service_endpoint);
    source.GetString("standby_snapshot_sources",
                     &config.standby_snapshot_sources,
                     FLAGS_standby_snapshot_sources);
    source.GetUInt32("standby_snapshot_chunk_size",
                     &config.standby_snapshot_chunk_size,
                     FLAGS_standby_snapshot_chunk_size);

    source.GetString("redis_endpoint", &config.service.redis_endpoint,
                     FLAGS_redis_endpoint);
    source.GetString("redis_username", &config.service.redis_username,
                     FLAGS_redis_username);
    source.GetString("redis_password", &config.service.redis_password,
                     FLAGS_redis_password);
    source.GetInt32("redis_db_index", &config.service.redis_db_index,
                    FLAGS_redis_db_index);
    source.GetInt32("redis_master_view_ttl_sec",
                    &config.redis_master_view_ttl_sec,
                    FLAGS_redis_master_view_ttl_sec);
    source.GetInt32("redis_heartbeat_interval_sec",
                    &config.redis_heartbeat_interval_sec,
                    FLAGS_redis_heartbeat_interval_sec);
}

template <typename T>
bool FlagWasSet(const char* name, T& value, const T& flag_value,
                bool config_loaded) {
    google::CommandLineFlagInfo info;
    if ((google::GetCommandLineFlagInfo(name, &info) && !info.is_default) ||
        !config_loaded) {
        value = flag_value;
        return true;
    }
    return false;
}

void LoadP2PMasterFlags(mooncake::P2PMasterConfig& config,
                        std::string& election_backend,
                        bool config_loaded) {
    const int legacy_thread_num =
        std::min(FLAGS_max_threads,
                 static_cast<int>(std::thread::hardware_concurrency()));
    if (FLAGS_rpc_thread_num > 0) {
        config.rpc_thread_num = FLAGS_rpc_thread_num;
        if (FLAGS_max_threads != 4) {
            LOG(WARNING) << "Both rpc_thread_num and max_threads are set; "
                            "using rpc_thread_num="
                         << FLAGS_rpc_thread_num;
        }
    } else if (!config_loaded) {
        config.rpc_thread_num = legacy_thread_num;
    }
    if (FLAGS_rpc_port > 0) {
        config.rpc_port = FLAGS_rpc_port;
        if (FLAGS_port != 50051) {
            LOG(WARNING) << "Both rpc_port and port are set; using rpc_port="
                         << FLAGS_rpc_port;
        }
    } else if (!config_loaded) {
        config.rpc_port = FLAGS_port;
    }

    FlagWasSet("enable_metric_reporting", config.enable_metric_reporting,
               FLAGS_enable_metric_reporting, config_loaded);
    FlagWasSet("metrics_port", config.metrics_port,
               static_cast<uint32_t>(FLAGS_metrics_port), config_loaded);
    FlagWasSet("rpc_address", config.rpc_address, FLAGS_rpc_address,
               config_loaded);
    FlagWasSet("rpc_conn_timeout_seconds", config.rpc_conn_timeout_seconds,
               FLAGS_rpc_conn_timeout_seconds, config_loaded);
    FlagWasSet("rpc_enable_tcp_no_delay", config.rpc_enable_tcp_no_delay,
               FLAGS_rpc_enable_tcp_no_delay, config_loaded);
    FlagWasSet("heartbeat_rpc_port", config.heartbeat_rpc_port,
               FLAGS_heartbeat_rpc_port, config_loaded);
    FlagWasSet("heartbeat_rpc_thread_num",
               config.heartbeat_rpc_thread_num,
               FLAGS_heartbeat_rpc_thread_num, config_loaded);

    FlagWasSet("enable_ha", config.enable_ha, FLAGS_enable_ha,
               config_loaded);
    FlagWasSet("election_backend", election_backend,
               FLAGS_election_backend, config_loaded);
    FlagWasSet("etcd_endpoints", config.etcd_endpoints,
               FLAGS_etcd_endpoints, config_loaded);
    FlagWasSet("client_ttl", config.service.client_live_ttl_sec,
               FLAGS_client_ttl, config_loaded);
    google::CommandLineFlagInfo crashed_ttl_info;
    if (google::GetCommandLineFlagInfo("client_crashed_ttl",
                                       &crashed_ttl_info) &&
        !crashed_ttl_info.is_default) {
        config.service.client_crashed_ttl_sec = FLAGS_client_crashed_ttl;
    }
    FlagWasSet("cluster_id", config.service.cluster_id, FLAGS_cluster_id,
               config_loaded);
    FlagWasSet("max_client_per_key", config.service.max_client_per_key,
               FLAGS_max_client_per_key, config_loaded);

    FlagWasSet("enable_oplog", config.service.enable_oplog,
               FLAGS_enable_oplog, config_loaded);
    FlagWasSet("oplog_store_type", config.service.oplog_store_type,
               FLAGS_oplog_store_type, config_loaded);
    FlagWasSet("oplog_data_dir", config.service.oplog_data_dir,
               FLAGS_oplog_data_dir, config_loaded);
    FlagWasSet("oplog_async_queue_max_entries",
               config.service.oplog_async_queue_max_entries,
               FLAGS_oplog_async_queue_max_entries, config_loaded);
    FlagWasSet("oplog_async_queue_overflow_mode",
               config.service.oplog_async_queue_overflow_mode,
               FLAGS_oplog_async_queue_overflow_mode, config_loaded);
    FlagWasSet("oplog_best_effort_max_retries",
               config.service.oplog_best_effort_max_retries,
               FLAGS_oplog_best_effort_max_retries, config_loaded);
    FlagWasSet("standby_snapshot_service_port",
               config.standby_snapshot_service_port,
               FLAGS_standby_snapshot_service_port, config_loaded);
    FlagWasSet("standby_snapshot_service_endpoint",
               config.standby_snapshot_service_endpoint,
               FLAGS_standby_snapshot_service_endpoint, config_loaded);
    FlagWasSet("standby_snapshot_sources", config.standby_snapshot_sources,
               FLAGS_standby_snapshot_sources, config_loaded);
    FlagWasSet("standby_snapshot_chunk_size",
               config.standby_snapshot_chunk_size,
               FLAGS_standby_snapshot_chunk_size, config_loaded);

    FlagWasSet("redis_endpoint", config.service.redis_endpoint,
               FLAGS_redis_endpoint, config_loaded);
    FlagWasSet("redis_username", config.service.redis_username,
               FLAGS_redis_username, config_loaded);
    FlagWasSet("redis_password", config.service.redis_password,
               FLAGS_redis_password, config_loaded);
    FlagWasSet("redis_db_index", config.service.redis_db_index,
               FLAGS_redis_db_index, config_loaded);
    FlagWasSet("redis_master_view_ttl_sec",
               config.redis_master_view_ttl_sec,
               FLAGS_redis_master_view_ttl_sec, config_loaded);
    FlagWasSet("redis_heartbeat_interval_sec",
               config.redis_heartbeat_interval_sec,
               FLAGS_redis_heartbeat_interval_sec, config_loaded);
}

bool ValidateP2PMasterConfig(mooncake::P2PMasterConfig& config,
                             const std::string& election_backend) {
    if (election_backend == "etcd") {
        config.election_backend = mooncake::ElectionBackend::ETCD;
    } else if (election_backend == "redis") {
        config.election_backend = mooncake::ElectionBackend::REDIS;
    } else if (config.enable_ha) {
        LOG(ERROR) << "Invalid election_backend: " << election_backend
                   << ". Must be 'etcd' or 'redis'";
        return false;
    } else {
        config.election_backend = mooncake::ElectionBackend::ETCD;
    }

    if (config.service.client_crashed_ttl_sec == -1) {
        config.service.client_crashed_ttl_sec =
            config.service.client_live_ttl_sec * 3;
    } else if (config.service.client_crashed_ttl_sec <
               config.service.client_live_ttl_sec) {
        LOG(ERROR) << "client_crashed_ttl ("
                   << config.service.client_crashed_ttl_sec
                   << ") must be >= client_ttl ("
                   << config.service.client_live_ttl_sec << ")";
        return false;
    }

    if (!config.enable_ha) {
        if (!config.etcd_endpoints.empty()) {
            LOG(WARNING)
                << "Etcd endpoints are set but will not be used in non-HA mode";
        }
        return true;
    }

    if (config.election_backend == mooncake::ElectionBackend::ETCD) {
        if (config.etcd_endpoints.empty()) {
            LOG(ERROR) << "Etcd endpoints must be set for etcd HA";
            return false;
        }
        return true;
    }

#ifndef STORE_USE_REDIS
    LOG(ERROR) << "Redis election requested but STORE_USE_REDIS is disabled";
    return false;
#else
    if (config.service.redis_endpoint.empty()) {
        LOG(ERROR) << "redis_endpoint must be set for Redis HA";
        return false;
    }
    if (config.redis_master_view_ttl_sec <= 0 ||
        config.redis_heartbeat_interval_sec <= 0 ||
        config.redis_heartbeat_interval_sec >=
            config.redis_master_view_ttl_sec) {
        LOG(ERROR) << "Redis heartbeat interval must be positive and smaller "
                      "than the master-view TTL";
        return false;
    }
    return true;
#endif
}

}  // namespace

int main(int argc, char* argv[]) {
    mooncake::init_ylt_log_level();
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }

    mooncake::P2PMasterConfig config;
    config.service.client_crashed_ttl_sec = -1;
    std::string election_backend = FLAGS_election_backend;
    const bool config_loaded = !FLAGS_config_path.empty();
    if (config_loaded) {
        mooncake::DefaultConfig source;
        source.SetPath(FLAGS_config_path);
        try {
            source.Load();
        } catch (const std::exception& error) {
            LOG(ERROR) << "Failed to load P2P master config: "
                       << error.what();
            return 1;
        }
        InitP2PMasterConfig(source, config, election_backend);
    }
    LoadP2PMasterFlags(config, election_backend, config_loaded);
    config.ApplyRedisEndpointDefaults();
    if (!ValidateP2PMasterConfig(config, election_backend)) {
        return 1;
    }

    const char* protocol_env = std::getenv("MC_RPC_PROTOCOL");
    const std::string protocol =
        protocol_env && std::string_view(protocol_env) == "rdma" ? "rdma"
                                                                  : "tcp";
    LOG(INFO) << "P2P master starting"
              << ", rpc_address=" << config.rpc_address
              << ", rpc_port=" << config.rpc_port
              << ", rpc_thread_num=" << config.rpc_thread_num
              << ", heartbeat_rpc_port=" << config.heartbeat_rpc_port
              << ", metrics_port=" << config.metrics_port
              << ", enable_ha=" << config.enable_ha
              << ", election_backend=" << election_backend
              << ", enable_oplog=" << config.service.enable_oplog
              << ", oplog_store_type=" << config.service.oplog_store_type
              << ", cluster_id=" << config.service.cluster_id
              << ", client_ttl=" << config.service.client_live_ttl_sec
              << ", client_crashed_ttl="
              << config.service.client_crashed_ttl_sec
              << ", max_client_per_key="
              << config.service.max_client_per_key
              << ", rpc_protocol=" << protocol;

    if (config.enable_ha) {
        return mooncake::P2PMasterHARunner(config).Run();
    }
    return mooncake::P2PMasterServer(config).Run();
}
