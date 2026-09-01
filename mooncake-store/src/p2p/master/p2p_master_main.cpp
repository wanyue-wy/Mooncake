#include <gflags/gflags.h>
#include <glog/logging.h>

#include <string>

#include "p2p/master/p2p_master.h"
#include "p2p/master/p2p_master_config_loader.h"
#include "types.h"

DEFINE_string(config_path, "", "P2P master config file path");
DEFINE_bool(enable_metric_reporting, true, "Enable metric reporting");
DEFINE_uint32(metrics_port, 9003, "P2P metrics HTTP port");
DEFINE_uint32(rpc_thread_num, 4, "P2P master RPC threads");
DEFINE_uint32(rpc_port, 50051, "P2P master RPC port");
DEFINE_string(rpc_address, "0.0.0.0", "P2P master RPC bind address");
DEFINE_int32(rpc_conn_timeout_seconds, 0, "RPC connection timeout");
DEFINE_bool(rpc_enable_tcp_no_delay, true, "Enable RPC TCP_NODELAY");
DEFINE_uint32(heartbeat_rpc_port, 0, "Dedicated heartbeat RPC port");
DEFINE_uint32(heartbeat_rpc_thread_num, 1, "Heartbeat RPC threads");
DEFINE_int64(client_ttl, mooncake::DEFAULT_CLIENT_LIVE_TTL_SEC,
             "P2P client live TTL");
DEFINE_int64(client_crashed_ttl, mooncake::DEFAULT_CLIENT_CRASHED_TTL_SEC,
             "P2P client crashed TTL");
DEFINE_uint64(max_client_per_key, 1, "Maximum client owners per key");
DEFINE_string(cluster_id, mooncake::DEFAULT_CLUSTER_ID, "P2P cluster ID");
DEFINE_string(redis_endpoint, "", "Redis endpoint");
DEFINE_string(redis_username, "", "Redis username");
DEFINE_string(redis_password, "", "Redis password");
DEFINE_int32(redis_db_index, 0, "Redis database index");
DEFINE_int32(redis_master_view_ttl_sec, 4, "Redis leader TTL");
DEFINE_int32(redis_heartbeat_interval_sec, 1, "Redis heartbeat interval");
DEFINE_bool(enable_oplog, false, "Enable P2P OpLog");
DEFINE_string(oplog_store_type, "localfs", "P2P OpLog backend");
DEFINE_string(oplog_data_dir, "/tmp/mooncake_oplog", "P2P OpLog directory");
DEFINE_uint64(oplog_async_queue_max_entries, 100000,
              "Maximum asynchronous OpLog queue entries");
DEFINE_string(oplog_async_queue_overflow_mode, "reject",
              "OpLog overflow mode");
DEFINE_uint64(oplog_best_effort_max_retries, 3,
              "Best-effort OpLog retry count");
DEFINE_bool(enable_ha, false, "Enable P2P master HA");
DEFINE_string(election_backend, "etcd", "P2P election backend");
DEFINE_string(etcd_endpoints, "", "Etcd endpoints");
DEFINE_uint32(standby_snapshot_service_port, 0, "Snapshot RPC port");
DEFINE_string(standby_snapshot_service_endpoint, "",
              "Advertised snapshot endpoint");
DEFINE_string(standby_snapshot_sources, "", "Snapshot source endpoints");
DEFINE_uint32(standby_snapshot_chunk_size, 256, "Snapshot chunk size");

namespace mooncake {
namespace {

bool Explicit(const char* name) {
    google::CommandLineFlagInfo info;
    return google::GetCommandLineFlagInfo(name, &info) && !info.is_default;
}

P2PMasterConfigOverrides BuildOverrides() {
    P2PMasterConfigOverrides values;
#define COPY_FLAG(flag)        \
    if (Explicit(#flag)) {     \
        values.flag = FLAGS_##flag; \
    }
    COPY_FLAG(enable_metric_reporting);
    COPY_FLAG(metrics_port);
    COPY_FLAG(rpc_port);
    COPY_FLAG(rpc_thread_num);
    COPY_FLAG(rpc_address);
    COPY_FLAG(rpc_conn_timeout_seconds);
    COPY_FLAG(rpc_enable_tcp_no_delay);
    COPY_FLAG(heartbeat_rpc_port);
    COPY_FLAG(heartbeat_rpc_thread_num);
    COPY_FLAG(client_ttl);
    COPY_FLAG(client_crashed_ttl);
    COPY_FLAG(max_client_per_key);
    COPY_FLAG(cluster_id);
    COPY_FLAG(redis_endpoint);
    COPY_FLAG(redis_username);
    COPY_FLAG(redis_password);
    COPY_FLAG(redis_db_index);
    COPY_FLAG(redis_master_view_ttl_sec);
    COPY_FLAG(redis_heartbeat_interval_sec);
    COPY_FLAG(enable_oplog);
    COPY_FLAG(oplog_store_type);
    COPY_FLAG(oplog_data_dir);
    COPY_FLAG(oplog_async_queue_max_entries);
    COPY_FLAG(oplog_async_queue_overflow_mode);
    COPY_FLAG(oplog_best_effort_max_retries);
    COPY_FLAG(enable_ha);
    COPY_FLAG(election_backend);
    COPY_FLAG(etcd_endpoints);
    COPY_FLAG(standby_snapshot_service_port);
    COPY_FLAG(standby_snapshot_service_endpoint);
    COPY_FLAG(standby_snapshot_sources);
    COPY_FLAG(standby_snapshot_chunk_size);
#undef COPY_FLAG
    return values;
}

}  // namespace
}  // namespace mooncake

int main(int argc, char* argv[]) {
    mooncake::init_ylt_log_level();
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }
    auto config = mooncake::LoadP2PMasterConfig(
        FLAGS_config_path, mooncake::BuildOverrides());
    if (!config.has_value()) {
        LOG(ERROR) << config.error();
        return 1;
    }
    return mooncake::P2PMaster(*config).Run();
}
