#include <gflags/gflags.h>
#include <csignal>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <optional>
#include <string>

#include "client_config_builder.h"
#include "real_client.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server connection string");
DEFINE_string(device_names, "", "Device names");
DEFINE_string(master_server_address, "127.0.0.1:50051",
              "Master server address");
DEFINE_string(protocol, "tcp", "Protocol");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(redis_cluster_id, mooncake::DEFAULT_CLUSTER_ID,
              "Redis HA cluster ID for redis:// master discovery");
DEFINE_string(redis_username, "",
              "Redis ACL username for redis:// master discovery");
DEFINE_string(redis_password, "",
              "Redis AUTH password for redis:// master discovery");
DEFINE_int32(redis_db_index, 0,
             "Redis database index for redis:// master discovery");
DEFINE_int32(redis_master_view_ttl_sec, 4,
             "Redis master view TTL for redis:// master discovery");
DEFINE_int32(redis_heartbeat_interval_sec, 1,
             "Redis heartbeat interval for redis:// master discovery");
DEFINE_uint32(
    heartbeat_rpc_port, 0,
    "Port of the master's dedicated heartbeat RPC server. When > 0, "
    "heartbeats are sent to <master host>:heartbeat_rpc_port instead of "
    "the main RPC port. Must match the master's --heartbeat_rpc_port.");
DEFINE_int32(threads, 1, "Number of rpc threads for dummy client");
DEFINE_string(tiered_backend_config, "conf/tiered_backend.json",
              "Tiered backend config: accepts a JSON string or a path to a "
              "JSON config file.");
DEFINE_uint32(client_rpc_port, 12345, "Client RPC service port");
DEFINE_uint32(rpc_thread_num, 16, "Number of threads for P2P RPC service");
DEFINE_uint64(lock_shard_count, 1024,
              "Number of metadata and key-lock shards");
DEFINE_string(route_cache_max_memory, "300 MB", "Max memory for RouteCache");
DEFINE_uint64(route_cache_ttl_ms, 60 * 1000,
              "TTL for RouteCache entries in ms");
DEFINE_uint64(async_sender_thread_count, 4,
              "Async route notifier sender thread count; 0 disables it");
DEFINE_uint64(async_max_batch_size, 2000,
              "Max ops per batch in async route notifier");
DEFINE_uint64(async_route_queue_size, 0,
              "Async route notifier queue size");
DEFINE_string(p2p_local_transfer_mode, "te",
              "Local transfer mode: memcpy|te");
DEFINE_string(p2p_transfer_direction_mode, "reverse",
              "Cross-node transfer direction: reverse|forward");
DEFINE_uint64(local_memcpy_async_worker_num, 32,
              "Worker count for async local memcpy; 0 disables it");
DEFINE_uint64(te_async_poll_worker_num, 32,
              "Worker count for async TE batch polling in DataManager; "
              "0 uses synchronous TE wait");
DEFINE_uint32(http_port, 9003, "Port for client HTTP server");
DEFINE_bool(enable_http_server, true, "Enable client HTTP server");
DEFINE_uint32(p2p_key_lease_duration_ms, 0,
              "Maximum key lease duration in ms; 0 uses the default");
DEFINE_uint32(p2p_key_lease_scan_interval_ms, 0,
              "Expired key lease scan interval in ms; 0 uses the default");
DEFINE_string(runtime_config, "",
              "Runtime read/write config as JSON or a JSON file path; can "
              "also be set via MC_RUNTIME_CONFIG");
DEFINE_bool(enable_client_metric_collection, true,
            "Enable client metric collection");
DEFINE_uint64(metric_report_interval_seconds, 60,
              "Periodic client metric reporting interval; 0 disables it");

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              RealClient& real_client) {
    server.register_handler<&RealClient::put_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::removeAllLocal_internal>(&real_client);
    server.register_handler<&RealClient::removeLocal_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::get_buffer_info_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
    server.register_handler<&RealClient::ping>(&real_client);
    server.register_handler<&RealClient::create_copy_task>(&real_client);
    server.register_handler<&RealClient::create_move_task>(&real_client);
    server.register_handler<&RealClient::query_task>(&real_client);
    server.register_handler<&RealClient::batch_get_offload_object>(
        &real_client);
}
}  // namespace mooncake

int main(int argc, char* argv[]) {
    // Initialize the signal mask before any other thread is spawned.
    mooncake::ResourceTracker::getInstance();

    gflags::ParseCommandLineFlags(&argc, &argv, true);
    const uint64_t local_buffer_size = 0;
    LOG(INFO) << "Using P2P client"
              << ", client_rpc_port=" << FLAGS_client_rpc_port;

    auto config = ClientConfigBuilder::build_p2p_real_client(
        FLAGS_host, FLAGS_metadata_server, FLAGS_protocol,
        FLAGS_device_names.empty()
            ? std::nullopt
            : std::optional<std::string>(FLAGS_device_names),
        FLAGS_master_server_address, FLAGS_tiered_backend_config,
        local_buffer_size, nullptr,
        "@mooncake_client_" + std::to_string(FLAGS_port) + ".sock",
        static_cast<uint16_t>(FLAGS_client_rpc_port),
        static_cast<uint32_t>(FLAGS_rpc_thread_num), FLAGS_lock_shard_count,
        string_to_byte_size(FLAGS_route_cache_max_memory),
        FLAGS_route_cache_ttl_ms, FLAGS_p2p_local_transfer_mode,
        static_cast<size_t>(FLAGS_local_memcpy_async_worker_num),
        static_cast<uint16_t>(FLAGS_http_port), FLAGS_enable_http_server, {},
        FLAGS_async_sender_thread_count, FLAGS_async_max_batch_size,
        FLAGS_async_route_queue_size, FLAGS_p2p_key_lease_duration_ms,
        FLAGS_p2p_key_lease_scan_interval_ms,
        FLAGS_p2p_transfer_direction_mode, FLAGS_runtime_config,
        FLAGS_enable_client_metric_collection,
        FLAGS_metric_report_interval_seconds, FLAGS_redis_cluster_id,
        FLAGS_redis_password, FLAGS_redis_db_index,
        FLAGS_redis_master_view_ttl_sec, FLAGS_redis_heartbeat_interval_sec,
        FLAGS_redis_username,
        static_cast<uint16_t>(FLAGS_heartbeat_rpc_port),
        static_cast<size_t>(FLAGS_te_async_poll_worker_num));

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(config);
    if (!res) {
        LOG(FATAL) << "Failed to setup P2P client: "
                   << toString(res.error());
        return -1;
    }

    coro_rpc::coro_rpc_server server(FLAGS_threads, FLAGS_port, "127.0.0.1");
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Starting P2P real client service on 127.0.0.1:"
              << FLAGS_port;
    return server.start();
}
