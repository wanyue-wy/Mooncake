#include "p2p/ha/p2p_master_service_supervisor.h"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <optional>
#include <random>
#include <sstream>
#include <thread>

#include "ha_helper.h"
#include "p2p/ha/oplog/p2p_hot_standby_service.h"
#include "p2p/ha/ha_metric_manager.h"
#include "p2p/master/p2p_rpc_service.h"
#include "utils.h"
#ifdef STORE_USE_REDIS
#include "ha/redis_election_helper.h"
#include "p2p/ha/oplog/redis_oplog_store.h"
#endif

namespace mooncake {
namespace {

#ifdef STORE_USE_REDIS
class P2PRedisElectionMetricSink final : public RedisElectionMetricSink {
   public:
    void IncElectionAttempts() override {
        HAMetricManager::instance().inc_election_attempts();
    }
    void IncElectionFailures() override {
        HAMetricManager::instance().inc_election_failures();
    }
    void IncElectionLeadershipLost() override {
        HAMetricManager::instance().inc_election_leadership_lost();
    }
    void IncElectionReconnects() override {
        HAMetricManager::instance().inc_election_reconnects();
    }
    void IncElectionWatchFailures() override {
        HAMetricManager::instance().inc_election_watch_failures();
    }
    void IncElectionPollingFallbacks() override {
        HAMetricManager::instance().inc_election_polling_fallbacks();
    }
    void SetElectionIsLeader(bool value) override {
        HAMetricManager::instance().set_election_is_leader(value);
    }
    void ObserveElectionDurationMs(int64_t duration_ms) override {
        HAMetricManager::instance().observe_election_duration_ms(duration_ms);
    }
};

std::string GenerateMasterInstanceId() {
    std::random_device random;
    std::mt19937_64 generator(random());
    std::ostringstream stream;
    stream << std::hex << std::setfill('0') << std::setw(16) << generator()
           << std::setw(16) << generator();
    return stream.str();
}

std::string BuildSnapshotEndpoint(const std::string& master_endpoint,
                                  uint32_t snapshot_port,
                                  const std::string& override_endpoint) {
    if (!override_endpoint.empty()) {
        return override_endpoint;
    }
    if (snapshot_port == 0 || master_endpoint.empty()) {
        return "";
    }

    std::string host;
    if (master_endpoint.front() == '[') {
        const auto closing_bracket = master_endpoint.find(']');
        if (closing_bracket == std::string::npos) {
            return "";
        }
        host = master_endpoint.substr(0, closing_bracket + 1);
    } else {
        const auto separator = master_endpoint.rfind(':');
        host = separator == std::string::npos
                   ? master_endpoint
                   : master_endpoint.substr(0, separator);
    }
    if (host.empty() || host == "0.0.0.0" || host == "[::]") {
        return "";
    }
    return host + ":" + std::to_string(snapshot_port);
}
#endif

}  // namespace

P2PMasterServiceSupervisor::P2PMasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

int P2PMasterServiceSupervisor::Start() {
    if (config_.deployment_mode != DeploymentMode::P2P) {
        LOG(ERROR) << "P2P supervisor received non-P2P config";
        return -1;
    }

    while (true) {
        LOG(INFO) << "Init P2P master service...";
        coro_rpc::coro_rpc_server server(
            config_.rpc_thread_num, config_.rpc_port, config_.rpc_address,
            config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            server.init_ibv();
        }

        LOG(INFO) << "Init leader election helper (backend="
                  << (config_.election_backend == ElectionBackend::REDIS
                          ? "redis"
                          : "etcd")
                  << ")...";

#ifdef STORE_USE_REDIS
        P2PRedisElectionMetricSink election_metric_sink;
        auto mv_helper =
            CreateMasterViewHelper(config_, &election_metric_sink);
#else
        auto mv_helper = CreateMasterViewHelper(config_);
#endif
        if (!mv_helper) {
            LOG(ERROR) << "Failed to create leader election helper, backend="
                       << (config_.election_backend == ElectionBackend::REDIS
                               ? "redis"
                               : "etcd");
            return -1;
        }

#ifdef STORE_USE_REDIS
        std::unique_ptr<RedisMasterRegistryHeartbeat> master_registry_heartbeat;
        std::string master_instance_id;
        if (config_.enable_oplog &&
            config_.election_backend == ElectionBackend::REDIS) {
            master_instance_id = GenerateMasterInstanceId();
            RedisMasterRegistryEntry entry;
            entry.instance_id = master_instance_id;
            entry.master_endpoint = config_.local_hostname;
            entry.snapshot_endpoint = BuildSnapshotEndpoint(
                config_.local_hostname, config_.standby_snapshot_service_port,
                config_.standby_snapshot_service_endpoint);
            entry.role = "starting";
            entry.snapshot_ready = false;
            master_registry_heartbeat =
                std::make_unique<RedisMasterRegistryHeartbeat>(
                    std::make_unique<RedisMasterRegistry>(
                        config_.cluster_id, config_.redis_endpoint,
                        config_.redis_username, config_.redis_password,
                        config_.redis_db_index),
                    std::move(entry));
            if (master_registry_heartbeat->Start() != ErrorCode::OK) {
                LOG(WARNING) << "Initial Redis Master registration failed; "
                                "heartbeat will retry"
                             << ", instance_id=" << master_instance_id;
            }
        }
#endif

        std::unique_ptr<P2PHotStandbyService> standby;
        if (config_.enable_oplog) {
            if (config_.standby_snapshot_service_port >
                    std::numeric_limits<uint16_t>::max() ||
                config_.standby_snapshot_chunk_size == 0 ||
                config_.standby_snapshot_chunk_size >
                    kMaxStandbySnapshotChunkSize) {
                LOG(ERROR) << "Invalid standby snapshot configuration"
                           << ", port=" << config_.standby_snapshot_service_port
                           << ", chunk_size="
                           << config_.standby_snapshot_chunk_size;
                return -1;
            }

            P2PHotStandbyConfig standby_config;
            standby_config.cluster_id = config_.cluster_id;
            standby_config.oplog_store_type =
                ParseOpLogStoreType(config_.oplog_store_type);
            standby_config.oplog_store_root_dir = config_.oplog_data_dir;
            standby_config.redis_endpoint = config_.redis_endpoint;
            standby_config.redis_username = config_.redis_username;
            standby_config.redis_password = config_.redis_password;
            standby_config.redis_db_index = config_.redis_db_index;
            standby_config.snapshot_service_port =
                static_cast<uint16_t>(config_.standby_snapshot_service_port);
#ifdef STORE_USE_REDIS
            standby_config.master_instance_id = master_instance_id;
#endif
            standby_config.snapshot_chunk_size =
                config_.standby_snapshot_chunk_size;
            if (!config_.standby_snapshot_sources.empty()) {
                standby_config.snapshot_source_endpoints = splitString(
                    config_.standby_snapshot_sources, ',', /*trim=*/true);
            }

            standby = std::make_unique<P2PHotStandbyService>(standby_config);
            auto standby_start = standby->Start();
            if (standby_start != ErrorCode::OK) {
                LOG(ERROR) << "Failed to start P2P hot standby service"
                           << ", error=" << toString(standby_start);
                return -1;
            }
#ifdef STORE_USE_REDIS
            if (master_registry_heartbeat) {
                master_registry_heartbeat->SetAppliedSequenceProvider(
                    [service = standby.get()] {
                        return service->GetLatestAppliedSequenceId();
                    });
                master_registry_heartbeat->SetSnapshotReadyProvider(
                    [service = standby.get()] {
                        return service->IsReadyForSnapshot();
                    });
                master_registry_heartbeat->UpdateRole(
                    "standby", standby->IsReadyForSnapshot());
            }
#endif
        }

        LOG(INFO) << "Trying to elect self as leader...";
        EtcdLeaseId lease_id = 0;
        ViewVersionId view_version = 0;
        mv_helper->ElectLeader(config_.local_hostname, view_version, lease_id);

        auto keep_leader_thread =
            std::thread([&server, helper = mv_helper.get(), lease_id]() {
                helper->KeepLeader(lease_id);
                LOG(INFO) << "Trying to stop P2P server...";
                server.stop();
            });

        std::this_thread::sleep_for(
            std::chrono::seconds(mv_helper->GetLeaderLeaseTTLSeconds()));

        std::optional<P2PStandbyMetadataStore::ExportedMetadata>
            promoted_metadata;
        uint64_t promoted_sequence_id = 0;
        if (standby) {
#ifdef STORE_USE_REDIS
            if (master_registry_heartbeat) {
                master_registry_heartbeat->UpdateRole("promoting", false);
            }
#endif
            auto promote_err = standby->Promote();
            if (promote_err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to promote P2P hot standby service"
                           << ", error=" << toString(promote_err);
                mv_helper->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
            promoted_sequence_id = standby->GetLatestAppliedSequenceId();
            promoted_metadata = standby->ExportMetadata();
#ifdef STORE_USE_REDIS
            if (master_registry_heartbeat) {
                master_registry_heartbeat->SetAppliedSequenceProvider({});
                master_registry_heartbeat->SetSnapshotReadyProvider({});
            }
#endif
            standby.reset();
        }

        auto wrapped_service = std::make_unique<WrappedP2PMasterService>(
            WrappedMasterServiceConfig(config_, view_version));
        wrapped_service->init();
        if (promoted_metadata.has_value()) {
            auto restore_err =
                wrapped_service->GetMasterService().RestoreFromStandbyMetadata(
                    promoted_metadata.value(), promoted_sequence_id);
            if (restore_err != ErrorCode::OK) {
                LOG(ERROR) << "Failed to restore P2P promoted metadata"
                           << ", error=" << toString(restore_err);
                mv_helper->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
        }

        const bool dedicated_heartbeat = config_.heartbeat_rpc_port > 0;
        RegisterP2PRpcService(server, *wrapped_service,
                              /*include_heartbeat=*/!dedicated_heartbeat);
#ifdef STORE_USE_REDIS
        if (master_registry_heartbeat) {
            master_registry_heartbeat->UpdateRole("primary", false);
        }
#endif

        std::optional<coro_rpc::coro_rpc_server> heartbeat_server;
        if (dedicated_heartbeat) {
            heartbeat_server.emplace(
                std::max<size_t>(1, config_.heartbeat_rpc_thread_num),
                config_.heartbeat_rpc_port, config_.rpc_address,
                config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
            RegisterP2PHeartbeatRpcService(*heartbeat_server, *wrapped_service);
            LOG(INFO) << "Starting dedicated heartbeat RPC server on port "
                      << config_.heartbeat_rpc_port;
            auto heartbeat_ec = heartbeat_server->async_start();
            if (heartbeat_ec.hasResult()) {
                LOG(ERROR) << "Failed to start heartbeat RPC server: "
                           << heartbeat_ec.result().value();
                mv_helper->CancelKeepAlive(lease_id);
                keep_leader_thread.join();
                return -1;
            }
        }

        auto server_future = server.async_start();
        if (server_future.hasResult()) {
            LOG(ERROR) << "Failed to start P2P master service: "
                       << server_future.result().value();
            heartbeat_server.reset();
            mv_helper->CancelKeepAlive(lease_id);
            keep_leader_thread.join();
            return -1;
        }

        auto server_err = std::move(server_future).get();
        LOG(ERROR) << "P2P master service stopped: " << server_err;
        heartbeat_server.reset();
        mv_helper->CancelKeepAlive(lease_id);
        LOG(INFO) << "Cancel keep leader alive requested";
        keep_leader_thread.join();
    }
    return 0;
}

}  // namespace mooncake
