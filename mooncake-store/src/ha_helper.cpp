#include "ha_helper.h"

#include <chrono>
#include <cstdlib>
#include <string_view>
#include <thread>
#include <utility>

#include "rpc_service.h"

namespace mooncake {

MasterServiceSupervisor::MasterServiceSupervisor(
    const MasterServiceSupervisorConfig& config)
    : config_(config) {}

int MasterServiceSupervisor::Start() {
    if (config_.deployment_mode != DeploymentMode::CENTRALIZATION) {
        LOG(ERROR) << "Centralized supervisor received non-centralized config";
        return -1;
    }
    if (config_.heartbeat_rpc_port != 0) {
        LOG(ERROR) << "Dedicated heartbeat RPC is not supported by the "
                      "centralized protocol";
        return -1;
    }
    if (config_.election_backend != ElectionBackend::ETCD) {
        LOG(ERROR) << "Centralized master HA only supports etcd election";
        return -1;
    }

    while (true) {
        LOG(INFO) << "Init master service...";
        coro_rpc::coro_rpc_server server(
            config_.rpc_thread_num, config_.rpc_port, config_.rpc_address,
            config_.rpc_conn_timeout, config_.rpc_enable_tcp_no_delay);
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            server.init_ibv();
        }

        LOG(INFO) << "Init leader election helper (backend=etcd)...";
        auto mv_helper = CreateMasterViewHelper(config_);
        if (!mv_helper) {
            LOG(ERROR) << "Failed to create leader election helper";
            return -1;
        }

        LOG(INFO) << "Trying to elect self as leader...";
        EtcdLeaseId lease_id = 0;
        ViewVersionId view_version = 0;
        mv_helper->ElectLeader(config_.local_hostname, view_version, lease_id);

        auto keep_leader_thread =
            std::thread([&server, mv_helper = mv_helper.get(), lease_id]() {
                mv_helper->KeepLeader(lease_id);
                LOG(INFO) << "Trying to stop server...";
                server.stop();
            });

        std::this_thread::sleep_for(
            std::chrono::seconds(mv_helper->GetLeaderLeaseTTLSeconds()));

        LOG(INFO) << "Starting master service...";
        auto wrapped_service = std::make_unique<WrappedMasterService>(
            WrappedMasterServiceConfig(config_, view_version));
        wrapped_service->init_http_server();
        RegisterRpcService(server, *wrapped_service);

        async_simple::Future<coro_rpc::err_code> ec = server.async_start();
        if (ec.hasResult()) {
            LOG(ERROR) << "Failed to start master service: "
                       << ec.result().value();
            mv_helper->CancelKeepAlive(lease_id);
            keep_leader_thread.join();
            return -1;
        }

        auto server_err = std::move(ec).get();
        LOG(ERROR) << "Master service stopped: " << server_err;
        mv_helper->CancelKeepAlive(lease_id);
        LOG(INFO) << "Cancel keep leader alive requested";
        keep_leader_thread.join();
    }
    return 0;
}

MasterServiceSupervisor::~MasterServiceSupervisor() {
    if (server_thread_.joinable()) {
        server_thread_.join();
    }
}

}  // namespace mooncake
