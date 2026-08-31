#include "p2p/master/p2p_master_server.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <utility>

#include <glog/logging.h>

#include "p2p/master/p2p_rpc_service.h"
#include "types.h"

namespace mooncake {

P2PMasterServer::P2PMasterServer(const P2PMasterConfig& config,
                                 ViewVersionId view_version)
    : config_(config),
      view_version_(view_version),
      server_(config.rpc_thread_num, config.rpc_port, config.rpc_address,
              std::chrono::seconds(config.rpc_conn_timeout_seconds),
              config.rpc_enable_tcp_no_delay) {
    const char* protocol = std::getenv("MC_RPC_PROTOCOL");
    if (protocol && std::string_view(protocol) == "rdma") {
        server_.init_ibv();
    }
}

void P2PMasterServer::SetViewVersion(ViewVersionId view_version) {
    view_version_ = view_version;
}

void P2PMasterServer::SetPromotedMetadata(
    P2PStandbyMetadataStore::ExportedMetadata metadata,
    uint64_t last_applied_sequence_id) {
    promoted_metadata_ = std::move(metadata);
    promoted_sequence_id_ = last_applied_sequence_id;
}

int P2PMasterServer::Run(std::function<void()> before_start) {
    auto wrapped_service = std::make_unique<WrappedP2PMasterService>(
        config_.BuildRpcConfig(view_version_));
    wrapped_service->init();

    if (promoted_metadata_.has_value()) {
        auto restore_err =
            wrapped_service->GetMasterService().RestoreFromStandbyMetadata(
                promoted_metadata_.value(), promoted_sequence_id_);
        if (restore_err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to restore P2P promoted metadata"
                       << ", error=" << toString(restore_err);
            return -1;
        }
    }

    const bool dedicated_heartbeat = config_.heartbeat_rpc_port > 0;
    RegisterP2PRpcService(server_, *wrapped_service,
                          /*include_heartbeat=*/!dedicated_heartbeat);
    if (before_start) {
        before_start();
    }

    std::optional<coro_rpc::coro_rpc_server> heartbeat_server;
    if (dedicated_heartbeat) {
        heartbeat_server.emplace(
            std::max<uint32_t>(1u, config_.heartbeat_rpc_thread_num),
            config_.heartbeat_rpc_port, config_.rpc_address,
            std::chrono::seconds(config_.rpc_conn_timeout_seconds),
            config_.rpc_enable_tcp_no_delay);
        RegisterP2PHeartbeatRpcService(*heartbeat_server, *wrapped_service);
        LOG(INFO) << "Starting dedicated heartbeat RPC server on port "
                  << config_.heartbeat_rpc_port;
        auto heartbeat_ec = heartbeat_server->async_start();
        if (heartbeat_ec.hasResult()) {
            LOG(ERROR) << "Failed to start heartbeat RPC server: "
                       << heartbeat_ec.result().value();
            return -1;
        }
    }

    if (!config_.enable_ha) {
        return server_.start();
    }

    auto server_future = server_.async_start();
    if (server_future.hasResult()) {
        LOG(ERROR) << "Failed to start master service: "
                   << server_future.result().value();
        heartbeat_server.reset();
        return -1;
    }

    auto server_err = std::move(server_future).get();
    LOG(ERROR) << "Master service stopped: " << server_err;
    heartbeat_server.reset();
    return 0;
}

void P2PMasterServer::Stop() { server_.stop(); }

}  // namespace mooncake
