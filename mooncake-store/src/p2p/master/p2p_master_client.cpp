#include "p2p/master/p2p_master_client.h"

#include "p2p/master/p2p_rpc_service.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

#define DEFINE_P2P_RPC_NAME(method)                                    \
    template <>                                                        \
    struct RpcNameTraits<&P2PMasterRpcService::method> {               \
        static constexpr const char* value = #method;                   \
    }

DEFINE_P2P_RPC_NAME(RegisterClient);
DEFINE_P2P_RPC_NAME(UnregisterClient);
DEFINE_P2P_RPC_NAME(Heartbeat);
DEFINE_P2P_RPC_NAME(QueryClientStatus);
DEFINE_P2P_RPC_NAME(MountSegment);
DEFINE_P2P_RPC_NAME(UnmountSegment);
DEFINE_P2P_RPC_NAME(RouteExists);
DEFINE_P2P_RPC_NAME(BatchRouteExists);
DEFINE_P2P_RPC_NAME(GetReadRoute);
DEFINE_P2P_RPC_NAME(BatchGetReadRoute);
DEFINE_P2P_RPC_NAME(GetWriteRoute);
DEFINE_P2P_RPC_NAME(BatchGetWriteRoute);
DEFINE_P2P_RPC_NAME(PublishRoute);
DEFINE_P2P_RPC_NAME(WithdrawRoute);
DEFINE_P2P_RPC_NAME(BatchWithdrawRoute);
DEFINE_P2P_RPC_NAME(BatchSyncRoutes);
DEFINE_P2P_RPC_NAME(CompleteRouteSync);
DEFINE_P2P_RPC_NAME(ServiceReady);
DEFINE_P2P_RPC_NAME(HeartbeatServiceReady);

#undef DEFINE_P2P_RPC_NAME

P2PMasterClient::P2PMasterClient(const UUID& client_id,
                                 MasterClientMetric* metrics)
    : client_id_(client_id), metrics_(metrics) {
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config config{};
    const char* protocol = std::getenv("MC_RPC_PROTOCOL");
    if (protocol && std::string_view(protocol) == "rdma") {
        config.client_config.socket_config = coro_io::ib_socket_t::config_t{};
    }
    pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            config);
}

ErrorCode P2PMasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Connect");
    MutexLocker lock(&connect_mutex_);
    const bool same_address = connected_address_ == master_addr;
    if (!same_address) {
        auto main_pool = pools_->at(master_addr);
        main_accessor_.SetClientPool(main_pool);
        connected_address_ = master_addr;
        if (heartbeat_rpc_port_ == 0) {
            heartbeat_accessor_.SetClientPool(main_pool);
        } else {
            const auto colon = master_addr.rfind(':');
            const std::string host = colon == std::string::npos
                                         ? master_addr
                                         : master_addr.substr(0, colon);
            heartbeat_accessor_.SetClientPool(
                pools_->at(host + ":" + std::to_string(heartbeat_rpc_port_)));
        }
    }

    auto ready = Invoke<&P2PMasterRpcService::ServiceReady, std::string>();
    if (!ready.has_value() && same_address) {
        ready = Invoke<&P2PMasterRpcService::ServiceReady, std::string>();
    }
    if (!ready.has_value()) {
        connected_address_.clear();
        return ready.error();
    }
    if (*ready != GetMooncakeStoreVersion()) {
        LOG(ERROR) << "P2P master version mismatch"
                   << ", server=" << *ready
                   << ", client=" << GetMooncakeStoreVersion();
        connected_address_.clear();
        return ErrorCode::INVALID_VERSION;
    }

    auto heartbeat_ready =
        Invoke<&P2PMasterRpcService::HeartbeatServiceReady,
               P2PHeartbeatServiceReadyResponse>();
    if (!heartbeat_ready.has_value()) {
        connected_address_.clear();
        return heartbeat_ready.error();
    }
    if ((heartbeat_rpc_port_ > 0) !=
        (heartbeat_ready->heartbeat_rpc_port > 0)) {
        LOG(ERROR) << "P2P heartbeat routing mismatch"
                   << ", client_port=" << heartbeat_rpc_port_
                   << ", server_port="
                   << heartbeat_ready->heartbeat_rpc_port;
        connected_address_.clear();
        return ErrorCode::HEARTBEAT_ROUTING_MISMATCH;
    }

    auto heartbeat_probe =
        InvokeVia<&P2PMasterRpcService::ServiceReady, std::string>(
            heartbeat_accessor_);
    if (!heartbeat_probe.has_value()) {
        connected_address_.clear();
        return heartbeat_probe.error();
    }
    return ErrorCode::OK;
}

auto P2PMasterClient::RegisterClient(
    const P2PRegisterClientRequest& request)
    -> tl::expected<P2PRegisterClientResponse, ErrorCode> {
    return Invoke<&P2PMasterRpcService::RegisterClient,
                  P2PRegisterClientResponse>(request);
}

auto P2PMasterClient::UnregisterClient(
    const P2PUnregisterClientRequest& request)
    -> tl::expected<P2PUnregisterClientResponse, ErrorCode> {
    return Invoke<&P2PMasterRpcService::UnregisterClient,
                  P2PUnregisterClientResponse>(request);
}

auto P2PMasterClient::Heartbeat(const P2PHeartbeatRequest& request)
    -> tl::expected<P2PHeartbeatResponse, ErrorCode> {
    return InvokeVia<&P2PMasterRpcService::Heartbeat, P2PHeartbeatResponse>(
        heartbeat_accessor_, request);
}

auto P2PMasterClient::QueryClientStatus(const UUID& client_id)
    -> tl::expected<P2PQueryClientStatusResponse, ErrorCode> {
    return Invoke<&P2PMasterRpcService::QueryClientStatus,
                  P2PQueryClientStatusResponse>(
        P2PQueryClientStatusRequest{.client_id = client_id});
}

auto P2PMasterClient::MountSegment(const P2PSegment& segment)
    -> tl::expected<void, ErrorCode> {
    return Invoke<&P2PMasterRpcService::MountSegment, void>(
        P2PMountSegmentRequest{.client_id = client_id_, .segment = segment});
}

auto P2PMasterClient::UnmountSegment(const UUID& segment_id)
    -> tl::expected<void, ErrorCode> {
    return Invoke<&P2PMasterRpcService::UnmountSegment, void>(
        P2PUnmountSegmentRequest{.client_id = client_id_,
                                 .segment_id = segment_id});
}

auto P2PMasterClient::RouteExists(std::string_view key)
    -> tl::expected<bool, ErrorCode> {
    auto result = Invoke<&P2PMasterRpcService::RouteExists,
                         P2PRouteExistsResponse>(
        P2PRouteExistsRequest{.key = std::string(key)});
    if (!result.has_value()) {
        return tl::make_unexpected(result.error());
    }
    return result->exists;
}

auto P2PMasterClient::BatchRouteExists(
    const std::vector<std::string_view>& keys)
    -> std::vector<tl::expected<bool, ErrorCode>> {
    P2PBatchRouteExistsRequest request;
    request.keys.reserve(keys.size());
    for (std::string_view key : keys) {
        request.keys.emplace_back(key);
    }
    auto result = Invoke<&P2PMasterRpcService::BatchRouteExists,
                         P2PBatchRouteExistsResponse>(request);
    std::vector<tl::expected<bool, ErrorCode>> response;
    response.reserve(keys.size());
    if (!result.has_value() || result->responses.size() != keys.size() ||
        result->error_codes.size() != keys.size()) {
        const auto error = result.has_value() ? ErrorCode::RPC_FAIL
                                              : result.error();
        response.assign(keys.size(), tl::make_unexpected(error));
        return response;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        if (result->error_codes[i] == ErrorCode::OK) {
            response.emplace_back(result->responses[i].exists);
        } else {
            response.emplace_back(
                tl::make_unexpected(result->error_codes[i]));
        }
    }
    return response;
}

auto P2PMasterClient::GetReadRoute(std::string_view key,
                                   const P2PReadRouteConfig& config)
    -> tl::expected<P2PGetReadRouteResponse, ErrorCode> {
    return Invoke<&P2PMasterRpcService::GetReadRoute,
                  P2PGetReadRouteResponse>(
        P2PGetReadRouteRequest{.key = std::string(key), .config = config});
}

auto P2PMasterClient::AsyncGetReadRoute(
    std::string_view key, const P2PReadRouteConfig& config)
    -> async_simple::coro::Lazy<
        tl::expected<P2PGetReadRouteResponse, ErrorCode>> {
    return InvokeAsync<&P2PMasterRpcService::GetReadRoute,
                       P2PGetReadRouteResponse>(
        P2PGetReadRouteRequest{.key = std::string(key), .config = config});
}

auto P2PMasterClient::BatchGetReadRoute(
    const std::vector<std::string_view>& keys,
    const P2PReadRouteConfig& config)
    -> std::vector<tl::expected<P2PGetReadRouteResponse, ErrorCode>> {
    P2PBatchGetReadRouteRequest request;
    request.config = config;
    request.keys.reserve(keys.size());
    for (std::string_view key : keys) {
        request.keys.emplace_back(key);
    }
    auto result = Invoke<&P2PMasterRpcService::BatchGetReadRoute,
                         P2PBatchGetReadRouteResponse>(request);
    std::vector<tl::expected<P2PGetReadRouteResponse, ErrorCode>> response;
    response.reserve(keys.size());
    if (!result.has_value() || result->responses.size() != keys.size() ||
        result->error_codes.size() != keys.size()) {
        const auto error = result.has_value() ? ErrorCode::RPC_FAIL
                                              : result.error();
        response.assign(keys.size(), tl::make_unexpected(error));
        return response;
    }
    for (size_t i = 0; i < keys.size(); ++i) {
        if (result->error_codes[i] == ErrorCode::OK) {
            response.push_back(std::move(result->responses[i]));
        } else {
            response.emplace_back(
                tl::make_unexpected(result->error_codes[i]));
        }
    }
    return response;
}

auto P2PMasterClient::GetWriteRoute(
    const P2PGetWriteRouteRequest& request)
    -> tl::expected<P2PGetWriteRouteResponse, ErrorCode> {
    return Invoke<&P2PMasterRpcService::GetWriteRoute,
                  P2PGetWriteRouteResponse>(request);
}

auto P2PMasterClient::BatchGetWriteRoute(
    const P2PBatchGetWriteRouteRequest& request)
    -> tl::expected<P2PBatchGetWriteRouteResponse, ErrorCode> {
    return Invoke<&P2PMasterRpcService::BatchGetWriteRoute,
                  P2PBatchGetWriteRouteResponse>(request);
}

auto P2PMasterClient::PublishRoute(const P2PPublishRouteRequest& request)
    -> tl::expected<void, ErrorCode> {
    return Invoke<&P2PMasterRpcService::PublishRoute, void>(request);
}

auto P2PMasterClient::WithdrawRoute(const P2PWithdrawRouteRequest& request)
    -> tl::expected<void, ErrorCode> {
    return Invoke<&P2PMasterRpcService::WithdrawRoute, void>(request);
}

auto P2PMasterClient::BatchWithdrawRoute(
    const P2PBatchWithdrawRouteRequest& request)
    -> std::vector<tl::expected<void, ErrorCode>> {
    auto result = Invoke<&P2PMasterRpcService::BatchWithdrawRoute,
                         P2PBatchWithdrawRouteResponse>(request);
    std::vector<tl::expected<void, ErrorCode>> response;
    response.reserve(request.segment_ids.size());
    if (!result.has_value() ||
        result->error_codes.size() != request.segment_ids.size()) {
        const auto error = result.has_value() ? ErrorCode::RPC_FAIL
                                              : result.error();
        response.assign(request.segment_ids.size(),
                        tl::make_unexpected(error));
        return response;
    }
    for (auto error : result->error_codes) {
        if (error == ErrorCode::OK) {
            response.emplace_back();
        } else {
            response.emplace_back(tl::make_unexpected(error));
        }
    }
    return response;
}

auto P2PMasterClient::BatchSyncRoutes(
    const P2PBatchSyncRoutesRequest& request)
    -> tl::expected<P2PBatchSyncRoutesResponse, ErrorCode> {
    return Invoke<&P2PMasterRpcService::BatchSyncRoutes,
                  P2PBatchSyncRoutesResponse>(request);
}

auto P2PMasterClient::CompleteRouteSync(const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    return Invoke<&P2PMasterRpcService::CompleteRouteSync, void>(
        P2PCompleteRouteSyncRequest{.client_id = client_id});
}

}  // namespace mooncake
