#include "p2p/master/p2p_master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <csignal>
#include <string>
#include <string_view>
#include <vector>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/master/p2p_rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

template <>
struct RpcNameTraits<&WrappedP2PMasterService::ExistKey> {
    static constexpr const char* value = "ExistKey";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchExistKey> {
    static constexpr const char* value = "BatchExistKey";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchQueryIp> {
    static constexpr const char* value = "BatchQueryIp";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::GetReplicaListByRegex> {
    static constexpr const char* value = "GetReplicaListByRegex";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::GetReplicaList> {
    static constexpr const char* value = "GetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchGetReplicaList> {
    static constexpr const char* value = "BatchGetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::Remove> {
    static constexpr const char* value = "Remove";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::RemoveByRegex> {
    static constexpr const char* value = "RemoveByRegex";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::RemoveAll> {
    static constexpr const char* value = "RemoveAll";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::MountSegment> {
    static constexpr const char* value = "MountSegment";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::UnmountSegment> {
    static constexpr const char* value = "UnmountSegment";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::Heartbeat> {
    static constexpr const char* value = "Heartbeat";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::RegisterClient> {
    static constexpr const char* value = "RegisterClient";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::UnregisterClient> {
    static constexpr const char* value = "UnregisterClient";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::QueryClientStatus> {
    static constexpr const char* value = "QueryClientStatus";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::ServiceReady> {
    static constexpr const char* value = "ServiceReady";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::HeartbeatServiceReady> {
    static constexpr const char* value = "HeartbeatServiceReady";
};

ErrorCode P2PMasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    bool is_same_addr = (client_addr_param_ == master_addr);
    if (!is_same_addr) {
        // WARNING: The existing client pool cannot be erased. So if there are a
        // lot of different addresses, there will be resource leak problems.
        auto client_pool = client_pools_->at(master_addr);
        client_accessor_.SetClientPool(client_pool);
        client_addr_param_ = master_addr;
        // Route heartbeats to the dedicated heartbeat server when configured.
        // The heartbeat endpoint is the same host as the master with the
        // dedicated heartbeat port, so it follows the leader automatically on
        // HA failover. When no dedicated port is set, heartbeats use the main
        // pool (legacy behavior).
        if (heartbeat_rpc_port_ > 0) {
            auto colon = master_addr.rfind(':');
            std::string host = (colon == std::string::npos)
                                   ? master_addr
                                   : master_addr.substr(0, colon);
            std::string heartbeat_addr =
                host + ":" + std::to_string(heartbeat_rpc_port_);
            heartbeat_accessor_.SetClientPool(
                client_pools_->at(heartbeat_addr));
        } else {
            heartbeat_accessor_.SetClientPool(client_pool);
        }
    }
    // The client pool does not have native connection check method, so we need
    // to use custom ServiceReady API.
    auto result =
        invoke_rpc<&WrappedP2PMasterService::ServiceReady, std::string>();
    if (!result.has_value() && is_same_addr) {
        timer.LogResponse("error_code=", result.error());
        // Stale connection pool might still exist.
        // Retrying once will force the pool to re-establish a new connection.
        result = invoke_rpc<&WrappedP2PMasterService::ServiceReady, std::string>();
    }

    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        client_addr_param_.clear();
        return result.error();
    }
    // Check if server version matches client version
    std::string server_version = result.value();
    std::string client_version = GetMooncakeStoreVersion();
    if (server_version != client_version) {
        LOG(ERROR) << "Version mismatch: server=" << server_version
                   << " client=" << client_version;
        timer.LogResponse("error_code=", ErrorCode::INVALID_VERSION);
        return ErrorCode::INVALID_VERSION;
    }
    // Ask the master how it routes heartbeats, then verify it matches the
    // client's expectation. Catches both mismatch directions at startup:
    //   - client expects a dedicated heartbeat server the master never opened
    //   - client is legacy but the master dropped Heartbeat from the main
    //     server in favor of a dedicated port
    // Either direction would otherwise silently starve heartbeats until the
    // client gets reaped (client_live_ttl expiry, segment reclaim).
    auto hb_ready = invoke_rpc<&WrappedP2PMasterService::HeartbeatServiceReady,
                               HeartbeatServiceReadyResponse>();
    if (!hb_ready.has_value()) {
        LOG(ERROR) << "HeartbeatServiceReady probe failed: error_code="
                   << hb_ready.error()
                   << " (master may predate this RPC; upgrade master first)";
        timer.LogResponse("error_code=", hb_ready.error());
        client_addr_param_.clear();
        return hb_ready.error();
    }
    const bool client_dedicated = heartbeat_rpc_port_ > 0;
    const bool master_dedicated = hb_ready->heartbeat_rpc_port > 0;
    if (client_dedicated != master_dedicated) {
        LOG(ERROR) << "Heartbeat routing mismatch: client_hb_port="
                   << heartbeat_rpc_port_
                   << " master_hb_port=" << hb_ready->heartbeat_rpc_port
                   << " (one side is dedicated, the other is legacy)";
        timer.LogResponse("error_code=", ErrorCode::HEARTBEAT_ROUTING_MISMATCH);
        client_addr_param_.clear();
        return ErrorCode::HEARTBEAT_ROUTING_MISMATCH;
    }
    // Both sides dedicated: confirm the dedicated heartbeat server is actually
    // reachable (catches a configured-but-dead dedicated server). Mirrors the
    // main-pool stale-connection retry above when reconnecting to the same
    // address.
    if (client_dedicated) {
        auto hb_result =
            invoke_rpc_via<&WrappedP2PMasterService::ServiceReady, std::string>(
                heartbeat_accessor_);
        if (!hb_result.has_value() && is_same_addr) {
            hb_result = invoke_rpc_via<&WrappedP2PMasterService::ServiceReady,
                                       std::string>(heartbeat_accessor_);
        }
        if (!hb_result.has_value()) {
            LOG(ERROR) << "Dedicated heartbeat RPC server unreachable at"
                       << " heartbeat_rpc_port=" << heartbeat_rpc_port_
                       << ": error_code=" << hb_result.error();
            timer.LogResponse("error_code=",
                              ErrorCode::HEARTBEAT_RPC_UNREACHABLE);
            client_addr_param_.clear();
            return ErrorCode::HEARTBEAT_RPC_UNREACHABLE;
        }
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return ErrorCode::OK;
}

tl::expected<bool, ErrorCode> P2PMasterClient::ExistKey(
    std::string_view object_key) {
    ScopedVLogTimer timer(1, "P2PMasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedP2PMasterService::ExistKey, bool>(object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> P2PMasterClient::BatchExistKey(
    const std::vector<std::string_view>& object_keys) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result = invoke_batch_rpc<&WrappedP2PMasterService::BatchExistKey, bool>(
        object_keys.size(), object_keys);
    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<GetReplicaListResponse, ErrorCode> P2PMasterClient::GetReplicaList(
    std::string_view key, const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "P2PMasterClient::GetReplicaList");
    timer.LogRequest("object_key=", key);

    auto result = invoke_rpc<&WrappedP2PMasterService::GetReplicaList,
                             GetReplicaListResponse>(key, config);
    timer.LogResponseExpected(result);
    return result;
}

async_simple::coro::Lazy<tl::expected<GetReplicaListResponse, ErrorCode>>
P2PMasterClient::AsyncGetReplicaList(std::string_view key,
                                  const GetReplicaListRequestConfig& config) {
    auto result =
        co_await invoke_rpc_async<&WrappedP2PMasterService::GetReplicaList,
                                  GetReplicaListResponse>(key, config);
    co_return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
P2PMasterClient::BatchGetReplicaList(const std::vector<std::string_view>& keys,
                                  const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchGetReplicaList");
    timer.LogRequest("requests_count=", keys.size());

    if (keys.empty()) {
        return {};
    }

    auto result = invoke_rpc<
        &WrappedP2PMasterService::BatchGetReplicaList,
        std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>>(keys,
                                                                      config);
    if (result.has_value()) {
        timer.LogResponse("result=", result.value().size(), " requests");
    }
    return result.value();
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
P2PMasterClient::BatchQueryIp(const std::vector<UUID>& client_ids) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchQueryIp");
    timer.LogRequest("client_ids_count=", client_ids.size());

    auto result = invoke_rpc<
        &WrappedP2PMasterService::BatchQueryIp,
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>>(
        client_ids);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
P2PMasterClient::GetReplicaListByRegex(const std::string& str) {
    ScopedVLogTimer timer(1, "P2PMasterClient::GetReplicaListByRegex");
    timer.LogRequest("Regex=", str);

    auto result = invoke_rpc<
        &WrappedP2PMasterService::GetReplicaListByRegex,
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>>(str);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::Remove(std::string_view key,
                                                   bool force) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Remove");
    timer.LogRequest("key=", key, ", force=", force);

    auto result = invoke_rpc<&WrappedP2PMasterService::Remove, void>(key, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> P2PMasterClient::RemoveByRegex(std::string_view str,
                                                          bool force) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RemoveByRegex");
    timer.LogRequest("key=", str, ", force=", force);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::RemoveByRegex, long>(str, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> P2PMasterClient::RemoveAll(bool force) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);

    auto result = invoke_rpc<&WrappedP2PMasterService::RemoveAll, long>(force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::UnmountSegment(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedP2PMasterService::UnmountSegment, void>(
        segment_id, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<HeartbeatResponse, ErrorCode> P2PMasterClient::Heartbeat(
    const HeartbeatRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Heartbeat");
    timer.LogRequest("client_id=", client_id_);

    // Send via the dedicated heartbeat accessor (separate pool that targets the
    // master's heartbeat server when configured, else the main pool).
    auto result =
        invoke_rpc_via<&WrappedP2PMasterService::Heartbeat, HeartbeatResponse>(
            heartbeat_accessor_, req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<QueryClientStatusResponse, ErrorCode>
P2PMasterClient::QueryClientStatus(const UUID& client_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::QueryClientStatus");
    timer.LogRequest("client_id=", client_id);

    QueryClientStatusRequest req;
    req.client_id = client_id;

    auto result = invoke_rpc<&WrappedP2PMasterService::QueryClientStatus,
                             QueryClientStatusResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::MountSegment(
    const Segment& segment) {
    ScopedVLogTimer timer(1, "P2PMasterClient::MountSegment");
    timer.LogRequest("segment_name=", segment.name, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedP2PMasterService::MountSegment, void>(
        segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<RegisterClientResponse, ErrorCode> P2PMasterClient::RegisterClient(
    const RegisterClientRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RegisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", segments_count=", req.segments.size(),
                     ", deployment_mode=", req.deployment_mode);

    auto result = invoke_rpc<&WrappedP2PMasterService::RegisterClient,
                             RegisterClientResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UnregisterClientResponse, ErrorCode>
P2PMasterClient::UnregisterClient(const UnregisterClientRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::UnregisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", deployment_mode=", req.deployment_mode);

    auto result = invoke_rpc<&WrappedP2PMasterService::UnregisterClient,
                             UnregisterClientResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
