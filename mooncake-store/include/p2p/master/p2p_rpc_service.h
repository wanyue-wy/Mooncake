#pragma once

#include <atomic>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_master_metric_manager.h"
#include "p2p/master/p2p_master_service.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

inline constexpr uint64_t kP2PMetricReportIntervalSeconds = 10;

/**
 * @brief Standalone P2P master RPC service.
 *
 * P2P handlers use their native coro_rpc method IDs. Rolling-upgrade
 * compatibility is intentionally out of scope because this code is still
 * under development and has not been deployed.
 */
class WrappedP2PMasterService final {
   public:
    explicit WrappedP2PMasterService(const P2PMasterConfig& config,
                                     ViewVersionId view_version = 0);
    ~WrappedP2PMasterService();

    WrappedP2PMasterService(const WrappedP2PMasterService&) = delete;
    WrappedP2PMasterService& operator=(const WrappedP2PMasterService&) = delete;

    void init();

    uint16_t GetHttpPort() const { return http_server_.port(); }

    P2PMasterService& GetMasterService() { return master_service_; }
    const P2PMasterService& GetMasterService() const { return master_service_; }

    // TODO(M8): Replace ExistKey/BatchExistKey with owning
    // RouteExists/BatchRouteExists requests.
    tl::expected<bool, ErrorCode> ExistKey(std::string_view key);

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);

    // TODO(M8): Remove BatchQueryIp and GetReplicaListByRegex from the P2P
    // business RPC surface.
    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    // TODO(M8): Replace with GetReadRoute/BatchGetReadRoute and P2P route
    // descriptors.
    tl::expected<P2PGetReplicaListResponse, ErrorCode> GetReplicaList(
        std::string_view key, const P2PGetReplicaListRequestConfig& config =
                                  P2PGetReplicaListRequestConfig());

    std::vector<tl::expected<P2PGetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string_view>& keys,
                        const P2PGetReplicaListRequestConfig& config =
                            P2PGetReplicaListRequestConfig());

    // TODO(M8): Remove the master-level remove/regex/remove-all compatibility
    // RPCs from P2P.
    tl::expected<void, ErrorCode> Remove(std::string_view key,
                                         bool force = false);
    tl::expected<long, ErrorCode> RemoveByRegex(std::string_view str,
                                                bool force = false);
    long RemoveAll(bool force = false);

    // TODO(M8): Replace the positional segment parameters with owning
    // P2PMountSegmentRequest/P2PUnmountSegmentRequest DTOs.
    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);
    tl::expected<void, ErrorCode> MountSegment(const P2PSegment& segment,
                                               const UUID& client_id);

    tl::expected<P2PHeartbeatResponse, ErrorCode> Heartbeat(
        const P2PHeartbeatRequest& req);
    tl::expected<P2PQueryClientStatusResponse, ErrorCode> QueryClientStatus(
        const P2PQueryClientStatusRequest& req);
    tl::expected<P2PRegisterClientResponse, ErrorCode> RegisterClient(
        const P2PRegisterClientRequest& req);
    tl::expected<P2PUnregisterClientResponse, ErrorCode> UnregisterClient(
        const P2PUnregisterClientRequest& req);

    tl::expected<std::string, ErrorCode> ServiceReady();
    tl::expected<P2PHeartbeatServiceReadyResponse, ErrorCode>
    HeartbeatServiceReady();

    tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);

    BatchGetWriteRouteResponse BatchGetWriteRoute(
        const BatchGetWriteRouteRequest& req);

    tl::expected<void, ErrorCode> AddReplica(const AddReplicaRequest& req);

    tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);

    std::vector<tl::expected<void, ErrorCode>> BatchRemoveReplica(
        const BatchRemoveReplicaRequest& req);

    BatchSyncReplicaResponse BatchSyncReplica(
        const BatchSyncReplicaRequest& req);

    tl::expected<void, ErrorCode> SetSyncCompleted(UUID client_id);

   private:
    void init_http_server();

    P2PMasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
    uint32_t heartbeat_rpc_port_ = 0;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service,
    bool include_heartbeat = true);

void RegisterP2PHeartbeatRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service);

}  // namespace mooncake
