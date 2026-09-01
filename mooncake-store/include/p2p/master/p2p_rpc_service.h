#pragma once

#include <atomic>
#include <csignal>
#include <cstdint>
#include <string>
#include <thread>

#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_master_service.h"

namespace mooncake {

inline constexpr uint64_t kP2PMetricReportIntervalSeconds = 10;

/** @brief RPC/HTTP boundary for the standalone P2P master protocol. */
class P2PMasterRpcService final {
   public:
    explicit P2PMasterRpcService(const P2PMasterConfig& config,
                                 ViewVersionId view_version = 0);
    ~P2PMasterRpcService();

    P2PMasterRpcService(const P2PMasterRpcService&) = delete;
    P2PMasterRpcService& operator=(const P2PMasterRpcService&) = delete;

    void init();
    uint16_t GetHttpPort() const { return http_server_.port(); }
    P2PMasterService& GetMasterService() { return master_service_; }
    const P2PMasterService& GetMasterService() const { return master_service_; }

    auto RegisterClient(const P2PRegisterClientRequest& request)
        -> tl::expected<P2PRegisterClientResponse, ErrorCode>;
    auto UnregisterClient(const P2PUnregisterClientRequest& request)
        -> tl::expected<P2PUnregisterClientResponse, ErrorCode>;
    auto Heartbeat(const P2PHeartbeatRequest& request)
        -> tl::expected<P2PHeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const P2PQueryClientStatusRequest& request)
        -> tl::expected<P2PQueryClientStatusResponse, ErrorCode>;

    auto MountSegment(const P2PMountSegmentRequest& request)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const P2PUnmountSegmentRequest& request)
        -> tl::expected<void, ErrorCode>;

    auto RouteExists(const P2PRouteExistsRequest& request)
        -> tl::expected<P2PRouteExistsResponse, ErrorCode>;
    auto BatchRouteExists(const P2PBatchRouteExistsRequest& request)
        -> P2PBatchRouteExistsResponse;
    auto GetReadRoute(const P2PGetReadRouteRequest& request)
        -> tl::expected<P2PGetReadRouteResponse, ErrorCode>;
    auto BatchGetReadRoute(const P2PBatchGetReadRouteRequest& request)
        -> P2PBatchGetReadRouteResponse;

    auto GetWriteRoute(const P2PGetWriteRouteRequest& request)
        -> tl::expected<P2PGetWriteRouteResponse, ErrorCode>;
    auto BatchGetWriteRoute(const P2PBatchGetWriteRouteRequest& request)
        -> P2PBatchGetWriteRouteResponse;

    auto PublishRoute(const P2PPublishRouteRequest& request)
        -> tl::expected<void, ErrorCode>;
    auto WithdrawRoute(const P2PWithdrawRouteRequest& request)
        -> tl::expected<void, ErrorCode>;
    auto BatchWithdrawRoute(const P2PBatchWithdrawRouteRequest& request)
        -> P2PBatchWithdrawRouteResponse;
    auto BatchSyncRoutes(const P2PBatchSyncRoutesRequest& request)
        -> P2PBatchSyncRoutesResponse;
    auto CompleteRouteSync(const P2PCompleteRouteSyncRequest& request)
        -> tl::expected<void, ErrorCode>;

    auto ServiceReady() -> tl::expected<std::string, ErrorCode>;
    auto HeartbeatServiceReady()
        -> tl::expected<P2PHeartbeatServiceReadyResponse, ErrorCode>;

   private:
    void init_http_server();

    P2PMasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
    uint32_t heartbeat_rpc_port_{0};
};

void RegisterP2PRpcService(coro_rpc::coro_rpc_server& server,
                           P2PMasterRpcService& service,
                           bool include_heartbeat = true);
void RegisterP2PHeartbeatRpcService(coro_rpc::coro_rpc_server& server,
                                    P2PMasterRpcService& service);

}  // namespace mooncake
