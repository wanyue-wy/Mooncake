#pragma once

#include <csignal>

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
#include <ylt/struct_pack/md5_constexpr.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_config.h"
#include "p2p/master/p2p_master_metric_manager.h"
#include "p2p/master/p2p_master_service.h"
#include "p2p/master/p2p_rpc_types.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

// Interval between periodic master metric report log lines.
static const uint64_t kP2PMetricReportIntervalSeconds = 10;

namespace p2p_rpc_wire {

// coro_rpc routes a request by the MD5-32 hash of the handler's
// fully-qualified function name (class name included, see
// ylt/util/function_name.h). The common master RPC surface has historically
// been served by WrappedMasterService, and deployed clients address those
// handlers with keys derived from that class name. The standalone P2P
// service keeps serving the very same keys via frozen literals below, so the
// P2P stack stays wire-compatible with existing clients without referencing
// any centralized class from p2p/ code.
constexpr uint32_t Key(std::string_view name) {
    return struct_pack::MD5::MD5Hash32Constexpr(name.data(), name.length());
}

inline constexpr uint32_t kExistKey =
    Key("mooncake::WrappedMasterService::ExistKey");
inline constexpr uint32_t kBatchExistKey =
    Key("mooncake::WrappedMasterService::BatchExistKey");
inline constexpr uint32_t kBatchQueryIp =
    Key("mooncake::WrappedMasterService::BatchQueryIp");
inline constexpr uint32_t kGetReplicaListByRegex =
    Key("mooncake::WrappedMasterService::GetReplicaListByRegex");
inline constexpr uint32_t kGetReplicaList =
    Key("mooncake::WrappedMasterService::GetReplicaList");
inline constexpr uint32_t kBatchGetReplicaList =
    Key("mooncake::WrappedMasterService::BatchGetReplicaList");
inline constexpr uint32_t kRemove =
    Key("mooncake::WrappedMasterService::Remove");
inline constexpr uint32_t kRemoveByRegex =
    Key("mooncake::WrappedMasterService::RemoveByRegex");
inline constexpr uint32_t kRemoveAll =
    Key("mooncake::WrappedMasterService::RemoveAll");
inline constexpr uint32_t kMountSegment =
    Key("mooncake::WrappedMasterService::MountSegment");
inline constexpr uint32_t kUnmountSegment =
    Key("mooncake::WrappedMasterService::UnmountSegment");
inline constexpr uint32_t kHeartbeat =
    Key("mooncake::WrappedMasterService::Heartbeat");
inline constexpr uint32_t kQueryClientStatus =
    Key("mooncake::WrappedMasterService::QueryClientStatus");
inline constexpr uint32_t kRegisterClient =
    Key("mooncake::WrappedMasterService::RegisterClient");
inline constexpr uint32_t kUnregisterClient =
    Key("mooncake::WrappedMasterService::UnregisterClient");
inline constexpr uint32_t kServiceReady =
    Key("mooncake::WrappedMasterService::ServiceReady");
inline constexpr uint32_t kHeartbeatServiceReady =
    Key("mooncake::WrappedMasterService::HeartbeatServiceReady");

}  // namespace p2p_rpc_wire

/**
 * @brief Standalone P2P master RPC service.
 *
 * Owns a P2PMasterService and exposes the full master RPC surface:
 * - the common master RPCs (registered under the historical
 *   WrappedMasterService wire keys, see p2p_rpc_wire above);
 * - the P2P-specific RPCs (GetWriteRoute / AddReplica / ...).
 *
 * It also runs the embedded HTTP server (/metrics, /metrics/summary,
 * /get_all_keys, /get_key_count, /health, /batch_query_keys) and the
 * periodic metric report thread.
 */
class WrappedP2PMasterService {
   public:
    WrappedP2PMasterService(const WrappedMasterServiceConfig& config);

    ~WrappedP2PMasterService();

    WrappedP2PMasterService(const WrappedP2PMasterService&) = delete;
    WrappedP2PMasterService& operator=(const WrappedP2PMasterService&) =
        delete;

    void init();

    uint16_t GetHttpPort() const { return http_server_.port(); }

    P2PMasterService& GetMasterService() { return master_service_; }
    const P2PMasterService& GetMasterService() const {
        return master_service_;
    }

    // ---- Common master RPC surface (wire-compatible with the historical
    // WrappedMasterService handlers) ----

    tl::expected<bool, ErrorCode> ExistKey(std::string_view key);

    tl::expected<P2PMasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);

    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        std::string_view key, const GetReplicaListRequestConfig& config =
                                  GetReplicaListRequestConfig());

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string_view>& keys,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig());

    tl::expected<void, ErrorCode> Remove(std::string_view key,
                                         bool force = false);

    tl::expected<long, ErrorCode> RemoveByRegex(std::string_view str,
                                                bool force = false);

    long RemoveAll(bool force = false);

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);

    tl::expected<HeartbeatResponse, ErrorCode> Heartbeat(
        const HeartbeatRequest& req);

    tl::expected<QueryClientStatusResponse, ErrorCode> QueryClientStatus(
        const QueryClientStatusRequest& req);

    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient(
        const RegisterClientRequest& req);

    tl::expected<UnregisterClientResponse, ErrorCode> UnregisterClient(
        const UnregisterClientRequest& req);

    tl::expected<std::string, ErrorCode> ServiceReady();

    // Reports the master's heartbeat routing (dedicated port vs legacy main
    // server) so clients can detect a heartbeat-port mismatch at Connect time.
    tl::expected<HeartbeatServiceReadyResponse, ErrorCode>
    HeartbeatServiceReady();

    // ---- P2P-specific RPC surface ----

    /**
     * @brief Get write route based on the config in the request
     */
    tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);

    /**
     * @brief Batch get write routes for multiple keys.
     *        Reuses GetWriteRoute logic per key.
     */
    BatchGetWriteRouteResponse BatchGetWriteRoute(
        const BatchGetWriteRouteRequest& req);

    /**
     * @brief Add a route replica to master
     */
    tl::expected<void, ErrorCode> AddReplica(const AddReplicaRequest& req);

    /**
     * @brief Remove a route replica from master
     */
    tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);

    /**
     * @brief Remove replicas from multiple segments in one call
     */
    std::vector<tl::expected<void, ErrorCode>> BatchRemoveReplica(
        const BatchRemoveReplicaRequest& req);

    /**
     * @brief Batch sync replicas with mixed ADD and REMOVE ops
     */
    BatchSyncReplicaResponse BatchSyncReplica(
        const BatchSyncReplicaRequest& req);

    /**
     * @brief Client notifies Master that metadata sync is complete
     */
    tl::expected<void, ErrorCode> SetSyncCompleted(UUID client_id);

   private:
    void init_http_server();

    P2PMasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
    // Dedicated heartbeat RPC server port configured on this master
    // (0 = legacy: Heartbeat served on the main server).
    uint32_t heartbeat_rpc_port_ = 0;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service,
    bool include_heartbeat = true);

// Registers only the Heartbeat handler (plus the ServiceReady probe) on a
// dedicated heartbeat server for the standalone P2P service. Used by the
// priority-scheduling path to serve heartbeats on a separate coro_rpc_server
// so heavy RPCs cannot head-of-line-block them.
void RegisterP2PHeartbeatRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service);

}  // namespace mooncake
