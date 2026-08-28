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
#include <ylt/struct_pack/md5_constexpr.hpp>
#include <ylt/util/tl/expected.hpp>
#include <ylt/util/utils.hpp>

#include "master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_master_metric_manager.h"
#include "p2p/master/p2p_master_service.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

inline constexpr uint64_t kP2PMetricReportIntervalSeconds = 10;

namespace p2p_rpc_wire {

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
 * Common handlers retain the historical WrappedMasterService wire keys while
 * using P2P-owned method implementations and DTOs.
 */
class WrappedP2PMasterService final {
   public:
    explicit WrappedP2PMasterService(
        const WrappedMasterServiceConfig& config);
    ~WrappedP2PMasterService();

    WrappedP2PMasterService(const WrappedP2PMasterService&) = delete;
    WrappedP2PMasterService& operator=(const WrappedP2PMasterService&) = delete;

    void init();

    uint16_t GetHttpPort() const { return http_server_.port(); }

    P2PMasterService& GetMasterService() { return master_service_; }
    const P2PMasterService& GetMasterService() const { return master_service_; }

    tl::expected<bool, ErrorCode> ExistKey(std::string_view key);

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
    tl::expected<void, ErrorCode> MountSegment(const P2PSegment& segment,
                                               const UUID& client_id);

    tl::expected<HeartbeatResponse, ErrorCode> Heartbeat(
        const HeartbeatRequest& req);
    tl::expected<QueryClientStatusResponse, ErrorCode> QueryClientStatus(
        const QueryClientStatusRequest& req);
    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient(
        const P2PRegisterClientRequest& req);
    tl::expected<UnregisterClientResponse, ErrorCode> UnregisterClient(
        const UnregisterClientRequest& req);

    tl::expected<std::string, ErrorCode> ServiceReady();
    tl::expected<HeartbeatServiceReadyResponse, ErrorCode>
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

namespace coro_rpc {

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::ExistKey>() {
    return mooncake::p2p_rpc_wire::kExistKey;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::BatchExistKey>() {
    return mooncake::p2p_rpc_wire::kBatchExistKey;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::BatchQueryIp>() {
    return mooncake::p2p_rpc_wire::kBatchQueryIp;
}

template <>
consteval auto
func_id<&mooncake::WrappedP2PMasterService::GetReplicaListByRegex>() {
    return mooncake::p2p_rpc_wire::kGetReplicaListByRegex;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::GetReplicaList>() {
    return mooncake::p2p_rpc_wire::kGetReplicaList;
}

template <>
consteval auto
func_id<&mooncake::WrappedP2PMasterService::BatchGetReplicaList>() {
    return mooncake::p2p_rpc_wire::kBatchGetReplicaList;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::Remove>() {
    return mooncake::p2p_rpc_wire::kRemove;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::RemoveByRegex>() {
    return mooncake::p2p_rpc_wire::kRemoveByRegex;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::RemoveAll>() {
    return mooncake::p2p_rpc_wire::kRemoveAll;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::MountSegment>() {
    return mooncake::p2p_rpc_wire::kMountSegment;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::UnmountSegment>() {
    return mooncake::p2p_rpc_wire::kUnmountSegment;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::Heartbeat>() {
    return mooncake::p2p_rpc_wire::kHeartbeat;
}

template <>
consteval auto
func_id<&mooncake::WrappedP2PMasterService::QueryClientStatus>() {
    return mooncake::p2p_rpc_wire::kQueryClientStatus;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::RegisterClient>() {
    return mooncake::p2p_rpc_wire::kRegisterClient;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::UnregisterClient>() {
    return mooncake::p2p_rpc_wire::kUnregisterClient;
}

template <>
consteval auto func_id<&mooncake::WrappedP2PMasterService::ServiceReady>() {
    return mooncake::p2p_rpc_wire::kServiceReady;
}

template <>
consteval auto
func_id<&mooncake::WrappedP2PMasterService::HeartbeatServiceReady>() {
    return mooncake::p2p_rpc_wire::kHeartbeatServiceReady;
}

}  // namespace coro_rpc
