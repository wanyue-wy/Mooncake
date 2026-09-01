#pragma once

#include <boost/functional/hash.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/client/heartbeat_type.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_client_meta.h"
#include "types.h"

namespace mooncake {

/** @brief Owns P2P client registration, heartbeat and monitor lifecycle. */
class P2PClientManager final {
   public:
    P2PClientManager(int64_t disconnect_timeout_sec,
                     int64_t crash_timeout_sec, ViewVersionId view_version);
    ~P2PClientManager();

    void Start();
    void Stop();

    auto RegisterClient(const P2PRegisterClientRequest& req)
        -> tl::expected<P2PRegisterClientResponse, ErrorCode>;
    auto UnregisterClient(const P2PUnregisterClientRequest& req)
        -> tl::expected<P2PUnregisterClientResponse, ErrorCode>;
    auto Heartbeat(const P2PHeartbeatRequest& req)
        -> tl::expected<P2PHeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const P2PQueryClientStatusRequest& req)
        -> tl::expected<P2PQueryClientStatusResponse, ErrorCode>;

    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& client_id, const UUID& segment_id)
        -> tl::expected<P2PSegment, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto GetClient(const UUID& client_id) const
        -> std::shared_ptr<P2PClientMeta>;
    auto GetAllClients() const
        -> std::vector<std::shared_ptr<P2PClientMeta>>;

    using ClientVisitor = std::function<tl::expected<bool, ErrorCode>(
        const std::shared_ptr<P2PClientMeta>& client)>;
    auto ListClients(P2PClientSelectionStrategy strategy) const
        -> tl::expected<std::vector<std::shared_ptr<P2PClientMeta>>, ErrorCode>;
    auto ForEachClient(P2PClientSelectionStrategy strategy,
                       const ClientVisitor& visitor)
        -> tl::expected<void, ErrorCode>;

    using SegmentRemovalCallback =
        std::function<void(const P2PRouteLocation& location)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback callback);

   private:
    static constexpr uint64_t kClientMonitorSleepMs = 1000;

    void ClientMonitorFunc();
    HeartbeatTaskResult ProcessTask(
        const std::shared_ptr<P2PClientMeta>& client,
        const HeartbeatTask& task);
    void ApplyHealthTransition(P2PClientStatus old_status,
                               P2PClientStatus new_status,
                               const UUID& client_id);
    void CleanupRoutes(const std::vector<P2PRouteLocation>& locations) const;

    const int64_t disconnect_timeout_sec_;
    const int64_t crash_timeout_sec_;
    const ViewVersionId view_version_;

    mutable SharedMutex clients_mutex_;
    std::unordered_map<UUID, std::shared_ptr<P2PClientMeta>, boost::hash<UUID>>
        client_metas_ GUARDED_BY(clients_mutex_);
    std::jthread client_monitor_thread_;
    SegmentRemovalCallback segment_removal_cb_;
};

}  // namespace mooncake
