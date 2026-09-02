#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
#include "p2p/master/p2p_client_manager.h"
#include "p2p/master/p2p_route_table.h"

namespace mooncake {

/** @brief P2P domain service for client, segment and route state. */
class P2PMasterService final {
   public:
    explicit P2PMasterService(const P2PMasterConfig& config,
                              ViewVersionId view_version = 0);

    P2PClientManager& GetClientManager() { return *client_manager_; }
    const P2PClientManager& GetClientManager() const {
        return *client_manager_;
    }

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

    std::vector<std::string> ListRouteKeys() const;
    size_t GetRouteKeyCount() const;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    OpLogManager* GetOpLogManager() const { return oplog_manager_.get(); }
    ErrorCode RestoreFromStandbyMetadata(
        const P2PStandbyMetadataStore::ExportedMetadata& metadata,
        uint64_t last_applied_sequence_id = 0);
    ErrorCode RecordOplog(OpType type, const std::string& key,
                          const std::string& payload = std::string());

   private:
    using OwnerClientSet = std::unordered_set<UUID, boost::hash<UUID>>;

    void InitializeClientManager();
    void OnSegmentRemoved(const P2PRouteLocation& location);
    static OwnerClientSet CollectRouteOwnerClients(
        const P2PRouteEntry& route);
    auto BuildRouteDescriptor(const P2PRouteLocation& location,
                              uint64_t object_size) const
        -> tl::expected<P2PRouteDescriptor, ErrorCode>;
    std::vector<P2PRouteDescriptor> FilterRoutes(
        const P2PReadRouteConfig& config, const P2PRouteEntry& route) const;
    auto InnerPublishRoute(std::string_view key, const UUID& client_id,
                           const UUID& segment_id, uint64_t object_size,
                           const std::shared_ptr<P2PClientMeta>& client)
        -> tl::expected<void, ErrorCode>;
    auto InnerWithdrawRoute(std::string_view key, const UUID& client_id,
                            const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;

    P2PRouteTable route_table_;
    uint64_t max_client_per_key_;
    bool enable_async_oplog_write_{false};
    ViewVersionId view_version_;
    std::unique_ptr<OpLogManager> oplog_manager_;
    std::shared_ptr<P2PClientManager> client_manager_;
};

}  // namespace mooncake
