#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
#include "p2p/master/p2p_client_manager.h"
#include "p2p/master/p2p_route_table.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Standalone P2P master service.
 *
 * P2PMasterService stores only P2P route locations and delegates client and
 * segment state to P2PClientManager. Replica::Descriptor remains solely as a
 * transitional facade/RPC return type until the M8 protocol stage.
 */
class P2PMasterService {
   public:
    explicit P2PMasterService(const P2PMasterConfig& config,
                              ViewVersionId view_version = 0);
    ~P2PMasterService() = default;

    P2PClientManager& GetClientManager() { return *client_manager_; }
    const P2PClientManager& GetClientManager() const {
        return *client_manager_;
    }

    auto RegisterClient(const P2PRegisterClientRequest& req)
        -> tl::expected<P2PRegisterClientResponse, ErrorCode>;
    auto UnregisterClient(const P2PUnregisterClientRequest& req)
        -> tl::expected<P2PUnregisterClientResponse, ErrorCode>;
    auto Heartbeat(const P2PHeartbeatRequest& req)
        -> tl::expected<P2PHeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const P2PQueryClientStatusRequest& req)
        -> tl::expected<P2PQueryClientStatusResponse, ErrorCode>;

    auto MountSegment(const P2PSegment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    auto ExistKey(std::string_view key) -> tl::expected<bool, ErrorCode>;
    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);
    auto GetAllKeys() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto BatchQueryIp(const std::vector<UUID>& client_ids) -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>;

    auto GetReplicaListByRegex(const std::string& regex_pattern)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;
    auto GetReplicaList(std::string_view key,
                        const P2PGetReplicaListRequestConfig& config =
                            P2PGetReplicaListRequestConfig())
        -> tl::expected<P2PGetReplicaListResponse, ErrorCode>;

    auto Remove(std::string_view key, bool force = false)
        -> tl::expected<void, ErrorCode>;
    auto RemoveByRegex(std::string_view regex_pattern, bool force = false)
        -> tl::expected<long, ErrorCode>;
    long RemoveAll(bool force = false);
    size_t GetKeyCount() const;

    OpLogManager* GetOpLogManager() const { return oplog_manager_.get(); }

    auto GetWriteRoute(const WriteRouteRequest& req)
        -> tl::expected<WriteRouteResponse, ErrorCode>;
    auto BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
        -> BatchGetWriteRouteResponse;
    auto AddReplica(const AddReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;
    auto RemoveReplica(const RemoveReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;
    auto BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
        -> std::vector<tl::expected<void, ErrorCode>>;
    auto BatchSyncReplica(const BatchSyncReplicaRequest& req)
        -> BatchSyncReplicaResponse;
    auto SetSyncCompleted(UUID client_id) -> tl::expected<void, ErrorCode>;

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

    auto BuildReplicaDescriptor(const P2PRouteLocation& location,
                                uint64_t object_size) const
        -> tl::expected<Replica::Descriptor, ErrorCode>;
    std::vector<Replica::Descriptor> FilterRoutes(
        const P2PGetReplicaListRequestConfig& config,
        const P2PRouteEntry& route) const;

    auto InnerAddReplica(std::string_view key, const UUID& client_id,
                         const UUID& segment_id, size_t size,
                         const std::shared_ptr<P2PClientMeta>& client)
        -> tl::expected<void, ErrorCode>;
    auto InnerRemoveReplica(std::string_view key, const UUID& client_id,
                            const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;

    P2PRouteTable route_table_;
    uint64_t max_client_per_key_;
    bool enable_async_oplog_write_{false};
    ViewVersionId view_version_;
    std::unique_ptr<OpLogManager> oplog_manager_;
    // Declared last so the monitor and its callbacks stop before route state.
    std::shared_ptr<P2PClientManager> client_manager_;
};

}  // namespace mooncake
