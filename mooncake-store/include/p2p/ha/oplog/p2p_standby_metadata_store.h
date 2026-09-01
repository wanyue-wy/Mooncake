#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>

#include "p2p/common/p2p_types.h"

namespace mooncake {

struct P2PStandbyClientInfo {
    UUID client_id{0, 0};
    std::string ip_address;
    uint16_t rpc_port{0};
    std::vector<P2PSegment> segments;
};
YLT_REFL(P2PStandbyClientInfo, client_id, ip_address, rpc_port, segments);

struct P2PStandbyRouteEntry {
    uint64_t object_size{0};
    std::vector<P2PRouteLocation> locations;
    uint64_t last_sequence_id{0};

    bool operator==(const P2PStandbyRouteEntry&) const = default;
};
YLT_REFL(P2PStandbyRouteEntry, object_size, locations, last_sequence_id);

/** @brief P2P-only standby state for clients, segments and routes. */
class P2PStandbyMetadataStore final {
   public:
    bool PublishRoute(const std::string& key,
                      const P2PRouteLocation& location,
                      uint64_t object_size, uint64_t sequence_id);
    void WithdrawRoute(const std::string& key,
                       const P2PRouteLocation& location);

    void RegisterClient(const UUID& client_id, const std::string& ip_address,
                        uint16_t rpc_port,
                        const std::vector<P2PSegment>& segments);
    void UnregisterClient(const UUID& client_id);
    void MountSegment(const UUID& client_id, const P2PSegment& segment);
    void UnmountSegment(const P2PRouteLocation& location);
    void RemoveAllMetadata();

    std::optional<P2PStandbyRouteEntry> GetRoute(
        const std::string& key) const;
    bool RouteExists(const std::string& key) const;
    size_t GetRouteKeyCount() const;
    std::vector<std::string> ListRouteKeys() const;
    std::vector<UUID> ListClientIds() const;
    std::optional<P2PStandbyClientInfo> GetClientInfo(
        const UUID& client_id) const;
    std::shared_ptr<const P2PStandbyClientInfo> GetClient(
        const UUID& client_id) const;

    void RestoreRoute(const std::string& key,
                      const P2PStandbyRouteEntry& route);

    struct ExportedMetadata {
        std::unordered_map<std::string, P2PStandbyRouteEntry> routes;
        std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>>
            clients;
    };
    ExportedMetadata ExportMetadata() const;
    std::unordered_map<std::string, P2PStandbyRouteEntry> GetRoutes() const;
    std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>>
    GetClients() const;

   private:
    void RemoveLocationLocked(const P2PRouteLocation& location);

    std::unordered_map<std::string, P2PStandbyRouteEntry> routes_;
    std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>> clients_;
    mutable std::mutex mutex_;
};

}  // namespace mooncake
