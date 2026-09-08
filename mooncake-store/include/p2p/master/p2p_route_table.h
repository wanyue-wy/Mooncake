#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_types.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

/**
 * @brief Unsynchronized in-memory index for one P2P route shard.
 */
class P2PRouteTable final {
   private:
    using RouteMap =
        std::unordered_map<std::string, P2PRouteEntry, StringHash,
                           std::equal_to<>>;

   public:
    struct MutationResult {
        bool created_key{false};
        bool removed_key{false};
    };

    struct CleanupResult {
        size_t removed_routes{0};
        std::vector<std::string> removed_keys;
    };

    using Mutation = tl::expected<MutationResult, ErrorCode>;

    auto Publish(std::string_view key, uint64_t object_size,
                 const P2PRouteLocation& location,
                 uint64_t max_client_per_key = 0) -> Mutation;

    auto Withdraw(std::string_view key, const P2PRouteLocation& location)
        -> Mutation;
    template <typename PreWithdraw>
    auto Withdraw(std::string_view key, const P2PRouteLocation& location,
                  PreWithdraw&& pre_withdraw) -> Mutation;

    bool RouteExists(std::string_view key) const;
    std::optional<P2PRouteEntry> GetRoute(std::string_view key) const;
    std::vector<std::string> ListRouteKeys() const;
    size_t GetRouteKeyCount() const;

    CleanupResult RemoveLocation(const P2PRouteLocation& location);
    bool RemoveKey(std::string_view key);
    size_t Clear();

   private:
    static size_t CountOwnerClients(const P2PRouteEntry& entry);
    void RemoveReverseIndex(std::string_view key,
                            const P2PRouteLocation& location);
    void RemoveAllReverseIndexes(std::string_view key,
                                 const P2PRouteEntry& entry);

    RouteMap routes_;
    std::unordered_map<P2PRouteLocation, std::unordered_set<std::string_view>,
                       P2PRouteLocationHash>
        keys_by_location_;
};

template <typename PreWithdraw>
auto P2PRouteTable::Withdraw(std::string_view key,
                             const P2PRouteLocation& location,
                             PreWithdraw&& pre_withdraw) -> Mutation {
    auto route_it = routes_.find(key);
    if (route_it == routes_.end()) {
        LOG(WARNING) << "Withdraw route rejected: key not found"
                     << ", key=" << key << ", client_id=" << location.client_id
                     << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& locations = route_it->second.locations;
    auto location_it = std::find(locations.begin(), locations.end(), location);
    if (location_it == locations.end()) {
        LOG(WARNING) << "Withdraw route rejected: location not found"
                     << ", key=" << key << ", client_id=" << location.client_id
                     << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    const auto error = pre_withdraw();
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "Withdraw route rejected by pre-mutation hook"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id
                   << ", error=" << toString(error);
        return tl::make_unexpected(error);
    }

    RemoveReverseIndex(route_it->first, location);
    locations.erase(location_it);
    if (locations.empty()) {
        routes_.erase(route_it);
        return MutationResult{.removed_key = true};
    }
    return MutationResult{};
}

}  // namespace mooncake
