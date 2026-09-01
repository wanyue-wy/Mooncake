#include "p2p/master/p2p_route_table.h"

#include <algorithm>

#include <glog/logging.h>

namespace mooncake {

size_t P2PRouteTable::CountOwnerClients(const P2PRouteEntry& entry) {
    std::unordered_set<UUID, boost::hash<UUID>> clients;
    for (const auto& location : entry.locations) {
        clients.insert(location.client_id);
    }
    return clients.size();
}

auto P2PRouteTable::Publish(std::string_view key, uint64_t object_size,
                            const P2PRouteLocation& location)
    -> tl::expected<MutationResult, ErrorCode> {
    if (object_size == 0) {
        LOG(ERROR) << "Publish route rejected: object_size must be positive"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& shard = shards_[GetShardIndex(key)];
    SharedMutexLocker lock(&shard.mutex);
    auto it = shard.routes.find(key);
    if (it != shard.routes.end()) {
        auto& entry = it->second;
        if (entry.object_size != object_size) {
            LOG(ERROR) << "Publish route rejected: object size mismatch"
                       << ", key=" << key
                       << ", existing_size=" << entry.object_size
                       << ", requested_size=" << object_size;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (std::find(entry.locations.begin(), entry.locations.end(),
                      location) != entry.locations.end()) {
            return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
        }
        const bool new_owner = std::none_of(
            entry.locations.begin(), entry.locations.end(),
            [&](const P2PRouteLocation& existing) {
                return existing.client_id == location.client_id;
            });
        if (new_owner && max_client_per_key_ > 0 &&
            CountOwnerClients(entry) >= max_client_per_key_) {
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }

        entry.locations.push_back(location);
        shard.keys_by_location[location].insert(std::string_view(it->first));
        return MutationResult{};
    }

    P2PRouteEntry entry;
    entry.object_size = object_size;
    entry.locations.push_back(location);
    auto [inserted_it, inserted] =
        shard.routes.emplace(std::string(key), std::move(entry));
    if (!inserted) {
        LOG(ERROR) << "Publish route failed to insert a new key"
                   << ", key=" << key;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    shard.keys_by_location[location].insert(
        std::string_view(inserted_it->first));
    return MutationResult{.created_key = true};
}

void P2PRouteTable::RemoveReverseIndex(
    RouteShard& shard, std::string_view key,
    const P2PRouteLocation& location) {
    auto location_it = shard.keys_by_location.find(location);
    if (location_it == shard.keys_by_location.end()) {
        LOG(ERROR) << "Route reverse index is missing a location"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id;
        return;
    }
    if (location_it->second.erase(key) == 0) {
        LOG(ERROR) << "Route reverse index is missing a key"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id;
    }
    if (location_it->second.empty()) {
        shard.keys_by_location.erase(location_it);
    }
}

void P2PRouteTable::RemoveAllReverseIndexes(RouteShard& shard,
                                            std::string_view key,
                                            const P2PRouteEntry& entry) {
    for (const auto& location : entry.locations) {
        RemoveReverseIndex(shard, key, location);
    }
}

auto P2PRouteTable::Withdraw(std::string_view key,
                             const P2PRouteLocation& location)
    -> tl::expected<MutationResult, ErrorCode> {
    auto& shard = shards_[GetShardIndex(key)];
    SharedMutexLocker lock(&shard.mutex);
    auto route_it = shard.routes.find(key);
    if (route_it == shard.routes.end()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& locations = route_it->second.locations;
    auto location_it = std::find(locations.begin(), locations.end(), location);
    if (location_it == locations.end()) {
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    RemoveReverseIndex(shard, route_it->first, location);
    locations.erase(location_it);
    if (locations.empty()) {
        shard.routes.erase(route_it);
        return MutationResult{.removed_key = true};
    }
    return MutationResult{};
}

bool P2PRouteTable::RouteExists(std::string_view key) const {
    const auto& shard = shards_[GetShardIndex(key)];
    SharedMutexLocker lock(&shard.mutex, shared_lock);
    auto it = shard.routes.find(key);
    return it != shard.routes.end() && !it->second.locations.empty();
}

std::optional<P2PRouteEntry> P2PRouteTable::GetRoute(
    std::string_view key) const {
    const auto& shard = shards_[GetShardIndex(key)];
    SharedMutexLocker lock(&shard.mutex, shared_lock);
    auto it = shard.routes.find(key);
    if (it == shard.routes.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<std::pair<std::string, P2PRouteEntry>> P2PRouteTable::Snapshot()
    const {
    std::vector<std::pair<std::string, P2PRouteEntry>> result;
    for (const auto& shard : shards_) {
        SharedMutexLocker lock(&shard.mutex, shared_lock);
        result.reserve(result.size() + shard.routes.size());
        for (const auto& route : shard.routes) {
            result.emplace_back(route.first, route.second);
        }
    }
    return result;
}

std::vector<std::string> P2PRouteTable::ListRouteKeys() const {
    std::vector<std::string> keys;
    for (const auto& shard : shards_) {
        SharedMutexLocker lock(&shard.mutex, shared_lock);
        keys.reserve(keys.size() + shard.routes.size());
        for (const auto& route : shard.routes) {
            keys.push_back(route.first);
        }
    }
    return keys;
}

size_t P2PRouteTable::GetRouteKeyCount() const {
    size_t count = 0;
    for (const auto& shard : shards_) {
        SharedMutexLocker lock(&shard.mutex, shared_lock);
        count += shard.routes.size();
    }
    return count;
}

P2PRouteTable::CleanupResult P2PRouteTable::RemoveLocation(
    const P2PRouteLocation& location) {
    CleanupResult result;
    for (auto& shard : shards_) {
        SharedMutexLocker lock(&shard.mutex);
        auto index_it = shard.keys_by_location.find(location);
        if (index_it == shard.keys_by_location.end()) {
            continue;
        }

        std::vector<std::string> affected_keys;
        affected_keys.reserve(index_it->second.size());
        for (std::string_view key : index_it->second) {
            affected_keys.emplace_back(key);
        }
        shard.keys_by_location.erase(index_it);

        for (const auto& key : affected_keys) {
            auto route_it = shard.routes.find(key);
            if (route_it == shard.routes.end()) {
                LOG(ERROR) << "Route reverse index references a missing key"
                           << ", key=" << key;
                continue;
            }
            auto& locations = route_it->second.locations;
            const size_t old_size = locations.size();
            std::erase(locations, location);
            result.removed_routes += old_size - locations.size();
            if (locations.empty()) {
                result.removed_keys.push_back(route_it->first);
                shard.routes.erase(route_it);
            }
        }
    }
    return result;
}

bool P2PRouteTable::RemoveKey(std::string_view key) {
    auto& shard = shards_[GetShardIndex(key)];
    SharedMutexLocker lock(&shard.mutex);
    auto it = shard.routes.find(key);
    if (it == shard.routes.end()) {
        return false;
    }
    RemoveAllReverseIndexes(shard, it->first, it->second);
    shard.routes.erase(it);
    return true;
}

size_t P2PRouteTable::Clear() {
    size_t removed_keys = 0;
    for (auto& shard : shards_) {
        SharedMutexLocker lock(&shard.mutex);
        removed_keys += shard.routes.size();
        shard.keys_by_location.clear();
        shard.routes.clear();
    }
    return removed_keys;
}

}  // namespace mooncake
