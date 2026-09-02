#include "p2p/master/p2p_route_table.h"

#include <algorithm>
#include <utility>

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
    -> Mutation {
    auto& shard = shards_[GetShardIndex(key)];
    SharedMutexLocker lock(&shard.mutex);
    return PublishLocked(shard, key, object_size, location);
}

auto P2PRouteTable::PublishLocked(RouteShard& shard, std::string_view key,
                                  uint64_t object_size,
                                  const P2PRouteLocation& location)
    -> Mutation {
    if (object_size == 0) {
        LOG(ERROR) << "Publish route rejected: object_size must be positive"
                   << ", key=" << key << ", client_id=" << location.client_id
                   << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

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
            LOG(WARNING) << "Publish route rejected: location already exists"
                         << ", key=" << key
                         << ", client_id=" << location.client_id
                         << ", segment_id=" << location.segment_id;
            return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
        }
        const bool new_owner =
            std::none_of(entry.locations.begin(), entry.locations.end(),
                         [&](const P2PRouteLocation& existing) {
                             return existing.client_id == location.client_id;
                         });
        // A client may publish the same key from multiple segments during
        // tier migration. The configured limit applies to owner clients, not
        // physical route locations.
        if (new_owner && max_client_per_key_ > 0 &&
            CountOwnerClients(entry) >= max_client_per_key_) {
            LOG(WARNING) << "Publish route rejected: owner client limit "
                            "exceeded"
                         << ", key=" << key
                         << ", client_id=" << location.client_id
                         << ", segment_id=" << location.segment_id
                         << ", max_clients_per_key=" << max_client_per_key_;
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
    -> Mutation {
    auto& shard = shards_[GetShardIndex(key)];
    SharedMutexLocker lock(&shard.mutex);
    return WithdrawLocked(shard, key, location);
}

auto P2PRouteTable::WithdrawLocked(
    RouteShard& shard, std::string_view key,
    const P2PRouteLocation& location,
    const P2PWithdrawRouteOperation* operation,
    const BeforeWithdrawCallback& before_withdraw) -> Mutation {
    auto route_it = shard.routes.find(key);
    if (route_it == shard.routes.end()) {
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

    if (before_withdraw && operation != nullptr) {
        const auto error = before_withdraw(*operation);
        if (error != ErrorCode::OK) {
            LOG(ERROR) << "Withdraw route rejected by pre-mutation callback"
                       << ", key=" << key
                       << ", client_id=" << location.client_id
                       << ", segment_id=" << location.segment_id
                       << ", error=" << toString(error);
            return tl::make_unexpected(error);
        }
    }

    RemoveReverseIndex(shard, route_it->first, location);
    locations.erase(location_it);
    if (locations.empty()) {
        shard.routes.erase(route_it);
        return MutationResult{.removed_key = true};
    }
    return MutationResult{};
}

void P2PRouteTable::BatchPublish(
    const UUID& client_id,
    std::span<const P2PPublishRouteOperation> operations,
    const BeforePublishCallback& before_publish,
    const PublishResultCallback& on_result) {
    BatchSync(client_id, operations,
              std::span<const P2PWithdrawRouteOperation>{}, before_publish,
              on_result, BeforeWithdrawCallback{}, WithdrawResultCallback{});
}

void P2PRouteTable::BatchWithdraw(
    const UUID& client_id,
    std::span<const P2PWithdrawRouteOperation> operations,
    const BeforeWithdrawCallback& before_withdraw,
    const WithdrawResultCallback& on_result) {
    BatchSync(client_id, std::span<const P2PPublishRouteOperation>{},
              operations, BeforePublishCallback{}, PublishResultCallback{},
              before_withdraw, on_result);
}

void P2PRouteTable::BatchSync(
    const UUID& client_id,
    std::span<const P2PPublishRouteOperation> publish_operations,
    std::span<const P2PWithdrawRouteOperation> withdraw_operations,
    const BeforePublishCallback& before_publish,
    const PublishResultCallback& on_publish_result,
    const BeforeWithdrawCallback& before_withdraw,
    const WithdrawResultCallback& on_withdraw_result) {
    std::unordered_map<size_t, std::vector<std::pair<size_t, bool>>>
        operations_by_shard;
    for (size_t index = 0; index < publish_operations.size(); ++index) {
        operations_by_shard[GetShardIndex(publish_operations[index].key)]
            .emplace_back(index, true);
    }
    for (size_t index = 0; index < withdraw_operations.size(); ++index) {
        operations_by_shard[GetShardIndex(withdraw_operations[index].key)]
            .emplace_back(index, false);
    }

    for (const auto& [shard_index, operations] : operations_by_shard) {
        auto& shard = shards_[shard_index];
        SharedMutexLocker lock(&shard.mutex);
        for (const auto& [operation_index, is_publish] : operations) {
            if (is_publish) {
                const auto& operation = publish_operations[operation_index];
                const P2PRouteLocation location{
                    .client_id = client_id,
                    .segment_id = operation.segment_id,
                };
                auto result = [&]() -> Mutation {
                    if (before_publish) {
                        const auto error = before_publish(operation);
                        if (error != ErrorCode::OK) {
                            LOG(ERROR)
                                << "Publish route rejected by pre-mutation "
                                   "callback"
                                << ", key=" << operation.key
                                << ", client_id=" << client_id
                                << ", segment_id=" << operation.segment_id
                                << ", error=" << toString(error);
                            return tl::make_unexpected(error);
                        }
                    }
                    return PublishLocked(shard, operation.key,
                                         operation.object_size, location);
                }();
                if (on_publish_result) {
                    on_publish_result(operation_index, operation, result);
                }
                continue;
            }

            const auto& operation = withdraw_operations[operation_index];
            const P2PRouteLocation location{
                .client_id = client_id,
                .segment_id = operation.segment_id,
            };
            auto result = WithdrawLocked(shard, operation.key, location,
                                         &operation, before_withdraw);
            if (on_withdraw_result) {
                on_withdraw_result(operation_index, operation, result);
            }
        }
    }
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
