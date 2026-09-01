#include "p2p/ha/oplog/p2p_standby_metadata_store.h"

#include <algorithm>

#include <glog/logging.h>

namespace mooncake {

bool P2PStandbyMetadataStore::PublishRoute(
    const std::string& key, const P2PRouteLocation& location,
    uint64_t object_size, uint64_t sequence_id) {
    if (object_size == 0) {
        LOG(ERROR) << "Standby rejected zero-sized route"
                   << ", key=" << key;
        return false;
    }
    std::lock_guard lock(mutex_);
    auto [it, inserted] = routes_.try_emplace(
        key, P2PStandbyRouteEntry{.object_size = object_size,
                                  .locations = {},
                                  .last_sequence_id = sequence_id});
    auto& route = it->second;
    if (!inserted && route.object_size != object_size) {
        LOG(ERROR) << "Standby rejected route size mismatch"
                   << ", key=" << key
                   << ", existing_size=" << route.object_size
                   << ", requested_size=" << object_size;
        return false;
    }
    route.last_sequence_id = sequence_id;
    if (std::find(route.locations.begin(), route.locations.end(), location) ==
        route.locations.end()) {
        route.locations.push_back(location);
    }
    return true;
}

void P2PStandbyMetadataStore::WithdrawRoute(
    const std::string& key, const P2PRouteLocation& location) {
    std::lock_guard lock(mutex_);
    auto it = routes_.find(key);
    if (it == routes_.end()) {
        return;
    }
    std::erase(it->second.locations, location);
    if (it->second.locations.empty()) {
        routes_.erase(it);
    }
}

void P2PStandbyMetadataStore::RegisterClient(
    const UUID& client_id, const std::string& ip_address, uint16_t rpc_port,
    const std::vector<P2PSegment>& segments) {
    std::lock_guard lock(mutex_);
    auto& client = clients_[client_id];
    client.client_id = client_id;
    client.ip_address = ip_address;
    client.rpc_port = rpc_port;
    for (const auto& segment : segments) {
        if (std::none_of(client.segments.begin(), client.segments.end(),
                         [&](const P2PSegment& existing) {
                             return existing.id == segment.id;
                         })) {
            client.segments.push_back(segment);
        }
    }
}

void P2PStandbyMetadataStore::UnregisterClient(const UUID& client_id) {
    std::lock_guard lock(mutex_);
    clients_.erase(client_id);
    for (auto it = routes_.begin(); it != routes_.end();) {
        std::erase_if(it->second.locations,
                      [&](const P2PRouteLocation& location) {
                          return location.client_id == client_id;
                      });
        if (it->second.locations.empty()) {
            it = routes_.erase(it);
        } else {
            ++it;
        }
    }
}

void P2PStandbyMetadataStore::MountSegment(const UUID& client_id,
                                           const P2PSegment& segment) {
    std::lock_guard lock(mutex_);
    auto& client = clients_[client_id];
    client.client_id = client_id;
    if (std::none_of(client.segments.begin(), client.segments.end(),
                     [&](const P2PSegment& existing) {
                         return existing.id == segment.id;
                     })) {
        client.segments.push_back(segment);
    }
}

void P2PStandbyMetadataStore::UnmountSegment(
    const P2PRouteLocation& location) {
    std::lock_guard lock(mutex_);
    auto client = clients_.find(location.client_id);
    if (client != clients_.end()) {
        std::erase_if(client->second.segments, [&](const P2PSegment& segment) {
            return segment.id == location.segment_id;
        });
    }
    RemoveLocationLocked(location);
}

void P2PStandbyMetadataStore::RemoveLocationLocked(
    const P2PRouteLocation& location) {
    for (auto it = routes_.begin(); it != routes_.end();) {
        std::erase(it->second.locations, location);
        if (it->second.locations.empty()) {
            it = routes_.erase(it);
        } else {
            ++it;
        }
    }
}

void P2PStandbyMetadataStore::RemoveAllMetadata() {
    std::lock_guard lock(mutex_);
    routes_.clear();
    clients_.clear();
}

std::optional<P2PStandbyRouteEntry> P2PStandbyMetadataStore::GetRoute(
    const std::string& key) const {
    std::lock_guard lock(mutex_);
    auto it = routes_.find(key);
    return it == routes_.end()
               ? std::optional<P2PStandbyRouteEntry>{}
               : std::optional<P2PStandbyRouteEntry>{it->second};
}

bool P2PStandbyMetadataStore::RouteExists(const std::string& key) const {
    std::lock_guard lock(mutex_);
    return routes_.contains(key);
}

size_t P2PStandbyMetadataStore::GetRouteKeyCount() const {
    std::lock_guard lock(mutex_);
    return routes_.size();
}

std::vector<std::string> P2PStandbyMetadataStore::ListRouteKeys() const {
    std::lock_guard lock(mutex_);
    std::vector<std::string> keys;
    keys.reserve(routes_.size());
    for (const auto& [key, route] : routes_) {
        keys.push_back(key);
    }
    return keys;
}

std::vector<UUID> P2PStandbyMetadataStore::ListClientIds() const {
    std::lock_guard lock(mutex_);
    std::vector<UUID> ids;
    ids.reserve(clients_.size());
    for (const auto& [id, client] : clients_) {
        ids.push_back(id);
    }
    return ids;
}

std::optional<P2PStandbyClientInfo>
P2PStandbyMetadataStore::GetClientInfo(const UUID& client_id) const {
    std::lock_guard lock(mutex_);
    auto it = clients_.find(client_id);
    return it == clients_.end()
               ? std::optional<P2PStandbyClientInfo>{}
               : std::optional<P2PStandbyClientInfo>{it->second};
}

std::shared_ptr<const P2PStandbyClientInfo>
P2PStandbyMetadataStore::GetClient(const UUID& client_id) const {
    auto client = GetClientInfo(client_id);
    return client.has_value()
               ? std::make_shared<P2PStandbyClientInfo>(std::move(*client))
               : nullptr;
}

void P2PStandbyMetadataStore::RestoreRoute(
    const std::string& key, const P2PStandbyRouteEntry& route) {
    std::lock_guard lock(mutex_);
    routes_[key] = route;
}

P2PStandbyMetadataStore::ExportedMetadata
P2PStandbyMetadataStore::ExportMetadata() const {
    std::lock_guard lock(mutex_);
    return ExportedMetadata{.routes = routes_, .clients = clients_};
}

std::unordered_map<std::string, P2PStandbyRouteEntry>
P2PStandbyMetadataStore::GetRoutes() const {
    std::lock_guard lock(mutex_);
    return routes_;
}

std::unordered_map<UUID, P2PStandbyClientInfo, boost::hash<UUID>>
P2PStandbyMetadataStore::GetClients() const {
    std::lock_guard lock(mutex_);
    return clients_;
}

}  // namespace mooncake
