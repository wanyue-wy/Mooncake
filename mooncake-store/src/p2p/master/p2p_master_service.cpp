#include "p2p/master/p2p_master_service.h"

#include <algorithm>
#include <regex>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <variant>

#include <glog/logging.h>

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/oplog/oplog_store_factory.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/master/p2p_client_meta.h"
#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

P2PMasterService::P2PMasterService(const P2PMasterConfig& config,
                                   ViewVersionId view_version)
    : route_table_(config.routes.max_clients_per_key),
      max_client_per_key_(config.routes.max_clients_per_key),
      enable_async_oplog_write_(ParseOpLogStoreType(config.oplog.store_type) ==
                                OpLogStoreType::REDIS),
      view_version_(view_version) {
    if (config.oplog.enabled) {
        auto store_type = ParseOpLogStoreType(config.oplog.store_type);
        const std::string& store_location = store_type == OpLogStoreType::REDIS
                                                ? config.redis.endpoint
                                                : config.oplog.data_dir;
        auto store = OpLogStoreFactory::Create(
            store_type, config.cluster_id, OpLogStoreRole::WRITER,
            store_location, kDefaultOpLogPollIntervalMs, config.redis.password,
            config.redis.username, config.redis.db_index,
            config.oplog.async_queue_max_entries,
            config.oplog.async_queue_overflow_mode,
            config.oplog.best_effort_max_retries);
        if (!store) {
            LOG(ERROR) << "P2PMasterService: failed to initialize OpLogStore"
                       << ", type=" << config.oplog.store_type
                       << ", location=" << store_location
                       << ", cluster_id=" << config.cluster_id;
            throw std::runtime_error(
                "failed to initialize OpLogStore while oplog is enabled: "
                "type=" +
                config.oplog.store_type + ", location=" + store_location +
                ", cluster_id=" + config.cluster_id);
        }

        oplog_manager_ = std::make_unique<OpLogManager>();
        oplog_manager_->SetOpLogStore(
            std::shared_ptr<OpLogStore>(std::move(store)));
    }

    P2PMasterMetricManager::instance();
    client_manager_ = std::make_shared<P2PClientManager>(
        config.client_lifecycle.live_ttl_seconds,
        config.client_lifecycle.crashed_ttl_seconds, view_version);
    InitializeClientManager();
    client_manager_->Start();
}

void P2PMasterService::InitializeClientManager() {
    client_manager_->SetSegmentRemovalCallback(
        [this](const P2PRouteLocation& location) {
            OnSegmentRemoved(location);
        });
}

auto P2PMasterService::Heartbeat(const P2PHeartbeatRequest& req)
    -> tl::expected<P2PHeartbeatResponse, ErrorCode> {
    return client_manager_->Heartbeat(req);
}

auto P2PMasterService::QueryClientStatus(const P2PQueryClientStatusRequest& req)
    -> tl::expected<P2PQueryClientStatusResponse, ErrorCode> {
    return client_manager_->QueryClientStatus(req);
}

auto P2PMasterService::ExistKey(std::string_view key)
    -> tl::expected<bool, ErrorCode> {
    return route_table_.RouteExists(key);
}

std::vector<tl::expected<bool, ErrorCode>> P2PMasterService::BatchExistKey(
    const std::vector<std::string_view>& keys) {
    std::vector<tl::expected<bool, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(ExistKey(key));
    }
    return results;
}

auto P2PMasterService::GetAllKeys()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    return route_table_.ListRouteKeys();
}

auto P2PMasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = client_manager_->GetAllSegments();
    if (!result.has_value()) {
        LOG(ERROR) << "fail to get all segments"
                   << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::GetClientSegments(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = client_manager_->GetClientSegments(client_id);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to get client segments"
                   << ", client_id=" << client_id << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    auto result = client_manager_->QuerySegments(segment);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", segment=" << segment << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto result = client_manager_->QueryIp(client_id);
    if (!result.has_value()) {
        LOG(ERROR) << "fail to query ip"
                   << ", client_id=" << client_id << ", ret=" << result.error();
    }
    return result;
}

auto P2PMasterService::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>
        results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        } else {
            LOG(WARNING) << "fail to query ip"
                         << ", client_id=" << client_id
                         << ", ret=" << ip_result.error();
        }
    }
    return results;
}

auto P2PMasterService::GetReplicaListByRegex(const std::string& regex_pattern)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<P2PRouteDescriptor>>,
        ErrorCode> {
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::unordered_map<std::string, std::vector<P2PRouteDescriptor>> results;
    for (const auto& key : route_table_.ListRouteKeys()) {
        if (!std::regex_search(key, pattern)) {
            continue;
        }
        auto route = route_table_.GetRoute(key);
        if (!route.has_value()) {
            VLOG(1) << "Route was removed during regex query"
                    << ", key=" << key;
            continue;
        }
        std::vector<P2PRouteDescriptor> descriptors;
        descriptors.reserve(route->locations.size());
        for (const auto& location : route->locations) {
            auto descriptor =
                BuildRouteDescriptor(location, route->object_size);
            if (descriptor.has_value()) {
                descriptors.push_back(std::move(*descriptor));
            }
        }
        if (descriptors.empty()) {
            LOG(WARNING) << "key=" << key
                         << " matched by regex, but has no available routes.";
            continue;
        }
        results.emplace(key, std::move(descriptors));
    }
    return results;
}

auto P2PMasterService::GetReplicaList(
    std::string_view key, const P2PReadRouteConfig& config)
    -> tl::expected<P2PGetReadRouteResponse, ErrorCode> {
    auto route = route_table_.GetRoute(key);
    if (!route.has_value()) {
        LOG(WARNING) << "GetReplicaList failed: key not found"
                     << ", key=" << key;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto descriptors = FilterRoutes(config, *route);
    if (descriptors.empty()) {
        LOG(WARNING) << "key=" << key << ", error=route_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    P2PGetReadRouteResponse response;
    response.routes = std::move(descriptors);
    return response;
}

auto P2PMasterService::Remove(std::string_view key, bool force)
    -> tl::expected<void, ErrorCode> {
    (void)force;
    if (!route_table_.RemoveKey(key)) {
        LOG(WARNING) << "Remove route failed: key not found"
                     << ", key=" << key;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    P2PMasterMetricManager::instance().dec_key_count(1);
    return {};
}

auto P2PMasterService::RemoveByRegex(std::string_view regex_pattern, bool force)
    -> tl::expected<long, ErrorCode> {
    (void)force;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern.begin(), regex_pattern.end(),
                             std::regex::ECMAScript);
    } catch (const std::regex_error& error) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << error.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    long removed_count = 0;
    for (const auto& key : route_table_.ListRouteKeys()) {
        if (std::regex_search(key, pattern) && route_table_.RemoveKey(key)) {
            VLOG(1) << "key=" << key << " matched by regex. Removing.";
            ++removed_count;
        }
    }

    if (removed_count > 0) {
        P2PMasterMetricManager::instance().dec_key_count(removed_count);
    }
    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long P2PMasterService::RemoveAll(bool force) {
    (void)force;
    const auto removed = route_table_.Clear();
    if (removed > 0) {
        P2PMasterMetricManager::instance().dec_key_count(removed);
    }
    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed;
    return static_cast<long>(removed);
}

size_t P2PMasterService::GetKeyCount() const {
    return route_table_.GetRouteKeyCount();
}

void P2PMasterService::OnSegmentRemoved(const P2PRouteLocation& location) {
    auto cleanup = route_table_.RemoveLocation(location);
    if (!cleanup.removed_keys.empty()) {
        P2PMasterMetricManager::instance().dec_key_count(
            cleanup.removed_keys.size());
    }
}

ErrorCode P2PMasterService::RecordOplog(OpType type, const std::string& key,
                                        const std::string& payload) {
    auto* manager = GetOpLogManager();
    if (manager == nullptr) {
        return ErrorCode::OK;
    }

    auto result = manager->AppendAndPersist(
        type, key, payload, /*sync=*/!enable_async_oplog_write_);
    if (!result.has_value()) {
        LOG(ERROR) << "P2PMasterService: failed to persist oplog"
                   << ", op_type=" << static_cast<int>(type) << ", key=" << key
                   << ", error=" << toString(result.error());
        return result.error();
    }
    return ErrorCode::OK;
}

auto P2PMasterService::RegisterClient(const P2PRegisterClientRequest& req)
    -> tl::expected<P2PRegisterClientResponse, ErrorCode> {
    if (req.ip_address.empty() || req.rpc_port == 0) {
        LOG(ERROR) << "RegisterClient(P2P): missing endpoint"
                   << ", client_id=" << req.client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto make_idempotent_response = [&]() {
        P2PRegisterClientResponse response;
        response.view_version = view_version_;
        LOG(INFO) << "RegisterClient(P2P): client already registered, "
                     "treating as idempotent re-register"
                  << ", client_id=" << req.client_id
                  << ", view_version=" << response.view_version;
        return response;
    };

    if (client_manager_->GetClient(req.client_id)) {
        return make_idempotent_response();
    }

    auto result = client_manager_->RegisterClient(req);
    if (!result.has_value()) {
        if (result.error() == ErrorCode::CLIENT_ALREADY_EXISTS &&
            client_manager_->GetClient(req.client_id)) {
            return make_idempotent_response();
        }
        LOG(ERROR) << "RegisterClient(P2P): failed"
                   << ", client_id=" << req.client_id
                   << ", error=" << result.error();
        return result;
    }

    RegisterClientPayload payload;
    payload.client_id = req.client_id;
    payload.ip_address = req.ip_address;
    payload.rpc_port = req.rpc_port;
    payload.segments = req.segments;
    auto err =
        RecordOplog(OpType_REGISTER_CLIENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "RegisterClient(P2P): failed to record oplog"
                   << ", client_id=" << req.client_id
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return result;
}

auto P2PMasterService::UnregisterClient(const P2PUnregisterClientRequest& req)
    -> tl::expected<P2PUnregisterClientResponse, ErrorCode> {
    auto result = client_manager_->UnregisterClient(req);
    if (!result.has_value()) {
        LOG(ERROR) << "UnregisterClient(P2P): failed"
                   << ", client_id=" << req.client_id
                   << ", error=" << result.error();
        return result;
    }

    UnregisterClientPayload payload;
    payload.client_id = req.client_id;
    auto err =
        RecordOplog(OpType_UNREGISTER_CLIENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "UnregisterClient(P2P): failed to record oplog"
                   << ", client_id=" << req.client_id
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return result;
}

auto P2PMasterService::MountSegment(const P2PSegment& segment,
                                    const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "MountSegment: client not found"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto result = client->MountSegment(segment);
    if (!result.has_value()) {
        LOG(ERROR) << "MountSegment(P2P): failed"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment.id
                   << ", segment_name=" << segment.name
                   << ", error=" << result.error();
        return result;
    }

    MountSegmentPayload payload;
    payload.client_id = client_id;
    payload.segment = segment;
    auto err =
        RecordOplog(OpType_MOUNT_SEGMENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "MountSegment(P2P): failed to record oplog"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment.id
                   << ", segment_name=" << segment.name
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return {};
}

auto P2PMasterService::UnmountSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "UnmountSegment: client not found"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto result = client->UnmountSegment(segment_id);
    if (!result.has_value()) {
        LOG(ERROR) << "UnmountSegment(P2P): failed"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment_id
                   << ", error=" << result.error();
        return result;
    }

    UnmountSegmentPayload payload;
    payload.segment_id = segment_id;
    payload.client_id = client_id;
    auto err =
        RecordOplog(OpType_UNMOUNT_SEGMENT, "", SerializeP2PPayload(payload));
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "UnmountSegment(P2P): failed to record oplog"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment_id
                   << ", error=" << toString(err);
        return tl::make_unexpected(err);
    }
    return {};
}

P2PMasterService::OwnerClientSet P2PMasterService::CollectRouteOwnerClients(
    const P2PRouteEntry& route) {
    OwnerClientSet clients;
    for (const auto& location : route.locations) {
        clients.insert(location.client_id);
    }
    return clients;
}

auto P2PMasterService::BuildRouteDescriptor(const P2PRouteLocation& location,
                                            uint64_t object_size) const
    -> tl::expected<P2PRouteDescriptor, ErrorCode> {
    auto client = client_manager_->GetClient(location.client_id);
    if (!client) {
        LOG(WARNING) << "Route references a missing client"
                     << ", client_id=" << location.client_id
                     << ", segment_id=" << location.segment_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto segment = client->QuerySegment(location.segment_id);
    if (!segment.has_value()) {
        LOG(WARNING) << "Route references an unavailable segment"
                     << ", client_id=" << location.client_id
                     << ", segment_id=" << location.segment_id
                     << ", error=" << toString(segment.error());
        return tl::make_unexpected(segment.error());
    }

    return P2PRouteDescriptor{
        .client_id = location.client_id,
        .segment_id = location.segment_id,
        .ip_address = client->get_ip_address(),
        .rpc_port = client->get_rpc_port(),
        .object_size = object_size,
    };
}

std::vector<P2PRouteDescriptor> P2PMasterService::FilterRoutes(
    const P2PReadRouteConfig& config, const P2PRouteEntry& route) const {
    // Candidates are kept at client granularity.
    std::vector<std::pair<int, P2PRouteDescriptor>> candidates;
    std::unordered_map<UUID, size_t, boost::hash<UUID>> best_by_client;

    // 1. Filter qualified routes.
    for (const auto& location : route.locations) {
        auto client = client_manager_->GetClient(location.client_id);
        if (!client || !client->is_health()) {
            // A disconnected client cannot serve the read RPC, so skip it.
            continue;
        }
        auto segment_result = client->QuerySegment(location.segment_id);
        if (!segment_result.has_value()) {
            LOG(WARNING) << "Skipping route with unavailable segment"
                         << ", client_id=" << location.client_id
                         << ", segment_id=" << location.segment_id
                         << ", error=" << toString(segment_result.error());
            continue;
        }
        const auto& segment = segment_result.value();

        // 1.1 Exclude routes whose segment contains a filtered tag.
        bool excluded_by_tag = false;
        const auto& p2p_tags = segment.tags;
        for (const auto& tag : config.tag_filters) {
            if (std::find(p2p_tags.begin(), p2p_tags.end(), tag) !=
                p2p_tags.end()) {
                excluded_by_tag = true;
                break;
            }
        }
        if (excluded_by_tag) continue;

        // 1.2 Filter by segment priority.
        if (segment.priority < config.priority_limit) {
            continue;
        }

        // 1.3 client-granularity: keep the highest-priority client.
        auto descriptor = BuildRouteDescriptor(location, route.object_size);
        if (!descriptor.has_value()) {
            continue;
        }
        auto it = best_by_client.find(location.client_id);
        if (it == best_by_client.end()) {
            best_by_client[location.client_id] = candidates.size();
            candidates.emplace_back(segment.priority, std::move(*descriptor));
        } else if (segment.priority > candidates[it->second].first) {
            candidates[it->second] =
                std::make_pair(segment.priority, std::move(*descriptor));
        }
    }  // for over

    if (config.max_candidates ==
            P2PReadRouteConfig::RETURN_ALL_CANDIDATES ||
        config.max_candidates >= candidates.size() || candidates.empty()) {
        // return all candidates
        std::vector<P2PRouteDescriptor> result;
        result.reserve(candidates.size());
        for (auto& candidate : candidates) {
            result.push_back(std::move(candidate.second));
        }
        return result;
    }

    // 2. Choose the highest-priority candidates when a limit is configured.
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<P2PRouteDescriptor> result;
    result.reserve(config.max_candidates);
    for (size_t i = 0; i < config.max_candidates; ++i) {
        result.push_back(std::move(candidates[i].second));
    }
    return result;
}

auto P2PMasterService::GetWriteRoute(const P2PGetWriteRouteRequest& req)
    -> tl::expected<P2PGetWriteRouteResponse, ErrorCode> {
    if (!req.config.IsValid()) {
        LOG(ERROR) << "invalid write route config: " << req.config
                   << ", client_id: " << req.client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // 1. Collect existing route owners and enforce the client limit.
    OwnerClientSet owners;
    if (!req.key.empty()) {
        auto route = route_table_.GetRoute(req.key);
        if (route.has_value()) {
            owners = CollectRouteOwnerClients(*route);
            if (max_client_per_key_ > 0 &&
                owners.size() >= max_client_per_key_) {
                LOG(WARNING)
                    << "route owner client num exceeded"
                    << ", key: " << req.key << ", client_id: " << req.client_id
                    << ", current: " << owners.size()
                    << ", max: " << max_client_per_key_;
                return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
            }
        }
    }

    // 2. Single pass: collect and score all candidates.
    //    score = free_ratio * (is_local ? (1 - remote_weight) : remote_weight)
    const double remote_weight = std::clamp(req.config.remote_weight, 0.0, 1.0);
    std::vector<P2PWriteCandidate> candidates;
    const bool can_early_stop =
        req.config.early_return &&
        req.config.max_candidates !=
            P2PWriteRouteConfig::RETURN_ALL_CANDIDATES;

    client_manager_->ForEachClient(
        req.config.strategy,
        [&](const std::shared_ptr<P2PClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            const UUID client_id = client->get_client_id();
            if (owners.contains(client_id)) {
                return false;
            }
            const bool is_local = client_id == req.client_id;
            const double weight =
                is_local ? (1.0 - remote_weight) : remote_weight;
            if (weight <= 0.0) {
                return false;
            }

            if (auto candidate = client->GetWriteRouteCandidate(req)) {
                candidate->score *= weight;
                candidates.push_back(std::move(*candidate));
                return can_early_stop &&
                       candidates.size() >= req.config.max_candidates;
            }
            return false;
        });

    if (candidates.empty()) {
        LOG(ERROR) << "no candidate found for key: " << req.key
                   << ", client_id: " << req.client_id
                   << ", size: " << req.object_size;
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_CANDIDATE);
    }
    // 3. Sort by score descending, using capacity as the tiebreaker, then
    // truncate the result.
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) {
                  return std::tie(b.score, b.available_capacity) <
                         std::tie(a.score, a.available_capacity);
              });
    if (req.config.max_candidates !=
            P2PWriteRouteConfig::RETURN_ALL_CANDIDATES &&
        candidates.size() > req.config.max_candidates) {
        candidates.resize(req.config.max_candidates);
    }
    P2PGetWriteRouteResponse response;
    response.candidates = std::move(candidates);
    return response;
}

auto P2PMasterService::BatchGetWriteRoute(const P2PBatchGetWriteRouteRequest& req)
    -> P2PBatchGetWriteRouteResponse {
    P2PBatchGetWriteRouteResponse response;
    response.responses.resize(req.keys.size());
    response.error_codes.resize(req.keys.size(), ErrorCode::OK);

    if (req.keys.size() != req.object_sizes.size()) {
        LOG(ERROR) << "BatchGetWriteRoute rejected inconsistent request arrays"
                   << ", keys=" << req.keys.size()
                   << ", sizes=" << req.object_sizes.size();
        std::fill(response.error_codes.begin(), response.error_codes.end(),
                  ErrorCode::INVALID_PARAMS);
        return response;
    }

    P2PGetWriteRouteRequest single_req;
    single_req.client_id = req.client_id;
    single_req.config = req.config;
    for (size_t i = 0; i < req.keys.size(); ++i) {
        single_req.key = req.keys[i];
        single_req.object_size = req.object_sizes[i];
        auto result = GetWriteRoute(single_req);
        if (result.has_value()) {
            response.responses[i] = std::move(*result);
        } else {
            response.error_codes[i] = result.error();
        }
    }
    return response;
}

auto P2PMasterService::AddReplica(const P2PPublishRouteRequest& req)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(req.client_id);
    if (!client) {
        LOG(ERROR) << "client not found"
                   << ", client_id: " << req.client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return InnerAddReplica(req.key, req.client_id, req.segment_id,
                           req.object_size, client);
}

auto P2PMasterService::InnerAddReplica(
    std::string_view key, const UUID& client_id, const UUID& segment_id,
    size_t size, const std::shared_ptr<P2PClientMeta>& client)
    -> tl::expected<void, ErrorCode> {
    auto segment = client->QuerySegment(segment_id);
    if (!segment.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", client_id: " << client_id
                   << ", segment_id: " << segment_id;
        return tl::make_unexpected(segment.error());
    }

    const P2PRouteLocation location{.client_id = client_id,
                                    .segment_id = segment_id};
    // AddReplica commits the in-memory route first. OpLog is best-effort;
    // returning an OpLog error could make the client delete its local replica.
    auto mutation = route_table_.Publish(key, size, location);
    if (!mutation.has_value()) {
        LOG(WARNING) << "fail to publish route"
                     << ", key: " << key << ", client_id: " << client_id
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(mutation.error());
    }
    if (mutation->created_key) {
        P2PMasterMetricManager::instance().inc_key_count(1);
        P2PMasterMetricManager::instance().observe_value_size(size);
    }

    AddReplicaPayload payload;
    payload.object_key = std::string(key);
    payload.client_id = client_id;
    payload.segment_id = segment_id;
    payload.size = size;
    const auto error = RecordOplog(OpType_ADD_REPLICA, payload.object_key,
                                   SerializeP2PPayload(payload));
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "AddReplica(P2P): failed to record oplog"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment_id
                   << ", error=" << toString(error)
                   << "; keeping the in-memory route";
    }
    return {};
}

auto P2PMasterService::RemoveReplica(const P2PWithdrawRouteRequest& req)
    -> tl::expected<void, ErrorCode> {
    return InnerRemoveReplica(req.key, req.client_id, req.segment_id);
}

auto P2PMasterService::InnerRemoveReplica(std::string_view key,
                                          const UUID& client_id,
                                          const UUID& segment_id)
    -> tl::expected<void, ErrorCode> {
    auto route = route_table_.GetRoute(key);
    if (!route.has_value()) {
        LOG(WARNING) << "object not found"
                     << ", key: " << key << ", client_id: " << client_id
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const P2PRouteLocation location{.client_id = client_id,
                                    .segment_id = segment_id};
    if (std::find(route->locations.begin(), route->locations.end(), location) ==
        route->locations.end()) {
        LOG(WARNING) << "route location not found"
                     << ", key: " << key << ", client_id: " << client_id
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    RemoveReplicaPayload payload;
    payload.object_key = std::string(key);
    payload.client_id = client_id;
    payload.segment_id = segment_id;
    const auto record_error =
        RecordOplog(OpType_REMOVE_REPLICA, payload.object_key,
                    SerializeP2PPayload(payload));
    if (record_error != ErrorCode::OK) {
        LOG(ERROR) << "RemoveReplica(P2P): failed to record oplog"
                   << ", client_id=" << client_id
                   << ", segment_id=" << segment_id
                   << ", error=" << toString(record_error);
        return tl::make_unexpected(record_error);
    }

    auto mutation = route_table_.Withdraw(key, location);
    if (!mutation.has_value()) {
        LOG(ERROR) << "Route changed while withdrawing after OpLog persistence"
                   << ", key=" << key << ", client_id=" << client_id
                   << ", segment_id=" << segment_id
                   << ", error=" << toString(mutation.error());
        return tl::make_unexpected(mutation.error());
    }
    if (mutation->removed_key) {
        P2PMasterMetricManager::instance().dec_key_count(1);
    }
    return {};
}

auto P2PMasterService::BatchRemoveReplica(const P2PBatchWithdrawRouteRequest& req)
    -> P2PBatchWithdrawRouteResponse {
    P2PBatchWithdrawRouteResponse response;
    response.error_codes.resize(req.segment_ids.size(), ErrorCode::OK);
    for (size_t index = 0; index < req.segment_ids.size(); ++index) {
        const auto& segment_id = req.segment_ids[index];
        auto result = InnerRemoveReplica(req.key, req.client_id, segment_id);
        if (!result.has_value()) {
            if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
                // The object may have been removed by another thread.
                LOG(INFO) << "object not found when batch removing route"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
            } else if (result.error() == ErrorCode::REPLICA_NOT_FOUND) {
                // The route may have been removed by another thread.
                LOG(INFO) << "route not found when batch removing route"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
            } else {
                LOG(ERROR) << "failed to remove route"
                           << ", key: " << req.key
                           << ", client_id: " << req.client_id
                           << ", segment_id: " << segment_id
                           << ", error: " << toString(result.error());
                response.error_codes[index] = result.error();
            }
        }
    }
    return response;
}

auto P2PMasterService::BatchSyncRoutes(
    const P2PBatchSyncRoutesRequest& request) -> P2PBatchSyncRoutesResponse {
    P2PBatchSyncRoutesResponse response;
    response.publish_results.resize(request.publish_operations.size(),
                                    ErrorCode::OK);
    response.withdraw_results.resize(request.withdraw_operations.size(),
                                     ErrorCode::OK);

    auto client = client_manager_->GetClient(request.client_id);
    if (!client) {
        LOG(ERROR) << "BatchSyncRoutes: client not found"
                   << ", client_id=" << request.client_id;
        std::fill(response.publish_results.begin(),
                  response.publish_results.end(),
                  ErrorCode::CLIENT_NOT_FOUND);
        std::fill(response.withdraw_results.begin(),
                  response.withdraw_results.end(),
                  ErrorCode::CLIENT_NOT_FOUND);
        return response;
    }

    route_table_.BatchSync(
        request.client_id, request.publish_operations,
        request.withdraw_operations,
        [client](const P2PPublishRouteOperation& operation) {
            auto segment = client->QuerySegment(operation.segment_id);
            if (!segment.has_value()) {
                LOG(ERROR) << "BatchSyncRoutes: segment not found"
                           << ", client_id=" << client->get_client_id()
                           << ", segment_id=" << operation.segment_id
                           << ", key=" << operation.key
                           << ", error=" << toString(segment.error());
                return segment.error();
            }
            return ErrorCode::OK;
        },
        [this, &response, &request](
            size_t index, const P2PPublishRouteOperation& operation,
            const P2PRouteTable::Mutation& result) {
            if (!result.has_value()) {
                response.publish_results[index] = result.error();
                return;
            }
            if (result->created_key) {
                P2PMasterMetricManager::instance().inc_key_count(1);
                P2PMasterMetricManager::instance().observe_value_size(
                    operation.object_size);
            }

            AddReplicaPayload payload;
            payload.object_key = operation.key;
            payload.client_id = request.client_id;
            payload.segment_id = operation.segment_id;
            payload.size = operation.object_size;
            const auto error =
                RecordOplog(OpType_ADD_REPLICA, payload.object_key,
                            SerializeP2PPayload(payload));
            if (error != ErrorCode::OK) {
                LOG(ERROR) << "BatchSyncRoutes: failed to record publish oplog"
                           << ", client_id=" << request.client_id
                           << ", segment_id=" << operation.segment_id
                           << ", key=" << operation.key
                           << ", error=" << toString(error)
                           << "; keeping the in-memory route";
            }
        },
        [this, &request](const P2PWithdrawRouteOperation& operation) {
            RemoveReplicaPayload payload;
            payload.object_key = operation.key;
            payload.client_id = request.client_id;
            payload.segment_id = operation.segment_id;
            const auto error =
                RecordOplog(OpType_REMOVE_REPLICA, payload.object_key,
                            SerializeP2PPayload(payload));
            if (error != ErrorCode::OK) {
                LOG(ERROR)
                    << "BatchSyncRoutes: failed to record withdraw oplog"
                    << ", client_id=" << request.client_id
                    << ", segment_id=" << operation.segment_id
                    << ", key=" << operation.key
                    << ", error=" << toString(error);
            }
            return error;
        },
        [&response](size_t index,
                    const P2PWithdrawRouteOperation& operation,
                    const P2PRouteTable::Mutation& result) {
            (void)operation;
            if (!result.has_value()) {
                if (result.error() != ErrorCode::OBJECT_NOT_FOUND &&
                    result.error() != ErrorCode::REPLICA_NOT_FOUND) {
                    response.withdraw_results[index] = result.error();
                }
                return;
            }
            if (result->removed_key) {
                P2PMasterMetricManager::instance().dec_key_count(1);
            }
        });
    return response;
}

auto P2PMasterService::SetSyncCompleted(UUID client_id)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(client_id);
    if (!client) {
        LOG(WARNING) << "SetSyncCompleted: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    client->SetSyncing(false);
    LOG(INFO) << "SetSyncCompleted: client_id=" << client_id;
    return {};
}

ErrorCode P2PMasterService::RestoreFromStandbyMetadata(
    const P2PStandbyMetadataStore::ExportedMetadata& metadata,
    uint64_t last_applied_sequence_id) {
    if (GetKeyCount() != 0 || !client_manager_->GetAllClients().empty()) {
        HAMetricManager::instance().inc_promotion_restore_failures();
        LOG(ERROR) << "RestoreFromStandbyMetadata: target service is not empty"
                   << ", existing_keys=" << GetKeyCount()
                   << ", existing_clients="
                   << client_manager_->GetAllClients().size();
        return ErrorCode::INVALID_PARAMS;
    }

    if (last_applied_sequence_id > 0) {
        if (auto* manager = GetOpLogManager()) {
            manager->SetInitialSequenceId(last_applied_sequence_id);
        } else {
            LOG(WARNING)
                << "RestoreFromStandbyMetadata: cannot set initial OpLog "
                   "sequence without OpLogManager"
                << ", last_applied_sequence_id=" << last_applied_sequence_id;
        }
    }

    size_t restored_clients = 0;
    size_t restored_objects = 0;
    size_t restored_routes = 0;
    size_t skipped_routes = 0;
    size_t skipped_objects = 0;

    for (const auto& [client_id, client_info] : metadata.clients) {
        P2PRegisterClientRequest req;
        req.client_id = client_id;
        req.ip_address = client_info.ip_address;
        req.rpc_port = client_info.rpc_port;
        req.segments = client_info.segments;

        auto result = client_manager_->RegisterClient(req);
        if (!result.has_value()) {
            HAMetricManager::instance().inc_promotion_restore_failures();
            LOG(ERROR) << "RestoreFromStandbyMetadata: failed to restore client"
                       << ", client_id=" << client_id
                       << ", error=" << toString(result.error());
            return result.error();
        }
        if (auto p2p_client = client_manager_->GetClient(client_id)) {
            p2p_client->SetSyncing(false);
        }
        ++restored_clients;
    }

    for (const auto& [key, standby_metadata] : metadata.objects) {
        bool restored_object = false;
        for (const auto& desc : standby_metadata.replicas) {
            if (!std::holds_alternative<P2PProxyDescriptor>(
                    desc.descriptor_variant)) {
                LOG(WARNING) << "RestoreFromStandbyMetadata: skipping non-P2P "
                                "route descriptor"
                             << ", key=" << key;
                ++skipped_routes;
                continue;
            }

            const auto& p2p_desc =
                std::get<P2PProxyDescriptor>(desc.descriptor_variant);
            auto client = client_manager_->GetClient(p2p_desc.client_id);
            if (!client ||
                !client->QuerySegment(p2p_desc.segment_id).has_value()) {
                LOG(WARNING)
                    << "RestoreFromStandbyMetadata: skipping route "
                       "with unavailable client or segment"
                    << ", key=" << key << ", client_id=" << p2p_desc.client_id
                    << ", segment_id=" << p2p_desc.segment_id;
                ++skipped_routes;
                continue;
            }

            const uint64_t object_size = standby_metadata.size != 0
                                             ? standby_metadata.size
                                             : p2p_desc.object_size;
            auto mutation = route_table_.Publish(
                key, object_size,
                P2PRouteLocation{.client_id = p2p_desc.client_id,
                                 .segment_id = p2p_desc.segment_id});
            if (!mutation.has_value()) {
                HAMetricManager::instance().inc_promotion_restore_failures();
                LOG(ERROR) << "RestoreFromStandbyMetadata: failed to restore "
                              "route"
                           << ", key=" << key
                           << ", error=" << toString(mutation.error());
                return mutation.error();
            }
            if (mutation->created_key) {
                P2PMasterMetricManager::instance().inc_key_count(1);
                P2PMasterMetricManager::instance().observe_value_size(
                    object_size);
                restored_object = true;
            }
            ++restored_routes;
        }
        if (restored_object) {
            ++restored_objects;
        } else if (!route_table_.RouteExists(key)) {
            ++skipped_objects;
        }
    }

    if (skipped_routes > 0 || skipped_objects > 0) {
        HAMetricManager::instance().set_primary_degraded(true);
        HAMetricManager::instance().inc_promotion_skipped_replicas(
            static_cast<int64_t>(skipped_routes));
        HAMetricManager::instance().inc_promotion_skipped_objects(
            static_cast<int64_t>(skipped_objects));
    }
    LOG(INFO) << "RestoreFromStandbyMetadata: restored"
              << ", clients=" << restored_clients
              << ", objects=" << restored_objects
              << ", routes=" << restored_routes
              << ", skipped_routes=" << skipped_routes
              << ", skipped_objects=" << skipped_objects
              << ", last_applied_sequence_id=" << last_applied_sequence_id;
    return ErrorCode::OK;
}

}  // namespace mooncake
