#include "p2p/master/p2p_master_service.h"

#include <algorithm>
#include <stdexcept>
#include <tuple>
#include <unordered_map>

#include <glog/logging.h>

#include "p2p/ha/ha_metric_manager.h"
#include "p2p/ha/oplog/oplog_store_factory.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
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
        const auto store_type = ParseOpLogStoreType(config.oplog.store_type);
        const std::string& location =
            store_type == OpLogStoreType::REDIS ? config.redis.endpoint
                                                : config.oplog.data_dir;
        auto store = OpLogStoreFactory::Create(
            store_type, config.cluster_id, OpLogStoreRole::WRITER, location,
            kDefaultOpLogPollIntervalMs, config.redis.password,
            config.redis.username, config.redis.db_index,
            config.oplog.async_queue_max_entries,
            config.oplog.async_queue_overflow_mode,
            config.oplog.best_effort_max_retries);
        if (!store) {
            LOG(ERROR) << "Failed to initialize P2P OpLog store"
                       << ", type=" << config.oplog.store_type
                       << ", location=" << location;
            throw std::runtime_error("failed to initialize P2P OpLog store");
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

auto P2PMasterService::RegisterClient(
    const P2PRegisterClientRequest& request)
    -> tl::expected<P2PRegisterClientResponse, ErrorCode> {
    if (request.ip_address.empty() || request.rpc_port == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (client_manager_->GetClient(request.client_id)) {
        return P2PRegisterClientResponse{.view_version = view_version_};
    }
    auto result = client_manager_->RegisterClient(request);
    if (!result.has_value()) {
        if (result.error() == ErrorCode::CLIENT_ALREADY_EXISTS &&
            client_manager_->GetClient(request.client_id)) {
            return P2PRegisterClientResponse{.view_version = view_version_};
        }
        return result;
    }

    RegisterClientPayload payload;
    payload.client_id = request.client_id;
    payload.ip_address = request.ip_address;
    payload.rpc_port = request.rpc_port;
    payload.segments = request.segments;
    const auto error = RecordOplog(OpType_REGISTER_CLIENT, "",
                                   SerializeP2PPayload(payload));
    return error == ErrorCode::OK
               ? result
               : tl::expected<P2PRegisterClientResponse, ErrorCode>(
                     tl::make_unexpected(error));
}

auto P2PMasterService::UnregisterClient(
    const P2PUnregisterClientRequest& request)
    -> tl::expected<P2PUnregisterClientResponse, ErrorCode> {
    auto result = client_manager_->UnregisterClient(request);
    if (!result.has_value()) {
        return result;
    }
    UnregisterClientPayload payload;
    payload.client_id = request.client_id;
    const auto error = RecordOplog(OpType_UNREGISTER_CLIENT, "",
                                   SerializeP2PPayload(payload));
    return error == ErrorCode::OK
               ? result
               : tl::expected<P2PUnregisterClientResponse, ErrorCode>(
                     tl::make_unexpected(error));
}

auto P2PMasterService::Heartbeat(const P2PHeartbeatRequest& request)
    -> tl::expected<P2PHeartbeatResponse, ErrorCode> {
    return client_manager_->Heartbeat(request);
}

auto P2PMasterService::QueryClientStatus(
    const P2PQueryClientStatusRequest& request)
    -> tl::expected<P2PQueryClientStatusResponse, ErrorCode> {
    return client_manager_->QueryClientStatus(request);
}

auto P2PMasterService::MountSegment(const P2PMountSegmentRequest& request)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(request.client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto result = client->MountSegment(request.segment);
    if (!result.has_value()) {
        return result;
    }
    MountSegmentPayload payload;
    payload.client_id = request.client_id;
    payload.segment = request.segment;
    const auto error = RecordOplog(OpType_MOUNT_SEGMENT, "",
                                   SerializeP2PPayload(payload));
    return error == ErrorCode::OK ? tl::expected<void, ErrorCode>{}
                                  : tl::expected<void, ErrorCode>(
                                        tl::make_unexpected(error));
}

auto P2PMasterService::UnmountSegment(
    const P2PUnmountSegmentRequest& request)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(request.client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto result = client->UnmountSegment(request.segment_id);
    if (!result.has_value()) {
        return result;
    }
    OnSegmentRemoved(P2PRouteLocation{.client_id = request.client_id,
                                      .segment_id = request.segment_id});

    UnmountSegmentPayload payload;
    payload.client_id = request.client_id;
    payload.segment_id = request.segment_id;
    const auto error = RecordOplog(OpType_UNMOUNT_SEGMENT, "",
                                   SerializeP2PPayload(payload));
    return error == ErrorCode::OK ? tl::expected<void, ErrorCode>{}
                                  : tl::expected<void, ErrorCode>(
                                        tl::make_unexpected(error));
}

auto P2PMasterService::RouteExists(const P2PRouteExistsRequest& request)
    -> tl::expected<P2PRouteExistsResponse, ErrorCode> {
    return P2PRouteExistsResponse{
        .exists = route_table_.RouteExists(request.key)};
}

auto P2PMasterService::BatchRouteExists(
    const P2PBatchRouteExistsRequest& request)
    -> P2PBatchRouteExistsResponse {
    P2PBatchRouteExistsResponse response;
    response.responses.resize(request.keys.size());
    response.error_codes.resize(request.keys.size(), ErrorCode::OK);
    for (size_t i = 0; i < request.keys.size(); ++i) {
        response.responses[i].exists =
            route_table_.RouteExists(request.keys[i]);
    }
    return response;
}

auto P2PMasterService::BuildRouteDescriptor(
    const P2PRouteLocation& location, uint64_t object_size) const
    -> tl::expected<P2PRouteDescriptor, ErrorCode> {
    auto client = client_manager_->GetClient(location.client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto segment = client->QuerySegment(location.segment_id);
    if (!segment.has_value()) {
        return tl::make_unexpected(segment.error());
    }
    return P2PRouteDescriptor{.client_id = location.client_id,
                              .segment_id = location.segment_id,
                              .ip_address = client->get_ip_address(),
                              .rpc_port = client->get_rpc_port(),
                              .object_size = object_size};
}

std::vector<P2PRouteDescriptor> P2PMasterService::FilterRoutes(
    const P2PReadRouteConfig& config, const P2PRouteEntry& route) const {
    std::vector<std::pair<int, P2PRouteDescriptor>> candidates;
    std::unordered_map<UUID, size_t, boost::hash<UUID>> best_by_client;
    for (const auto& location : route.locations) {
        auto client = client_manager_->GetClient(location.client_id);
        if (!client || !client->is_health()) {
            continue;
        }
        auto segment = client->QuerySegment(location.segment_id);
        if (!segment.has_value()) {
            continue;
        }
        const bool excluded = std::any_of(
            config.tag_filters.begin(), config.tag_filters.end(),
            [&](const std::string& tag) {
                return std::find(segment->tags.begin(), segment->tags.end(),
                                 tag) != segment->tags.end();
            });
        if (excluded || segment->priority < config.priority_limit) {
            continue;
        }
        auto descriptor = BuildRouteDescriptor(location, route.object_size);
        if (!descriptor.has_value()) {
            continue;
        }
        auto owner = best_by_client.find(location.client_id);
        if (owner == best_by_client.end()) {
            best_by_client[location.client_id] = candidates.size();
            candidates.emplace_back(segment->priority, std::move(*descriptor));
        } else if (segment->priority > candidates[owner->second].first) {
            candidates[owner->second] =
                std::make_pair(segment->priority, std::move(*descriptor));
        }
    }
    if (config.max_candidates != P2PReadRouteConfig::RETURN_ALL_CANDIDATES &&
        candidates.size() > config.max_candidates) {
        std::sort(candidates.begin(), candidates.end(),
                  [](const auto& lhs, const auto& rhs) {
                      return lhs.first > rhs.first;
                  });
        candidates.resize(config.max_candidates);
    }
    std::vector<P2PRouteDescriptor> result;
    result.reserve(candidates.size());
    for (auto& candidate : candidates) {
        result.push_back(std::move(candidate.second));
    }
    return result;
}

auto P2PMasterService::GetReadRoute(const P2PGetReadRouteRequest& request)
    -> tl::expected<P2PGetReadRouteResponse, ErrorCode> {
    auto route = route_table_.GetRoute(request.key);
    if (!route.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    auto routes = FilterRoutes(request.config, *route);
    if (routes.empty()) {
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    return P2PGetReadRouteResponse{.routes = std::move(routes)};
}

auto P2PMasterService::BatchGetReadRoute(
    const P2PBatchGetReadRouteRequest& request)
    -> P2PBatchGetReadRouteResponse {
    P2PBatchGetReadRouteResponse response;
    response.responses.resize(request.keys.size());
    response.error_codes.resize(request.keys.size(), ErrorCode::OK);
    for (size_t i = 0; i < request.keys.size(); ++i) {
        auto result = GetReadRoute(P2PGetReadRouteRequest{
            .key = request.keys[i], .config = request.config});
        if (result.has_value()) {
            response.responses[i] = std::move(*result);
        } else {
            response.error_codes[i] = result.error();
        }
    }
    return response;
}

P2PMasterService::OwnerClientSet P2PMasterService::CollectRouteOwnerClients(
    const P2PRouteEntry& route) {
    OwnerClientSet clients;
    for (const auto& location : route.locations) {
        clients.insert(location.client_id);
    }
    return clients;
}

auto P2PMasterService::GetWriteRoute(const P2PGetWriteRouteRequest& request)
    -> tl::expected<P2PGetWriteRouteResponse, ErrorCode> {
    if (!request.config.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    OwnerClientSet owners;
    if (auto route = route_table_.GetRoute(request.key)) {
        owners = CollectRouteOwnerClients(*route);
        if (max_client_per_key_ > 0 && owners.size() >= max_client_per_key_) {
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }
    }

    const double remote_weight =
        std::clamp(request.config.remote_weight, 0.0, 1.0);
    std::vector<P2PWriteCandidate> candidates;
    const bool can_early_stop =
        request.config.early_return &&
        request.config.max_candidates !=
            P2PWriteRouteConfig::RETURN_ALL_CANDIDATES;
    client_manager_->ForEachClient(
        request.config.strategy,
        [&](const std::shared_ptr<P2PClientMeta>& client)
            -> tl::expected<bool, ErrorCode> {
            const auto client_id = client->get_client_id();
            if (owners.contains(client_id)) {
                return false;
            }
            const double weight = client_id == request.client_id
                                      ? 1.0 - remote_weight
                                      : remote_weight;
            if (weight <= 0.0) {
                return false;
            }
            if (auto candidate = client->GetWriteRouteCandidate(request)) {
                candidate->score *= weight;
                candidates.push_back(std::move(*candidate));
                return can_early_stop && candidates.size() >=
                                             request.config.max_candidates;
            }
            return false;
        });
    if (candidates.empty()) {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_CANDIDATE);
    }
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& lhs, const auto& rhs) {
                  return std::tie(rhs.score, rhs.available_capacity) <
                         std::tie(lhs.score, lhs.available_capacity);
              });
    if (request.config.max_candidates !=
            P2PWriteRouteConfig::RETURN_ALL_CANDIDATES &&
        candidates.size() > request.config.max_candidates) {
        candidates.resize(request.config.max_candidates);
    }
    return P2PGetWriteRouteResponse{.candidates = std::move(candidates)};
}

auto P2PMasterService::BatchGetWriteRoute(
    const P2PBatchGetWriteRouteRequest& request)
    -> P2PBatchGetWriteRouteResponse {
    P2PBatchGetWriteRouteResponse response;
    response.responses.resize(request.keys.size());
    response.error_codes.resize(request.keys.size(), ErrorCode::OK);
    if (request.keys.size() != request.object_sizes.size()) {
        std::fill(response.error_codes.begin(), response.error_codes.end(),
                  ErrorCode::INVALID_PARAMS);
        return response;
    }
    for (size_t i = 0; i < request.keys.size(); ++i) {
        auto result = GetWriteRoute(P2PGetWriteRouteRequest{
            .key = request.keys[i],
            .client_id = request.client_id,
            .object_size = request.object_sizes[i],
            .config = request.config});
        if (result.has_value()) {
            response.responses[i] = std::move(*result);
        } else {
            response.error_codes[i] = result.error();
        }
    }
    return response;
}

auto P2PMasterService::PublishRoute(const P2PPublishRouteRequest& request)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(request.client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return InnerPublishRoute(request.key, request.client_id,
                             request.segment_id, request.object_size, client);
}

auto P2PMasterService::InnerPublishRoute(
    std::string_view key, const UUID& client_id, const UUID& segment_id,
    uint64_t object_size, const std::shared_ptr<P2PClientMeta>& client)
    -> tl::expected<void, ErrorCode> {
    if (!client->QuerySegment(segment_id).has_value()) {
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    const P2PRouteLocation location{.client_id = client_id,
                                    .segment_id = segment_id};
    auto mutation = route_table_.Publish(key, object_size, location);
    if (!mutation.has_value()) {
        return tl::make_unexpected(mutation.error());
    }
    if (mutation->created_key) {
        P2PMasterMetricManager::instance().inc_key_count(1);
        P2PMasterMetricManager::instance().observe_value_size(object_size);
    }

    PublishRoutePayload payload;
    payload.object_key = std::string(key);
    payload.client_id = client_id;
    payload.segment_id = segment_id;
    payload.size = object_size;
    const auto error = RecordOplog(OpType_PUBLISH_ROUTE, payload.object_key,
                                   SerializeP2PPayload(payload));
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "Failed to persist publish route; retaining memory route"
                   << ", key=" << key << ", error=" << toString(error);
    }
    return {};
}

auto P2PMasterService::WithdrawRoute(const P2PWithdrawRouteRequest& request)
    -> tl::expected<void, ErrorCode> {
    return InnerWithdrawRoute(request.key, request.client_id,
                              request.segment_id);
}

auto P2PMasterService::InnerWithdrawRoute(std::string_view key,
                                          const UUID& client_id,
                                          const UUID& segment_id)
    -> tl::expected<void, ErrorCode> {
    auto route = route_table_.GetRoute(key);
    if (!route.has_value()) {
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const P2PRouteLocation location{.client_id = client_id,
                                    .segment_id = segment_id};
    if (std::find(route->locations.begin(), route->locations.end(), location) ==
        route->locations.end()) {
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    WithdrawRoutePayload payload;
    payload.object_key = std::string(key);
    payload.client_id = client_id;
    payload.segment_id = segment_id;
    const auto error = RecordOplog(OpType_WITHDRAW_ROUTE, payload.object_key,
                                   SerializeP2PPayload(payload));
    if (error != ErrorCode::OK) {
        return tl::make_unexpected(error);
    }
    auto mutation = route_table_.Withdraw(key, location);
    if (!mutation.has_value()) {
        LOG(ERROR) << "Route changed after withdraw was persisted"
                   << ", key=" << key
                   << ", error=" << toString(mutation.error());
        return tl::make_unexpected(mutation.error());
    }
    if (mutation->removed_key) {
        P2PMasterMetricManager::instance().dec_key_count(1);
    }
    return {};
}

auto P2PMasterService::BatchWithdrawRoute(
    const P2PBatchWithdrawRouteRequest& request)
    -> P2PBatchWithdrawRouteResponse {
    P2PBatchWithdrawRouteResponse response;
    response.error_codes.reserve(request.segment_ids.size());
    for (const auto& segment_id : request.segment_ids) {
        auto result = InnerWithdrawRoute(request.key, request.client_id,
                                         segment_id);
        response.error_codes.push_back(
            !result.has_value() &&
                    result.error() != ErrorCode::OBJECT_NOT_FOUND &&
                    result.error() != ErrorCode::REPLICA_NOT_FOUND
                ? result.error()
                : ErrorCode::OK);
    }
    return response;
}

auto P2PMasterService::BatchSyncRoutes(
    const P2PBatchSyncRoutesRequest& request) -> P2PBatchSyncRoutesResponse {
    P2PBatchSyncRoutesResponse response;
    if (request.publish_keys.size() != request.publish_sizes.size() ||
        request.publish_keys.size() != request.publish_segment_ids.size() ||
        request.withdraw_keys.size() != request.withdraw_segment_ids.size()) {
        response.publish_results.assign(request.publish_keys.size(),
                                        ErrorCode::INVALID_PARAMS);
        response.withdraw_results.assign(request.withdraw_keys.size(),
                                         ErrorCode::INVALID_PARAMS);
        return response;
    }
    response.publish_results.resize(request.publish_keys.size(), ErrorCode::OK);
    response.withdraw_results.resize(request.withdraw_keys.size(),
                                     ErrorCode::OK);
    auto client = client_manager_->GetClient(request.client_id);
    if (!client) {
        std::fill(response.publish_results.begin(),
                  response.publish_results.end(), ErrorCode::CLIENT_NOT_FOUND);
        std::fill(response.withdraw_results.begin(),
                  response.withdraw_results.end(),
                  ErrorCode::CLIENT_NOT_FOUND);
        return response;
    }
    for (size_t i = 0; i < request.publish_keys.size(); ++i) {
        auto result = InnerPublishRoute(
            request.publish_keys[i], request.client_id,
            request.publish_segment_ids[i], request.publish_sizes[i], client);
        if (!result.has_value()) {
            response.publish_results[i] = result.error();
        }
    }
    for (size_t i = 0; i < request.withdraw_keys.size(); ++i) {
        auto result = InnerWithdrawRoute(request.withdraw_keys[i],
                                         request.client_id,
                                         request.withdraw_segment_ids[i]);
        if (!result.has_value() &&
            result.error() != ErrorCode::OBJECT_NOT_FOUND &&
            result.error() != ErrorCode::REPLICA_NOT_FOUND) {
            response.withdraw_results[i] = result.error();
        }
    }
    return response;
}

auto P2PMasterService::CompleteRouteSync(
    const P2PCompleteRouteSyncRequest& request)
    -> tl::expected<void, ErrorCode> {
    auto client = client_manager_->GetClient(request.client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    client->SetSyncing(false);
    return {};
}

std::vector<std::string> P2PMasterService::ListRouteKeys() const {
    return route_table_.ListRouteKeys();
}

size_t P2PMasterService::GetRouteKeyCount() const {
    return route_table_.GetRouteKeyCount();
}

auto P2PMasterService::GetClientSegments(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    return client_manager_->GetClientSegments(client_id);
}

auto P2PMasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    return client_manager_->QueryIp(client_id);
}

void P2PMasterService::OnSegmentRemoved(
    const P2PRouteLocation& location) {
    auto cleanup = route_table_.RemoveLocation(location);
    if (!cleanup.removed_keys.empty()) {
        P2PMasterMetricManager::instance().dec_key_count(
            cleanup.removed_keys.size());
    }
}

ErrorCode P2PMasterService::RecordOplog(OpType type, const std::string& key,
                                        const std::string& payload) {
    if (!oplog_manager_) {
        return ErrorCode::OK;
    }
    auto result = oplog_manager_->AppendAndPersist(
        type, key, payload, /*sync=*/!enable_async_oplog_write_);
    if (!result.has_value()) {
        LOG(ERROR) << "Failed to persist P2P OpLog"
                   << ", op_type=" << static_cast<int>(type)
                   << ", key=" << key
                   << ", error=" << toString(result.error());
        return result.error();
    }
    return ErrorCode::OK;
}

ErrorCode P2PMasterService::RestoreFromStandbyMetadata(
    const P2PStandbyMetadataStore::ExportedMetadata& metadata,
    uint64_t last_applied_sequence_id) {
    if (GetRouteKeyCount() != 0 || !client_manager_->GetAllClients().empty()) {
        HAMetricManager::instance().inc_promotion_restore_failures();
        return ErrorCode::INVALID_PARAMS;
    }
    if (last_applied_sequence_id > 0 && oplog_manager_) {
        oplog_manager_->SetInitialSequenceId(last_applied_sequence_id);
    }
    for (const auto& [client_id, info] : metadata.clients) {
        auto result = client_manager_->RegisterClient(P2PRegisterClientRequest{
            .client_id = client_id,
            .segments = info.segments,
            .ip_address = info.ip_address,
            .rpc_port = info.rpc_port});
        if (!result.has_value()) {
            HAMetricManager::instance().inc_promotion_restore_failures();
            return result.error();
        }
        client_manager_->GetClient(client_id)->SetSyncing(false);
    }

    size_t skipped_routes = 0;
    size_t skipped_keys = 0;
    for (const auto& [key, route] : metadata.routes) {
        bool restored = false;
        for (const auto& location : route.locations) {
            auto client = client_manager_->GetClient(location.client_id);
            if (!client ||
                !client->QuerySegment(location.segment_id).has_value()) {
                ++skipped_routes;
                continue;
            }
            auto mutation =
                route_table_.Publish(key, route.object_size, location);
            if (!mutation.has_value()) {
                HAMetricManager::instance().inc_promotion_restore_failures();
                return mutation.error();
            }
            if (mutation->created_key) {
                P2PMasterMetricManager::instance().inc_key_count(1);
                P2PMasterMetricManager::instance().observe_value_size(
                    route.object_size);
            }
            restored = true;
        }
        if (!restored) {
            ++skipped_keys;
        }
    }
    if (skipped_routes > 0 || skipped_keys > 0) {
        HAMetricManager::instance().set_primary_degraded(true);
        HAMetricManager::instance().inc_promotion_skipped_replicas(
            skipped_routes);
        HAMetricManager::instance().inc_promotion_skipped_objects(skipped_keys);
    }
    return ErrorCode::OK;
}

}  // namespace mooncake
