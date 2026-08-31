#include "p2p/master/p2p_master_service.h"

#include <glog/logging.h>
#include <algorithm>
#include <regex>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <variant>

#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/oplog_store_factory.h"
#include "p2p/master/p2p_client_meta.h"
#include "p2p/ha/ha_metric_manager.h"
#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

P2PMasterService::P2PMasterService(const P2PMasterConfig& config,
                                   ViewVersionId view_version)
    : max_client_per_key_(config.routes.max_clients_per_key),
      enable_async_oplog_write_(ParseOpLogStoreType(config.oplog.store_type) ==
                                OpLogStoreType::REDIS),
      view_version_(view_version) {
    if (config.oplog.enabled) {
        auto store_type = ParseOpLogStoreType(config.oplog.store_type);
        const std::string& store_location =
            store_type == OpLogStoreType::REDIS ? config.redis.endpoint
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

P2PMasterService::ObjectMetadata::~ObjectMetadata() {
    P2PMasterMetricManager::instance().dec_key_count(1);
}

P2PMasterService::ObjectMetadata::ObjectMetadata(
    size_t value_length, std::vector<Replica>&& replicas)
    : replicas_(std::move(replicas)), size_(value_length) {
    P2PMasterMetricManager::instance().inc_key_count(1);
    P2PMasterMetricManager::instance().observe_value_size(value_length);
}

void P2PMasterService::InitializeClientManager() {
    client_manager_->SetSegmentRemovalCallback(
        [this](const UUID& segment_id) { OnSegmentRemoved(segment_id); });
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
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }
    return accessor.Get().IsObjectAccessible();
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
    std::vector<std::string> all_keys;
    for (size_t i = 0; i < GetShardCount(); ++i) {
        MetadataShardAccessorRO shard(this, i);
        for (const auto& item : shard->metadata) {
            all_keys.push_back(item.first);
        }
    }
    return all_keys;
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

auto P2PMasterService::GetReplicaListByRegex(
    const std::string& regex_pattern)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode> {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>> results;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < GetShardCount(); ++i) {
        MetadataShardAccessorRO shard(this, i);
        for (const auto& [key, metadata] : shard->metadata) {
            if (!std::regex_search(key, pattern)) {
                continue;
            }

            std::vector<Replica::Descriptor> replica_list;
            replica_list.reserve(metadata->replicas_.size());
            for (const auto& replica : metadata->replicas_) {
                replica_list.emplace_back(replica.get_descriptor());
            }
            if (replica_list.empty()) {
                LOG(WARNING)
                    << "key=" << key
                    << " matched by regex, but has no complete replicas.";
                continue;
            }
            results.emplace(key, std::move(replica_list));
        }
    }
    return results;
}

auto P2PMasterService::GetReplicaList(
    std::string_view key, const P2PGetReplicaListRequestConfig& config)
    -> tl::expected<P2PGetReplicaListResponse, ErrorCode> {
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto replica_list = FilterReplicas(config, accessor.Get());
    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    P2PGetReplicaListResponse response;
    response.replicas = std::move(replica_list);
    return response;
}

auto P2PMasterService::Remove(std::string_view key, bool force)
    -> tl::expected<void, ErrorCode> {
    (void)force;
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    accessor.Erase();
    return {};
}

auto P2PMasterService::RemoveByRegex(std::string_view regex_pattern, bool force)
    -> tl::expected<long, ErrorCode> {
    (void)force;
    long removed_count = 0;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern.data(),
                             regex_pattern.data() + regex_pattern.size(),
                             std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    for (size_t i = 0; i < GetShardCount(); ++i) {
        MetadataShardAccessorRW shard(this, i);
        for (auto it = shard->metadata.begin(); it != shard->metadata.end();) {
            if (!std::regex_search(it->first, pattern)) {
                ++it;
                continue;
            }
            VLOG(1) << "key=" << it->first
                    << " matched by regex. Removing.";
            RemoveReplicaFromSegmentIndex(shard.GetRef(), it->first,
                                          it->second->replicas_);
            it = shard->metadata.erase(it);
            ++removed_count;
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long P2PMasterService::RemoveAll(bool force) {
    (void)force;
    long removed_count = 0;
    uint64_t total_freed_size = 0;

    for (size_t i = 0; i < GetShardCount(); ++i) {
        MetadataShardAccessorRW shard(this, i);
        auto it = shard->metadata.begin();
        while (it != shard->metadata.end()) {
            auto mem_rep_count =
                it->second->CountReplicas(&Replica::fn_is_memory_replica);
            total_freed_size += it->second->size_ * mem_rep_count;
            RemoveReplicaFromSegmentIndex(shard.GetRef(), it->first,
                                          it->second->replicas_);
            it = shard->metadata.erase(it);
            ++removed_count;
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

size_t P2PMasterService::GetKeyCount() const {
    size_t total = 0;
    for (size_t i = 0; i < GetShardCount(); ++i) {
        MetadataShardAccessorRO shard(this, i);
        total += shard->metadata.size();
    }
    return total;
}

void P2PMasterService::OnSegmentRemoved(const UUID& segment_id) {
    for (size_t i = 0; i < GetShardCount(); ++i) {
        MetadataShardAccessorRW shard_accessor(this, i);
        auto& shard = shard_accessor.GetRef();

        auto index_it = shard.segment_key_index.find(segment_id);
        if (index_it == shard.segment_key_index.end()) {
            continue;
        }

        std::vector<std::string> affected_keys;
        affected_keys.reserve(index_it->second.size());
        for (const auto& [key, count] : index_it->second) {
            affected_keys.emplace_back(key);
        }
        shard.segment_key_index.erase(index_it);

        for (const auto& key : affected_keys) {
            auto metadata_it = shard.metadata.find(key);
            if (metadata_it == shard.metadata.end()) {
                continue;
            }

            auto& replicas = metadata_it->second->replicas_;
            for (int index = static_cast<int>(replicas.size()) - 1; index >= 0;
                 --index) {
                auto replica_segment_id = replicas[index].get_segment_id();
                if (replica_segment_id.has_value() &&
                    replica_segment_id.value() == segment_id) {
                    replicas.erase(replicas.begin() + index);
                    break;
                }
            }

            if (replicas.empty()) {
                shard.metadata.erase(metadata_it);
            }
        }
    }
}

void P2PMasterService::AddReplicaToSegmentIndex(MetadataShard& shard,
                                                const std::string& key,
                                                const Replica& replica) {
    if (replica.status() != ReplicaStatus::COMPLETE) {
        return;
    }
    auto segment_id = replica.get_segment_id();
    if (segment_id.has_value()) {
        shard.segment_key_index[segment_id.value()][std::string_view(key)]++;
    }
}

void P2PMasterService::RemoveReplicaFromSegmentIndex(
    MetadataShard& shard, const std::string& key,
    const std::vector<Replica>& replicas) {
    for (const auto& replica : replicas) {
        RemoveReplicaFromSegmentIndex(shard, key, replica);
    }
}

void P2PMasterService::RemoveReplicaFromSegmentIndex(
    MetadataShard& shard, const std::string& key, const Replica& replica) {
    if (replica.status() != ReplicaStatus::COMPLETE) {
        return;
    }

    auto segment_id = replica.get_segment_id();
    if (!segment_id.has_value()) {
        return;
    }

    auto segment_it = shard.segment_key_index.find(segment_id.value());
    if (segment_it == shard.segment_key_index.end()) {
        LOG(WARNING) << "RemoveReplicaFromSegmentIndex: segment not found"
                     << ", segment_id=" << segment_id.value()
                     << ", key=" << key;
        return;
    }

    auto key_it = segment_it->second.find(key);
    if (key_it == segment_it->second.end()) {
        LOG(WARNING) << "RemoveReplicaFromSegmentIndex: key not found"
                     << ", segment_id=" << segment_id.value()
                     << ", key=" << key;
        return;
    }

    if (--key_it->second == 0) {
        segment_it->second.erase(key_it);
    }
    if (segment_it->second.empty()) {
        shard.segment_key_index.erase(segment_it);
    }
}

ErrorCode P2PMasterService::RecordOplog(OpType type, const std::string& key,
                                        const std::string& payload) {
    // TODO: Record remaining failover-visible P2P mutations: client crash
    // cleanup, heartbeat state transitions, replica eviction/rebalance, and
    // task metadata.
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
    tl::expected<void, ErrorCode> result;
    auto client = client_manager_->GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "MountSegment: client not found"
                   << ", client_id=" << client_id;
        result = tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    } else {
        result = client->MountSegment(segment);
        if (!result.has_value()) {
            LOG(ERROR) << "fail to mount segment"
                       << ", segment=" << segment.name
                       << ", client_id=" << client_id
                       << ", ret=" << result.error();
        }
    }
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
    tl::expected<void, ErrorCode> result;
    auto client = client_manager_->GetClient(client_id);
    if (!client) {
        LOG(ERROR) << "UnmountSegment: client not found"
                   << ", client_id=" << client_id;
        result = tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    } else {
        result = client->UnmountSegment(segment_id);
        if (!result.has_value()) {
            LOG(ERROR) << "fail to unmount segment"
                       << ", segment_id=" << segment_id
                       << ", client_id=" << client_id
                       << ", ret=" << result.error();
        }
    }
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

auto P2PMasterService::CollectReplicaOwnerClients(
    const ObjectMetadata& metadata, std::string_view key)
    -> tl::expected<std::unordered_set<UUID, boost::hash<UUID>>, ErrorCode> {
    std::unordered_set<UUID, boost::hash<UUID>> owner_clients;
    for (const auto& replica : metadata.replicas_) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "unexpected replica type"
                       << ", key: " << key << ", replica:" << replica;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        auto client_id = replica.get_p2p_client_id();
        if (!client_id) {
            LOG(ERROR) << "invalid p2p replica"
                       << ", key: " << key << ", replica:" << replica;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        owner_clients.insert(*client_id);
    }
    return owner_clients;
}

std::vector<Replica::Descriptor> P2PMasterService::FilterReplicas(
    const P2PGetReplicaListRequestConfig& config, const ObjectMetadata& metadata) {
    const auto& p2p_config = config.p2p_config ? config.p2p_config.value()
                                               : P2PReadRouteConfigExtra();
    // candidates kept at client granularity
    std::vector<std::pair<uint32_t, Replica::Descriptor>> candidates;
    std::unordered_map<UUID, size_t, boost::hash<UUID>> best_by_client;

    // 1. filter qualified replicas
    for (const auto& replica : metadata.replicas_) {
        if (!replica.is_p2p_proxy_replica()) {
            LOG(ERROR) << "invalid replica type"
                       << ", replica: " << replica;
            continue;
        } else if (!replica.get_p2p_client()->is_health()) {
            // The client of the replica might be disconnected, just skip it.
            // Moreover, it is no need to check health status with client_lock.
            // Although a health client is to be unhealthy in following code,
            // the wrong route will not result in acquiring incorrect data.
            // Because the read is based on client rpc, a disconnected client
            // can't be accessed
            continue;
        }

        // filter with config
        // 1.1 tag filter: exclude replicas whose segment contains
        // any tag listed in tag_filters.
        bool excluded_by_tag = false;
        const auto& p2p_tags = replica.get_p2p_tags();
        for (const auto& tag : p2p_config.tag_filters) {
            if (std::find(p2p_tags.begin(), p2p_tags.end(), tag) !=
                p2p_tags.end()) {
                excluded_by_tag = true;
                break;
            }
        }
        if (excluded_by_tag) continue;

        // 1.2 priority filter
        auto priority_opt = replica.get_p2p_priority();
        if (!priority_opt) {
            LOG(ERROR) << "invalid priority"
                       << ", replica: " << replica;
            continue;
        }
        if (*priority_opt < p2p_config.priority_limit) continue;

        // 1.3 client-granularity: keep the highest-priority replica
        auto cid_opt = replica.get_p2p_client_id();
        if (!cid_opt) continue;
        const UUID cid = *cid_opt;
        auto it = best_by_client.find(cid);
        if (it == best_by_client.end()) {
            best_by_client[cid] = candidates.size();
            candidates.push_back({*priority_opt, replica.get_descriptor()});
        } else if (*priority_opt > candidates[it->second].first) {
            candidates[it->second] = {*priority_opt, replica.get_descriptor()};
        }
    }  // iter replicas over

    if (config.max_candidates ==
            P2PGetReplicaListRequestConfig::RETURN_ALL_CANDIDATES ||
        config.max_candidates >= candidates.size() || candidates.empty()) {
        // return all candidates
        std::vector<Replica::Descriptor> result;
        result.reserve(candidates.size());
        for (const auto& p : candidates) {
            result.push_back(p.second);
        }
        return result;
    }

    // 3. the number of qualified replicas is larger than limit,
    // choose the best ones.
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    std::vector<Replica::Descriptor> result;
    result.reserve(config.max_candidates);
    for (size_t i = 0; i < config.max_candidates; ++i) {
        result.push_back(candidates[i].second);
    }
    return result;
}

auto P2PMasterService::GetWriteRoute(const WriteRouteRequest& req)
    -> tl::expected<WriteRouteResponse, ErrorCode> {
    if (!req.config.IsValid()) {
        LOG(ERROR) << "invalid write route config: " << req.config
                   << ", client_id: " << req.client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // 1. Collect existing replica owners and enforce the client limit.
    OwnerClientSet owners;
    if (!req.key.empty()) {
        MetadataAccessorRO accessor(this, req.key);
        if (accessor.Exists()) {
            auto res = CollectReplicaOwnerClients(accessor.Get(), req.key);
            if (!res) {
                LOG(ERROR) << "failed to collect replica owner clients"
                           << ", key: " << req.key
                           << ", error: " << res.error();
                return tl::make_unexpected(res.error());
            }
            if (max_client_per_key_ > 0 && res->size() >= max_client_per_key_) {
                LOG(WARNING)
                    << "replica owner client num exceeded"
                    << ", key: " << req.key << ", client_id: " << req.client_id
                    << ", current: " << res->size()
                    << ", max: " << max_client_per_key_;
                return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
            }
            owners = std::move(*res);
        }
    }

    // 2. Single pass: collect and score all candidates.
    //    score = free_ratio * (is_local ? (1 - remote_weight) : remote_weight)
    const double remote_weight = std::clamp(req.config.remote_weight, 0.0, 1.0);
    std::vector<WriteCandidate> candidates;
    const bool can_early_stop =
        req.config.early_return &&
        req.config.max_candidates !=
            WriteRouteRequestConfig::RETURN_ALL_CANDIDATES;

    client_manager_->ForEachClient(
        req.config.strategy,
        [&](const std::shared_ptr<P2PClientMeta>& p2p)
            -> tl::expected<bool, ErrorCode> {
            const UUID cid = p2p->get_client_id();
            if (owners.count(cid)) {
                return false;
            }
            const bool is_local = (cid == req.client_id);
            const double weight =
                is_local ? (1.0 - remote_weight) : remote_weight;
            if (weight <= 0.0) {
                return false;
            }

            if (auto cand = p2p->GetWriteRouteCandidate(req)) {
                cand->score *= weight;
                candidates.push_back(std::move(*cand));
                if (can_early_stop &&
                    candidates.size() >= req.config.max_candidates)
                    return true;
            }
            return false;
        });

    // 3. Sort by score desc (capacity desc as tiebreaker), then truncate.
    if (candidates.empty()) {
        LOG(ERROR) << "no candidate found for key: " << req.key
                   << ", client_id: " << req.client_id
                   << ", size: " << req.size;
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_CANDIDATE);
    }
    std::sort(candidates.begin(), candidates.end(),
              [](const auto& a, const auto& b) {
                  return std::tie(b.score, b.available_capacity) <
                         std::tie(a.score, a.available_capacity);
              });
    if (req.config.max_candidates !=
            WriteRouteRequestConfig::RETURN_ALL_CANDIDATES &&
        candidates.size() > req.config.max_candidates) {
        candidates.resize(req.config.max_candidates);
    }
    WriteRouteResponse response;
    response.candidates = std::move(candidates);
    return response;
}

auto P2PMasterService::BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
    -> BatchGetWriteRouteResponse {
    const size_t n = req.keys.size();
    BatchGetWriteRouteResponse response;
    response.responses.resize(n);
    response.error_codes.resize(n, ErrorCode::OK);

    if (req.keys.size() != req.sizes.size()) {
        std::fill(response.error_codes.begin(), response.error_codes.end(),
                  ErrorCode::INVALID_PARAMS);
        return response;
    }

    WriteRouteRequest single_req;
    single_req.client_id = req.client_id;
    single_req.config = req.config;
    for (size_t i = 0; i < n; ++i) {
        single_req.key = req.keys[i];
        single_req.size = req.sizes[i];
        auto result = GetWriteRoute(single_req);
        if (result.has_value()) {
            response.responses[i] = std::move(*result);
        } else {
            response.error_codes[i] = result.error();
        }
    }
    return response;
}

auto P2PMasterService::AddReplica(const AddReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, req.key);
    auto client = client_manager_->GetClient(req.client_id);
    if (!client) {
        LOG(ERROR) << "client not found"
                   << ", client_id: " << req.client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return InnerAddReplica(accessor.GetShard().GetRef(), req.key, req.client_id,
                           req.segment_id, req.size, client);
}

tl::expected<void, ErrorCode> P2PMasterService::InnerAddReplica(
    MetadataShard& shard, std::string_view key, const UUID& client_id,
    const UUID& segment_id, size_t size,
    const std::shared_ptr<P2PClientMeta>& client) {
    auto segment_res = client->QuerySegment(segment_id);
    if (!segment_res.has_value()) {
        LOG(ERROR) << "fail to query segment"
                   << ", client_id: " << client_id
                   << ", segment_id: " << segment_id;
        return tl::make_unexpected(segment_res.error());
    }

    Replica new_replica(P2PProxyReplicaData(client, segment_res.value(), size),
                        ReplicaStatus::COMPLETE);

    // AddReplica commits the in-memory route first. OpLog is best-effort;
    // returning an OpLog error could make the client delete its local replica.
    auto it = shard.metadata.find(key);
    if (it != shard.metadata.end()) {
        auto& metadata = *it->second;
        auto owner_clients_res = CollectReplicaOwnerClients(metadata, key);
        if (!owner_clients_res.has_value()) {
            LOG(ERROR) << "failed to collect replica owner clients"
                       << ", key: " << key
                       << ", error: " << owner_clients_res.error();
            return tl::make_unexpected(owner_clients_res.error());
        }
        const auto& owner_clients = owner_clients_res.value();
        for (const auto& replica : metadata.replicas_) {
            auto seg_id = replica.get_segment_id();
            auto cli_id = replica.get_p2p_client_id();
            if (cli_id && seg_id && *cli_id == client_id &&
                *seg_id == segment_id) {
                LOG(WARNING) << "replica has existed"
                             << ", key: " << key << ", client_id: " << client_id
                             << ", segment_id: " << segment_id;
                return tl::make_unexpected(ErrorCode::REPLICA_ALREADY_EXISTS);
            }
        }
        // AddReplica is also used by in-client tier migration to publish a new
        // physical replica. Existing owner clients may add replicas on new
        // segments, but a new owner client must honor max_client_per_key_.
        if (max_client_per_key_ > 0 &&
            owner_clients.find(client_id) == owner_clients.end() &&
            owner_clients.size() >= max_client_per_key_) {
            LOG(WARNING) << "replica owner client num exceeded"
                         << ", key: " << key << ", client_id: " << client_id
                         << ", segment_id: " << segment_id
                         << ", current owner client num:"
                         << owner_clients.size()
                         << ", max owner client num: " << max_client_per_key_;
            return tl::make_unexpected(ErrorCode::REPLICA_NUM_EXCEEDED);
        }
        AddReplicaPayload payload;
        payload.object_key = std::string(key);
        payload.client_id = client_id;
        payload.segment_id = segment_id;
        payload.size = size;
        AddReplicaToSegmentIndex(shard, it->first, new_replica);
        metadata.replicas_.push_back(std::move(new_replica));
        ErrorCode record_err =
            RecordOplog(OpType_ADD_REPLICA, payload.object_key,
                        SerializeP2PPayload(payload));
        if (record_err != ErrorCode::OK) {
            LOG(ERROR) << "AddReplica(P2P): failed to record oplog"
                       << ", client_id=" << client_id
                       << ", segment_id=" << segment_id
                       << ", error=" << toString(record_err)
                       << "; keeping the in-memory route";
        }
    } else {
        std::vector<Replica> replicas;
        replicas.push_back(std::move(new_replica));
        auto new_meta =
            std::make_unique<ObjectMetadata>(size, std::move(replicas));
        AddReplicaPayload payload;
        payload.object_key = std::string(key);
        payload.client_id = client_id;
        payload.segment_id = segment_id;
        payload.size = size;
        auto emplace_it =
            shard.metadata.emplace(std::string(key), std::move(new_meta)).first;
        AddReplicaToSegmentIndex(shard, emplace_it->first,
                                 emplace_it->second->replicas_[0]);
        ErrorCode record_err =
            RecordOplog(OpType_ADD_REPLICA, payload.object_key,
                        SerializeP2PPayload(payload));
        if (record_err != ErrorCode::OK) {
            LOG(ERROR) << "AddReplica(P2P): failed to record oplog"
                       << ", client_id=" << client_id
                       << ", segment_id=" << segment_id
                       << ", error=" << toString(record_err)
                       << "; keeping the in-memory route";
        }
    }
    return {};
}

auto P2PMasterService::RemoveReplica(const RemoveReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, req.key);
    return InnerRemoveReplica(accessor.GetShard().GetRef(), req.key,
                              req.client_id, req.segment_id);
}

tl::expected<void, ErrorCode> P2PMasterService::InnerRemoveReplica(
    MetadataShard& shard, std::string_view key, const UUID& client_id,
    const UUID& segment_id) {
    auto it = shard.metadata.find(key);
    if (it == shard.metadata.end()) {
        LOG(WARNING) << "object not found"
                     << ", key: " << key << ", client_id: " << client_id
                     << ", segment_id: " << segment_id;
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = *it->second;
    for (auto rit = metadata.replicas_.begin(); rit != metadata.replicas_.end();
         ++rit) {
        if (!rit->is_p2p_proxy_replica()) {
            LOG(ERROR) << "unexpected replica type"
                       << ", key: " << key << ", client_id: " << client_id
                       << ", segment_id: " << segment_id
                       << ", replica: " << *rit;
            return tl::make_unexpected(ErrorCode::INVALID_REPLICA);
        }
        auto seg_id = rit->get_segment_id();
        auto cli_id = rit->get_p2p_client_id();
        if (cli_id && seg_id && cli_id == client_id && *seg_id == segment_id) {
            RemoveReplicaPayload payload;
            payload.object_key = std::string(key);
            payload.client_id = client_id;
            payload.segment_id = segment_id;
            ErrorCode record_err =
                RecordOplog(OpType_REMOVE_REPLICA, payload.object_key,
                            SerializeP2PPayload(payload));
            if (record_err != ErrorCode::OK) {
                LOG(ERROR) << "RemoveReplica(P2P): failed to record oplog"
                           << ", client_id=" << client_id
                           << ", segment_id=" << segment_id
                           << ", error=" << toString(record_err);
                return tl::make_unexpected(record_err);
            }
            RemoveReplicaFromSegmentIndex(shard, it->first, *rit);
            metadata.replicas_.erase(rit);
            if (metadata.replicas_.empty()) {
                shard.metadata.erase(it);
            }
            return {};
        }
    }

    LOG(WARNING) << "replica not found"
                 << ", key: " << key << ", client_id: " << client_id
                 << ", segment_id: " << segment_id;
    return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
}

auto P2PMasterService::BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
    -> std::vector<tl::expected<void, ErrorCode>> {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(req.segment_ids.size());

    RemoveReplicaRequest single_req;
    single_req.key = req.key;
    single_req.client_id = req.client_id;
    for (const auto& segment_id : req.segment_ids) {
        single_req.segment_id = segment_id;
        auto result = RemoveReplica(single_req);
        if (!result.has_value()) {
            if (result.error() == ErrorCode::OBJECT_NOT_FOUND) {
                // This may happen if the object is removed by another thread
                LOG(INFO) << "object not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else if (result.error() == ErrorCode::REPLICA_NOT_FOUND) {
                // This may happen if the replica is removed by another thread
                LOG(INFO) << "replica not found when batch remove replica"
                          << ", key: " << req.key
                          << ", client_id: " << req.client_id
                          << ", segment_id: " << segment_id;
                results.push_back({});
            } else {
                LOG(ERROR) << "failed to remove replica"
                           << ", key: " << req.key
                           << ", client_id: " << req.client_id
                           << ", segment_id: " << segment_id
                           << ", error: " << toString(result.error());
                results.push_back(tl::make_unexpected(result.error()));
            }
        } else {
            results.push_back({});
        }
    }
    return results;
}

auto P2PMasterService::BatchSyncReplica(const BatchSyncReplicaRequest& req)
    -> BatchSyncReplicaResponse {
    // Validate SoA array lengths are consistent
    if (req.add_keys.size() != req.add_sizes.size() ||
        req.add_keys.size() != req.add_segment_ids.size() ||
        req.remove_keys.size() != req.remove_segment_ids.size()) {
        LOG(ERROR) << "BatchSyncReplica: mismatched array sizes"
                   << ", add_keys=" << req.add_keys.size()
                   << ", add_sizes=" << req.add_sizes.size()
                   << ", add_segment_ids=" << req.add_segment_ids.size()
                   << ", remove_keys=" << req.remove_keys.size()
                   << ", remove_segment_ids=" << req.remove_segment_ids.size();
        BatchSyncReplicaResponse err_resp;
        err_resp.add_results.assign(req.add_keys.size(),
                                    ErrorCode::INVALID_PARAMS);
        err_resp.remove_results.assign(req.remove_keys.size(),
                                       ErrorCode::INVALID_PARAMS);
        return err_resp;
    }

    BatchSyncReplicaResponse response;
    response.add_results.resize(req.add_keys.size(), ErrorCode::OK);
    response.remove_results.resize(req.remove_keys.size(), ErrorCode::OK);

    // Resolve client once for all operations
    auto client = client_manager_->GetClient(req.client_id);
    if (!client) {
        LOG(ERROR) << "BatchSyncReplica: client not found"
                   << ", client_id=" << req.client_id;
        std::fill(response.add_results.begin(), response.add_results.end(),
                  ErrorCode::CLIENT_NOT_FOUND);
        std::fill(response.remove_results.begin(),
                  response.remove_results.end(), ErrorCode::CLIENT_NOT_FOUND);
        return response;
    }

    // Group operations by shard index.
    // Each entry: (original_index, is_add=true/false)
    std::unordered_map<size_t, std::vector<std::pair<size_t, bool>>>
        shard_groups;

    for (size_t i = 0; i < req.add_keys.size(); ++i) {
        size_t shard_idx = GetShardIndex(req.add_keys[i]);
        shard_groups[shard_idx].emplace_back(i, true);
    }
    for (size_t i = 0; i < req.remove_keys.size(); ++i) {
        size_t shard_idx = GetShardIndex(req.remove_keys[i]);
        shard_groups[shard_idx].emplace_back(i, false);
    }

    // Process each shard group with one lock acquisition
    for (auto& [shard_idx, ops] : shard_groups) {
        MetadataShardAccessorRW shard_rw(this, shard_idx);
        auto& shard = shard_rw.GetRef();

        for (auto& [idx, is_add] : ops) {
            if (is_add) {
                auto result = InnerAddReplica(
                    shard, req.add_keys[idx], req.client_id,
                    req.add_segment_ids[idx], req.add_sizes[idx], client);
                if (!result.has_value()) {
                    response.add_results[idx] = result.error();
                }
            } else {
                auto result = InnerRemoveReplica(shard, req.remove_keys[idx],
                                                 req.client_id,
                                                 req.remove_segment_ids[idx]);
                if (!result.has_value() &&
                    result.error() != ErrorCode::OBJECT_NOT_FOUND &&
                    result.error() != ErrorCode::REPLICA_NOT_FOUND) {
                    response.remove_results[idx] = result.error();
                }
            }
        }
    }

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
        auto* manager = GetOpLogManager();
        if (manager) {
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
    size_t restored_replicas = 0;
    size_t skipped_replicas = 0;
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

        auto p2p_client = client_manager_->GetClient(client_id);
        if (p2p_client) {
            p2p_client->SetSyncing(false);
        }
        ++restored_clients;
    }

    for (const auto& [key, standby_metadata] : metadata.objects) {
        std::vector<Replica> replicas;
        replicas.reserve(standby_metadata.replicas.size());

        // TODO: Make promotion restore strict once gap/out-of-order handling is
        // hardened. Missing clients/segments should follow an explicit policy
        // instead of silently producing a partially restored object.
        for (const auto& desc : standby_metadata.replicas) {
            if (!std::holds_alternative<P2PProxyDescriptor>(
                    desc.descriptor_variant)) {
                LOG(WARNING)
                    << "RestoreFromStandbyMetadata: skip non-P2P replica"
                    << ", key=" << key << ", replica=" << desc;
                ++skipped_replicas;
                continue;
            }

            const auto& p2p_desc =
                std::get<P2PProxyDescriptor>(desc.descriptor_variant);
            auto client = client_manager_->GetClient(p2p_desc.client_id);
            if (!client) {
                LOG(WARNING)
                    << "RestoreFromStandbyMetadata: skip replica with missing "
                       "client"
                    << ", key=" << key << ", client_id=" << p2p_desc.client_id
                    << ", segment_id=" << p2p_desc.segment_id;
                ++skipped_replicas;
                continue;
            }

            auto segment = client->QuerySegment(p2p_desc.segment_id);
            if (!segment.has_value()) {
                LOG(WARNING)
                    << "RestoreFromStandbyMetadata: skip replica with missing "
                       "segment"
                    << ", key=" << key << ", client_id=" << p2p_desc.client_id
                    << ", segment_id=" << p2p_desc.segment_id
                    << ", error=" << toString(segment.error());
                ++skipped_replicas;
                continue;
            }

            const uint64_t object_size = p2p_desc.object_size != 0
                                             ? p2p_desc.object_size
                                             : standby_metadata.size;
            replicas.emplace_back(
                P2PProxyReplicaData(client, segment.value(), object_size),
                ReplicaStatus::COMPLETE);
        }

        if (replicas.empty()) {
            LOG(WARNING)
                << "RestoreFromStandbyMetadata: skip object with no restorable "
                   "replicas"
                << ", key=" << key;
            ++skipped_objects;
            continue;
        }

        MetadataAccessorRW accessor(this, key);
        auto& shard = accessor.GetShard().GetRef();
        auto new_meta = std::make_unique<ObjectMetadata>(standby_metadata.size,
                                                         std::move(replicas));
        auto [it, inserted] = shard.metadata.emplace(key, std::move(new_meta));
        if (!inserted) {
            HAMetricManager::instance().inc_promotion_restore_failures();
            LOG(ERROR)
                << "RestoreFromStandbyMetadata: object already exists despite "
                   "empty-target check"
                << ", key=" << key;
            return ErrorCode::INTERNAL_ERROR;
        }

        for (const auto& replica : it->second->replicas_) {
            AddReplicaToSegmentIndex(shard, it->first, replica);
            ++restored_replicas;
        }
        ++restored_objects;
    }

    if (skipped_replicas > 0 || skipped_objects > 0) {
        HAMetricManager::instance().set_primary_degraded(true);
        HAMetricManager::instance().inc_promotion_skipped_replicas(
            static_cast<int64_t>(skipped_replicas));
        HAMetricManager::instance().inc_promotion_skipped_objects(
            static_cast<int64_t>(skipped_objects));
    }

    LOG(INFO) << "RestoreFromStandbyMetadata: restored"
              << ", clients=" << restored_clients
              << ", objects=" << restored_objects
              << ", replicas=" << restored_replicas
              << ", skipped_replicas=" << skipped_replicas
              << ", skipped_objects=" << skipped_objects
              << ", last_applied_sequence_id=" << last_applied_sequence_id;
    return ErrorCode::OK;
}

}  // namespace mooncake
