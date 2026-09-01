#include "p2p/master/p2p_client_manager.h"

#include <algorithm>
#include <chrono>
#include <glog/logging.h>
#include <random>

#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

P2PClientManager::P2PClientManager(int64_t disconnect_timeout_sec,
                                   int64_t crash_timeout_sec,
                                   ViewVersionId view_version)
    : view_version_(view_version) {
    P2PClientMeta::SetTimeouts(disconnect_timeout_sec, crash_timeout_sec);
}

P2PClientManager::~P2PClientManager() { Stop(); }

void P2PClientManager::Start() { StartClientMonitor(); }

void P2PClientManager::StartClientMonitor() {
    if (client_monitor_running_) return;
    client_monitor_running_ = true;
    client_monitor_thread_ = std::thread([this]() {
        while (client_monitor_running_) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kClientMonitorSleepMs));
            if (client_monitor_running_) ClientMonitorFunc();
        }
    });
    VLOG(1) << "action=start_client_monitor_thread";
}

void P2PClientManager::Stop() { StopClientMonitor(); }

void P2PClientManager::StopClientMonitor() {
    client_monitor_running_ = false;
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
}

auto P2PClientManager::GetClient(const UUID& client_id)
    -> std::shared_ptr<P2PClientMeta> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        return nullptr;
    }
    return it->second;
}

std::vector<std::shared_ptr<P2PClientMeta>>
P2PClientManager::GetAllClients() {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    std::vector<std::shared_ptr<P2PClientMeta>> clients;
    clients.reserve(client_metas_.size());
    for (const auto& [id, meta] : client_metas_) {
        clients.push_back(meta);
    }
    return clients;
}

auto P2PClientManager::BuildClientList(ObjectIterateStrategy strategy) const
    -> std::optional<std::vector<std::shared_ptr<P2PClientMeta>>> {
    std::vector<std::shared_ptr<P2PClientMeta>> clients;
    clients.reserve(client_metas_.size());

    switch (strategy) {
        case ObjectIterateStrategy::ORDERED: {
            for (const auto& [id, meta] : client_metas_) {
                clients.emplace_back(meta);
            }
            break;
        }
        case ObjectIterateStrategy::RANDOM: {
            for (const auto& [id, meta] : client_metas_) {
                clients.emplace_back(meta);
            }
            std::random_device rd;
            std::mt19937 generator(rd());
            std::shuffle(clients.begin(), clients.end(), generator);
            break;
        }
        case ObjectIterateStrategy::CAPACITY_PRIORITY: {
            std::vector<std::pair<size_t, std::shared_ptr<P2PClientMeta>>>
                clients_with_capacity;
            clients_with_capacity.reserve(client_metas_.size());
            for (const auto& [id, meta] : client_metas_) {
                clients_with_capacity.emplace_back(
                    meta->GetAvailableCapacity(), meta);
            }
            std::sort(clients_with_capacity.begin(),
                      clients_with_capacity.end(),
                      [](const auto& lhs, const auto& rhs) {
                          return lhs.first > rhs.first;
                      });
            for (auto& [capacity, client] : clients_with_capacity) {
                clients.emplace_back(std::move(client));
            }
            break;
        }
        default:
            return std::nullopt;
    }
    return clients;
}

auto P2PClientManager::ForEachClient(ObjectIterateStrategy strategy,
                                     const ClientVisitor& visitor)
    -> tl::expected<void, ErrorCode> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto clients = BuildClientList(strategy);
    if (!clients) {
        LOG(WARNING) << "fail to get client iterator"
                     << ", strategy=" << strategy;
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    for (const auto& client : *clients) {
        auto ret = visitor(client);
        if (!ret) {
            LOG(WARNING) << "client visitor returned error"
                         << ", strategy=" << strategy
                         << ", client_id=" << client->get_client_id()
                         << ", ret=" << ret.error();
            return tl::make_unexpected(ret.error());
        }
        if (ret.value()) {  // early stop
            break;
        }
    }
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode>
P2PClientManager::GetAllSegments() {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    std::vector<std::string> all_segments;
    for (const auto& [id, meta] : client_metas_) {
        auto segments_res = meta->GetSegments();
        if (!segments_res) {
            LOG(WARNING) << "GetAllSegments: failed to get segments"
                         << ", client_id=" << id
                         << ", error=" << segments_res.error();
            continue;
        }
        for (const auto& segment : segments_res.value()) {
            all_segments.emplace_back(std::move(segment.name));
        }
    }
    return all_segments;
}

tl::expected<std::vector<std::string>, ErrorCode>
P2PClientManager::GetClientSegments(const UUID& client_id) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        LOG(WARNING) << "GetClientSegments: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    auto segments_res = it->second->GetSegments();
    if (!segments_res) {
        LOG(WARNING) << "GetClientSegments: failed to get segments"
                     << ", client_id=" << client_id
                     << ", error=" << segments_res.error();
        return tl::make_unexpected(segments_res.error());
    }
    std::vector<std::string> segment_names;
    segment_names.reserve(segments_res.value().size());
    for (const auto& segment : segments_res.value()) {
        segment_names.emplace_back(std::move(segment.name));
    }
    return segment_names;
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PClientManager::QuerySegments(const std::string& segment) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    for (const auto& [id, meta] : client_metas_) {
        auto ret = meta->QuerySegments(segment);
        if (ret.has_value()) {
            return ret;
        } else if (ret.error() != ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(ERROR)
                << "QuerySegments: failed to query segments for client_id="
                << id << ", error=" << ret.error();
            return ret;
        }
    }
    LOG(WARNING) << "QuerySegments: segment not found"
                 << ", segment=" << segment;
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

// TODO: wanyue-wy
// To ensure the compatibility of the code,
// currently we assume that segment_name is globally unique among all clients.
// However, the actual code does not guarantee this premise
// In the future, we need to impose constraints on this premise,
// or replace segment name with segment id
tl::expected<UUID, ErrorCode> P2PClientManager::GetClientIdBySegmentName(
    const std::string& segment_name) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    for (const auto& [client_id, meta] : client_metas_) {
        auto segments = meta->GetSegments();
        if (!segments.has_value()) continue;
        for (const auto& segment : segments.value()) {
            if (segment.name == segment_name) return client_id;
        }
    }
    LOG(WARNING) << "GetClientIdBySegmentName: segment not found"
                 << ", segment_name=" << segment_name;
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

tl::expected<std::vector<std::string>, ErrorCode> P2PClientManager::QueryIp(
    const UUID& client_id) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        LOG(WARNING) << "QueryIp: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return it->second->QueryIp(client_id);
}

tl::expected<std::shared_ptr<P2PSegment>, ErrorCode>
P2PClientManager::QuerySegment(const UUID& client_id,
                               const UUID& segment_id) {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        LOG(WARNING) << "QuerySegment: client not found"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return it->second->QuerySegment(segment_id);
}

void P2PClientManager::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    segment_removal_cb_ = std::move(cb);
}

auto P2PClientManager::RegisterClient(const P2PRegisterClientRequest& req)
    -> tl::expected<P2PRegisterClientResponse, ErrorCode> {
    const auto& client_id = req.client_id;
    {
        SharedMutexLocker lock(&clients_mutex_, shared_lock);
        auto it = client_metas_.find(client_id);
        if (it != client_metas_.end()) {
            LOG(WARNING) << "RegisterClient: client already exists"
                         << ", client_id=" << client_id;
            return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
        }
    }

    LOG(INFO) << "RegisterClient(P2P): client_id=" << req.client_id
              << ", ip_address='" << req.ip_address
              << "', rpc_port=" << req.rpc_port
              << ", segments=" << req.segments.size();
    if (req.ip_address.empty()) {
        LOG(ERROR) << "RegisterClient(P2P): rejected, empty ip_address"
                   << ", client_id=" << req.client_id;
        LOG(WARNING) << "RegisterClient: register request failed"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (req.rpc_port == 0) {
        LOG(ERROR) << "RegisterClient(P2P): rejected, invalid rpc_port=0"
                   << ", client_id=" << req.client_id;
        LOG(WARNING) << "RegisterClient: register request failed"
                     << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto meta = std::make_shared<P2PClientMeta>(req.client_id, req.ip_address,
                                                req.rpc_port);
    if (segment_removal_cb_) {
        meta->SetSegmentRemovalCallback(
            [callback = segment_removal_cb_, client_id](
                const UUID& segment_id) {
                callback(P2PRouteLocation{.client_id = client_id,
                                          .segment_id = segment_id});
            });
    }
    for (const auto& segment : req.segments) {
        auto result = meta->MountSegment(segment);
        if (!result) {
            LOG(ERROR) << "RegisterClient: failed to mount segment"
                       << ", segment_name=" << segment.name
                       << ", client_id=" << client_id
                       << ", error=" << result.error();
            return tl::make_unexpected(result.error());
        }
    }

    meta->SetSyncing(true);

    SharedMutexLocker lock(&clients_mutex_);
    if (client_metas_.count(client_id)) {
        LOG(WARNING)
            << "RegisterClient: client already exists (lost registration race)"
            << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
    }
    client_metas_[client_id] = std::move(meta);

    P2PMasterMetricManager::instance().inc_active_clients();

    P2PRegisterClientResponse response;
    response.view_version = view_version_;

    LOG(INFO) << "RegisterClient: client_id=" << client_id
              << ", segments=" << req.segments.size()
              << ", view_version=" << response.view_version;

    return response;
}

auto P2PClientManager::UnregisterClient(const P2PUnregisterClientRequest& req)
    -> tl::expected<P2PUnregisterClientResponse, ErrorCode> {
    if (req.deployment_mode != DeploymentMode::P2P) {
        LOG(ERROR) << "UnregisterClient: architecture mismatch"
                   << ", client_mode=" << static_cast<int>(req.deployment_mode)
                   << ", master_mode=" << static_cast<int>(DeploymentMode::P2P)
                   << ", client_id=" << req.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    const auto& client_id = req.client_id;
    std::shared_ptr<P2PClientMeta> meta;
    bool was_health = false;
    {
        SharedMutexLocker lock(&clients_mutex_);
        auto it = client_metas_.find(client_id);
        if (it == client_metas_.end()) {
            // Idempotent: already absent (crashed-out or double unregister).
            P2PUnregisterClientResponse response;
            response.view_version = view_version_;
            LOG(INFO) << "UnregisterClient: client not found (idempotent ok)"
                      << ", client_id=" << client_id;
            return response;
        }
        meta = std::move(it->second);
        was_health = (meta->get_health_state().status == P2PClientStatus::HEALTH);
        client_metas_.erase(it);
    }

    // The client is out of client_metas_ now. Recycle its segments WITHOUT
    // crash accounting (this is a proactive unregister, not a crash).
    meta->RecycleMeta();

    // Decrement the active gauge only if the client was still HEALTH: the
    // unhealthy path already decremented it in OnDisconnected().
    if (was_health) {
        P2PMasterMetricManager::instance().dec_active_clients();
    }

    P2PUnregisterClientResponse response;
    response.view_version = view_version_;
    LOG(INFO) << "UnregisterClient: client_id=" << client_id
              << ", was_health=" << was_health;
    return response;
}

auto P2PClientManager::Heartbeat(const P2PHeartbeatRequest& req)
    -> tl::expected<P2PHeartbeatResponse, ErrorCode> {
    const auto& client_id = req.client_id;
    P2PHeartbeatResponse response;
    response.view_version = view_version_;

    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        // Client not in client_metas_: master restarted or client heartbeat
        // timed out and the meta of client was cleaned up. Return UNDEFINED +
        // view_version to inform client to re-register.
        response.status = P2PClientStatus::UNDEFINED;
        return response;
    }

    auto& meta = it->second;

    // Update Heartbeat
    auto [old_status, new_status] = meta->Heartbeat();
    response.status = new_status;
    if (new_status == P2PClientStatus::HEALTH) {
        if (old_status != new_status) {
            LOG(INFO) << "client recovered"
                      << ", client_id=" << client_id;
            meta->OnRecovered();
        }
        for (const auto& task : req.tasks) {
            response.task_results.push_back(ProcessTask(client_id, task));
        }
    }

    return response;
}

auto P2PClientManager::QueryClientStatus(const P2PQueryClientStatusRequest& req)
    -> tl::expected<P2PQueryClientStatusResponse, ErrorCode> {
    const auto& client_id = req.client_id;
    P2PQueryClientStatusResponse response;

    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        response.status = P2PClientStatus::UNDEFINED;
    } else {
        response.status = it->second->get_health_state().status;
    }

    return response;
}

HeartbeatTaskResult P2PClientManager::ProcessTask(const UUID& client_id,
                                                  const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type_;

    switch (task.type_) {
        case HeartbeatTaskType::SYNC_SEGMENT_META: {
            auto client_meta = GetClient(client_id);
            const auto* param = std::get_if<SyncSegmentMetaParam>(&task.param_);
            if (client_meta && param) {
                auto sync_res =
                    client_meta->UpdateSegmentUsages(param->tier_usages);
                result.detail = sync_res;
                for (const auto& sub : sync_res.sub_results) {
                    if (sub.error != ErrorCode::OK) {
                        // result.error means the task is failed.
                        // here just sub task error, don't affect task result.
                        LOG(ERROR) << "fail to update segment usages"
                                   << ", client_id=" << client_id
                                   << ", segment_id=" << sub.segment_id
                                   << ", error=" << sub.error;
                    }
                }
            } else {
                result.error = ErrorCode::INVALID_PARAMS;
            }
            break;
        }
        case HeartbeatTaskType::SYNC_CLIENT_METRIC: {
            const auto* param =
                std::get_if<SyncClientMetricParam>(&task.param_);
            if (param == nullptr) {
                result.error = ErrorCode::INVALID_PARAMS;
                LOG(ERROR) << "SYNC_CLIENT_METRIC: invalid param"
                           << ", client_id=" << client_id;
                break;
            }
            P2PMasterMetricManager::instance().UpdateClientMetrics(
                client_id, param->snapshot);
            break;
        }
        default:
            result.error = ErrorCode::NOT_IMPLEMENTED;
            break;
    }
    return result;
}

// 1. Phase 1 (Shared Lock): Check health status
// 2. Phase 2 (No Lock): Execute crashed client hooks
// 3. Phase 3 (Write Lock): Clean up crashed clients
void P2PClientManager::ClientMonitorFunc() {
    // Attention:
    // 1. DISCONNECTED is not finnal status. The clients in
    // newly_disconnected might change its status.
    // 2. CRASHED is finnal status. The clients in newly_crashed will always
    // be crashed.
    std::vector<std::shared_ptr<P2PClientMeta>> newly_disconnected;
    std::vector<std::shared_ptr<P2PClientMeta>> newly_crashed;

    // Phase 1: Check health status
    {
        SharedMutexLocker lock(&clients_mutex_, shared_lock);
        for (auto& [client_id, meta] : client_metas_) {
            auto [old_status, new_status] = meta->CheckHealth();
            if (old_status != new_status) {
                if (new_status == P2PClientStatus::DISCONNECTION) {
                    newly_disconnected.push_back(meta);
                } else if (new_status == P2PClientStatus::CRASHED) {
                    newly_crashed.push_back(meta);
                }
            }
        }
    }

    // Phase 2: Execute hooks (No clients_mutex_ lock)
    // We can safely execute hooks because we hold shared_ptrs to
    // P2PClientMeta, so they won't be destroyed.
    // And hooks don't need clients_mutex_ because they are protected by
    // P2PClientMeta itself.
    for (const auto& client : newly_disconnected) {
        // The client might change to Healthy by concurrent heartbeat.
        // So OnDisconnected() need to check the status again.
        client->OnDisconnected();
    }

    for (const auto& client : newly_crashed) {
        client->OnCrashed();
    }

    // Phase 3: Clean up crashed clients (Write Lock)
    if (!newly_crashed.empty()) {
        SharedMutexLocker lock(&clients_mutex_);
        for (const auto& client : newly_crashed) {
            client_metas_.erase(client->get_client_id());
        }
    }
}

}  // namespace mooncake
