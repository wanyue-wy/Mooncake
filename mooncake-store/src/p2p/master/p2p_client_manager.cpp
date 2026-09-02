#include "p2p/master/p2p_client_manager.h"

#include <algorithm>
#include <chrono>
#include <random>

#include <glog/logging.h>

#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

P2PClientManager::P2PClientManager(int64_t disconnect_timeout_sec,
                                   int64_t crash_timeout_sec,
                                   ViewVersionId view_version)
    : disconnect_timeout_sec_(disconnect_timeout_sec),
      crash_timeout_sec_(crash_timeout_sec),
      view_version_(view_version) {}

P2PClientManager::~P2PClientManager() {
    Stop();
    std::vector<std::shared_ptr<P2PClientMeta>> clients;
    {
        SharedMutexLocker lock(&clients_mutex_);
        clients.reserve(client_metas_.size());
        for (auto& [id, client] : client_metas_) {
            clients.push_back(std::move(client));
        }
        client_metas_.clear();
    }
    for (const auto& client : clients) {
        const auto status = client->get_health_state().status;
        CleanupRoutes(client->RecycleSegments());
        if (status == P2PClientStatus::HEALTHY) {
            P2PMasterMetricManager::instance().dec_active_clients();
        }
    }
}

void P2PClientManager::Start() {
    if (client_monitor_thread_.joinable()) {
        return;
    }
    client_monitor_thread_ = std::jthread([this](std::stop_token stop_token) {
        while (!stop_token.stop_requested()) {
            constexpr auto kPollInterval = std::chrono::milliseconds(10);
            uint64_t waited_ms = 0;
            while (!stop_token.stop_requested() &&
                   waited_ms < kClientMonitorSleepMs) {
                std::this_thread::sleep_for(kPollInterval);
                waited_ms += kPollInterval.count();
            }
            if (!stop_token.stop_requested()) {
                ClientMonitorFunc();
            }
        }
    });
    VLOG(1) << "action=start_client_monitor_thread";
}

void P2PClientManager::Stop() {
    if (!client_monitor_thread_.joinable()) {
        return;
    }
    client_monitor_thread_.request_stop();
    client_monitor_thread_.join();
}

auto P2PClientManager::GetClient(const UUID& client_id) const
    -> std::shared_ptr<P2PClientMeta> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    auto it = client_metas_.find(client_id);
    return it == client_metas_.end() ? nullptr : it->second;
}

auto P2PClientManager::GetAllClients() const
    -> std::vector<std::shared_ptr<P2PClientMeta>> {
    SharedMutexLocker lock(&clients_mutex_, shared_lock);
    std::vector<std::shared_ptr<P2PClientMeta>> clients;
    clients.reserve(client_metas_.size());
    for (const auto& [id, client] : client_metas_) {
        clients.push_back(client);
    }
    return clients;
}

auto P2PClientManager::ListClients(P2PClientSelectionStrategy strategy) const
    -> tl::expected<std::vector<std::shared_ptr<P2PClientMeta>>, ErrorCode> {
    auto clients = GetAllClients();
    switch (strategy) {
        case P2PClientSelectionStrategy::ORDERED:
            break;
        case P2PClientSelectionStrategy::RANDOM: {
            std::random_device device;
            std::mt19937 generator(device());
            std::shuffle(clients.begin(), clients.end(), generator);
            break;
        }
        case P2PClientSelectionStrategy::CAPACITY_PRIORITY:
            std::sort(clients.begin(), clients.end(),
                      [](const auto& lhs, const auto& rhs) {
                          return lhs->GetAvailableCapacity() >
                                 rhs->GetAvailableCapacity();
                      });
            break;
        default:
            LOG(ERROR) << "Unsupported P2P client selection strategy"
                       << ", strategy=" << strategy;
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return clients;
}

auto P2PClientManager::ForEachClient(P2PClientSelectionStrategy strategy,
                                     const ClientVisitor& visitor)
    -> tl::expected<void, ErrorCode> {
    auto clients = ListClients(strategy);
    if (!clients.has_value()) {
        return tl::make_unexpected(clients.error());
    }
    for (const auto& client : *clients) {
        auto result = visitor(client);
        if (!result.has_value()) {
            LOG(ERROR) << "client visitor returned error"
                       << ", strategy=" << strategy
                       << ", client_id=" << client->get_client_id()
                       << ", error=" << result.error();
            return tl::make_unexpected(result.error());
        }
        if (*result) {
            break;
        }
    }
    return {};
}

auto P2PClientManager::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> names;
    for (const auto& client : GetAllClients()) {
        for (const auto& segment : client->GetSegments()) {
            names.push_back(segment.name);
        }
    }
    return names;
}

auto P2PClientManager::GetClientSegments(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto client = GetClient(client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    std::vector<std::string> names;
    for (const auto& segment : client->GetSegments()) {
        names.push_back(segment.name);
    }
    return names;
}

auto P2PClientManager::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    for (const auto& client : GetAllClients()) {
        auto result = client->QuerySegments(segment);
        if (result.has_value()) {
            return result;
        }
        if (result.error() != ErrorCode::SEGMENT_NOT_FOUND) {
            return result;
        }
    }
    return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
}

auto P2PClientManager::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    auto client = GetClient(client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return client->QueryIp();
}

auto P2PClientManager::QuerySegment(const UUID& client_id,
                                    const UUID& segment_id)
    -> tl::expected<P2PSegment, ErrorCode> {
    auto client = GetClient(client_id);
    if (!client) {
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }
    return client->QuerySegment(segment_id);
}

void P2PClientManager::SetSegmentRemovalCallback(
    SegmentRemovalCallback callback) {
    segment_removal_cb_ = std::move(callback);
}

auto P2PClientManager::RegisterClient(const P2PRegisterClientRequest& req)
    -> tl::expected<P2PRegisterClientResponse, ErrorCode> {
    if (req.ip_address.empty() || req.rpc_port == 0) {
        LOG(ERROR) << "RegisterClient(P2P): rejected invalid endpoint"
                   << ", client_id=" << req.client_id
                   << ", ip_address=" << req.ip_address
                   << ", rpc_port=" << req.rpc_port;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (GetClient(req.client_id)) {
        return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
    }

    auto client = std::make_shared<P2PClientMeta>(
        req.client_id, req.ip_address, req.rpc_port, disconnect_timeout_sec_,
        crash_timeout_sec_);
    for (const auto& segment : req.segments) {
        auto result = client->MountSegment(segment);
        if (!result.has_value()) {
            LOG(ERROR) << "RegisterClient: failed to mount segment"
                       << ", client_id=" << req.client_id
                       << ", segment_id=" << segment.id
                       << ", error=" << result.error();
            CleanupRoutes(client->RecycleSegments());
            return tl::make_unexpected(result.error());
        }
    }
    client->SetSyncing(true);

    bool inserted = false;
    {
        SharedMutexLocker lock(&clients_mutex_);
        auto [it, was_inserted] = client_metas_.emplace(req.client_id, client);
        inserted = was_inserted;
        if (inserted) {
            client->MarkRegistered();
        }
    }
    if (!inserted) {
        LOG(WARNING) << "RegisterClient: lost registration race"
                     << ", client_id=" << req.client_id;
        CleanupRoutes(client->RecycleSegments());
        return tl::make_unexpected(ErrorCode::CLIENT_ALREADY_EXISTS);
    }

    P2PMasterMetricManager::instance().inc_active_clients();
    P2PRegisterClientResponse response;
    response.view_version = view_version_;
    return response;
}

void P2PClientManager::CleanupRoutes(
    const std::vector<P2PRouteLocation>& locations) const {
    if (!segment_removal_cb_) {
        return;
    }
    for (const auto& location : locations) {
        segment_removal_cb_(location);
    }
}

auto P2PClientManager::UnregisterClient(const P2PUnregisterClientRequest& req)
    -> tl::expected<P2PUnregisterClientResponse, ErrorCode> {
    std::shared_ptr<P2PClientMeta> client;
    P2PClientStatus status = P2PClientStatus::UNREGISTERED;
    {
        SharedMutexLocker lock(&clients_mutex_);
        auto it = client_metas_.find(req.client_id);
        if (it == client_metas_.end()) {
            P2PUnregisterClientResponse response;
            response.view_version = view_version_;
            return response;
        }
        client = it->second;
        status = client->get_health_state().status;
        client_metas_.erase(it);
    }

    CleanupRoutes(client->RecycleSegments());
    if (status == P2PClientStatus::HEALTHY) {
        P2PMasterMetricManager::instance().dec_active_clients();
    }
    P2PUnregisterClientResponse response;
    response.view_version = view_version_;
    return response;
}

void P2PClientManager::ApplyHealthTransition(P2PClientStatus old_status,
                                             P2PClientStatus new_status,
                                             const UUID& client_id) {
    if (old_status == new_status) {
        return;
    }
    auto& metrics = P2PMasterMetricManager::instance();
    if (old_status == P2PClientStatus::HEALTHY &&
        new_status == P2PClientStatus::DISCONNECTED) {
        metrics.dec_active_clients();
        metrics.inc_clients_disconnected_total();
    } else if (old_status == P2PClientStatus::DISCONNECTED &&
               new_status == P2PClientStatus::HEALTHY) {
        metrics.inc_active_clients();
        metrics.inc_clients_recovered_total();
    } else if (new_status == P2PClientStatus::CRASHED) {
        if (old_status == P2PClientStatus::HEALTHY) {
            metrics.dec_active_clients();
        }
        metrics.inc_clients_crashed_total();
    } else {
        LOG(WARNING) << "Unexpected P2P client health transition"
                     << ", client_id=" << client_id
                     << ", old_status=" << old_status
                     << ", new_status=" << new_status;
    }
}

auto P2PClientManager::Heartbeat(const P2PHeartbeatRequest& req)
    -> tl::expected<P2PHeartbeatResponse, ErrorCode> {
    P2PHeartbeatResponse response;
    response.view_version = view_version_;
    auto client = GetClient(req.client_id);
    if (!client) {
        response.status = P2PClientStatus::UNREGISTERED;
        return response;
    }

    const auto [old_status, new_status] = client->Heartbeat();
    ApplyHealthTransition(old_status, new_status, req.client_id);
    response.status = new_status;
    if (new_status == P2PClientStatus::HEALTHY) {
        response.task_results.reserve(req.tasks.size());
        for (const auto& task : req.tasks) {
            response.task_results.push_back(ProcessTask(client, task));
        }
    }
    return response;
}

auto P2PClientManager::QueryClientStatus(
    const P2PQueryClientStatusRequest& req)
    -> tl::expected<P2PQueryClientStatusResponse, ErrorCode> {
    P2PQueryClientStatusResponse response;
    auto client = GetClient(req.client_id);
    response.status = client ? client->get_health_state().status
                             : P2PClientStatus::UNREGISTERED;
    return response;
}

HeartbeatTaskResult P2PClientManager::ProcessTask(
    const std::shared_ptr<P2PClientMeta>& client, const HeartbeatTask& task) {
    HeartbeatTaskResult result;
    result.type = task.type_;
    switch (task.type_) {
        case HeartbeatTaskType::SYNC_SEGMENT_META: {
            const auto* param = std::get_if<SyncSegmentMetaParam>(&task.param_);
            if (!param) {
                result.error = ErrorCode::INVALID_PARAMS;
                break;
            }
            auto sync_result =
                client->UpdateSegmentUsages(param->tier_usages);
            result.detail = sync_result;
            for (const auto& sub_result : sync_result.sub_results) {
                if (sub_result.error != ErrorCode::OK) {
                    LOG(ERROR) << "fail to update segment usage"
                               << ", client_id=" << client->get_client_id()
                               << ", segment_id=" << sub_result.segment_id
                               << ", error=" << sub_result.error;
                }
            }
            break;
        }
        case HeartbeatTaskType::SYNC_CLIENT_METRIC: {
            const auto* param =
                std::get_if<SyncClientMetricParam>(&task.param_);
            if (!param) {
                result.error = ErrorCode::INVALID_PARAMS;
                break;
            }
            P2PMasterMetricManager::instance().UpdateClientMetrics(
                client->get_client_id(), param->snapshot);
            break;
        }
        default:
            result.error = ErrorCode::NOT_IMPLEMENTED;
            break;
    }
    return result;
}

void P2PClientManager::ClientMonitorFunc() {
    std::vector<std::shared_ptr<P2PClientMeta>> crashed_clients;
    for (const auto& client : GetAllClients()) {
        const auto [old_status, new_status] = client->CheckHealth();
        ApplyHealthTransition(old_status, new_status, client->get_client_id());
        if (old_status != new_status &&
            new_status == P2PClientStatus::CRASHED) {
            crashed_clients.push_back(client);
        }
    }

    for (const auto& client : crashed_clients) {
        CleanupRoutes(client->RecycleSegments());
        SharedMutexLocker lock(&clients_mutex_);
        auto it = client_metas_.find(client->get_client_id());
        if (it != client_metas_.end() && it->second == client) {
            client_metas_.erase(it);
        }
    }
}

}  // namespace mooncake
