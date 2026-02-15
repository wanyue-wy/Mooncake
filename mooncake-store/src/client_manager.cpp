#include "client_manager.h"
#include "master_metric_manager.h"
#include <glog/logging.h>

namespace mooncake {

ClientManager::ClientManager(const int64_t disconnect_timeout_sec,
                             const int64_t crash_timeout_sec,
                             const ViewVersionId view_version)
    : disconnect_timeout_sec_(disconnect_timeout_sec),
      crash_timeout_sec_(crash_timeout_sec),
      view_version_(view_version) {}

void ClientManager::Start() {
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&ClientManager::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";
}

ClientManager::~ClientManager() {
    client_monitor_running_ = false;
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
}

// ===== Client Registration =====

auto ClientManager::RegisterClient(const RegisterClientRequest& req)
    -> tl::expected<RegisterClientResponse, ErrorCode> {
    const auto& client_id = req.client_id;

    // Create architecture-specific client meta via virtual factory
    auto meta = CreateClientMeta(req);
    meta->segment_manager = segment_manager_;

    SharedMutexLocker lock(&client_mutex_);

    // Mount segments first? Or update meta first?
    // If we fail to mount, we abort?
    // RegisterClient implies success = all segments mounted or at least client
    // registered.

    // Let's iterate segments and mount them.
    for (const auto& segment : req.segments) {
        auto result = segment_manager_->MountSegment(segment, client_id);
        if (!result.has_value()) {
            if (result.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
                // Warn but continue? Or fail?
                // If it exists, maybe it's from previous crash.
                // We can treat it as success?
                LOG(WARNING) << "RegisterClient: segment already exists"
                             << ", segment=" << segment.name;
                // We still need to record it in our local meta segments list.
                // But wait, if it exists in SegmentManager, does it exist in
                // our Meta? We are creating NEW meta. So we should just add it.
            } else {
                LOG(ERROR) << "RegisterClient: failed to mount segment"
                           << ", segment_name=" << segment.name
                           << ", client_id=" << client_id
                           << ", error=" << result.error();
                return tl::make_unexpected(result.error());
            }
        }

        // Add to ClientMeta list
        meta->segments.push_back(std::make_shared<Segment>(segment));
    }

    // Write to client_metas_ (overwrites if re-registering after crash)
    client_metas_[client_id] = std::move(meta);

    MasterMetricManager::instance().inc_active_clients();

    RegisterClientResponse response;
    response.view_version = view_version_;

    LOG(INFO) << "RegisterClient: client_id=" << client_id
              << ", segments=" << req.segments.size()
              << ", view_version=" << response.view_version;

    return response;
}

// ===== Client Registration Check =====

bool ClientManager::IsClientRegistered(const UUID& client_id) const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return client_metas_.find(client_id) != client_metas_.end();
}

// ===== Unified Heartbeat Interface =====

auto ClientManager::Heartbeat(const HeartbeatRequest& req)
    -> tl::expected<HeartbeatResponse, ErrorCode> {
    const auto& client_id = req.client_id;
    HeartbeatResponse response;
    response.view_version = view_version_;

    SharedMutexLocker lock(&client_mutex_);
    auto it = client_metas_.find(client_id);
    if (it == client_metas_.end()) {
        // Client not in client_metas_: master restarted or client
        // timed out and was cleaned up. Return UNDEFINED + view_version
        // so client can decide whether to re-register or full-sync.
        response.status = ClientStatus::UNDEFINED;
        return response;
    }

    auto& meta = it->second;
    auto& state = meta->health_state;

    if (state.status == ClientStatus::CRASHED) {
        // Master is actively cleaning up this client's metadata.
        // Client should keep retrying heartbeat until it sees UNDEFINED.
        response.status = ClientStatus::CRASHED;
        return response;
    }

    // Update heartbeat timestamp
    state.last_heartbeat = std::chrono::steady_clock::now();

    if (state.status == ClientStatus::DISCONNECTION) {
        // Recovery: DISCONNECTION -> HEALTH
        LOG(INFO) << "client_id=" << client_id << ", action=client_recovered";
        state.status = ClientStatus::HEALTH;
        OnClientRecovered(client_id);
    }

    response.status = state.status;

    for (const auto& task : req.tasks) {
        response.task_results.push_back(ProcessTask(client_id, task));
    }

    return response;
}

// ===== Segment Management =====

auto ClientManager::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    // Segment Ops use Read Lock
    SharedMutexLocker lock(&client_mutex_, shared_lock);

    // Check client is registered
    if (client_metas_.find(client_id) == client_metas_.end()) {
        LOG(ERROR) << "MountSegment: client not registered"
                   << ", client_id=" << client_id;
        return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
    }

    auto err = segment_manager_->MountSegment(segment, client_id);
    if (!err.has_value()) {
        if (err.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            return {};
        } else {
            LOG(ERROR) << "fail to mount segment"
                       << ", segment_name=" << segment.name
                       << ", client_id=" << client_id
                       << ", ret=" << err.error();
            return err;
        }
    }

    MasterMetricManager::instance().inc_total_mem_capacity(segment.name,
                                                           segment.size);
    return {};
}

tl::expected<std::vector<std::string>, ErrorCode>
ClientManager::GetAllSegments() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    std::vector<std::string> all_segments;
    ErrorCode err = segment_manager_->GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to get all segments" << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return all_segments;
}

tl::expected<std::pair<size_t, size_t>, ErrorCode> ClientManager::QuerySegments(
    const std::string& segment) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    size_t used = 0;
    size_t capacity = 0;
    ErrorCode err = segment_manager_->QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query segments" << ", segment=" << segment
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

tl::expected<std::vector<std::string>, ErrorCode> ClientManager::QueryIp(
    const UUID& client_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    std::vector<std::string> result;
    ErrorCode err = segment_manager_->QueryIp(client_id, result);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "fail to query ip" << ", client_id=" << client_id
                   << ", ret=" << err;
        return tl::make_unexpected(err);
    }
    return result;
}

// ===== Default Client Monitor (three-state machine) =====

void ClientManager::ClientMonitorFunc() {
    while (client_monitor_running_) {
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));

        // Monitor holds Write Lock throughout execution
        SharedMutexLocker lock(&client_mutex_);

        auto now = std::chrono::steady_clock::now();
        std::vector<UUID> newly_disconnected;
        std::vector<UUID> newly_crashed;

        for (auto& [client_id, meta] : client_metas_) {
            auto& state = meta->health_state;
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               now - state.last_heartbeat)
                               .count();

            switch (state.status) {
                case ClientStatus::HEALTH: {
                    if (elapsed > disconnect_timeout_sec_) {
                        LOG(WARNING) << "client_id=" << client_id
                                     << ", action=client_disconnected"
                                     << ", elapsed_sec=" << elapsed;
                        state.status = ClientStatus::DISCONNECTION;
                        state.disconnection_start = now;
                        newly_disconnected.push_back(client_id);
                    }
                    break;
                }
                case ClientStatus::DISCONNECTION: {
                    auto disconnected_sec =
                        std::chrono::duration_cast<std::chrono::seconds>(
                            now - state.disconnection_start)
                            .count();
                    if (disconnected_sec > crash_timeout_sec_) {
                        LOG(ERROR) << "client_id=" << client_id
                                   << ", action=client_crashed"
                                   << ", disconnected_sec=" << disconnected_sec;
                        state.status = ClientStatus::CRASHED;
                        newly_crashed.push_back(client_id);
                    }
                    break;
                }
                default:
                    break;
            }
        }  // end for

        // Phase 2: Execute hooks inside lock
        for (const auto& client_id : newly_disconnected) {
            OnClientDisconnected(client_id);
        }

        for (const auto& client_id : newly_crashed) {
            OnClientCrashed(client_id);
        }

        // Phase 3: Remove crashed clients
        for (const auto& client_id : newly_crashed) {
            // Already holding Write Lock from line 219
            client_metas_.erase(client_id);
            MasterMetricManager::instance().dec_active_clients();
        }
    }
}

}  // namespace mooncake
