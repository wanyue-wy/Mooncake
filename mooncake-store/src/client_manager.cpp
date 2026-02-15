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

    SharedMutexLocker lock(&client_mutex_);
    // Write to client_metas_ (overwrites if re-registering after crash)
    client_metas_[client_id] = std::move(meta);

    // Batch mount segments
    // Note: InnerMountSegment needs to be called with Write Lock held (from
    // RegisterClient) But InnerMountSegment usually takes Read Lock? User
    // requirement:
    // "RegisterClient中要全程持有client_mutex_锁...并使用InnerMountSegment来注册segment"
    // Issue: InnerMountSegment likely acquires lock internally if it follows
    // standard pattern. Solution: We need to check if InnerMountSegment locks.
    // If we implement InnerMountSegment in subclasses to grab lock, we have
    // deadlock. However, RegisterClient holds Write Lock. InnerMountSegment
    // should probably expect lock to be held or we need a version that doesn't
    // lock. BUT, RegisterClient is in base class. InnerMountSegment is virtual.
    // Let's look at the instruction:
    // "ClientManager::RegisterClient中要全程持有client_mutex_锁...并且应该移除InnerReMountSegment接口，使用InnerMountSegment来注册segment"
    // Implementation:
    for (const auto& segment : req.segments) {
        auto result = InnerMountSegment(segment, client_id);
        if (!result.has_value()) {
            LOG(ERROR) << "RegisterClient: failed to mount segment"
                       << ", segment_name=" << segment.name
                       << ", client_id=" << client_id
                       << ", error=" << result.error();
            // Rollback: remove client meta on segment mount failure
            client_metas_.erase(client_id);
            return tl::make_unexpected(result.error());
        }
    }

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

    auto err = InnerMountSegment(segment, client_id);
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
            client_metas_.erase(client_id);
            MasterMetricManager::instance().dec_active_clients();
        }
    }
}

}  // namespace mooncake
