#include "p2p/master/p2p_client_meta.h"

#include <algorithm>
#include <glog/logging.h>
#include <limits>

#include "master_metric_manager.h"
#include "p2p/master/p2p_master_metric_manager.h"

namespace mooncake {

int64_t P2PClientMeta::disconnect_timeout_sec_ = 0;
int64_t P2PClientMeta::crash_timeout_sec_ = 0;

P2PClientMeta::P2PClientMeta(const UUID& client_id,
                             const std::string& ip_address, uint16_t rpc_port)
    : client_id_(client_id),
      ip_address_(ip_address),
      rpc_port_(rpc_port),
      segment_manager_(std::make_shared<P2PSegmentManager>()) {
    health_state_.status = ClientStatus::HEALTH;
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
    segment_manager_->SetSegmentChangeCallbacks(
        [this](const P2PSegment& segment) {
            // OnSegmentAddedCallback
            SpinRWLockLocker lock(&capacity_mutex_);
            client_capacity_ += segment.size;
            client_usage_ += segment.usage;
        },
        [this](const P2PSegment& segment) {
            // OnSegmentRemovedCallback
            SpinRWLockLocker lock(&capacity_mutex_);
            client_capacity_ -= segment.size;
            client_usage_ -= segment.usage;
        });
}

P2PClientMeta::~P2PClientMeta() {
    P2PMasterMetricManager::instance().OnClientRemoved(client_id_);
}

tl::expected<void, ErrorCode> P2PClientMeta::MountSegment(
    const P2PSegment& segment) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }

    auto ret = segment_manager_->MountSegment(segment);
    if (!ret.has_value()) {
        if (ret.error() == ErrorCode::SEGMENT_ALREADY_EXISTS) {
            LOG(WARNING) << "attempt to mount segment but it already exists"
                         << ", client_id=" << client_id_
                         << ", segment_id =" << segment.id
                         << ", segment_name=" << segment.name
                         << ", ret=" << ret.error();
            return {};
        }
        LOG(ERROR) << "fail to mount segment"
                   << ", client_id=" << client_id_
                   << ", segment_id =" << segment.id
                   << ", segment_name=" << segment.name
                   << ", ret=" << ret.error();
        return ret;
    }
    LOG(INFO) << "Mount segment success"
              << ", client_id=" << client_id_ << ", segment_id =" << segment.id
              << ", segment_name=" << segment.name;
    return {};
}

tl::expected<void, ErrorCode> P2PClientMeta::UnmountSegment(
    const UUID& segment_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return check_ret;
    }

    auto ret = segment_manager_->UnmountSegment(segment_id);
    if (!ret.has_value()) {
        if (ret.error() == ErrorCode::SEGMENT_NOT_FOUND) {
            LOG(WARNING) << "attempt to unmount segment but it does not exist"
                         << ", client_id=" << client_id_
                         << ", segment_id=" << segment_id
                         << ", ret=" << ret.error();
            return {};
        }
        LOG(ERROR) << "fail to unmount segment"
                   << ", client_id=" << client_id_
                   << ", segment_id=" << segment_id
                   << ", ret=" << ret.error();
        return ret;
    }
    LOG(INFO) << "Unmount segment success"
              << ", client_id=" << client_id_
              << ", segment_id =" << segment_id;
    return {};
}

tl::expected<std::vector<P2PSegment>, ErrorCode>
P2PClientMeta::GetSegments() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_->GetSegments();
}

tl::expected<std::pair<size_t, size_t>, ErrorCode>
P2PClientMeta::QuerySegments(const std::string& segment_name) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_->QuerySegments(segment_name);
}

tl::expected<std::shared_ptr<P2PSegment>, ErrorCode>
P2PClientMeta::QuerySegment(const UUID& segment_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return segment_manager_->QuerySegment(segment_id);
}

void P2PClientMeta::SetSegmentRemovalCallback(SegmentRemovalCallback cb) {
    segment_manager_->SetSegmentRemovalCallback(std::move(cb));
}

void P2PClientMeta::SetTimeouts(int64_t disconnect_sec, int64_t crash_sec) {
    disconnect_timeout_sec_ = disconnect_sec;
    crash_timeout_sec_ = crash_sec;
}

P2PClientHealthState P2PClientMeta::get_health_state() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return health_state_;
}

bool P2PClientMeta::is_health() const {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    return health_state_.status == ClientStatus::HEALTH;
}

std::pair<ClientStatus, ClientStatus> P2PClientMeta::Heartbeat() {
    SharedMutexLocker lock(&client_mutex_);
    InnerUpdateHeartbeat();
    return InnerUpdateHealthStatus();
}

std::pair<ClientStatus, ClientStatus> P2PClientMeta::CheckHealth() {
    SharedMutexLocker lock(&client_mutex_);
    return InnerUpdateHealthStatus();
}

void P2PClientMeta::InnerUpdateHeartbeat() {
    if (health_state_.status == ClientStatus::CRASHED) {
        LOG(WARNING) << "heartbeat received while in CRASHED state, "
                        "timestamp will not update"
                     << ", client_id=" << client_id_;
        return;
    } else if (health_state_.status == ClientStatus::DISCONNECTION) {
        LOG(WARNING) << "heartbeat received while in DISCONNECTION state, "
                        "the state might change to HEALTH as soon as possible"
                     << ", client_id=" << client_id_;
    }
    health_state_.last_heartbeat = std::chrono::steady_clock::now();
}

std::pair<ClientStatus, ClientStatus>
P2PClientMeta::InnerUpdateHealthStatus() {
    const auto now = std::chrono::steady_clock::now();
    const ClientStatus old_status = health_state_.status;
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - health_state_.last_heartbeat)
            .count();

    const int64_t disconnect_timeout_ms = disconnect_timeout_sec_ * 1000;
    const int64_t crash_timeout_ms = crash_timeout_sec_ * 1000;

    switch (health_state_.status) {
        case ClientStatus::HEALTH:
            if (elapsed_ms >= disconnect_timeout_ms) {
                if (elapsed_ms >= crash_timeout_ms) {
                    health_state_.status = ClientStatus::CRASHED;
                } else {
                    health_state_.status = ClientStatus::DISCONNECTION;
                }
            }
            break;
        case ClientStatus::DISCONNECTION:
            if (elapsed_ms < disconnect_timeout_ms) {
                health_state_.status = ClientStatus::HEALTH;
            } else if (elapsed_ms >= crash_timeout_ms) {
                health_state_.status = ClientStatus::CRASHED;
            }
            break;
        case ClientStatus::CRASHED:
        case ClientStatus::UNDEFINED:
            break;
    }

    if (health_state_.status != old_status) {
        LOG(INFO) << "Client status changed"
                  << ", client_id=" << client_id_
                  << ", old_status=" << HealthToString(old_status)
                  << ", new_status=" << HealthToString(health_state_.status);
    }
    return {old_status, health_state_.status};
}

tl::expected<void, ErrorCode> P2PClientMeta::InnerStatusCheck() const {
    if (health_state_.status != ClientStatus::HEALTH) {
        LOG(WARNING) << "Client is not HEALTH"
                     << ", client_id=" << client_id_
                     << ", status=" << HealthToString(health_state_.status);
        return tl::make_unexpected(ErrorCode::CLIENT_UNHEALTHY);
    }
    return {};
}

void P2PClientMeta::OnDisconnected() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (health_state_.status == ClientStatus::HEALTH) {
        return;
    } else if (health_state_.status != ClientStatus::DISCONNECTION) {
        LOG(ERROR) << "unexpected hook calling"
                   << ", client_id=" << client_id_ << ", current status="
                   << HealthToString(health_state_.status)
                   << ", expected status="
                   << HealthToString(ClientStatus::DISCONNECTION);
        return;
    }
    LOG(INFO) << "the client is disconnected"
              << ", client_id=" << client_id_;
    MasterMetricManager::instance().dec_active_clients();
    MasterMetricManager::instance().inc_clients_disconnected_total();
}

void P2PClientMeta::OnRecovered() {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    if (health_state_.status != ClientStatus::HEALTH) {
        LOG(ERROR) << "unexpected hook calling"
                   << ", client_id=" << client_id_ << ", current status="
                   << HealthToString(health_state_.status)
                   << ", expected status="
                   << HealthToString(ClientStatus::HEALTH);
        return;
    }
    LOG(INFO) << "the client is recovered"
              << ", client_id=" << client_id_;
    MasterMetricManager::instance().inc_active_clients();
    MasterMetricManager::instance().inc_clients_recovered_total();
}

void P2PClientMeta::OnCrashed() {
    LOG(INFO) << "the client is crashed"
              << ", client_id=" << client_id_;
    MasterMetricManager::instance().inc_clients_crashed_total();
    RecycleMeta();
}

void P2PClientMeta::RecycleMeta() {
    if (recycled_.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    LOG(INFO) << "start to recycle client meta"
              << ", client_id=" << client_id_;
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto segments_res = segment_manager_->GetSegments();
    if (segments_res) {
        for (const auto& segment : *segments_res) {
            auto ret = segment_manager_->UnmountSegment(segment.id);
            if (!ret.has_value()) {
                LOG(ERROR) << "Failed to unmount segment"
                           << ", client_id=" << client_id_
                           << ", segment_id=" << segment.id
                           << " error=" << ret.error();
            }
        }
    }
    LOG(INFO) << "the client meta is recycled over"
              << ", client_id=" << client_id_;
}

std::string P2PClientMeta::HealthToString(ClientStatus status) const {
    switch (status) {
        case ClientStatus::HEALTH:
            return "HEALTH";
        case ClientStatus::DISCONNECTION:
            return "DISCONNECTION";
        case ClientStatus::CRASHED:
            return "CRASHED";
        case ClientStatus::UNDEFINED:
            return "UNDEFINED";
    }
    return "UNKNOWN";
}

std::shared_ptr<P2PSegmentManager> P2PClientMeta::GetSegmentManager() {
    return segment_manager_;
}

tl::expected<std::vector<std::string>, ErrorCode> P2PClientMeta::QueryIp(
    const UUID& client_id) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(ERROR) << "fail to inner check client status"
                   << ", client_id=" << client_id_
                   << ", ret=" << check_ret.error();
        return tl::make_unexpected(check_ret.error());
    }
    return std::vector<std::string>{ip_address_};
}

SyncSegmentMetaResult P2PClientMeta::UpdateSegmentUsages(
    const std::vector<TierUsageInfo>& usages) {
    SyncSegmentMetaResult result;
    int64_t usage_delta = 0;
    for (const auto& usage : usages) {
        SyncSegmentMetaResult::SubResult sub_res;
        sub_res.segment_id = usage.segment_id;

        auto old_usage =
            segment_manager_->UpdateSegmentUsage(usage.segment_id, usage.usage);
        if (!old_usage.has_value()) {
            LOG(WARNING) << "fail to update segment usage"
                         << ", client_id: " << client_id_
                         << ", segment_id: " << usage.segment_id
                         << ", usage: " << usage.usage
                         << ", error: " << old_usage.error();
            sub_res.error = old_usage.error();
            result.sub_results.push_back(sub_res);
            continue;
        }

        usage_delta += static_cast<int64_t>(usage.usage) -
                       static_cast<int64_t>(old_usage.value());
        sub_res.error = ErrorCode::OK;
        result.sub_results.push_back(sub_res);
    }

    if (usage_delta != 0) {
        SpinRWLockLocker lock(&capacity_mutex_);
        const int64_t new_usage =
            static_cast<int64_t>(client_usage_) + usage_delta;
        if (new_usage < 0) {
            LOG(ERROR) << "client usage would go negative, clamp to 0"
                       << ", client_id=" << client_id_
                       << ", client_usage=" << client_usage_
                       << ", usage_delta=" << usage_delta;
        }
        client_usage_ = new_usage > 0 ? static_cast<size_t>(new_usage) : 0;
    }
    return result;
}

size_t P2PClientMeta::GetAvailableCapacity() const {
    SpinRWLockLocker lock(&capacity_mutex_, shared_lock);
    if (client_capacity_ <= client_usage_) return 0;
    return client_capacity_ - client_usage_;
}

P2PClientMeta::CapacityStat P2PClientMeta::GetWriteScoreCapacity(
    const std::vector<std::string>& tag_filters, int priority_limit,
    bool top_tier_only) const {
    // A segment is eligible for scoring if it carries no filtered tag and its
    // priority is >= priority_limit.
    auto eligible = [&](const P2PSegment& segment) -> bool {
        if (segment.priority < priority_limit) return false;
        for (const auto& tag : tag_filters) {
            if (std::find(segment.tags.begin(), segment.tags.end(), tag) !=
                segment.tags.end()) {
                return false;
            }
        }
        return true;
    };

    CapacityStat all, top;
    int max_priority = std::numeric_limits<int>::min();
    segment_manager_->ForEachSegment([&](const P2PSegment& seg) -> bool {
        if (!eligible(seg)) return false;
        const size_t free = seg.size > seg.usage ? seg.size - seg.usage : 0;
        all.total += seg.size;
        all.free += free;
        if (seg.priority > max_priority) {
            max_priority = seg.priority;
            top = {free, seg.size};
        } else if (seg.priority == max_priority) {
            top.total += seg.size;
            top.free += free;
        }
        return false;  // always continue to next segment
    });
    return top_tier_only ? top : all;
}

// Returns std::nullopt when this client is not a write-route candidate
std::optional<WriteCandidate> P2PClientMeta::GetWriteRouteCandidate(
    const WriteRouteRequest& req) {
    SharedMutexLocker lock(&client_mutex_, shared_lock);

    // Check health status under lock protection.
    auto check_ret = InnerStatusCheck();
    if (!check_ret.has_value()) {
        LOG(WARNING) << "client could not route"
                     << ", client_id: " << client_id_;
        // Unhealthy is not an error: the client is simply not a candidate.
        return std::nullopt;
    }

    // Client-granular routing: the master routes to a client; the client picks
    // the concrete segment/tier. The score is the raw free ratio; the master
    // multiplies it by (1-w) for local or w for remote.
    const CapacityStat cap =
        GetWriteScoreCapacity(req.config.tag_filters, req.config.priority_limit,
                              req.config.top_tier_only);
    if (cap.total == 0) return std::nullopt;  // no eligible tier
    if (cap.free < req.size)
        return std::nullopt;  // cannot hold (master's view)

    const double free_ratio = static_cast<double>(cap.free) / cap.total;

    WriteCandidate candidate;
    candidate.client_id = client_id_;
    candidate.ip_address = ip_address_;
    candidate.rpc_port = rpc_port_;
    candidate.available_capacity = cap.free;
    candidate.score = free_ratio;
    return candidate;
}

}  // namespace mooncake
