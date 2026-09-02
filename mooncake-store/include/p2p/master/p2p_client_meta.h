#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/client/heartbeat_type.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_segment_manager.h"
#include "types.h"

namespace mooncake {

struct P2PClientHealthState {
    P2PClientStatus status = P2PClientStatus::UNREGISTERED;
    std::chrono::steady_clock::time_point last_heartbeat;
};

/** @brief One P2P client's health, endpoint and mounted segment state. */
class P2PClientMeta final {
   public:
    P2PClientMeta(const UUID& client_id, std::string ip_address,
                  uint16_t rpc_port, int64_t disconnect_timeout_sec,
                  int64_t crash_timeout_sec);
    ~P2PClientMeta();

    auto MountSegment(const P2PSegment& segment)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;
    std::vector<P2PSegment> GetSegments() const;
    auto QuerySegments(const std::string& segment_name) const
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& segment_id) const
        -> tl::expected<P2PSegment, ErrorCode>;

    std::pair<P2PClientStatus, P2PClientStatus> Heartbeat();
    std::pair<P2PClientStatus, P2PClientStatus> CheckHealth();
    std::vector<P2PRouteLocation> RecycleSegments();

    UUID get_client_id() const { return client_id_; }
    P2PClientHealthState get_health_state() const;
    bool is_health() const;

    auto UpdateSegmentUsages(const std::vector<TierUsageInfo>& usages)
        -> SyncSegmentMetaResult;
    size_t GetAvailableCapacity() const;

    const std::string& get_ip_address() const { return ip_address_; }
    uint16_t get_rpc_port() const { return rpc_port_; }
    auto QueryIp() const -> tl::expected<std::vector<std::string>, ErrorCode>;

    void MarkRegistered() {
        registered_.store(true, std::memory_order_release);
    }

    std::optional<P2PWriteCandidate> GetWriteRouteCandidate(
        const P2PGetWriteRouteRequest& req) const;

    void SetSyncing(bool syncing) {
        is_syncing_.store(syncing, std::memory_order_release);
    }
    bool IsSyncing() const {
        return is_syncing_.load(std::memory_order_acquire);
    }

   private:
    struct CapacityStat {
        size_t free = 0;
        size_t total = 0;
    };

    auto InnerStatusCheck() const -> tl::expected<void, ErrorCode>;
    void InnerUpdateHeartbeat();
    std::pair<P2PClientStatus, P2PClientStatus> InnerUpdateHealthStatus();
    static std::string HealthToString(P2PClientStatus status);
    CapacityStat GetWriteScoreCapacity(
        const std::vector<std::string>& tag_filters, int priority_limit,
        bool top_tier_only) const;

    const UUID client_id_;
    const std::string ip_address_;
    const uint16_t rpc_port_;
    const int64_t disconnect_timeout_sec_;
    const int64_t crash_timeout_sec_;

    mutable SharedMutex client_mutex_;
    P2PClientHealthState health_state_ GUARDED_BY(client_mutex_);
    std::atomic<bool> recycled_{false};
    std::atomic<bool> registered_{false};
    P2PSegmentManager segment_manager_;
    std::atomic<bool> is_syncing_{false};
};

}  // namespace mooncake
