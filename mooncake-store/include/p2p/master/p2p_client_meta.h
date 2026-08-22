#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/client/heartbeat_type.h"
#include "p2p/master/p2p_rpc_types.h"
#include "p2p/master/p2p_segment_manager.h"
#include "types.h"

namespace mooncake {

/**
 * @brief P2PClientMeta records the meta data of a P2P client including
 * health status and segment information.
 *
 * Standalone class: the generic health/segment bookkeeping formerly provided
 * by the ClientMeta base class has been absorbed here; the class no longer
 * participates in any inheritance hierarchy.
 */
class P2PClientMeta {
   public:
    P2PClientMeta(const UUID& client_id, const std::string& ip_address,
                  uint16_t rpc_port);

    ~P2PClientMeta();

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment);

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id);

    tl::expected<std::vector<Segment>, ErrorCode> GetSegments();

    tl::expected<std::pair<size_t, size_t>, ErrorCode> QuerySegments(
        const std::string& segment_name);
    tl::expected<std::shared_ptr<Segment>, ErrorCode> QuerySegment(
        const UUID& segment_id);

    tl::expected<std::vector<std::string>, ErrorCode> QueryIp(
        const UUID& client_id);

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   public:
    static void SetTimeouts(int64_t disconnect_sec, int64_t crash_sec);

    /**
     * @brief Update heartbeat timestamp and health status.
     * Attention: if client is CRASHED, the heartbeat will not be updated.
     * @return std::pair<ClientStatus, ClientStatus> {old_status, new_status}
     */
    std::pair<ClientStatus, ClientStatus> Heartbeat();

    /**
     * @brief Based on last heartbeat timestamp, update health status
     *
     * States machine:
     * - HEALTH:
     *   -> DISCONNECTION: If (now - last_heartbeat) > disconnect_timeout_sec.
     *
     * - DISCONNECTION:
     *   -> HEALTH: If (now - last_heartbeat) <= disconnect_timeout_sec.
     *      (Implies a Heartbeat() call updated the timestamp).
     *   -> CRASHED: If (now - last_heartbeat) > crash_timeout_sec.
     *
     * - CRASHED: Final state.
     *
     * @return std::pair<ClientStatus, ClientStatus> {old_status, new_status}
     */
    std::pair<ClientStatus, ClientStatus> CheckHealth();

   public:
    // Hooks for health status changes. P2P has no architecture-specific
    // disconnect/recover actions (the former DoOnDisconnected/DoOnRecovered
    // overrides were no-ops), so only the metric accounting remains.
    void OnDisconnected();
    void OnRecovered();
    // Crash hook: counts the crash and recycles the meta (segments). Like
    // OnDisconnected/OnRecovered, the metric accounting lives inside the hook.
    void OnCrashed();
    // Releases the meta's resources WITHOUT crash accounting.
    // Reused by both OnCrashed() and a proactive UnregisterClient().
    void RecycleMeta();

   public:
    UUID get_client_id() const { return client_id_; }

    ClientHealthState get_health_state() const;
    bool is_health() const;

   public:
    // ---- P2P-specific surface ----

    auto UpdateSegmentUsages(const std::vector<TierUsageInfo>& usages)
        -> SyncSegmentMetaResult;

    size_t GetAvailableCapacity() const;

    const std::string& get_ip_address() const { return ip_address_; }
    uint16_t get_rpc_port() const { return rpc_port_; }

   public:
    /**
     * @brief Evaluate this client as a write-route candidate.
     *
     * Performs health check and capacity filtering (tag_filters /
     * priority_limit / top_tier_only) internally and, on success, returns a
     * WriteCandidate whose `score` is the raw free ratio (free/total over
     * eligible tiers).
     *
     * @return A populated WriteCandidate when this client is routable;
     *         std::nullopt when it is not a candidate (unhealthy, no eligible
     *         tier, or insufficient free capacity).
     */
    std::optional<WriteCandidate> GetWriteRouteCandidate(
        const WriteRouteRequest& req);

   public:
    // HA sync tracking
    void SetSyncing(bool syncing) {
        is_syncing_.store(syncing, std::memory_order_release);
    }
    bool IsSyncing() const {
        return is_syncing_.load(std::memory_order_acquire);
    }

   private:
    tl::expected<void, ErrorCode> InnerStatusCheck() const;
    void InnerUpdateHeartbeat();
    std::pair<ClientStatus, ClientStatus> InnerUpdateHealthStatus();
    std::string HealthToString(ClientStatus status) const;

    /// Free/total capacity over the segments eligible for write-route scoring.
    struct CapacityStat {
        size_t free = 0;
        size_t total = 0;
    };
    /**
     * @brief Aggregate free/total over the eligible segments for write-route
     *        scoring. A segment is eligible when it carries no tag in
     *        `tag_filters` and its priority is >= `priority_limit`. When
     *        `top_tier_only` is true, only the highest-priority eligible
     *        segment(s) contribute (a client may not spill to lower tiers under
     *        memory pressure). Returns {0,0} when no segment is eligible.
     */
    CapacityStat GetWriteScoreCapacity(
        const std::vector<std::string>& tag_filters, int priority_limit,
        bool top_tier_only) const;

   private:
    // Absorbed from the former ClientMeta base.
    mutable SharedMutex client_mutex_;
    UUID client_id_;
    ClientHealthState health_state_ GUARDED_BY(client_mutex_);
    std::atomic<bool> recycled_{false};
    static int64_t disconnect_timeout_sec_;
    static int64_t crash_timeout_sec_;

    // P2P-specific state.
    std::string ip_address_;
    uint16_t rpc_port_ = 0;
    std::shared_ptr<P2PSegmentManager> segment_manager_;

    mutable SpinRWLock capacity_mutex_;
    size_t client_capacity_ GUARDED_BY(capacity_mutex_) = 0;
    size_t client_usage_ GUARDED_BY(capacity_mutex_) = 0;

    std::atomic<bool> is_syncing_{false};
};

}  // namespace mooncake
