#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include "segment_manager.h"
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "heartbeat_type.h"
#include "mutex.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Internal state tracking for client health.
 */
struct ClientHealthState {
    ClientStatus status = ClientStatus::UNDEFINED;
    std::chrono::steady_clock::time_point last_heartbeat;
    std::chrono::steady_clock::time_point disconnection_start;
};

/**
 * @brief Base client metadata. Contains health state.
 * Subclasses (e.g., P2PClientMeta) extend with architecture-specific fields.
 */
struct ClientMeta {
    virtual ~ClientMeta() = default;
    ClientHealthState health_state;
    // SegmentManager for this client (Centralized or P2P)
    std::shared_ptr<SegmentManager> segment_manager;
    // List of segments allocated to this client.
    std::vector<std::shared_ptr<Segment>> segments;
};

/**
 * @brief ClientManager is a base class for managing client lifecycle and
 * heartbeat. It provides a unified Heartbeat interface for both Centralized
 * and P2P architectures, with a three-state state machine
 * (HEALTH/DISCONNECTION/CRASHED).
 *
 * Subclasses implement hook functions for architecture-specific behavior.
 */
class ClientManager {
   public:
    ClientManager(const int64_t disconnect_timeout_sec,
                  const int64_t crash_timeout_sec,
                  const ViewVersionId view_version);
    virtual ~ClientManager();

    void Start();

    // ===== Client Registration =====
    /**
     * @brief Register a client with its segments.
     * Writes ClientMeta to client_metas_ and batch-mounts segments.
     * Must be called before any other client/segment operations.
     * @return RegisterClientResponse containing master's view_version
     */
    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;

    // ===== Unified Heartbeat Interface =====
    /**
     * @brief Process a heartbeat from a client.
     * - If client not in client_metas_: returns UNDEFINED + view_version
     * - If CRASHED: returns CRASHED (master cleaning up, client retries)
     * - If DISCONNECTION: recovers to HEALTH
     * - Processes lightweight sync tasks
     */
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;

    // ===== Segment Management =====
    /**
     * @brief Mount a single segment (for runtime expansion only).
     * Client must be registered first.
     */
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    virtual auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;

    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;

    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

   protected:
    // ===== Monitor =====
    /**
     * @brief Default client monitor implementation with three-state machine.
     * Subclasses can override for custom monitoring logic (e.g.,
     * CentralizedClientManager skips DISCONNECTION).
     */
    void ClientMonitorFunc();

    bool IsClientRegistered(const UUID& client_id) const;

   protected:
    // ===== Virtual Factory for ClientMeta =====
    /**
     * @brief Create architecture-specific ClientMeta from
     * RegisterClientRequest. Centralized: returns base ClientMeta. P2P: returns
     * P2PClientMeta with ip/port.
     */
    virtual std::unique_ptr<ClientMeta> CreateClientMeta(
        const RegisterClientRequest& req) = 0;

    // ===== Hook Functions (subclass implements) =====

    /**
     * @brief Called when a client transitions to DISCONNECTION state.
     * Centralized: no-op (only HEALTH/CRASHED). P2P: pauses routing.
     */
    virtual void OnClientDisconnected(const UUID& client_id) = 0;

    /**
     * @brief Called when a client transitions to CRASHED state.
     * Centralized: unmounts segments + cleans handles. P2P: cleans all
     * metadata + replicas.
     */
    virtual void OnClientCrashed(const UUID& client_id) = 0;

    /**
     * @brief Called when a client recovers from DISCONNECTION to HEALTH.
     * Centralized: no-op. P2P: resumes routing.
     */
    virtual void OnClientRecovered(const UUID& client_id) = 0;

    /**
     * @brief Process a single heartbeat task. Subclass implements
     * architecture-specific logic for each task type.
     */
    virtual HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                            const HeartbeatTask& task) = 0;

   protected:
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks

   protected:
    std::shared_ptr<SegmentManager> segment_manager_;
    mutable SharedMutex client_mutex_;
    // Client metadata: client_id -> metadata (including health state)
    std::unordered_map<UUID, std::unique_ptr<ClientMeta>, boost::hash<UUID>>
        client_metas_ GUARDED_BY(client_mutex_);
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    const int64_t disconnect_timeout_sec_;  // HEALTH -> DISCONNECTION
    const int64_t crash_timeout_sec_;       // DISCONNECTION -> CRASHED
    const ViewVersionId view_version_;      // Passed from MasterService
};

}  // namespace mooncake
