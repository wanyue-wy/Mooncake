#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <memory>
#include <string>
#include <thread>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "heartbeat_type.h"
#include "client_meta.h"
#include "mutex.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {
/**
 * @brief ClientManager is a base class for managing clients' lifecycle and
 * heartbeat with a three-state state machine (HEALTH/DISCONNECTION/CRASHED).
 */
class ClientManager {
   public:
    ClientManager(const int64_t disconnect_timeout_sec,
                  const int64_t crash_timeout_sec,
                  const ViewVersionId view_version);
    virtual ~ClientManager();

    void Start();

    /**
     * @brief Register a client with its segments.
     * Writes ClientMeta to client_metas_ and batch-mounts segments.
     * Must be called before any other client/segment operations.
     * @return RegisterClientResponse containing master's view_version
     */
    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;

    /**
     * @brief Process a heartbeat from a client.
     * 1. maintain client healthy status machine:
     * - If client not in client_metas_: returns UNDEFINED + view_version,
     * client should register it again.
     * - If CRASHED: returns CRASHED:
     * master is cleaning up the client meta, client should retry until cleaning
     * over and register it again
     * - If DISCONNECTION: recovers to HEALTH
     * 2. Processes lightweight sync tasks
     */
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;

    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;

    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto GetClient(const UUID& client_id) -> std::shared_ptr<ClientMeta>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   protected:
    /**
     * @brief Client monitor implementation with three-state machine.
     */
    void ClientMonitorFunc();

    /**
     * @brief simple heartbeat task dispatcher
     */
    virtual HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                            const HeartbeatTask& task) = 0;

   protected:
    /**
     * @brief Create architecture-specific ClientMeta
     */
    virtual std::shared_ptr<ClientMeta> CreateClientMeta(
        const RegisterClientRequest& req) = 0;

   protected:
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks

   protected:
    mutable SharedMutex client_mutex_;
    // Client metadata: client_id -> metadata (including health state)
    std::unordered_map<UUID, std::shared_ptr<ClientMeta>, boost::hash<UUID>>
        client_metas_ GUARDED_BY(client_mutex_);
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    const ViewVersionId view_version_;  // Passed from MasterService
    SegmentRemovalCallback segment_removal_cb_;
};

}  // namespace mooncake
