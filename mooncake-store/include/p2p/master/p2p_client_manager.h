#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/client/heartbeat_type.h"
#include "p2p/master/p2p_client_meta.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Iterator over the clients held by P2PClientManager.
 */
class P2PClientIterator {
   public:
    virtual ~P2PClientIterator() = default;

    std::shared_ptr<P2PClientMeta> Next() {
        if (index_ < clients_.size()) {
            return clients_[index_++];
        }
        return nullptr;
    }

   protected:
    P2PClientIterator() = default;

    std::vector<std::shared_ptr<P2PClientMeta>> clients_;
    size_t index_ = 0;
};

class P2POrderedClientIterator : public P2PClientIterator {
   public:
    explicit P2POrderedClientIterator(
        const std::unordered_map<UUID, std::shared_ptr<P2PClientMeta>,
                                 boost::hash<UUID>>& client_metas) {
        clients_.reserve(client_metas.size());
        for (const auto& [id, meta] : client_metas) {
            clients_.emplace_back(meta);
        }
    }
};

class P2PRandomClientIterator : public P2PClientIterator {
   public:
    explicit P2PRandomClientIterator(
        const std::unordered_map<UUID, std::shared_ptr<P2PClientMeta>,
                                 boost::hash<UUID>>& client_metas) {
        clients_.reserve(client_metas.size());
        for (const auto& [id, meta] : client_metas) {
            clients_.emplace_back(meta);
        }
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(clients_.begin(), clients_.end(), g);
    }
};

/**
 * @brief P2PClientManager manages the P2P clients' lifecycle and heartbeat
 * with a three-state state machine (HEALTH/DISCONNECTION/CRASHED).
 *
 * Standalone class: the generic client-management logic formerly provided by
 * the ClientManager base class has been absorbed here.
 */
class P2PClientManager {
   public:
    P2PClientManager(const int64_t disconnect_timeout_sec,
                     const int64_t crash_timeout_sec,
                     const ViewVersionId view_version);
    ~P2PClientManager();

    void Start();
    void Stop();

    void StartClientMonitor();
    void StopClientMonitor();

    /**
     * @brief Register a client with its segments.
     * Writes P2PClientMeta to client_metas_ and batch-mounts segments.
     * Must be called before any other client/segment operations.
     * @return RegisterClientResponse containing master's view_version
     */
    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;

    /**
     * @brief Proactively unregister a client and remove all routing metadata.
     */
    auto UnregisterClient(const UnregisterClientRequest& req)
        -> tl::expected<UnregisterClientResponse, ErrorCode>;

    /**
     * @brief Process a heartbeat from a client.
     * 1. maintain client healthy status machine:
     * - If client not in client_metas_: returns UNDEFINED + view_version,
     * client should register it again.
     * - If CRASHED: returns CRASHED:
     * master is cleaning up the client meta, client should retry until
     * cleaning over and register it again
     * - If DISCONNECTION: recovers to HEALTH
     * 2. Processes lightweight sync tasks
     */
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;

    auto QueryClientStatus(const QueryClientStatusRequest& req)
        -> tl::expected<QueryClientStatusResponse, ErrorCode>;

    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;

    auto QuerySegment(const UUID& client_id, const UUID& segment_id)
        -> tl::expected<std::shared_ptr<Segment>, ErrorCode>;

    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto GetClient(const UUID& client_id) -> std::shared_ptr<P2PClientMeta>;
    auto GetAllClients() -> std::vector<std::shared_ptr<P2PClientMeta>>;

    /**
     * @brief Find the client that owns the segment with the given name.
     * @param segment_name The segment name to look up.
     * @return The UUID of the client owning the segment, or
     *         ErrorCode::SEGMENT_NOT_FOUND if not found.
     */
    auto GetClientIdBySegmentName(const std::string& segment_name)
        -> tl::expected<UUID, ErrorCode>;

    /**
     * @brief Iterate clients in the order determined by strategy.
     * @param strategy Client iteration strategy
     * @param visitor Callback invoked for each client, return
     *                <is_continue, error_reason>:
     *                - if visitor occurs error, just return the `error_code`.
     *                - otherwise, return bool value to indicate whether
     *                  the iteration is over.
     * @return if clients iteration correctly, just return nothing,
     * otherwise return the first non-OK ErrorCode from the visitor.
     */
    using ClientVisitor = std::function<tl::expected<bool, ErrorCode>(
        const std::shared_ptr<P2PClientMeta>& client)>;
    auto ForEachClient(ObjectIterateStrategy strategy,
                       const ClientVisitor& visitor)
        -> tl::expected<void, ErrorCode>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   private:
    /**
     * @brief Client monitor implementation with three-state machine.
     */
    void ClientMonitorFunc();

    /**
     * @brief simple heartbeat task dispatcher
     */
    HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                    const HeartbeatTask& task);

    std::unique_ptr<P2PClientIterator> BuildClientIterator(
        ObjectIterateStrategy strategy);

    tl::expected<void, ErrorCode> ValidateRegisterRequest(
        const RegisterClientRequest& req);

    /**
     * @brief Create the P2P-specific ClientMeta
     */
    std::shared_ptr<P2PClientMeta> CreateClientMeta(
        const RegisterClientRequest& req);

   private:
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks

   private:
    mutable SharedMutex clients_mutex_;
    // Client metadata: client_id -> metadata (including health state)
    std::unordered_map<UUID, std::shared_ptr<P2PClientMeta>,
                       boost::hash<UUID>>
        client_metas_ GUARDED_BY(clients_mutex_);
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    const ViewVersionId view_version_;  // Passed from P2PMasterService
    SegmentRemovalCallback segment_removal_cb_;
};

}  // namespace mooncake
