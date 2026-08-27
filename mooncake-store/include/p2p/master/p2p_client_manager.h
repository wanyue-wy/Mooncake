#pragma once

#include <algorithm>
#include <atomic>
#include <boost/functional/hash.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/client/heartbeat_type.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_client_meta.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

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
 * @brief Manages P2P clients' lifecycle and heartbeat state.
 */
class P2PClientManager final {
   public:
    P2PClientManager(int64_t disconnect_timeout_sec,
                     int64_t crash_timeout_sec, ViewVersionId view_version);
    ~P2PClientManager();

    void Start();
    void Stop();

    void StartClientMonitor();
    void StopClientMonitor();

    auto RegisterClient(const P2PRegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;
    auto UnregisterClient(const UnregisterClientRequest& req)
        -> tl::expected<UnregisterClientResponse, ErrorCode>;
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
        -> tl::expected<std::shared_ptr<P2PSegment>, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto GetClient(const UUID& client_id) -> std::shared_ptr<P2PClientMeta>;
    auto GetAllClients() -> std::vector<std::shared_ptr<P2PClientMeta>>;

    auto GetClientIdBySegmentName(const std::string& segment_name)
        -> tl::expected<UUID, ErrorCode>;

    using ClientVisitor = std::function<tl::expected<bool, ErrorCode>(
        const std::shared_ptr<P2PClientMeta>& client)>;
    auto ForEachClient(ObjectIterateStrategy strategy,
                       const ClientVisitor& visitor)
        -> tl::expected<void, ErrorCode>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   protected:
    void ClientMonitorFunc();

    HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                    const HeartbeatTask& task);

    std::unique_ptr<P2PClientIterator> InnerBuildClientIterator(
        ObjectIterateStrategy strategy);

    // Retained while merging the former base-class call sequence. These
    // single-architecture helpers are audited in M3 stage 3.
    auto ValidateRegisterRequest(const P2PRegisterClientRequest& req)
        -> tl::expected<void, ErrorCode>;
    auto CreateClientMeta(const P2PRegisterClientRequest& req)
        -> std::shared_ptr<P2PClientMeta>;
    void OnClientRegistered(const std::shared_ptr<P2PClientMeta>& meta) {
        meta->SetSyncing(true);
    }

    static constexpr uint64_t kClientMonitorSleepMs = 1000;

    mutable SharedMutex clients_mutex_;
    std::unordered_map<UUID, std::shared_ptr<P2PClientMeta>, boost::hash<UUID>>
        client_metas_ GUARDED_BY(clients_mutex_);
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    const ViewVersionId view_version_;
    SegmentRemovalCallback segment_removal_cb_;
};

}  // namespace mooncake
