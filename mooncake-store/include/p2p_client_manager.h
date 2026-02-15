#pragma once

#include "client_manager.h"
#include "p2p_segment_manager.h"

namespace mooncake {

/**
 * @brief P2P Client Manager implementation.
 * Manages client lifecycle, heartbeat, and segment metadata
 * for P2P architecture.
 */
class P2PClientManager final : public ClientManager {
   public:
    P2PClientManager(const int64_t disconnect_timeout_sec,
                     const int64_t crash_timeout_sec,
                     const ViewVersionId view_version);

    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

   protected:
    // ===== Virtual Factory =====
    std::unique_ptr<ClientMeta> CreateClientMeta(
        const RegisterClientRequest& req) override;

    // ===== Hook Functions =====
    void OnClientDisconnected(const UUID& client_id) override;
    void OnClientCrashed(const UUID& client_id) override;
    void OnClientRecovered(const UUID& client_id) override;
    HeartbeatTaskResult ProcessTask(const UUID& client_id,
                                    const HeartbeatTask& task) override;

   protected:
    std::shared_ptr<P2PSegmentManager> p2p_segment_manager_;
};

}  // namespace mooncake
