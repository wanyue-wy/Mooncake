#pragma once
#include <functional>

#include "client_manager.h"
#include "centralized_segment_manager.h"

namespace mooncake {
class CentralizedClientManager final : public ClientManager {
   public:
    /**
     * @brief Construct CentralizedClientManager with a single timeout.
     * Only HEALTH and CRASHED states (no DISCONNECTION).
     */
    CentralizedClientManager(const int64_t client_live_ttl_sec,
                             const BufferAllocatorType memory_allocator_type,
                             std::function<void()> segment_clean_func,
                             const ViewVersionId view_version);

    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode> override;

    auto MountLocalDiskSegment(const UUID& client_id, bool enable_offloading)
        -> tl::expected<void, ErrorCode>;
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;
    auto PushOffloadingQueue(const std::string& key, const int64_t size,
                             const std::string& segment_name)
        -> tl::expected<void, ErrorCode>;

    auto Allocate(const uint64_t slice_length, const size_t replica_num,
                  const std::vector<std::string>& preferred_segments)
        -> tl::expected<std::vector<Replica>, ErrorCode> {
        return centralized_segment_manager_->Allocate(
            allocator_manager_, slice_length, replica_num, preferred_segments);
    }

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
    std::function<void()> segment_clean_func_;
    std::shared_ptr<CentralizedSegmentManager> centralized_segment_manager_;
};

}  // namespace mooncake
