#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/common/p2p_types.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

/**
 * @brief Sharded in-memory index of P2P object routes.
 *
 * The table owns route keys and is the only component allowed to retain
 * string_view references to them. No map, iterator, or lock escapes this
 * class.
 */
class P2PRouteTable final {
   public:
    struct MutationResult {
        bool created_key{false};
        bool removed_key{false};
    };

    struct CleanupResult {
        size_t removed_routes{0};
        std::vector<std::string> removed_keys;
    };

    using Mutation = tl::expected<MutationResult, ErrorCode>;
    using BeforePublishCallback =
        std::function<ErrorCode(const P2PPublishRouteOperation& operation)>;
    using PublishResultCallback = std::function<void(
        size_t index, const P2PPublishRouteOperation& operation,
        const Mutation& result)>;
    using BeforeWithdrawCallback =
        std::function<ErrorCode(const P2PWithdrawRouteOperation& operation)>;
    using WithdrawResultCallback = std::function<void(
        size_t index, const P2PWithdrawRouteOperation& operation,
        const Mutation& result)>;

   public:
    explicit P2PRouteTable(uint64_t max_client_per_key = 0)
        : max_client_per_key_(max_client_per_key) {}

    auto Publish(std::string_view key, uint64_t object_size,
                 const P2PRouteLocation& location)
        -> Mutation;
    auto Withdraw(std::string_view key, const P2PRouteLocation& location)
        -> Mutation;

    /**
     * Batch callbacks execute while the corresponding route shard is write
     * locked. They must not call P2PRouteTable or acquire a lock ordered before
     * the route shard lock.
     */
    void BatchPublish(
        const UUID& client_id,
        std::span<const P2PPublishRouteOperation> operations,
        const BeforePublishCallback& before_publish,
        const PublishResultCallback& on_result);
    void BatchWithdraw(
        const UUID& client_id,
        std::span<const P2PWithdrawRouteOperation> operations,
        const BeforeWithdrawCallback& before_withdraw,
        const WithdrawResultCallback& on_result);
    void BatchSync(
        const UUID& client_id,
        std::span<const P2PPublishRouteOperation> publish_operations,
        std::span<const P2PWithdrawRouteOperation> withdraw_operations,
        const BeforePublishCallback& before_publish,
        const PublishResultCallback& on_publish_result,
        const BeforeWithdrawCallback& before_withdraw,
        const WithdrawResultCallback& on_withdraw_result);

    bool RouteExists(std::string_view key) const;
    std::optional<P2PRouteEntry> GetRoute(std::string_view key) const;
    std::vector<std::string> ListRouteKeys() const;
    size_t GetRouteKeyCount() const;

    CleanupResult RemoveLocation(const P2PRouteLocation& location);
    bool RemoveKey(std::string_view key);
    size_t Clear();

   private:
    struct RouteShard {
        mutable SharedMutex mutex;
        std::unordered_map<std::string, P2PRouteEntry, StringHash,
                           std::equal_to<>>
            routes GUARDED_BY(mutex);
        std::unordered_map<P2PRouteLocation,
                           std::unordered_set<std::string_view>,
                           P2PRouteLocationHash>
            keys_by_location GUARDED_BY(mutex);
    };

   private:
    static constexpr size_t kShardCount = 1024;

    static size_t CountOwnerClients(const P2PRouteEntry& entry);
    static void RemoveReverseIndex(RouteShard& shard, std::string_view key,
                                   const P2PRouteLocation& location)
        NO_THREAD_SAFETY_ANALYSIS;
    static void RemoveAllReverseIndexes(RouteShard& shard,
                                        std::string_view key,
                                        const P2PRouteEntry& entry)
        NO_THREAD_SAFETY_ANALYSIS;

   private:
    size_t GetShardIndex(std::string_view key) const {
        return std::hash<std::string_view>{}(key) % kShardCount;
    }

    auto PublishLocked(RouteShard& shard, std::string_view key,
                       uint64_t object_size,
                       const P2PRouteLocation& location) -> Mutation
        NO_THREAD_SAFETY_ANALYSIS;
    auto WithdrawLocked(
        RouteShard& shard, std::string_view key,
        const P2PRouteLocation& location,
        const P2PWithdrawRouteOperation* operation = nullptr,
        const BeforeWithdrawCallback& before_withdraw = {}) -> Mutation
        NO_THREAD_SAFETY_ANALYSIS;

   private:
    std::array<RouteShard, kShardCount> shards_;
    const uint64_t max_client_per_key_;
};

}  // namespace mooncake
