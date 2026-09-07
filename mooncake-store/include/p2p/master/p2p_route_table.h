#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
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

   public:
    explicit P2PRouteTable(uint64_t max_client_per_key = 0)
        : max_client_per_key_(max_client_per_key) {}

    auto Publish(std::string_view key, uint64_t object_size,
                 const P2PRouteLocation& location)
        -> Mutation;
    auto Withdraw(std::string_view key, const P2PRouteLocation& location)
        -> Mutation;

    /**
     * pre_publish and post_publish execute while the corresponding route shard
     * is write locked. They must not call P2PRouteTable or acquire a lock
     * ordered before the route shard lock. post_publish is called only after a
     * successful mutation.
     */
    template <typename PrePublish, typename PostPublish>
    std::vector<Mutation> BatchPublish(
        const UUID& client_id,
        std::span<const P2PPublishRouteOperation> operations,
        PrePublish&& pre_publish, PostPublish&& post_publish);

    /**
     * pre_withdraw executes after the route location is found and immediately
     * before it is removed. post_withdraw is called only after a successful
     * mutation. Both execute while the corresponding route shard is write
     * locked and must obey the same lock-order restriction as the publish
     * hooks.
     */
    template <typename PreWithdraw, typename PostWithdraw>
    std::vector<Mutation> BatchWithdraw(
        const UUID& client_id,
        std::span<const P2PWithdrawRouteOperation> operations,
        PreWithdraw&& pre_withdraw, PostWithdraw&& post_withdraw);

    bool RouteExists(std::string_view key) const;
    std::optional<P2PRouteEntry> GetRoute(std::string_view key) const;
    std::vector<std::string> ListRouteKeys() const;
    size_t GetRouteKeyCount() const;

    CleanupResult RemoveLocation(const P2PRouteLocation& location);
    bool RemoveKey(std::string_view key);
    size_t Clear();

   private:
    using RouteMap =
        std::unordered_map<std::string, P2PRouteEntry, StringHash,
                           std::equal_to<>>;
    using OperationsByShard =
        std::unordered_map<size_t, std::vector<size_t>>;

    struct RouteShard {
        mutable SharedMutex mutex;
        RouteMap routes GUARDED_BY(mutex);
        std::unordered_map<P2PRouteLocation,
                           std::unordered_set<std::string_view>,
                           P2PRouteLocationHash>
            keys_by_location GUARDED_BY(mutex);
    };

    struct WithdrawTarget {
        RouteMap::iterator route;
        std::vector<P2PRouteLocation>::iterator location;
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
    auto FindWithdrawTargetLocked(RouteShard& shard, std::string_view key,
                                  const P2PRouteLocation& location)
        -> tl::expected<WithdrawTarget, ErrorCode> NO_THREAD_SAFETY_ANALYSIS;
    auto CommitWithdrawLocked(RouteShard& shard, WithdrawTarget target)
        -> Mutation NO_THREAD_SAFETY_ANALYSIS;
    auto WithdrawLocked(RouteShard& shard, std::string_view key,
                        const P2PRouteLocation& location) -> Mutation
        NO_THREAD_SAFETY_ANALYSIS;

    OperationsByShard GroupOperationsByShard(
        std::span<const P2PPublishRouteOperation> operations) const;
    OperationsByShard GroupOperationsByShard(
        std::span<const P2PWithdrawRouteOperation> operations) const;
    static void LogBatchPreconditionFailure(
        std::string_view action, std::string_view key,
        const UUID& client_id, const UUID& segment_id, ErrorCode error);

   private:
    std::array<RouteShard, kShardCount> shards_;
    const uint64_t max_client_per_key_;
};

template <typename PrePublish, typename PostPublish>
std::vector<P2PRouteTable::Mutation> P2PRouteTable::BatchPublish(
    const UUID& client_id,
    std::span<const P2PPublishRouteOperation> operations,
    PrePublish&& pre_publish, PostPublish&& post_publish) {
    std::vector<Mutation> results(
        operations.size(), tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    auto operations_by_shard = GroupOperationsByShard(operations);

    for (const auto& [shard_index, operation_indices] : operations_by_shard) {
        auto& shard = shards_[shard_index];
        SharedMutexLocker lock(&shard.mutex);
        for (size_t index : operation_indices) {
            const auto& operation = operations[index];
            const auto error = pre_publish(index, operation);
            if (error != ErrorCode::OK) {
                LogBatchPreconditionFailure(
                    "Publish route", operation.key, client_id,
                    operation.segment_id, error);
                results[index] = tl::make_unexpected(error);
                continue;
            }

            const P2PRouteLocation location{
                .client_id = client_id,
                .segment_id = operation.segment_id,
            };
            auto result = PublishLocked(shard, operation.key,
                                        operation.object_size, location);
            if (result.has_value()) {
                post_publish(index, operation, *result);
            }
            results[index] = std::move(result);
        }
    }
    return results;
}

template <typename PreWithdraw, typename PostWithdraw>
std::vector<P2PRouteTable::Mutation> P2PRouteTable::BatchWithdraw(
    const UUID& client_id,
    std::span<const P2PWithdrawRouteOperation> operations,
    PreWithdraw&& pre_withdraw, PostWithdraw&& post_withdraw) {
    std::vector<Mutation> results(
        operations.size(), tl::make_unexpected(ErrorCode::INTERNAL_ERROR));
    auto operations_by_shard = GroupOperationsByShard(operations);

    for (const auto& [shard_index, operation_indices] : operations_by_shard) {
        auto& shard = shards_[shard_index];
        SharedMutexLocker lock(&shard.mutex);
        for (size_t index : operation_indices) {
            const auto& operation = operations[index];
            const P2PRouteLocation location{
                .client_id = client_id,
                .segment_id = operation.segment_id,
            };
            auto target =
                FindWithdrawTargetLocked(shard, operation.key, location);
            if (!target.has_value()) {
                results[index] = tl::make_unexpected(target.error());
                continue;
            }

            const auto error = pre_withdraw(index, operation);
            if (error != ErrorCode::OK) {
                LogBatchPreconditionFailure(
                    "Withdraw", operation.key, client_id,
                    operation.segment_id, error);
                results[index] = tl::make_unexpected(error);
                continue;
            }
            auto result = CommitWithdrawLocked(shard, *target);
            post_withdraw(index, operation, *result);
            results[index] = std::move(result);
        }
    }
    return results;
}

}  // namespace mooncake
