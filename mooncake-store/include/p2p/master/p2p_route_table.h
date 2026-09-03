#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
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

   public:
    explicit P2PRouteTable(uint64_t max_client_per_key = 0)
        : max_client_per_key_(max_client_per_key) {}

    // TODO(M8.3; see p2p-master-final-refactor-plan.md): Add batch publish and
    // withdraw APIs that consume owning AoS route operations directly. Batch
    // execution must group once by shard, execute once, and expose no shard
    // accessor outside P2PRouteTable.
    auto Publish(std::string_view key, uint64_t object_size,
                 const P2PRouteLocation& location)
        -> tl::expected<MutationResult, ErrorCode>;
    auto Withdraw(std::string_view key, const P2PRouteLocation& location)
        -> tl::expected<MutationResult, ErrorCode>;

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

   private:
    std::array<RouteShard, kShardCount> shards_;
    const uint64_t max_client_per_key_;
};

}  // namespace mooncake
