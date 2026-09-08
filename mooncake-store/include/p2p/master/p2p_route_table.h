#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_types.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

/**
 * @brief Unsynchronized in-memory index for one P2P route shard.
 *
 * The caller owns synchronization. The table owns route keys and is the only
 * component allowed to retain string_view references to them. No map or
 * iterator escapes except through a WithdrawHandle, which must be committed
 * while the caller still holds the shard write lock.
 */
class P2PRouteTable final {
   private:
    using RouteMap =
        std::unordered_map<std::string, P2PRouteEntry, StringHash,
                           std::equal_to<>>;

   public:
    struct MutationResult {
        bool created_key{false};
        bool removed_key{false};
    };

    struct CleanupResult {
        size_t removed_routes{0};
        std::vector<std::string> removed_keys;
    };

    class WithdrawHandle {
       public:
        WithdrawHandle(WithdrawHandle&&) = default;
        WithdrawHandle& operator=(WithdrawHandle&&) = default;
        WithdrawHandle(const WithdrawHandle&) = delete;
        WithdrawHandle& operator=(const WithdrawHandle&) = delete;

       private:
        friend class P2PRouteTable;

        WithdrawHandle(
            RouteMap::iterator route,
            std::vector<P2PRouteLocation>::iterator location)
            : route_(route), location_(location) {}

        RouteMap::iterator route_;
        std::vector<P2PRouteLocation>::iterator location_;
    };

    using Mutation = tl::expected<MutationResult, ErrorCode>;

    auto Publish(std::string_view key, uint64_t object_size,
                 const P2PRouteLocation& location,
                 uint64_t max_client_per_key = 0) -> Mutation;

    auto PrepareWithdraw(std::string_view key,
                         const P2PRouteLocation& location)
        -> tl::expected<WithdrawHandle, ErrorCode>;
    MutationResult CommitWithdraw(WithdrawHandle handle);
    auto Withdraw(std::string_view key, const P2PRouteLocation& location)
        -> Mutation;

    bool RouteExists(std::string_view key) const;
    std::optional<P2PRouteEntry> GetRoute(std::string_view key) const;
    std::vector<std::string> ListRouteKeys() const;
    size_t GetRouteKeyCount() const;

    CleanupResult RemoveLocation(const P2PRouteLocation& location);
    bool RemoveKey(std::string_view key);
    size_t Clear();

   private:
    static size_t CountOwnerClients(const P2PRouteEntry& entry);
    void RemoveReverseIndex(std::string_view key,
                            const P2PRouteLocation& location);
    void RemoveAllReverseIndexes(std::string_view key,
                                 const P2PRouteEntry& entry);

    RouteMap routes_;
    std::unordered_map<P2PRouteLocation, std::unordered_set<std::string_view>,
                       P2PRouteLocationHash>
        keys_by_location_;
};

}  // namespace mooncake
