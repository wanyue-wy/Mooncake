#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "client_manager.h"
#include "tiered_cache/cache_tier.h"
#include "types.h"

namespace mooncake {

// =====================================================================
// P2P Client / Segment Metadata
// =====================================================================

/**
 * @brief P2P client metadata, extends ClientMeta with network endpoint info.
 */
struct P2PClientMeta : ClientMeta {
    std::string ip_address;
    uint16_t rpc_port = 0;
};

/**
 * @brief Segment metadata for P2P architecture.
 */
struct P2PSegmentMetadata {
    UUID segment_id;
    UUID owner_client_id;
    MemoryType type = MemoryType::UNKNOWN;
    size_t capacity = 0;
    size_t usage = 0;
    int priority = 0;
    std::vector<std::string> tags;
};

// =====================================================================
// Read Route Types
// =====================================================================

/**
 * @brief Config for read route requests (per-call).
 */
struct ReadRouteRequestConfig {
    uint32_t max_replicas = 0;  // 0 = return all
    std::optional<MemoryType> storage_filter;
    std::optional<std::vector<std::string>> tag_filters;
};

/**
 * @brief A single read route result.
 */
struct ReplicaRoute {
    UUID client_id;
    std::string ip_address;
    uint16_t rpc_port = 0;
    MemoryType storage_type = MemoryType::UNKNOWN;
    int priority = 0;
};

// =====================================================================
// Write Route Types
// =====================================================================

/**
 * @brief Config for write route requests (per-call).
 */
struct WriteRouteRequestConfig {
    uint32_t max_candidates = 0;  // 0 = no limit
    std::optional<MemoryType> storage_filter;
    std::optional<std::vector<std::string>> tag_filters;
    bool exclude_requester = false;
    bool allow_same_client_diff_segment = false;
};

/**
 * @brief A single write route candidate.
 */
struct WriteCandidate {
    UUID client_id;
    std::string ip_address;
    uint16_t rpc_port = 0;
    UUID segment_id;
    MemoryType storage_type = MemoryType::UNKNOWN;
    size_t available_capacity = 0;
    int priority = 0;
};

/**
 * @brief Response for write route requests.
 */
struct WriteRouteResponse {
    std::vector<WriteCandidate> candidates;
    std::vector<ErrorCode> key_check_results;  // 1:1 with writing_keys
};

// =====================================================================
// Replica Management Strategies
// =====================================================================

/**
 * @brief Strategy for managing replicas.
 * Configured at master startup, not modifiable at runtime.
 */
enum class ReplicaStrategy {
    FIXED_COUNT,  // Fixed max replicas per key
    UNLIMITED,    // No limit on replica count
    AUTO,         // Load-balanced (TODO)
};

}  // namespace mooncake
