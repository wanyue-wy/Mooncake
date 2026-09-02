#pragma once

#include <optional>
#include <string>
#include <vector>

#include <boost/functional/hash.hpp>

#include "types.h"

namespace mooncake {

static constexpr int64_t DEFAULT_CLIENT_CRASHED_TTL_SEC = 30;

/**
 * @brief Client health state owned by the P2P master.
 */
enum class P2PClientStatus {
    UNDEFINED = 0,
    HEALTH,
    DISCONNECTION,
    CRASHED,
};

inline std::ostream& operator<<(std::ostream& os,
                                P2PClientStatus status) noexcept {
    switch (status) {
        case P2PClientStatus::HEALTH:
            return os << "HEALTH";
        case P2PClientStatus::DISCONNECTION:
            return os << "DISCONNECTION";
        case P2PClientStatus::CRASHED:
            return os << "CRASHED";
        default:
            return os << "UNDEFINED";
    }
}

struct P2PReadRouteConfigExtra {
    std::vector<std::string> tag_filters;
    int priority_limit = 0;
};
YLT_REFL(P2PReadRouteConfigExtra, tag_filters, priority_limit);

/**
 * @brief Temporary unified-facade read selection config.
 *
 * This is not a master RPC DTO. The centralized baseline API has no
 * read-route config parameter.
 *
 * TODO(C4 / external interface): After C4 separates the concrete
 * ClientService business APIs, rename this to P2PReadRouteConfig and remove it
 * from all centralized Query/Get/Batch methods. The external-interface phase
 * must then update RealClient/PyClient/DummyClient deployment-mode dispatch
 * and delete the shared ReadRouteConfig name.
 */
struct ReadRouteConfig {
    static constexpr size_t RETURN_ALL_CANDIDATES = 0;

    ReadRouteConfig() = default;
    explicit ReadRouteConfig(size_t max_c) : max_candidates(max_c) {}

    size_t max_candidates = RETURN_ALL_CANDIDATES;
    std::optional<P2PReadRouteConfigExtra> p2p_config;
};
YLT_REFL(ReadRouteConfig, max_candidates, p2p_config);

/**
 * @brief Describes a storage segment managed by a P2P client.
 */
struct P2PSegment {
    UUID id{0, 0};
    std::string name;
    size_t size{0};
    int priority{0};
    std::vector<std::string> tags;
    MemoryType memory_type{MemoryType::DRAM};
    size_t usage{0};
};
YLT_REFL(P2PSegment, id, name, size, priority, tags, memory_type, usage);

/**
 * @brief Stable identity of a P2P route inside the master.
 *
 * Segment IDs are client-local. The pair, rather than segment_id alone, is
 * therefore the only valid identity for indexing and cleanup.
 */
struct P2PRouteLocation {
    UUID client_id{0, 0};
    UUID segment_id{0, 0};

    bool operator==(const P2PRouteLocation&) const = default;
};
YLT_REFL(P2PRouteLocation, client_id, segment_id);

struct P2PRouteLocationHash {
    size_t operator()(const P2PRouteLocation& location) const noexcept {
        size_t seed = 0;
        boost::hash_combine(seed, boost::hash<UUID>{}(location.client_id));
        boost::hash_combine(seed, boost::hash<UUID>{}(location.segment_id));
        return seed;
    }
};

struct P2PRouteEntry {
    uint64_t object_size{0};
    std::vector<P2PRouteLocation> locations;
};
YLT_REFL(P2PRouteEntry, object_size, locations);

}  // namespace mooncake
