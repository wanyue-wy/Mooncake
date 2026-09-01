#pragma once

#include <optional>
#include <string>
#include <vector>

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

}  // namespace mooncake
