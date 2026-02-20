#pragma once

#include <string>
#include <vector>

#include "replica.h"
#include "types.h"
#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

/**
 * @brief Request config for read route
 */
struct ReadRouteRequestConfig {
    size_t max_candidates = 3;
    std::vector<std::string> tag_filters;
    int priority = 0;
};
YLT_REFL(ReadRouteRequestConfig, max_candidates, tag_filters, priority);

/**
 * @brief Request structure for getting read route.
 */
struct ReadRouteRequest {
    std::string key;
    ReadRouteRequestConfig config;
};
YLT_REFL(ReadRouteRequest, key, config);

/**
 * @brief Response structure for getting read route.
 */
struct ReadRouteResponse {
    std::string key;
    std::vector<P2PProxyDescriptor> replicas;
};
YLT_REFL(ReadRouteResponse, key, replicas);

/**
 * @brief Request config for write route
 */
struct WriteRouteRequestConfig {
    size_t max_candidates = 1;
    std::vector<std::string> tag_filters;
    int priority = 0;
    bool allow_local = false;
    bool prefer_local = false;  // works only when allow_local==true
};
YLT_REFL(WriteRouteRequestConfig, max_candidates, tag_filters, priority,
         allow_local, prefer_local);

/**
 * @brief Request structure for getting write route.
 */
struct WriteRouteRequest {
    std::string key;
    UUID client_id;
    size_t size = 0;
    WriteRouteRequestConfig config;
};
YLT_REFL(WriteRouteRequest, key, client_id, size, config);

/**
 * @brief Candidate node for writing
 */
struct WriteCandidate {
    P2PProxyDescriptor replica;
    size_t available_capacity = 0;
};
YLT_REFL(WriteCandidate, replica, available_capacity);

/**
 * @brief Response structure for getting write route.
 */
struct WriteRouteResponse {
    std::vector<WriteCandidate> candidates;
};
YLT_REFL(WriteRouteResponse, candidates);

/**
 * @brief Request to add a replica
 */
struct AddReplicaRequest {
    std::string key;
    Replica::Descriptor replica;
};
YLT_REFL(AddReplicaRequest, key, replica);

/**
 * @brief Request to remove a replica
 */
struct RemoveReplicaRequest {
    std::string key;
    Replica::Descriptor replica;
};
YLT_REFL(RemoveReplicaRequest, key, replica);

}  // namespace mooncake
