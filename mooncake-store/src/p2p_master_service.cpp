#include "p2p_master_service.h"

#include <glog/logging.h>

namespace mooncake {

P2PMasterService::P2PMasterService(const MasterServiceConfig& config)
    : MasterService(config) {
    client_manager_ = std::make_shared<P2PClientManager>(
        config.client_live_ttl_sec, config.client_live_ttl_sec * 3,
        config.view_version);
}

auto P2PMasterService::GetReadRoute(const ReadRouteRequest& req)
    -> tl::expected<ReadRouteResponse, ErrorCode> {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

auto P2PMasterService::GetWriteRoute(const WriteRouteRequest& req)
    -> tl::expected<WriteRouteResponse, ErrorCode> {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

auto P2PMasterService::AddReplica(const AddReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

auto P2PMasterService::RemoveReplica(const RemoveReplicaRequest& req)
    -> tl::expected<void, ErrorCode> {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

void P2PMasterService::OnObjectAccessed(ObjectMetadata& metadata) {
    // TODO: wanyue-wy
    // update metrics
}

void P2PMasterService::OnObjectRemoved(ObjectMetadata& metadata) {
    // TODO: wanyue-wy
    // cleanup usage stats
}

void P2PMasterService::OnObjectHit(const ObjectMetadata& metadata) {
    // TODO: wanyue-wy
    // update metrics
}

void P2PMasterService::OnReplicaRemoved(const Replica& replica) {
    // TODO: wanyue-wy
    // update metrics
}

}  // namespace mooncake
