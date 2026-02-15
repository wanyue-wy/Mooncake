#include "p2p_master_service.h"

#include <glog/logging.h>

namespace mooncake {

P2PMasterService::P2PMasterService(const MasterServiceConfig& config)
    : MasterService(config), shard_count_(config.metadata_shard_count) {
    if (shard_count_ == 0) {
        shard_count_ = 1;
    }
    metadata_shards_.reserve(shard_count_);
    for (size_t i = 0; i < shard_count_; ++i) {
        metadata_shards_.push_back(std::make_unique<P2PMetadataShard>());
    }
    client_manager_ = std::make_shared<P2PClientManager>(
        config.disconnect_timeout_sec, config.crash_timeout_sec, view_version_);
}

MasterService::MetadataShard& P2PMasterService::GetShard(size_t idx) {
    return *metadata_shards_[idx];
}

const MasterService::MetadataShard& P2PMasterService::GetShard(
    size_t idx) const {
    return *metadata_shards_[idx];
}

size_t P2PMasterService::getShardIndex(const std::string& key) const {
    return std::hash<std::string>{}(key) % shard_count_;
}

size_t P2PMasterService::GetShardCount() const { return shard_count_; }

void P2PMasterService::OnObjectAccessed(ObjectMetadata& metadata) {
    // TODO: implement LRU or any other replacement policy update
}

void P2PMasterService::OnObjectRemoved(ObjectMetadata& metadata) {
    // TODO: cleanup usage stats
}

void P2PMasterService::OnObjectHit(const ObjectMetadata& metadata) {
    // TODO: update metrics
}

void P2PMasterService::OnReplicaRemoved(const Replica& replica) {
    // TODO: update segment usage
}

}  // namespace mooncake
