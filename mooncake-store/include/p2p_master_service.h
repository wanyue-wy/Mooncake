#pragma once

#include "master_service.h"
#include "p2p_client_manager.h"

namespace mooncake {

class P2PMasterService : public MasterService {
   public:
    explicit P2PMasterService(const MasterServiceConfig& config);
    ~P2PMasterService() override = default;

    ClientManager& GetClientManager() override { return *client_manager_; }
    const ClientManager& GetClientManager() const override {
        return *client_manager_;
    }

   protected:
    // Metadata implementation
    MetadataShard& GetShard(size_t idx) override;
    const MetadataShard& GetShard(size_t idx) const override;
    size_t getShardIndex(const std::string& key) const override;
    size_t GetShardCount() const override;

    // Hooks
    void OnObjectAccessed(ObjectMetadata& metadata) override;
    void OnObjectRemoved(ObjectMetadata& metadata) override;
    void OnObjectHit(const ObjectMetadata& metadata) override;
    void OnReplicaRemoved(const Replica& replica) override;

   private:
    struct P2PMetadataShard : public MetadataShard {};

    std::shared_ptr<P2PClientManager> client_manager_;
    std::vector<std::unique_ptr<P2PMetadataShard>> metadata_shards_;
    size_t shard_count_;
};

}  // namespace mooncake
