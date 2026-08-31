#pragma once

#include <algorithm>
#include <array>
#include <boost/functional/hash.hpp>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/common/p2p_master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
#include "p2p/master/p2p_client_manager.h"
#include "replica.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

/**
 * @brief Standalone P2P master service.
 *
 * P2PMasterService owns P2P object metadata and delegates client and segment
 * metadata to P2PClientManager.
 *
 * Lock order:
 * 1. MetadataShard::mutex
 * 2. P2PClientManager::clients_mutex_
 * 3. P2PSegmentManager::segments_mutex_
 */
class P2PMasterService {
   protected:
    struct ObjectMetadata;

   public:
    explicit P2PMasterService(const P2PMasterServiceConfig& config);
    ~P2PMasterService() = default;

    P2PClientManager& GetClientManager() { return *client_manager_; }
    const P2PClientManager& GetClientManager() const {
        return *client_manager_;
    }

    auto RegisterClient(const P2PRegisterClientRequest& req)
        -> tl::expected<P2PRegisterClientResponse, ErrorCode>;
    auto UnregisterClient(const P2PUnregisterClientRequest& req)
        -> tl::expected<P2PUnregisterClientResponse, ErrorCode>;
    auto Heartbeat(const P2PHeartbeatRequest& req)
        -> tl::expected<P2PHeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const P2PQueryClientStatusRequest& req)
        -> tl::expected<P2PQueryClientStatusResponse, ErrorCode>;

    auto MountSegment(const P2PSegment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    auto ExistKey(std::string_view key) -> tl::expected<bool, ErrorCode>;
    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);
    auto GetAllKeys() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto BatchQueryIp(const std::vector<UUID>& client_ids) -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>;

    auto GetReplicaListByRegex(const std::string& regex_pattern)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;
    auto GetReplicaList(std::string_view key,
                        const P2PGetReplicaListRequestConfig& config =
                            P2PGetReplicaListRequestConfig())
        -> tl::expected<P2PGetReplicaListResponse, ErrorCode>;

    auto Remove(std::string_view key, bool force = false)
        -> tl::expected<void, ErrorCode>;
    auto RemoveByRegex(std::string_view regex_pattern, bool force = false)
        -> tl::expected<long, ErrorCode>;
    long RemoveAll(bool force = false);
    size_t GetKeyCount() const;

    OpLogManager* GetOpLogManager() const { return oplog_manager_.get(); }

    auto GetWriteRoute(const WriteRouteRequest& req)
        -> tl::expected<WriteRouteResponse, ErrorCode>;

    /**
     * @brief Batch get write routes for multiple keys.
     *        Reuses GetWriteRoute logic per key.
     */
    auto BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
        -> BatchGetWriteRouteResponse;

    /**
     * @brief Add a route replica to master
     */
    auto AddReplica(const AddReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Remove a route replica from master
     */
    auto RemoveReplica(const RemoveReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Remove replicas from multiple segments in one call
     */
    auto BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
        -> std::vector<tl::expected<void, ErrorCode>>;

    /**
     * @brief Batch sync replicas with mixed ADD and REMOVE ops
     */
    auto BatchSyncReplica(const BatchSyncReplicaRequest& req)
        -> BatchSyncReplicaResponse;

    /**
     * @brief Client notifies Master that metadata sync is complete
     */
    auto SetSyncCompleted(UUID client_id) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Restore P2P metadata exported by P2PHotStandbyService promotion.
     *
     * The target service must be empty. Restore registers clients/segments and
     * rebuilds object metadata plus segment reverse indexes without recording
     * new OpLog entries. If last_applied_sequence_id is provided, the target
     * OpLogManager starts future writes after that sequence.
     */
    ErrorCode RestoreFromStandbyMetadata(
        const P2PStandbyMetadataStore::ExportedMetadata& metadata,
        uint64_t last_applied_sequence_id = 0);

    ErrorCode RecordOplog(OpType type, const std::string& key,
                          const std::string& payload = std::string());

   protected:
    struct ObjectMetadata {
       public:
        ~ObjectMetadata();

        ObjectMetadata(size_t value_length, std::vector<Replica>&& replicas);
        ObjectMetadata() = delete;
        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        bool IsValid() const { return CountReplicas() > 0 && size_ > 0; }

        bool HasReplica(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas() const { return replicas_.size(); }

        bool IsObjectAccessible() const {
            return HasReplica(&Replica::fn_is_completed);
        }

        std::vector<Replica> replicas_;
        size_t size_;
    };

    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                           StringHash, std::equal_to<>>
            metadata GUARDED_BY(mutex);

        std::unordered_map<UUID, std::unordered_map<std::string_view, size_t>,
                           boost::hash<UUID>>
            segment_key_index GUARDED_BY(mutex);
    };

    class MetadataShardAccessorRW {
       public:
        MetadataShardAccessorRW(P2PMasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->GetShard(shard_index)),
              lock_(&shard_.mutex) {}

        MetadataShard* operator->() { return &shard_; }
        const MetadataShard* operator->() const { return &shard_; }
        MetadataShard& GetRef() NO_THREAD_SAFETY_ANALYSIS { return shard_; }

       private:
        MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    class MetadataShardAccessorRO {
       public:
        MetadataShardAccessorRO(const P2PMasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->GetShard(shard_index)),
              lock_(&shard_.mutex, shared_lock) {}

        const MetadataShard* operator->() const { return &shard_; }

       private:
        const MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    void AddReplicaToSegmentIndex(MetadataShard& shard, const std::string& key,
                                  const Replica& replica)
        NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(
        MetadataShard& shard, const std::string& key,
        const std::vector<Replica>& replicas) NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(MetadataShard& shard,
                                       const std::string& key,
                                       const Replica& replica)
        NO_THREAD_SAFETY_ANALYSIS;

    MetadataShard& GetShard(size_t idx) { return metadata_shards_[idx]; }
    const MetadataShard& GetShard(size_t idx) const {
        return metadata_shards_[idx];
    }

    static constexpr size_t kNumShards = 1024;

    size_t GetShardIndex(std::string_view key) const {
        return std::hash<std::string_view>{}(key) % kNumShards;
    }

    size_t GetShardCount() const { return kNumShards; }

    class MetadataAccessorRW {
       public:
        MetadataAccessorRW(P2PMasterService* service, std::string_view key)
            : service_(service),
              shard_guard_(service_, service_->GetShardIndex(key)),
              it_(shard_guard_->metadata.find(key)) {}

        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return *it_->second; }

        MetadataShardAccessorRW& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            if (it_ == shard_guard_->metadata.end()) {
                return;
            }
            service_->RemoveReplicaFromSegmentIndex(
                shard_guard_.GetRef(), it_->first, it_->second->replicas_);
            shard_guard_->metadata.erase(it_);
            it_ = shard_guard_->metadata.end();
        }

       private:
        P2PMasterService* service_;
        MetadataShardAccessorRW shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::iterator it_;
    };

    class MetadataAccessorRO {
       public:
        MetadataAccessorRO(const P2PMasterService* service,
                           std::string_view key)
            : shard_guard_(service, service->GetShardIndex(key)),
              it_(shard_guard_->metadata.find(key)) {}

        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        const ObjectMetadata& Get() const NO_THREAD_SAFETY_ANALYSIS {
            return *it_->second;
        }

       private:
        MetadataShardAccessorRO shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::const_iterator it_;
    };

    void OnSegmentRemoved(const UUID& segment_id);
    void InitializeClientManager();

   private:
    using OwnerClientSet = std::unordered_set<UUID, boost::hash<UUID>>;

    static auto CollectReplicaOwnerClients(const ObjectMetadata& metadata,
                                           std::string_view key)
        -> tl::expected<OwnerClientSet, ErrorCode>;

    std::vector<Replica::Descriptor> FilterReplicas(
        const P2PGetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata);

    tl::expected<void, ErrorCode> InnerAddReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id, size_t size,
        const std::shared_ptr<P2PClientMeta>& client) NO_THREAD_SAFETY_ANALYSIS;
    tl::expected<void, ErrorCode> InnerRemoveReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id) NO_THREAD_SAFETY_ANALYSIS;

    std::shared_ptr<P2PClientManager> client_manager_;
    std::array<MetadataShard, kNumShards> metadata_shards_;
    uint64_t max_client_per_key_;
    bool enable_async_oplog_write_{false};
    ViewVersionId view_version_;
    std::unique_ptr<OpLogManager> oplog_manager_;

    friend class MetadataAccessorRW;
    friend class MetadataAccessorRO;
};

}  // namespace mooncake
