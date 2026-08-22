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

#include "master_config.h"
#include "mutex.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
#include "p2p/master/p2p_client_manager.h"
#include "p2p/master/p2p_rpc_types.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

/**
 * @brief Standalone P2P master service.
 *
 * 1. This class serves the master metadata for the P2P architecture. The
 *    generic object/shard bookkeeping formerly provided by the MasterService
 *    base class has been absorbed here; the class no longer participates in
 *    any inheritance hierarchy.
 *
 * 2. The multiple metadata of Master are hierarchical, P2PMasterService
 *    manages metadata of key (ObjectMetadata), and its P2PClientManager
 *    manages metadata of client (P2PClientMeta), each P2PClientMeta uses
 *    P2PSegmentManager to manage its segments. The relationship of metadata:
 *    a. Client (1) —— (0..*) Segment
 *    b. Key (1) —— (1..*) Replica
 *    c. Replica (1) —— (1) Segment
 *
 * 3. The lock order is:
 *    a. MetadataShard's mutex
 *    b. P2PClientManager's client_mutex_
 *    c. P2PSegmentManager's segment_mutex_
 *    For avoiding deadlock, each metadata managers should follow this order.
 */
class P2PMasterService {
   public:
    explicit P2PMasterService(const MasterServiceConfig& config);
    ~P2PMasterService() = default;

    P2PClientManager& GetClientManager() { return *client_manager_; }
    const P2PClientManager& GetClientManager() const {
        return *client_manager_;
    }

    /**
     * @brief Register a client with its segments.
     */
    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;

    /**
     * @brief Unregister a client, removing all its routing metadata
     */
    auto UnregisterClient(const UnregisterClientRequest& req)
        -> tl::expected<UnregisterClientResponse, ErrorCode>;

    /**
     * @brief heartbeat interface for client to sync its status
     * @param req HeartbeatRequest containing client_id and tasks
     * @return HeartbeatResponse containing client status, view_version,
     *         and task results
     */
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;

    /**
     * @brief Queries the status of a client
     */
    auto QueryClientStatus(const QueryClientStatusRequest& req)
        -> tl::expected<QueryClientStatusResponse, ErrorCode>;

    /**
     * @brief Mount a memory segment.
     * @return ErrorCode::SEGMENT_ALREADY_EXISTS if it is already mounted.
     *         ErrorCode::CLIENT_UNHEALTHY if the client is unhealthy.
     */
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Unmount a memory segment.
     * @return ErrorCode::OK on success,
     *         ErrorCode::SEGMENT_NOT_FOUND if the segment doesn't exist
     *         ErrorCode::CLIENT_UNHEALTHY if the client is unhealthy
     */
    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Check if an object exists
     * @return ErrorCode::OK if exists, otherwise return other ErrorCode
     */
    auto ExistKey(std::string_view key) -> tl::expected<bool, ErrorCode>;

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);

    /**
     * @brief Fetch all keys
     * @return ErrorCode::OK if exists
     */
    auto GetAllKeys() -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Fetch all segments, each node has a unique real client with fixed
     * segment name : segment name, preferred format : {ip}:{port}, bad format :
     * localhost:{port}
     * @return ErrorCode::OK if exists
     */
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Get all segments belonging to a specific client.
     * @param client_id The UUID of the client.
     * @return An expected object containing a vector of segment names on
     * success, or ErrorCode on failure.
     */
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Query a segment's capacity and used size in bytes.
     * Conductor should use these information to schedule new requests.
     * @return ErrorCode::OK if exists
     */
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;

    /**
     * @brief Query IP addresses for a given client ID.
     * @param client_id The UUID of the client to query.
     * @return An expected object containing a vector of IP addresses on success
     * (empty vector if client has no IPs), or ErrorCode::CLIENT_NOT_FOUND if
     * the client doesn't exist, or another ErrorCode on other failures.
     */
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure. Non-existent
     * clients are omitted from the result map. Clients that exist but have no
     * IPs are included with empty vectors.
     */
    auto BatchQueryIp(const std::vector<UUID>& client_ids) -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>;

    /**
     * @brief Retrieves replica lists for object keys that match a regex
     * pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     *         replica descriptors on success, or an ErrorCode on failure.
     */
    auto GetReplicaListByRegex(const std::string& regex_pattern)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;

    /**
     * @brief Get list of replicas for an object
     * @param key The key of the object
     * @param config The filter configuration for the replica list
     * @return An expected object containing the replica list on success, or an
     * ErrorCode on failure.
     */
    auto GetReplicaList(std::string_view key,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig())
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    /**
     * @brief Remove an object and its replicas
     * @param key The key to remove
     * @param force If true, skip lease and replication task checks.
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    auto Remove(std::string_view key, bool force = false)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks.
     * @return An expected object containing the number of removed objects on
     * success, or ErrorCode on failure.
     */
    auto RemoveByRegex(std::string_view str, bool force = false)
        -> tl::expected<long, ErrorCode>;

    /**
     * @brief Remove all objects and their replicas
     * @param force If true, skip lease and replication task checks.
     * @return return the number of objects removed
     */
    long RemoveAll(bool force = false);

    /**
     * @brief Get the count of keys
     * @return The count of keys
     */
    size_t GetKeyCount() const;

    OpLogManager* GetOpLogManager() const { return oplog_manager_.get(); }

    /**
     * @brief Get write route based on the config in the request
     */
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

    std::vector<Replica::Descriptor> FilterReplicas(
        const GetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata);

   protected:
    struct ObjectMetadata {
       public:
        ~ObjectMetadata();

        ObjectMetadata(size_t value_length, std::vector<Replica>&& reps);
        ObjectMetadata() = delete;

        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        // Check if the metadata is valid
        // Valid means it has at least one replica and size is greater than 0
        bool IsValid() const { return CountReplicas() > 0 && size_ > 0; }

        void AddReplicas(std::vector<Replica>&& replicas) {
            replicas_.insert(replicas_.end(),
                             std::move_iterator(replicas.begin()),
                             std::move_iterator(replicas.end()));
        }

        std::vector<Replica> PopReplicas(
            const std::function<bool(const Replica&)>& pred_fn) {
            auto partition_point =
                std::partition(replicas_.begin(), replicas_.end(),
                               [&pred_fn](const Replica& replica) {
                                   return !pred_fn(replica);
                               });

            std::vector<Replica> popped_replicas;
            if (partition_point != replicas_.end()) {
                popped_replicas.reserve(
                    std::distance(partition_point, replicas_.end()));
                std::move(partition_point, replicas_.end(),
                          std::back_inserter(popped_replicas));
                replicas_.erase(partition_point, replicas_.end());
            }
            return popped_replicas;
        }

        std::vector<Replica> PopReplicas() { return std::move(replicas_); }

        size_t EraseReplicas(
            const std::function<bool(const Replica&)>& pred_fn) {
            auto erased_replicas = PopReplicas(pred_fn);
            return erased_replicas.size();
        }

        size_t EraseReplicas() {
            auto erased_replicas = PopReplicas();
            return erased_replicas.size();
        }

        size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                             const std::function<void(Replica&)>& visit_fn) {
            size_t num_visited = 0;

            for (auto& replica : replicas_) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    ++num_visited;
                }
            }
            return num_visited;
        }

        size_t VisitReplicas(
            const std::function<bool(const Replica&)>& pred_fn,
            const std::function<void(const Replica&)>& visit_fn) const {
            size_t num_visited = 0;

            for (const auto& replica : replicas_) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    ++num_visited;
                }
            }
            return num_visited;
        }

        bool HasReplica(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
        }

        bool AllReplicas(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::all_of(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas() const { return replicas_.size(); }

        Replica* GetFirstReplica(
            const std::function<bool(const Replica&)>& pred_fn) {
            const auto it =
                std::find_if(replicas_.begin(), replicas_.end(), pred_fn);
            return it != replicas_.end() ? &(*it) : nullptr;
        }

        Replica* GetReplicaByID(const ReplicaID& id) {
            return GetFirstReplica(
                [&id](const Replica& replica) { return replica.id() == id; });
        }

        bool EraseReplicaByID(const ReplicaID& id) {
            auto num_erased = EraseReplicas(
                [&id](const Replica& replica) { return replica.id() == id; });
            return num_erased > 0;
        }

        Replica* GetReplicaBySegmentName(const std::string& segment_name) {
            return GetFirstReplica([&segment_name](const Replica& replica) {
                auto names = replica.get_segment_names();
                for (auto& name_opt : names) {
                    if (name_opt == segment_name) {
                        return true;
                    }
                }
                return false;
            });
        }

       public:
        // P2P keeps the generic status semantics of the former base
        // ObjectMetadata (no P2P-specific subclass ever existed).

        /**
         * @brief Whether the object is readable
         * @return true if the object is readable, false otherwise
         */
        bool IsObjectAccessible() const {
            return HasReplica(&Replica::fn_is_completed);
        }

        /**
         * @brief Whether the object is removable
         * @return ErrorCode::OK if removable, otherwise return error specific
         * to the reason
         */
        tl::expected<void, ErrorCode> IsObjectRemovable(
            bool force = false) const {
            return {};
        }

        /**
         * @brief Whether the replica is readable
         * @return true if the replica is readable, false otherwise
         */
        bool IsReplicaAccessible(const Replica& replica) const { return true; };

        /**
         * @brief Whether the replica is removable
         * @return ErrorCode::OK if removable, otherwise return error specific
         * to the reason
         */
        tl::expected<void, ErrorCode> IsReplicaRemovable(
            const Replica& replica) const {
            return {};
        }

       public:
        std::vector<Replica> replicas_;
        size_t size_;
    };

   protected:
    // Sharded metadata maps and their mutexes.
    // Attention:
    // `segment_key_index` is a reverse index for `metadata` and segment.
    // Due to the object key in `segment_key_index` is a string_view acquired
    // from `metadata`, when removing an entry from `metadata`, you MUST
    // first remove the corresponding key from `segment_key_index`.
    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                           StringHash, std::equal_to<>>
            metadata GUARDED_BY(mutex);

        // segment_id -> { key -> replica_reference_count }.
        std::unordered_map<UUID, std::unordered_map<std::string_view, size_t>,
                           boost::hash<UUID>>
            segment_key_index GUARDED_BY(mutex);
    };

    // For accessing a metadata shard with exclusive (read-write) lock
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

    // For accessing a metadata shard with shared (read-only) lock
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

    // Helpers for maintaining per-shard segment_key_index.
    // 1. Must be called while holding shard.mutex.
    // 2. When add or remove a replica, must call the following functions to
    //    update the segment_key_index.
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

    // Shard access (formerly virtual overrides of the MasterService base).
    MetadataShard& GetShard(size_t idx) { return metadata_shards_[idx]; }
    const MetadataShard& GetShard(size_t idx) const {
        return metadata_shards_[idx];
    }
    static constexpr size_t kNumShards = 1024;  // Number of metadata shards
    // Helper to get shard index from key
    size_t GetShardIndex(std::string_view key) const {
        return std::hash<std::string_view>{}(key) % kNumShards;
    }
    size_t GetShardCount() const { return kNumShards; }

   protected:
    // Helper class for accessing metadata with automatic locking
    class MetadataAccessorRW {
       public:
        MetadataAccessorRW(P2PMasterService* service, std::string_view key)
            : service_(service),
              shard_idx_(service_->GetShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)) {}

        ~MetadataAccessorRW() = default;

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return *it_->second; }

        MetadataShardAccessorRW& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        // Delete current metadata.
        // To prevent dangling string_views in segment_key_index, segment index
        // should be cleaned up before erasing the metadata entry.
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            if (it_ != shard_guard_->metadata.end()) {
                service_->RemoveReplicaFromSegmentIndex(
                    shard_guard_.GetRef(), it_->first, it_->second->replicas_);
                shard_guard_->metadata.erase(it_);
                it_ = shard_guard_->metadata.end();
            }
        }

       protected:
        P2PMasterService* service_;
        size_t shard_idx_;
        MetadataShardAccessorRW shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::iterator it_;
    };

    // Key-level read-only accessor
    class MetadataAccessorRO {
       public:
        MetadataAccessorRO(const P2PMasterService* service,
                           std::string_view key)
            : service_(service),
              shard_idx_(service_->GetShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)) {}

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        // Get metadata (only call when Exists() is true)
        const ObjectMetadata& Get() const NO_THREAD_SAFETY_ANALYSIS {
            return *it_->second;
        }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

       private:
        const P2PMasterService* service_;
        const size_t shard_idx_;
        MetadataShardAccessorRO shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::const_iterator it_;
    };

   protected:
    // Hooks for metadata lifecycle events (formerly virtual; P2P
    // implementations internalized).

    // Triggered when the metadata of an object is accessed (e.g. Get or Exist)
    void OnObjectAccessed(const ObjectMetadata& metadata);

    // Triggered when the object is removed
    void OnObjectRemoved(ObjectMetadata& metadata);

    // Triggered when the object is hit (e.g. Get)
    void OnObjectHit(const ObjectMetadata& metadata);

    // Triggered when the replica is removed
    void OnReplicaRemoved(const Replica& replica);

    // Triggered when the replica is added
    void OnReplicaAdded(const Replica& replica);

    // Callback for segment removal (triggered by P2PClientManager via
    // P2PSegmentManager)
    void OnSegmentRemoved(const UUID& segment_id);

    // Wires the segment-removal callback into the client manager.
    void InitializeClientManager();

   private:
    using OwnerClientSet = std::unordered_set<UUID, boost::hash<UUID>>;

    static auto CollectReplicaOwnerClients(const ObjectMetadata& metadata,
                                           std::string_view key)
        -> tl::expected<OwnerClientSet, ErrorCode>;

    tl::expected<void, ErrorCode> InnerAddReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id, size_t size,
        const std::shared_ptr<P2PClientMeta>& client) NO_THREAD_SAFETY_ANALYSIS;
    tl::expected<void, ErrorCode> InnerRemoveReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id) NO_THREAD_SAFETY_ANALYSIS;

    std::shared_ptr<P2PClientManager> client_manager_;
    std::array<MetadataShard, kNumShards> metadata_shards_;
    // for the number of clients owning a key:
    // 1. max_client_per_key_ == 0 means no limitation
    // 2. max_client_per_key_ > 0 means the max client owner count
    uint64_t max_client_per_key_;
    bool enable_async_oplog_write_{false};

    // Absorbed from the former MasterService base.
    ViewVersionId view_version_;
    std::unique_ptr<OpLogManager> oplog_manager_;

    friend class MetadataAccessorRW;
    friend class MetadataAccessorRO;
};

}  // namespace mooncake
