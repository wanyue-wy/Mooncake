#pragma once

#include <boost/functional/hash.hpp>

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "client_buffer.hpp"
#include "client_config_builder.h"
#include "master_metric_manager.h"
#include "p2p/client/runtime_config_store.h"
#include "p2p/master/p2p_rpc_types.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

using WriteConfig = std::variant<ReplicateConfig, WriteRouteRequestConfig>;

struct ClientMasterDiscoveryConfig {
    std::string redis_cluster_id = DEFAULT_CLUSTER_ID;
    std::string redis_username;
    std::string redis_password;
    int redis_db_index = 0;
    int redis_master_view_ttl_sec = 4;
    int redis_heartbeat_interval_sec = 1;
};

class QueryResult {
   public:
    const std::vector<Replica::Descriptor> replicas;

    explicit QueryResult(std::vector<Replica::Descriptor>&& replicas_param)
        : replicas(std::move(replicas_param)) {}

    virtual ~QueryResult() = default;

    QueryResult(const QueryResult&) = delete;
    QueryResult& operator=(const QueryResult&) = delete;
    QueryResult(QueryResult&&) = default;
    QueryResult& operator=(QueryResult&&) = default;
};

/**
 * @brief Stable polymorphic client interface shared by all deployment modes.
 */
class ClientService {
   public:
    virtual ~ClientService() = default;

    static std::optional<std::shared_ptr<ClientService>> Create(
        const CentralizedClientConfig& config);
    static std::optional<std::shared_ptr<ClientService>> Create(
        const P2PClientConfig& config);

    virtual void Stop() = 0;
    virtual void StopHeartbeat() = 0;
    virtual void Destroy() = 0;

    virtual DeploymentMode deployment_mode() const = 0;

    virtual tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids) = 0;

    virtual tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    QueryByRegex(const std::string& str) = 0;

    virtual tl::expected<std::unique_ptr<QueryResult>, ErrorCode> Query(
        const std::string& object_key, const ReadRouteConfig& config = {}) = 0;

    virtual std::vector<tl::expected<std::unique_ptr<QueryResult>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys,
               const ReadRouteConfig& config = {}) = 0;

    virtual tl::expected<std::shared_ptr<BufferHandle>, ErrorCode> Get(
        const std::string& key,
        std::shared_ptr<ClientBufferAllocator> allocator,
        const ReadRouteConfig& config = {}) = 0;

    virtual std::vector<tl::expected<std::shared_ptr<BufferHandle>, ErrorCode>>
    BatchGet(const std::vector<std::string>& keys,
             std::shared_ptr<ClientBufferAllocator> allocator,
             const ReadRouteConfig& config = {}) = 0;

    virtual tl::expected<int64_t, ErrorCode> Get(
        const std::string& key, const std::vector<void*>& buffers,
        const std::vector<size_t>& sizes,
        const ReadRouteConfig& config = {}) = 0;

    virtual std::vector<tl::expected<int64_t, ErrorCode>> BatchGet(
        const std::vector<std::string>& keys,
        const std::vector<std::vector<void*>>& all_buffers,
        const std::vector<std::vector<size_t>>& all_sizes,
        const ReadRouteConfig& config = {},
        bool aggregate_same_segment_task = false) = 0;

    virtual tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                              std::vector<Slice>& slices,
                                              const WriteConfig& config) = 0;

    virtual std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const WriteConfig& config) = 0;

    virtual tl::expected<void, ErrorCode> Remove(const ObjectKey& key,
                                                 bool force = false) = 0;
    virtual tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str,
                                                        bool force = false) = 0;
    virtual tl::expected<long, ErrorCode> RemoveAll(bool force = false) = 0;
    virtual tl::expected<long, ErrorCode> RemoveAllLocal() = 0;
    virtual tl::expected<void, ErrorCode> RemoveLocal(const ObjectKey& key) = 0;

    virtual tl::expected<void, ErrorCode> MountSegment(
        const void* buffer, size_t size,
        const std::string& protocol = "tcp") = 0;
    virtual tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                         size_t size) = 0;

    virtual tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true) = 0;
    virtual tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true) = 0;

    virtual tl::expected<bool, ErrorCode> IsExist(const std::string& key) = 0;
    virtual std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys) = 0;

    virtual tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets) = 0;
    virtual tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& source,
        const std::string& target) = 0;
    virtual tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id) = 0;
    virtual tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        size_t batch_size) = 0;
    virtual tl::expected<void, ErrorCode> MarkTaskToComplete(
        const TaskCompleteRequest& task_complete) = 0;

    virtual tl::expected<std::string, ErrorCode> GetSummaryMetrics() = 0;
    virtual tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats() = 0;
    virtual tl::expected<std::string, ErrorCode> SerializeMetrics() = 0;

    virtual uint16_t GetHttpPort() const = 0;
    virtual bool IsHttpServerEnabled() const = 0;
    virtual std::shared_ptr<ClientBufferAllocator> GetBufferAllocator() const = 0;
    virtual std::string GetHealthStatus() const = 0;

    virtual RuntimeConfigStore& getRuntimeConfigStore() = 0;
    virtual const RuntimeConfigStore& getRuntimeConfigStore() const = 0;
    virtual RuntimeConfigStore::WriteConfig getDefaultWriteConfig() const = 0;
    virtual ReadRouteConfig getDefaultReadConfig() const = 0;

    virtual std::string local_endpoint() const = 0;
    virtual std::string GetTransportEndpoint() = 0;
    virtual UUID GetClientID() const = 0;
    virtual ViewVersionId GetViewVersion() const = 0;

    static tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
        const void* addr, size_t length);
    [[nodiscard]] static size_t CalculateSliceSize(
        const std::vector<Slice>& slices);
    [[nodiscard]] static size_t CalculateSliceSize(
        std::span<const Slice> slices);
};

}  // namespace mooncake
