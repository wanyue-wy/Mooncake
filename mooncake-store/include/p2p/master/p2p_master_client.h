#pragma once

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <boost/functional/hash.hpp>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <glog/logging.h>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "client_metric.h"
#include "mutex.h"
#include "p2p/common/p2p_rpc_types.h"
#include "types.h"

namespace mooncake {

template <auto Method>
struct RpcNameTraits;

inline const std::string kDefaultP2PMasterAddress = "localhost:50051";

/**
 * @brief Standalone client for the P2P master RPC service.
 */
class P2PMasterClient final {
   public:
    P2PMasterClient(const UUID& client_id,
                    MasterClientMetric* metrics = nullptr)
        : client_id_(client_id), metrics_(metrics) {
        coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config
            pool_conf{};
        const char* value = std::getenv("MC_RPC_PROTOCOL");
        if (value && std::string_view(value) == "rdma") {
            pool_conf.client_config.socket_config =
                coro_io::ib_socket_t::config_t{};
        }
        client_pools_ =
            std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
                pool_conf);
    }

    ~P2PMasterClient() = default;

    P2PMasterClient(const P2PMasterClient&) = delete;
    P2PMasterClient& operator=(const P2PMasterClient&) = delete;

    [[nodiscard]] ErrorCode Connect(
        const std::string& master_addr = kDefaultP2PMasterAddress);

    void SetHeartbeatRpcPort(uint16_t port) { heartbeat_rpc_port_ = port; }

    [[nodiscard]] tl::expected<bool, ErrorCode> ExistKey(
        std::string_view object_key);

    [[nodiscard]] std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& object_keys);

    [[nodiscard]] tl::expected<P2PGetReplicaListResponse, ErrorCode>
    GetReplicaList(std::string_view key,
                   const P2PGetReplicaListRequestConfig& config =
                       P2PGetReplicaListRequestConfig());

    [[nodiscard]] async_simple::coro::Lazy<
        tl::expected<P2PGetReplicaListResponse, ErrorCode>>
    AsyncGetReplicaList(std::string_view key,
                        const P2PGetReplicaListRequestConfig& config =
                            P2PGetReplicaListRequestConfig());

    [[nodiscard]] std::vector<tl::expected<P2PGetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string_view>& keys,
                        const P2PGetReplicaListRequestConfig& config =
                            P2PGetReplicaListRequestConfig());

    [[nodiscard]] tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    [[nodiscard]] tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    [[nodiscard]] tl::expected<void, ErrorCode> Remove(std::string_view key,
                                                       bool force = false);

    [[nodiscard]] tl::expected<long, ErrorCode> RemoveByRegex(
        std::string_view str, bool force = false);

    [[nodiscard]] tl::expected<long, ErrorCode> RemoveAll(bool force = false);

    [[nodiscard]] tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id);

    [[nodiscard]] tl::expected<P2PQueryClientStatusResponse, ErrorCode>
    QueryClientStatus(const UUID& client_id);

    [[nodiscard]] tl::expected<P2PHeartbeatResponse, ErrorCode> Heartbeat(
        const P2PHeartbeatRequest& req);

    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const P2PSegment& segment);

    [[nodiscard]] tl::expected<P2PRegisterClientResponse, ErrorCode>
    RegisterClient(const P2PRegisterClientRequest& req);

    [[nodiscard]] tl::expected<P2PUnregisterClientResponse, ErrorCode>
    UnregisterClient(const P2PUnregisterClientRequest& req);

    [[nodiscard]] tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);

    /**
     * @brief Batch gets write candidate routes for multiple keys in one RPC
     */
    [[nodiscard]] tl::expected<BatchGetWriteRouteResponse, ErrorCode>
    BatchGetWriteRoute(const BatchGetWriteRouteRequest& req);

    /**
     * @brief Adds a replica to master
     */
    [[nodiscard]] tl::expected<void, ErrorCode> AddReplica(
        const AddReplicaRequest& req);

    /**
     * @brief Removes a replica from master
     */
    [[nodiscard]] tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);

    /**
     * @brief Removes replicas from multiple segments in one call
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchRemoveReplica(
        const BatchRemoveReplicaRequest& req);

    /**
     * @brief Batch sync replicas with mixed ADD and REMOVE ops
     */
    [[nodiscard]] tl::expected<BatchSyncReplicaResponse, ErrorCode>
    BatchSyncReplica(const BatchSyncReplicaRequest& req);

    /**
     * @brief Notify Master that this client has finished syncing metadata
     */
    [[nodiscard]] tl::expected<void, ErrorCode> SetSyncCompleted(
        UUID client_id);

   private:
    // TODO(M8): Re-evaluate extracting the pool access, invoke, batch-failure
    // and RPC-metric plumbing into a stateless shared transport helper after the
    // architecture-specific protocols stabilize. Keep this implementation
    // local for now so centralized MasterClient remains identical to the A00
    // baseline.
    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>>
    invoke_rpc_async(Args&&... args) {
        return invoke_rpc_async_with_pool<ServiceMethod, ReturnType>(
            client_accessor_.GetClientPool(), std::forward<Args>(args)...);
    }

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>>
    invoke_rpc_async_with_pool(
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>> pool,
        Args&&... args) {
        if (metrics_) {
            metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
        }

        auto start_time = std::chrono::steady_clock::now();
        auto ret = co_await pool->send_request(
            [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
                return client.send_request<ServiceMethod>(
                    std::forward<Args>(args)...);
            });
        if (!ret.has_value()) {
            LOG(ERROR) << "Client not available";
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        auto result = co_await std::move(ret.value());
        if (!result) {
            LOG(ERROR) << "RPC call failed: " << result.error().msg;
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        if (metrics_) {
            auto end_time = std::chrono::steady_clock::now();
            auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    end_time - start_time);
            metrics_->rpc_latency.observe({RpcNameTraits<ServiceMethod>::value},
                                          latency.count());
        }
        co_return result->result();
    }

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc(
        Args&&... args) {
        return async_simple::coro::syncAwait(
            invoke_rpc_async<ServiceMethod, ReturnType>(
                std::forward<Args>(args)...));
    }

    template <auto ServiceMethod, typename ResultType, typename... Args>
    [[nodiscard]] std::vector<tl::expected<ResultType, ErrorCode>>
    invoke_batch_rpc(size_t input_size, Args&&... args) {
        auto pool = client_accessor_.GetClientPool();

        if (metrics_) {
            metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
        }

        auto start_time = std::chrono::steady_clock::now();
        return async_simple::coro::syncAwait(
            [&]() -> async_simple::coro::Lazy<
                      std::vector<tl::expected<ResultType, ErrorCode>>> {
                auto ret = co_await pool->send_request(
                    [&](coro_io::client_reuse_hint,
                        coro_rpc::coro_rpc_client& client) {
                        return client.send_request<ServiceMethod>(
                            std::forward<Args>(args)...);
                    });
                if (!ret.has_value()) {
                    LOG(ERROR) << "Client not available";
                    co_return std::vector<tl::expected<ResultType, ErrorCode>>(
                        input_size, tl::make_unexpected(ErrorCode::RPC_FAIL));
                }
                auto result = co_await std::move(ret.value());
                if (!result) {
                    LOG(ERROR)
                        << "Batch RPC call failed: " << result.error().msg;
                    std::vector<tl::expected<ResultType, ErrorCode>>
                        error_results;
                    error_results.reserve(input_size);
                    for (size_t i = 0; i < input_size; ++i) {
                        error_results.emplace_back(
                            tl::make_unexpected(ErrorCode::RPC_FAIL));
                    }
                    co_return error_results;
                }
                if (metrics_) {
                    auto end_time = std::chrono::steady_clock::now();
                    auto latency =
                        std::chrono::duration_cast<std::chrono::microseconds>(
                            end_time - start_time);
                    metrics_->rpc_latency.observe(
                        {RpcNameTraits<ServiceMethod>::value}, latency.count());
                }
                co_return result->result();
            }());
    }

    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard<std::shared_mutex> lock(client_mutex_);
            client_pool_ = client_pool;
        }

        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
        GetClientPool() {
            std::shared_lock<std::shared_mutex> lock(client_mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex client_mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    [[nodiscard]] tl::expected<ReturnType, ErrorCode> invoke_rpc_via(
        RpcClientAccessor& accessor, Args&&... args) {
        return async_simple::coro::syncAwait(
            invoke_rpc_async_with_pool<ServiceMethod, ReturnType>(
                accessor.GetClientPool(), std::forward<Args>(args)...));
    }

    RpcClientAccessor client_accessor_;
    RpcClientAccessor heartbeat_accessor_;
    uint16_t heartbeat_rpc_port_ = 0;

    const UUID client_id_;
    MasterClientMetric* metrics_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>
        client_pools_;

    mutable Mutex connect_mutex_;
    std::string client_addr_param_ GUARDED_BY(connect_mutex_);
};

}  // namespace mooncake
