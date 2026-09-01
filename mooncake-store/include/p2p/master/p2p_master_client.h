#pragma once

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "client_metric.h"
#include "mutex.h"
#include "p2p/common/p2p_rpc_types.h"

namespace mooncake {

template <auto Method>
struct RpcNameTraits;

inline const std::string kDefaultP2PMasterAddress = "localhost:50051";

/** @brief Client for the standalone owning P2P master protocol. */
class P2PMasterClient final {
   public:
    explicit P2PMasterClient(const UUID& client_id,
                             MasterClientMetric* metrics = nullptr);

    P2PMasterClient(const P2PMasterClient&) = delete;
    P2PMasterClient& operator=(const P2PMasterClient&) = delete;

    ErrorCode Connect(
        const std::string& master_addr = kDefaultP2PMasterAddress);
    void SetHeartbeatRpcPort(uint16_t port) { heartbeat_rpc_port_ = port; }

    auto RegisterClient(const P2PRegisterClientRequest& request)
        -> tl::expected<P2PRegisterClientResponse, ErrorCode>;
    auto UnregisterClient(const P2PUnregisterClientRequest& request)
        -> tl::expected<P2PUnregisterClientResponse, ErrorCode>;
    auto Heartbeat(const P2PHeartbeatRequest& request)
        -> tl::expected<P2PHeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const UUID& client_id)
        -> tl::expected<P2PQueryClientStatusResponse, ErrorCode>;

    auto MountSegment(const P2PSegment& segment)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;

    auto RouteExists(std::string_view key)
        -> tl::expected<bool, ErrorCode>;
    auto BatchRouteExists(const std::vector<std::string_view>& keys)
        -> std::vector<tl::expected<bool, ErrorCode>>;
    auto GetReadRoute(std::string_view key, const P2PReadRouteConfig& config)
        -> tl::expected<P2PGetReadRouteResponse, ErrorCode>;
    auto AsyncGetReadRoute(std::string_view key,
                           const P2PReadRouteConfig& config)
        -> async_simple::coro::Lazy<
            tl::expected<P2PGetReadRouteResponse, ErrorCode>>;
    auto BatchGetReadRoute(const std::vector<std::string_view>& keys,
                           const P2PReadRouteConfig& config)
        -> std::vector<tl::expected<P2PGetReadRouteResponse, ErrorCode>>;

    auto GetWriteRoute(const P2PGetWriteRouteRequest& request)
        -> tl::expected<P2PGetWriteRouteResponse, ErrorCode>;
    auto BatchGetWriteRoute(const P2PBatchGetWriteRouteRequest& request)
        -> tl::expected<P2PBatchGetWriteRouteResponse, ErrorCode>;
    auto PublishRoute(const P2PPublishRouteRequest& request)
        -> tl::expected<void, ErrorCode>;
    auto WithdrawRoute(const P2PWithdrawRouteRequest& request)
        -> tl::expected<void, ErrorCode>;
    auto BatchWithdrawRoute(const P2PBatchWithdrawRouteRequest& request)
        -> std::vector<tl::expected<void, ErrorCode>>;
    auto BatchSyncRoutes(const P2PBatchSyncRoutesRequest& request)
        -> tl::expected<P2PBatchSyncRoutesResponse, ErrorCode>;
    auto CompleteRouteSync(const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

   private:
    class RpcClientAccessor {
       public:
        void SetClientPool(
            std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
                client_pool) {
            std::lock_guard lock(mutex_);
            client_pool_ = std::move(client_pool);
        }

        auto GetClientPool() const
            -> std::shared_ptr<
                coro_io::client_pool<coro_rpc::coro_rpc_client>> {
            std::shared_lock lock(mutex_);
            return client_pool_;
        }

       private:
        mutable std::shared_mutex mutex_;
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
            client_pool_;
    };

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    auto InvokeAsyncWithPool(
        std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>> pool,
        Args&&... args)
        -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
        if (!pool) {
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        if (metrics_) {
            metrics_->rpc_count.inc({RpcNameTraits<ServiceMethod>::value});
        }
        const auto start = std::chrono::steady_clock::now();
        auto pending = co_await pool->send_request(
            [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
                return client.send_request<ServiceMethod>(
                    std::forward<Args>(args)...);
            });
        if (!pending.has_value()) {
            LOG(ERROR) << "P2P master RPC client is unavailable";
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        auto result = co_await std::move(*pending);
        if (!result) {
            LOG(ERROR) << "P2P master RPC failed: " << result.error().msg;
            co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
        }
        if (metrics_) {
            const auto latency =
                std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - start);
            metrics_->rpc_latency.observe({RpcNameTraits<ServiceMethod>::value},
                                          latency.count());
        }
        co_return result->result();
    }

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    auto InvokeAsync(Args&&... args)
        -> async_simple::coro::Lazy<tl::expected<ReturnType, ErrorCode>> {
        return InvokeAsyncWithPool<ServiceMethod, ReturnType>(
            main_accessor_.GetClientPool(), std::forward<Args>(args)...);
    }

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    auto Invoke(Args&&... args) -> tl::expected<ReturnType, ErrorCode> {
        return async_simple::coro::syncAwait(
            InvokeAsync<ServiceMethod, ReturnType>(
                std::forward<Args>(args)...));
    }

    template <auto ServiceMethod, typename ReturnType, typename... Args>
    auto InvokeVia(RpcClientAccessor& accessor, Args&&... args)
        -> tl::expected<ReturnType, ErrorCode> {
        return async_simple::coro::syncAwait(
            InvokeAsyncWithPool<ServiceMethod, ReturnType>(
                accessor.GetClientPool(), std::forward<Args>(args)...));
    }

    RpcClientAccessor main_accessor_;
    RpcClientAccessor heartbeat_accessor_;
    uint16_t heartbeat_rpc_port_{0};
    const UUID client_id_;
    MasterClientMetric* metrics_;
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> pools_;
    mutable Mutex connect_mutex_;
    std::string connected_address_ GUARDED_BY(connect_mutex_);
};

}  // namespace mooncake
