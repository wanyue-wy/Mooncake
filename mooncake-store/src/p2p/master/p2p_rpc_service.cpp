#include "p2p/master/p2p_rpc_service.h"

#include <algorithm>
#include <chrono>
#include <csignal>
#include <sstream>
#include <thread>

#include <glog/logging.h>
#include <ylt/struct_json/json_writer.h>

#include "p2p/master/p2p_master_metric_manager.h"
#include "rpc_helper.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

P2PMasterRpcService::P2PMasterRpcService(const P2PMasterConfig& config,
                                         ViewVersionId view_version)
    : master_service_(config, view_version),
      http_server_(4, static_cast<uint16_t>(config.metrics.http_port)),
      metric_report_running_(config.metrics.enable_reporting),
      heartbeat_rpc_port_(config.rpc.heartbeat_port) {}

P2PMasterRpcService::~P2PMasterRpcService() {
    metric_report_running_ = false;
    if (metric_report_thread_.joinable()) {
        metric_report_thread_.join();
    }
    http_server_.stop();
}

void P2PMasterRpcService::init() {
    init_http_server();
    if (metric_report_running_) {
        metric_report_thread_ = std::thread([this]() {
            while (metric_report_running_) {
                LOG(INFO) << "Master Metrics: "
                          << P2PMasterMetricManager::instance()
                                 .get_summary_string();
                std::this_thread::sleep_for(std::chrono::seconds(
                    kP2PMetricReportIntervalSeconds));
            }
        });
    }
}

void P2PMasterRpcService::init_http_server() {
    using namespace coro_http;
    http_server_.set_http_handler<GET>(
        "/metrics", [](coro_http_request&, coro_http_response& response) {
            response.add_header("Content-Type", "text/plain; version=0.0.4");
            response.set_status_and_content(
                status_type::ok,
                P2PMasterMetricManager::instance().serialize_metrics());
        });
    http_server_.set_http_handler<GET>(
        "/metrics/summary",
        [](coro_http_request&, coro_http_response& response) {
            response.add_header("Content-Type", "text/plain; version=0.0.4");
            response.set_status_and_content(
                status_type::ok,
                P2PMasterMetricManager::instance().get_summary_string());
        });
    http_server_.set_http_handler<GET>(
        "/get_all_keys",
        [&](coro_http_request&, coro_http_response& response) {
            auto result = master_service_.ListRouteKeys();
            std::string body;
            for (const auto& key : result) {
                body += key + "\n";
            }
            response.set_status_and_content(status_type::ok, std::move(body));
        });
    http_server_.set_http_handler<GET>(
        "/get_key_count",
        [&](coro_http_request&, coro_http_response& response) {
            response.set_status_and_content(
                status_type::ok,
                std::to_string(master_service_.GetRouteKeyCount()));
        });
    http_server_.set_http_handler<GET>(
        "/health", [](coro_http_request&, coro_http_response& response) {
            response.set_status_and_content(status_type::ok, "OK");
        });
    http_server_.set_http_handler<GET>(
        "/batch_query_routes",
        [&](coro_http_request& request, coro_http_response& response) {
            auto keys_value = request.get_query_value("keys");
            P2PBatchGetReadRouteRequest rpc_request;
            if (!keys_value.empty()) {
                std::istringstream input{std::string(keys_value)};
                std::string key;
                while (std::getline(input, key, ',')) {
                    rpc_request.keys.push_back(std::move(key));
                }
            }
            if (rpc_request.keys.empty()) {
                response.set_status_and_content(
                    status_type::bad_request,
                    "{\"success\":false,\"error\":\"No keys provided\"}");
                return;
            }
            auto routes = BatchGetReadRoute(rpc_request);
            std::string json;
            struct_json::to_json(routes, json);
            response.add_header("Content-Type",
                                "application/json; charset=utf-8");
            response.set_status_and_content(status_type::ok, std::move(json));
        });
    http_server_.async_start();
    LOG(INFO) << "P2P master HTTP server started on port "
              << http_server_.port();
}

auto P2PMasterRpcService::RegisterClient(
    const P2PRegisterClientRequest& request)
    -> tl::expected<P2PRegisterClientResponse, ErrorCode> {
    return execute_rpc(
        "RegisterClient",
        [&] { return master_service_.RegisterClient(request); },
        [&](auto& timer) { timer.LogRequest("client_id=", request.client_id); },
        [] { P2PMasterMetricManager::instance().inc_register_client_requests(); },
        [] { P2PMasterMetricManager::instance().inc_register_client_failures(); });
}

auto P2PMasterRpcService::UnregisterClient(
    const P2PUnregisterClientRequest& request)
    -> tl::expected<P2PUnregisterClientResponse, ErrorCode> {
    return execute_rpc(
        "UnregisterClient",
        [&] { return master_service_.UnregisterClient(request); },
        [&](auto& timer) { timer.LogRequest("client_id=", request.client_id); },
        [] { P2PMasterMetricManager::instance().inc_unregister_client_requests(); },
        [] { P2PMasterMetricManager::instance().inc_unregister_client_failures(); });
}

auto P2PMasterRpcService::Heartbeat(const P2PHeartbeatRequest& request)
    -> tl::expected<P2PHeartbeatResponse, ErrorCode> {
    P2PMasterMetricManager::instance().inc_heartbeat_requests();
    return master_service_.Heartbeat(request);
}

auto P2PMasterRpcService::QueryClientStatus(
    const P2PQueryClientStatusRequest& request)
    -> tl::expected<P2PQueryClientStatusResponse, ErrorCode> {
    return master_service_.QueryClientStatus(request);
}

auto P2PMasterRpcService::MountSegment(
    const P2PMountSegmentRequest& request) -> tl::expected<void, ErrorCode> {
    return execute_rpc(
        "MountSegment",
        [&] { return master_service_.MountSegment(request); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", request.client_id,
                             ", segment_id=", request.segment.id);
        },
        [] { P2PMasterMetricManager::instance().inc_mount_segment_requests(); },
        [] { P2PMasterMetricManager::instance().inc_mount_segment_failures(); });
}

auto P2PMasterRpcService::UnmountSegment(
    const P2PUnmountSegmentRequest& request) -> tl::expected<void, ErrorCode> {
    return execute_rpc(
        "UnmountSegment",
        [&] { return master_service_.UnmountSegment(request); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", request.client_id,
                             ", segment_id=", request.segment_id);
        },
        [] { P2PMasterMetricManager::instance().inc_unmount_segment_requests(); },
        [] { P2PMasterMetricManager::instance().inc_unmount_segment_failures(); });
}

auto P2PMasterRpcService::RouteExists(
    const P2PRouteExistsRequest& request)
    -> tl::expected<P2PRouteExistsResponse, ErrorCode> {
    return master_service_.RouteExists(request);
}

auto P2PMasterRpcService::BatchRouteExists(
    const P2PBatchRouteExistsRequest& request)
    -> P2PBatchRouteExistsResponse {
    return master_service_.BatchRouteExists(request);
}

auto P2PMasterRpcService::GetReadRoute(
    const P2PGetReadRouteRequest& request)
    -> tl::expected<P2PGetReadRouteResponse, ErrorCode> {
    return master_service_.GetReadRoute(request);
}

auto P2PMasterRpcService::BatchGetReadRoute(
    const P2PBatchGetReadRouteRequest& request)
    -> P2PBatchGetReadRouteResponse {
    return master_service_.BatchGetReadRoute(request);
}

auto P2PMasterRpcService::GetWriteRoute(
    const P2PGetWriteRouteRequest& request)
    -> tl::expected<P2PGetWriteRouteResponse, ErrorCode> {
    return master_service_.GetWriteRoute(request);
}

auto P2PMasterRpcService::BatchGetWriteRoute(
    const P2PBatchGetWriteRouteRequest& request)
    -> P2PBatchGetWriteRouteResponse {
    return master_service_.BatchGetWriteRoute(request);
}

auto P2PMasterRpcService::PublishRoute(
    const P2PPublishRouteRequest& request) -> tl::expected<void, ErrorCode> {
    return execute_rpc(
        "PublishRoute",
        [&] { return master_service_.PublishRoute(request); },
        [&](auto& timer) { timer.LogRequest("key=", request.key); },
        [] { P2PMasterMetricManager::instance().inc_add_replica_requests(); },
        [] { P2PMasterMetricManager::instance().inc_add_replica_failures(); });
}

auto P2PMasterRpcService::WithdrawRoute(
    const P2PWithdrawRouteRequest& request) -> tl::expected<void, ErrorCode> {
    return execute_rpc(
        "WithdrawRoute",
        [&] { return master_service_.WithdrawRoute(request); },
        [&](auto& timer) { timer.LogRequest("key=", request.key); },
        [] { P2PMasterMetricManager::instance().inc_remove_replica_requests(); },
        [] { P2PMasterMetricManager::instance().inc_remove_replica_failures(); });
}

auto P2PMasterRpcService::BatchWithdrawRoute(
    const P2PBatchWithdrawRouteRequest& request)
    -> P2PBatchWithdrawRouteResponse {
    return master_service_.BatchWithdrawRoute(request);
}

auto P2PMasterRpcService::BatchSyncRoutes(
    const P2PBatchSyncRoutesRequest& request) -> P2PBatchSyncRoutesResponse {
    return master_service_.BatchSyncRoutes(request);
}

auto P2PMasterRpcService::CompleteRouteSync(
    const P2PCompleteRouteSyncRequest& request)
    -> tl::expected<void, ErrorCode> {
    return master_service_.CompleteRouteSync(request);
}

auto P2PMasterRpcService::ServiceReady()
    -> tl::expected<std::string, ErrorCode> {
    return GetMooncakeStoreVersion();
}

auto P2PMasterRpcService::HeartbeatServiceReady()
    -> tl::expected<P2PHeartbeatServiceReadyResponse, ErrorCode> {
    return P2PHeartbeatServiceReadyResponse{
        .heartbeat_rpc_port = heartbeat_rpc_port_};
}

void RegisterP2PRpcService(coro_rpc::coro_rpc_server& server,
                           P2PMasterRpcService& service,
                           bool include_heartbeat) {
    server.register_handler<&P2PMasterRpcService::RegisterClient>(&service);
    server.register_handler<&P2PMasterRpcService::UnregisterClient>(&service);
    if (include_heartbeat) {
        server.register_handler<&P2PMasterRpcService::Heartbeat>(&service);
    }
    server.register_handler<&P2PMasterRpcService::QueryClientStatus>(&service);
    server.register_handler<&P2PMasterRpcService::MountSegment>(&service);
    server.register_handler<&P2PMasterRpcService::UnmountSegment>(&service);
    server.register_handler<&P2PMasterRpcService::RouteExists>(&service);
    server.register_handler<&P2PMasterRpcService::BatchRouteExists>(&service);
    server.register_handler<&P2PMasterRpcService::GetReadRoute>(&service);
    server.register_handler<&P2PMasterRpcService::BatchGetReadRoute>(&service);
    server.register_handler<&P2PMasterRpcService::GetWriteRoute>(&service);
    server.register_handler<&P2PMasterRpcService::BatchGetWriteRoute>(&service);
    server.register_handler<&P2PMasterRpcService::PublishRoute>(&service);
    server.register_handler<&P2PMasterRpcService::WithdrawRoute>(&service);
    server.register_handler<&P2PMasterRpcService::BatchWithdrawRoute>(&service);
    server.register_handler<&P2PMasterRpcService::BatchSyncRoutes>(&service);
    server.register_handler<&P2PMasterRpcService::CompleteRouteSync>(&service);
    server.register_handler<&P2PMasterRpcService::ServiceReady>(&service);
    server.register_handler<&P2PMasterRpcService::HeartbeatServiceReady>(
        &service);
}

void RegisterP2PHeartbeatRpcService(coro_rpc::coro_rpc_server& server,
                                    P2PMasterRpcService& service) {
    server.register_handler<&P2PMasterRpcService::Heartbeat>(&service);
    server.register_handler<&P2PMasterRpcService::ServiceReady>(&service);
}

}  // namespace mooncake
