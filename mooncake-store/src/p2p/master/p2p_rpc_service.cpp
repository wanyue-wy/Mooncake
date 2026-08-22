#include "p2p/master/p2p_rpc_service.h"

#include <csignal>

#include <chrono>
#include <sstream>
#include <thread>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/reflection/user_reflect_macro.hpp>
#include <ylt/struct_json/json_reader.h>
#include <ylt/struct_json/json_writer.h>
#include <ylt/util/tl/expected.hpp>
#include <ylt/util/utils.hpp>

#include <glog/logging.h>

#include "p2p/master/p2p_master_metric_manager.h"
#include "rpc_helper.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

// Transitional (Phase 2): included only to verify the frozen wire keys below
// against the live WrappedMasterService route keys while the base class still
// exists. Removed in Phase 3 when centralized is restored to a00f757.
#include "rpc_service.h"

namespace mooncake {

// Transitional wire-key verification: the frozen literals in p2p_rpc_wire
// must equal the route keys that clients derive from the current
// WrappedMasterService member functions (coro_rpc routes by MD5-32 of the
// fully-qualified function name).
static_assert(p2p_rpc_wire::kExistKey ==
              coro_rpc::func_id<&WrappedMasterService::ExistKey>());
static_assert(p2p_rpc_wire::kBatchExistKey ==
              coro_rpc::func_id<&WrappedMasterService::BatchExistKey>());
static_assert(p2p_rpc_wire::kBatchQueryIp ==
              coro_rpc::func_id<&WrappedMasterService::BatchQueryIp>());
static_assert(
    p2p_rpc_wire::kGetReplicaListByRegex ==
    coro_rpc::func_id<&WrappedMasterService::GetReplicaListByRegex>());
static_assert(p2p_rpc_wire::kGetReplicaList ==
              coro_rpc::func_id<&WrappedMasterService::GetReplicaList>());
static_assert(p2p_rpc_wire::kBatchGetReplicaList ==
              coro_rpc::func_id<&WrappedMasterService::BatchGetReplicaList>());
static_assert(p2p_rpc_wire::kRemove ==
              coro_rpc::func_id<&WrappedMasterService::Remove>());
static_assert(p2p_rpc_wire::kRemoveByRegex ==
              coro_rpc::func_id<&WrappedMasterService::RemoveByRegex>());
static_assert(p2p_rpc_wire::kRemoveAll ==
              coro_rpc::func_id<&WrappedMasterService::RemoveAll>());
static_assert(p2p_rpc_wire::kMountSegment ==
              coro_rpc::func_id<&WrappedMasterService::MountSegment>());
static_assert(p2p_rpc_wire::kUnmountSegment ==
              coro_rpc::func_id<&WrappedMasterService::UnmountSegment>());
static_assert(p2p_rpc_wire::kHeartbeat ==
              coro_rpc::func_id<&WrappedMasterService::Heartbeat>());
static_assert(p2p_rpc_wire::kQueryClientStatus ==
              coro_rpc::func_id<&WrappedMasterService::QueryClientStatus>());
static_assert(p2p_rpc_wire::kRegisterClient ==
              coro_rpc::func_id<&WrappedMasterService::RegisterClient>());
static_assert(p2p_rpc_wire::kUnregisterClient ==
              coro_rpc::func_id<&WrappedMasterService::UnregisterClient>());
static_assert(p2p_rpc_wire::kServiceReady ==
              coro_rpc::func_id<&WrappedMasterService::ServiceReady>());
static_assert(
    p2p_rpc_wire::kHeartbeatServiceReady ==
    coro_rpc::func_id<&WrappedMasterService::HeartbeatServiceReady>());

WrappedP2PMasterService::WrappedP2PMasterService(
    const WrappedMasterServiceConfig& config)
    : master_service_(config),
      http_server_(4, config.http_port),
      metric_report_running_(config.enable_metric_reporting),
      heartbeat_rpc_port_(config.heartbeat_rpc_port) {}

WrappedP2PMasterService::~WrappedP2PMasterService() {
    metric_report_running_ = false;
    if (metric_report_thread_.joinable()) {
        metric_report_thread_.join();
    }
    http_server_.stop();
}

void WrappedP2PMasterService::init() {
    init_http_server();

    if (metric_report_running_) {
        metric_report_thread_ = std::thread([this]() {
            while (metric_report_running_) {
                std::string metrics_summary =
                    P2PMasterMetricManager::instance().get_summary_string();
                LOG(INFO) << "Master Metrics: " << metrics_summary;
                std::this_thread::sleep_for(std::chrono::seconds(
                    kP2PMetricReportIntervalSeconds));
            }
        });
    }
}

void WrappedP2PMasterService::init_http_server() {
    using namespace coro_http;

    http_server_.set_http_handler<GET>(
        "/metrics", [](coro_http_request& req, coro_http_response& resp) {
            std::string metrics =
                P2PMasterMetricManager::instance().serialize_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(metrics));
        });

    http_server_.set_http_handler<GET>(
        "/metrics/summary",
        [](coro_http_request& req, coro_http_response& resp) {
            std::string summary =
                P2PMasterMetricManager::instance().get_summary_string();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    http_server_.set_http_handler<GET>(
        "/get_all_keys", [&](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");

            auto result = master_service_.GetAllKeys();
            if (result) {
                std::string ss = "";
                auto keys = result.value();
                for (const auto& key : keys) {
                    ss += key;
                    ss += "\n";
                }
                resp.set_status_and_content(status_type::ok, std::move(ss));
            } else {
                resp.set_status_and_content(status_type::internal_server_error,
                                            "Failed to get all keys");
            }
        });

    http_server_.set_http_handler<GET>(
        "/get_key_count",
        [&](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(
                status_type::ok,
                std::to_string(master_service_.GetKeyCount()));
        });

    http_server_.set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, "OK");
        });

    http_server_.set_http_handler<GET>(
        "/batch_query_keys",
        [&](coro_http_request& req, coro_http_response& resp) {
            auto keys_view = req.get_query_value("keys");
            std::vector<std::string> keys;

            if (!keys_view.empty()) {
                std::string keys_str(keys_view);
                std::string key;
                std::istringstream iss(keys_str);
                while (std::getline(iss, key, ',')) {
                    keys.push_back(std::move(key));
                }
            }

            resp.add_header("Content-Type", "application/json; charset=utf-8");

            if (keys.empty()) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "{\"success\":false,\"error\":\"No keys provided. Use "
                    "?keys=key1,key2,...\"}");
                return;
            }

            std::vector<std::string_view> key_views;
            key_views.reserve(keys.size());
            for (const auto& k : keys) {
                key_views.push_back(k);
            }
            auto results = this->BatchGetReplicaList(key_views);
            const size_t n = std::min(keys.size(), results.size());

            std::string ss;
            ss.reserve(n * 512);

            ss += "{\"success\":true,\"data\":{";

            for (size_t i = 0; i < n; ++i) {
                if (i > 0) ss += ",";

                const auto& key = keys[i];
                const auto& r = results[i];

                ss += "\"";
                ss += key;
                ss += "\":";

                if (!r.has_value()) {
                    ss += "{\"ok\":false,\"error\":\"";
                    ss += toString(r.error());
                    ss += "\"}";
                    continue;
                }

                ss += "{\"ok\":true,\"values\":[";
                bool first = true;

                const auto& replicas = r.value().replicas;
                for (const auto& rep : replicas) {
                    if (!rep.is_memory_replica()) continue;

                    auto& mem_desc = rep.get_memory_descriptor();
                    std::string tmp;
                    struct_json::to_json(mem_desc.buffer_descriptor, tmp);
                    if (!first) ss += ",";
                    ss += tmp;
                    first = false;
                }
                ss += "]}";
            }

            ss += "}}";

            if (results.size() != keys.size()) {
                LOG(WARNING)
                    << "BatchGetReplicaList size mismatch: keys=" << keys.size()
                    << " results=" << results.size();
            }

            resp.set_status_and_content(status_type::ok, std::move(ss));
        });

    http_server_.async_start();
    LOG(INFO) << "HTTP metrics server started on port " << http_server_.port();
}

tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
WrappedP2PMasterService::CalcCacheStats() {
    return P2PMasterMetricManager::instance().calculate_cache_stats();
}

tl::expected<bool, ErrorCode> WrappedP2PMasterService::ExistKey(
    std::string_view key) {
    return execute_rpc(
        "ExistKey", [&] { return master_service_.ExistKey(key); },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] { P2PMasterMetricManager::instance().inc_exist_key_requests(); },
        [] { P2PMasterMetricManager::instance().inc_exist_key_failures(); });
}

std::vector<tl::expected<bool, ErrorCode>>
WrappedP2PMasterService::BatchExistKey(
    const std::vector<std::string_view>& keys) {
    ScopedVLogTimer timer(1, "BatchExistKey");
    const size_t total_keys = keys.size();
    timer.LogRequest("keys_count=", total_keys);
    P2PMasterMetricManager::instance().inc_batch_exist_key_requests(
        total_keys);

    auto result = master_service_.BatchExistKey(keys);

    size_t failure_count = 0;
    for (size_t i = 0; i < result.size(); ++i) {
        if (!result[i].has_value()) {
            failure_count++;
            auto error = result[i].error();
            LOG(ERROR) << "BatchExistKey failed for key[" << i << "] '"
                       << keys[i] << "': " << toString(error);
        }
    }

    if (failure_count == total_keys) {
        P2PMasterMetricManager::instance().inc_batch_exist_key_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_exist_key_partial_success(failure_count);
    }

    timer.LogResponse("total=", result.size(),
                      ", success=", result.size() - failure_count,
                      ", failures=", failure_count);
    return result;
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
WrappedP2PMasterService::BatchQueryIp(const std::vector<UUID>& client_ids) {
    ScopedVLogTimer timer(1, "BatchQueryIp");
    const size_t total_client_ids = client_ids.size();
    timer.LogRequest("client_ids_count=", total_client_ids);
    P2PMasterMetricManager::instance().inc_batch_query_ip_requests(
        total_client_ids);

    auto result = master_service_.BatchQueryIp(client_ids);

    size_t failure_count = 0;
    if (!result.has_value()) {
        failure_count = total_client_ids;
    } else {
        for (size_t i = 0; i < client_ids.size(); ++i) {
            const auto& client_id = client_ids[i];
            if (result.value().find(client_id) == result.value().end()) {
                failure_count++;
                VLOG(1) << "BatchQueryIp failed for client_id[" << i << "] '"
                        << client_id << "': not found in results";
            }
        }
    }

    if (failure_count == total_client_ids) {
        P2PMasterMetricManager::instance().inc_batch_query_ip_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_query_ip_partial_success(failure_count);
    }

    timer.LogResponse("total=", total_client_ids,
                      ", success=", total_client_ids - failure_count,
                      ", failures=", failure_count);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
WrappedP2PMasterService::GetReplicaListByRegex(const std::string& str) {
    return execute_rpc(
        "GetReplicaListByRegex",
        [&] { return master_service_.GetReplicaListByRegex(str); },
        [&](auto& timer) { timer.LogRequest("Regex=", str); },
        [] {
            P2PMasterMetricManager::instance()
                .inc_get_replica_list_by_regex_requests();
        },
        [] {
            P2PMasterMetricManager::instance()
                .inc_get_replica_list_by_regex_failures();
        });
}

tl::expected<GetReplicaListResponse, ErrorCode>
WrappedP2PMasterService::GetReplicaList(
    std::string_view key, const GetReplicaListRequestConfig& config) {
    return execute_rpc(
        "GetReplicaList",
        [&] { return master_service_.GetReplicaList(key, config); },
        [&](auto& timer) { timer.LogRequest("key=", key); },
        [] {
            P2PMasterMetricManager::instance().inc_get_replica_list_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_get_replica_list_failures();
        });
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
WrappedP2PMasterService::BatchGetReplicaList(
    const std::vector<std::string_view>& keys,
    const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "BatchGetReplicaList");
    const size_t total_requests = keys.size();
    timer.LogRequest("requests_count=", total_requests);
    P2PMasterMetricManager::instance().inc_batch_get_replica_list_requests(
        total_requests);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>> results;
    results.reserve(total_requests);

    for (const auto& key : keys) {
        results.emplace_back(master_service_.GetReplicaList(key, config));
    }

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            if (error == ErrorCode::OBJECT_NOT_FOUND ||
                error == ErrorCode::REPLICA_IS_NOT_READY) {
                VLOG(1) << "BatchGetReplicaList failed for key[" << i << "] '"
                        << keys[i] << "': " << toString(error);
            } else {
                LOG(ERROR) << "BatchGetReplicaList failed for key[" << i
                           << "] '" << keys[i] << "': " << toString(error);
            }
        }
    }

    if (failure_count == total_requests) {
        P2PMasterMetricManager::instance().inc_batch_get_replica_list_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_get_replica_list_partial_success(failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::Remove(
    std::string_view key, bool force) {
    return execute_rpc(
        "Remove", [&] { return master_service_.Remove(key, force); },
        [&](auto& timer) { timer.LogRequest("key=", key, ", force=", force); },
        [] { P2PMasterMetricManager::instance().inc_remove_requests(); },
        [] { P2PMasterMetricManager::instance().inc_remove_failures(); });
}

tl::expected<long, ErrorCode> WrappedP2PMasterService::RemoveByRegex(
    std::string_view str, bool force) {
    return execute_rpc(
        "RemoveByRegex",
        [&] { return master_service_.RemoveByRegex(str, force); },
        [&](auto& timer) {
            timer.LogRequest("regex=", str, ", force=", force);
        },
        [] {
            P2PMasterMetricManager::instance().inc_remove_by_regex_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_remove_by_regex_failures();
        });
}

long WrappedP2PMasterService::RemoveAll(bool force) {
    ScopedVLogTimer timer(1, "RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);
    P2PMasterMetricManager::instance().inc_remove_all_requests();
    long result = master_service_.RemoveAll(force);
    timer.LogResponse("items_removed=", result);
    return result;
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::UnmountSegment(
    const UUID& segment_id, const UUID& client_id) {
    return execute_rpc(
        "UnmountSegment",
        [&] { return master_service_.UnmountSegment(segment_id, client_id); },
        [&](auto& timer) {
            timer.LogRequest("segment_id=", segment_id,
                             ", client_id=", client_id);
        },
        [] {
            P2PMasterMetricManager::instance().inc_unmount_segment_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_unmount_segment_failures();
        });
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::MountSegment(
    const Segment& segment, const UUID& client_id) {
    return execute_rpc(
        "MountSegment",
        [&] { return master_service_.MountSegment(segment, client_id); },
        [&](auto& timer) {
            timer.LogRequest("segment_name=", segment.name,
                             ", client_id=", client_id);
        },
        [] {
            P2PMasterMetricManager::instance().inc_mount_segment_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_mount_segment_failures();
        });
}

tl::expected<HeartbeatResponse, ErrorCode> WrappedP2PMasterService::Heartbeat(
    const HeartbeatRequest& req) {
    ScopedVLogTimer timer(1, "Heartbeat");
    timer.LogRequest("client_id=", req.client_id);

    P2PMasterMetricManager::instance().inc_heartbeat_requests();

    auto result = master_service_.Heartbeat(req);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<QueryClientStatusResponse, ErrorCode>
WrappedP2PMasterService::QueryClientStatus(
    const QueryClientStatusRequest& req) {
    ScopedVLogTimer timer(1, "QueryClientStatus");
    timer.LogRequest("client_id=", req.client_id);

    auto result = master_service_.QueryClientStatus(req);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<RegisterClientResponse, ErrorCode>
WrappedP2PMasterService::RegisterClient(const RegisterClientRequest& req) {
    return execute_rpc(
        "RegisterClient",
        [&] { return master_service_.RegisterClient(req); },
        [&](auto& timer) {
            timer.LogRequest("client_id=", req.client_id,
                             ", segments=", req.segments.size());
        },
        [] {
            P2PMasterMetricManager::instance().inc_register_client_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_register_client_failures();
        });
}

tl::expected<UnregisterClientResponse, ErrorCode>
WrappedP2PMasterService::UnregisterClient(const UnregisterClientRequest& req) {
    return execute_rpc(
        "UnregisterClient",
        [&] { return master_service_.UnregisterClient(req); },
        [&](auto& timer) { timer.LogRequest("client_id=", req.client_id); },
        [] {
            P2PMasterMetricManager::instance()
                .inc_unregister_client_requests();
        },
        [] {
            P2PMasterMetricManager::instance()
                .inc_unregister_client_failures();
        });
}

tl::expected<std::string, ErrorCode> WrappedP2PMasterService::ServiceReady() {
    return GetMooncakeStoreVersion();
}

tl::expected<HeartbeatServiceReadyResponse, ErrorCode>
WrappedP2PMasterService::HeartbeatServiceReady() {
    HeartbeatServiceReadyResponse response;
    response.heartbeat_rpc_port = heartbeat_rpc_port_;
    return response;
}

tl::expected<WriteRouteResponse, ErrorCode>
WrappedP2PMasterService::GetWriteRoute(const WriteRouteRequest& req) {
    return execute_rpc(
        "GetWriteRoute", [&] { return master_service_.GetWriteRoute(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] {
            P2PMasterMetricManager::instance().inc_get_write_route_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_get_write_route_failures();
        });
}

BatchGetWriteRouteResponse WrappedP2PMasterService::BatchGetWriteRoute(
    const BatchGetWriteRouteRequest& req) {
    ScopedVLogTimer timer(1, "BatchGetWriteRoute");
    const size_t total = req.keys.size();
    timer.LogRequest("client_id=", req.client_id, ", key_count=", total);
    P2PMasterMetricManager::instance().inc_batch_get_write_route_requests(
        total);

    auto response = master_service_.BatchGetWriteRoute(req);

    size_t failure_count = 0;
    for (size_t i = 0; i < response.error_codes.size(); ++i) {
        if (response.error_codes[i] != ErrorCode::OK) {
            failure_count++;
            LOG(ERROR) << "BatchGetWriteRoute failed for key '" << req.keys[i]
                       << "': " << toString(response.error_codes[i]);
        }
    }
    if (failure_count == total && total > 0) {
        P2PMasterMetricManager::instance().inc_batch_get_write_route_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_get_write_route_partial_success(failure_count);
    }
    timer.LogResponse("total=", total, ", success=", total - failure_count,
                      ", failures=", failure_count);
    return response;
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::AddReplica(
    const AddReplicaRequest& req) {
    return execute_rpc(
        "AddReplica", [&] { return master_service_.AddReplica(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] { P2PMasterMetricManager::instance().inc_add_replica_requests(); },
        [] { P2PMasterMetricManager::instance().inc_add_replica_failures(); });
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::RemoveReplica(
    const RemoveReplicaRequest& req) {
    return execute_rpc(
        "RemoveReplica", [&] { return master_service_.RemoveReplica(req); },
        [&](auto& timer) { timer.LogRequest("key=", req.key); },
        [] {
            P2PMasterMetricManager::instance().inc_remove_replica_requests();
        },
        [] {
            P2PMasterMetricManager::instance().inc_remove_replica_failures();
        });
}

std::vector<tl::expected<void, ErrorCode>>
WrappedP2PMasterService::BatchRemoveReplica(
    const BatchRemoveReplicaRequest& req) {
    ScopedVLogTimer timer(1, "BatchRemoveReplica");
    const size_t total_requests = req.segment_ids.size();
    timer.LogRequest("key=", req.key, "segment_count=", total_requests);
    P2PMasterMetricManager::instance().inc_batch_remove_replica_requests(
        total_requests);

    auto results = master_service_.BatchRemoveReplica(req);

    size_t failure_count = 0;
    for (size_t i = 0; i < results.size(); ++i) {
        if (!results[i].has_value()) {
            failure_count++;
            auto error = results[i].error();
            LOG(ERROR) << "BatchRemoveReplica failed for key '" << req.key
                       << "', segment_id: " << req.segment_ids[i] << ": "
                       << toString(error);
        }
    }

    if (failure_count == total_requests && total_requests > 0) {
        P2PMasterMetricManager::instance().inc_batch_remove_replica_failures(
            failure_count);
    } else if (failure_count != 0) {
        P2PMasterMetricManager::instance()
            .inc_batch_remove_replica_partial_success(failure_count);
    }

    timer.LogResponse("total=", results.size(),
                      ", success=", results.size() - failure_count,
                      ", failures=", failure_count);
    return results;
}

BatchSyncReplicaResponse WrappedP2PMasterService::BatchSyncReplica(
    const BatchSyncReplicaRequest& req) {
    ScopedVLogTimer timer(1, "BatchSyncReplica");
    timer.LogRequest("client_id=", req.client_id,
                     ", adds=", req.add_keys.size(),
                     ", removes=", req.remove_keys.size());

    auto response = master_service_.BatchSyncReplica(req);

    size_t add_failures = 0;
    for (auto ec : response.add_results) {
        if (ec != ErrorCode::OK) add_failures++;
    }
    size_t remove_failures = 0;
    for (auto ec : response.remove_results) {
        if (ec != ErrorCode::OK) remove_failures++;
    }

    P2PMasterMetricManager::instance().inc_add_replica_requests(
        req.add_keys.size());
    P2PMasterMetricManager::instance().inc_add_replica_failures(add_failures);
    P2PMasterMetricManager::instance().inc_remove_replica_requests(
        req.remove_keys.size());
    P2PMasterMetricManager::instance().inc_remove_replica_failures(
        remove_failures);
    timer.LogResponse("add_failures=", add_failures,
                      ", remove_failures=", remove_failures);
    return response;
}

tl::expected<void, ErrorCode> WrappedP2PMasterService::SetSyncCompleted(
    UUID client_id) {
    ScopedVLogTimer timer(1, "SetSyncCompleted");
    timer.LogRequest("client_id=", client_id);

    auto result = master_service_.SetSyncCompleted(client_id);
    if (!result) {
        LOG(ERROR) << "SetSyncCompleted failed: " << toString(result.error());
    }
    return result;
}

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service,
    bool include_heartbeat) {
    // Common master RPCs are registered under the historical
    // WrappedMasterService wire keys (see p2p_rpc_wire) so that existing
    // clients, which address handlers via WrappedMasterService member
    // pointers, keep routing correctly to the standalone P2P service.
    server.register_handler<&WrappedP2PMasterService::ExistKey>(
        &wrapped_master_service, p2p_rpc_wire::kExistKey);
    server.register_handler<&WrappedP2PMasterService::BatchQueryIp>(
        &wrapped_master_service, p2p_rpc_wire::kBatchQueryIp);
    server.register_handler<&WrappedP2PMasterService::GetReplicaListByRegex>(
        &wrapped_master_service, p2p_rpc_wire::kGetReplicaListByRegex);
    server.register_handler<&WrappedP2PMasterService::GetReplicaList>(
        &wrapped_master_service, p2p_rpc_wire::kGetReplicaList);
    server.register_handler<&WrappedP2PMasterService::BatchGetReplicaList>(
        &wrapped_master_service, p2p_rpc_wire::kBatchGetReplicaList);
    server.register_handler<&WrappedP2PMasterService::Remove>(
        &wrapped_master_service, p2p_rpc_wire::kRemove);
    server.register_handler<&WrappedP2PMasterService::RemoveByRegex>(
        &wrapped_master_service, p2p_rpc_wire::kRemoveByRegex);
    server.register_handler<&WrappedP2PMasterService::RemoveAll>(
        &wrapped_master_service, p2p_rpc_wire::kRemoveAll);
    server.register_handler<&WrappedP2PMasterService::UnmountSegment>(
        &wrapped_master_service, p2p_rpc_wire::kUnmountSegment);
    server.register_handler<&WrappedP2PMasterService::MountSegment>(
        &wrapped_master_service, p2p_rpc_wire::kMountSegment);
    if (include_heartbeat) {
        server.register_handler<&WrappedP2PMasterService::Heartbeat>(
            &wrapped_master_service, p2p_rpc_wire::kHeartbeat);
    }
    server.register_handler<&WrappedP2PMasterService::QueryClientStatus>(
        &wrapped_master_service, p2p_rpc_wire::kQueryClientStatus);
    server.register_handler<&WrappedP2PMasterService::RegisterClient>(
        &wrapped_master_service, p2p_rpc_wire::kRegisterClient);
    server.register_handler<&WrappedP2PMasterService::UnregisterClient>(
        &wrapped_master_service, p2p_rpc_wire::kUnregisterClient);
    server.register_handler<&WrappedP2PMasterService::BatchExistKey>(
        &wrapped_master_service, p2p_rpc_wire::kBatchExistKey);
    server.register_handler<&WrappedP2PMasterService::ServiceReady>(
        &wrapped_master_service, p2p_rpc_wire::kServiceReady);
    // HeartbeatServiceReady is always on the main server so legacy clients
    // (which only talk to the main server) can discover the master's heartbeat
    // routing and fail fast on a client/master port mismatch at Connect time.
    server.register_handler<&WrappedP2PMasterService::HeartbeatServiceReady>(
        &wrapped_master_service, p2p_rpc_wire::kHeartbeatServiceReady);

    // P2P-specific RPCs keep their own (class-derived) route keys.
    server.register_handler<&WrappedP2PMasterService::GetWriteRoute>(
        &wrapped_master_service);
    server.register_handler<&WrappedP2PMasterService::BatchGetWriteRoute>(
        &wrapped_master_service);
    server.register_handler<&WrappedP2PMasterService::AddReplica>(
        &wrapped_master_service);
    server.register_handler<&WrappedP2PMasterService::RemoveReplica>(
        &wrapped_master_service);
    server.register_handler<&WrappedP2PMasterService::BatchRemoveReplica>(
        &wrapped_master_service);
    server.register_handler<&WrappedP2PMasterService::BatchSyncReplica>(
        &wrapped_master_service);
    server.register_handler<&WrappedP2PMasterService::SetSyncCompleted>(
        &wrapped_master_service);
}

void RegisterP2PHeartbeatRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service) {
    server.register_handler<&WrappedP2PMasterService::Heartbeat>(
        &wrapped_master_service, p2p_rpc_wire::kHeartbeat);
    // ServiceReady gives clients a lightweight, side-effect-free probe to
    // verify the dedicated heartbeat server is actually listening (used by
    // MasterClient::Connect to fail fast on a client/master port mismatch).
    server.register_handler<&WrappedP2PMasterService::ServiceReady>(
        &wrapped_master_service, p2p_rpc_wire::kServiceReady);
}

}  // namespace mooncake
