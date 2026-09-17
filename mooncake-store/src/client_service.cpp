#include "client_service.h"

#include <glog/logging.h>

#include <csignal>
#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <thread>

#include "config.h"
#include "p2p/client/runtime_config_store.h"
#include "transfer_engine.h"
#include "types.h"
#include "centralized_client_service.h"
#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake {

void ClientService::initTeEndpoint() {
    if (metadata_connstring_ == P2PHANDSHAKE) {
        te_endpoint_ = resources_.GetTransferEngine()->getLocalIpAndPort();
    } else {
        te_endpoint_ = local_endpoint();
    }
}

ClientService::ClientService(const std::string& metadata_connstring,
                             uint16_t http_port, bool enable_http_server,
                             const std::map<std::string, std::string>& labels)
    : client_id_(generate_uuid()),
      metadata_connstring_(metadata_connstring),
      http_port_(http_port) {
    LOG(INFO) << "client_id=" << client_id_;
    if (enable_http_server) {
        try {
            http_server_ =
                std::make_unique<coro_http::coro_http_server>(1, http_port_);
            LOG(INFO) << "Client HTTP server created on port " << http_port_;
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to create client HTTP server: " << e.what();
            http_server_.reset();
            http_port_ = 0;
        }
    } else {
        LOG(INFO) << "Client HTTP server disabled";
        http_port_ = 0;
    }
}

std::optional<std::shared_ptr<ClientService>> ClientService::Create(
    const CentralizedClientConfig& config) {
    auto client = std::make_shared<CentralizedClientService>(
        config.metadata_connstring, config.protocol, config.http_port,
        config.enable_http_server, config.labels,
        config.enable_metric_collection);

    auto err = client->Init(config);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize centralized client service"
                   << ", ret = " << err;
        return std::nullopt;
    }

    return client;
}

ClientService::~ClientService() {
    Stop();
    Destroy();
}

void ClientService::Stop() {
    StopHttpServer();
    resources_.ReleaseLocalBuffer(false);
    StopHeartbeat();
}

void ClientService::StopHeartbeat() {
    MutexLocker lk(&registration_mutex_);
    InnerStopHeartbeat();
}

void ClientService::InnerStopHeartbeat() {
    if (heartbeat_running_) {
        {
            std::lock_guard<std::mutex> lock(heartbeat_mtx_);
            heartbeat_running_ = false;
        }
        heartbeat_cv_.notify_all();
        if (heartbeat_thread_.joinable()) {
            heartbeat_thread_.join();
        }
    }
}

void ClientService::Destroy() {}

tl::expected<ViewVersionId, ErrorCode> ClientService::RegisterClient() {
    MutexLocker lk(&registration_mutex_);
    InflightTracker::Guard guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        LOG(WARNING) << "inflight guard invalid";
        return tl::make_unexpected(ErrorCode::SHUTTING_DOWN);
    }
    return InnerRegisterClient();
}

ErrorCode ClientService::InitTransferEngine(
    uint16_t te_port, const std::string& metadata_connstring,
    const std::string& protocol,
    const std::optional<std::string>& device_names) {
    return resources_.InitTransferEngine(
        te_port, metadata_connstring_, protocol, device_names, local_ip_);
}

tl::expected<void, ErrorCode> ClientService::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    return resources_.RegisterLocalMemory(addr, length, location,
                                          remote_accessible, update_metadata);
}

tl::expected<void, ErrorCode> ClientService::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    return resources_.unregisterLocalMemory(addr, update_metadata);
}

void ClientService::WaitForNextHeartbeat(int interval_ms) {
    std::unique_lock<std::mutex> lock{heartbeat_mtx_};
    heartbeat_cv_.wait_for(lock, std::chrono::milliseconds(interval_ms),
                           [this] { return !heartbeat_running_; });
}

void ClientService::HeartbeatTryRegister() NO_THREAD_SAFETY_ANALYSIS {
    MutexLocker lk(&registration_mutex_, /*lock_now=*/false);
    if (!lk.TryLock()) {
        // try_lock to avoid a deadlock between the unregister path and this
        // background worker:
        // 1. UnregisterClient/StopHeartbeat hold registration_mutex_ while
        // joining the heartbeat thread
        // 2. this worker should take registration_mutex_ to register. Thus it
        // has a conflict with unregister method.
        LOG(INFO)
            << "Skip heartbeat-driven register: op in progress, client_id="
            << client_id_;
        return;
    }
    InflightTracker::Guard guard = AcquireInflightGuard();
    if (!guard.is_valid()) {
        // Service is shutting down; do not register at the master.
        LOG(INFO) << "Skip heartbeat-driven register: shutting down, client_id="
                  << client_id_;
        return;
    }
    tl::expected<ViewVersionId, ErrorCode> res;
    try {
        res = InnerRegisterClient();
    } catch (const std::exception& e) {
        LOG(ERROR) << "InnerRegisterClient threw, client_id=" << client_id_
                   << ", what=" << e.what();
        return;
    } catch (...) {
        LOG(ERROR) << "InnerRegisterClient threw unknown, client_id="
                   << client_id_;
        return;
    }
    // Recovery is driven inside InnerRegisterClient on a successful register,
    // so nothing more to do here besides surfacing a failure.
    if (!res) {
        LOG(ERROR) << "Failed to register client, client_id=" << client_id_
                   << ", error=" << res.error();
    }
}

void ClientService::RegisterHttpMethods() {
    if (!http_server_) return;

    using namespace coro_http;

    // Prometheus-style metrics endpoint
    http_server_->set_http_handler<GET>(
        "/metrics", [this](coro_http_request& req, coro_http_response& resp) {
            auto metrics = SerializeMetrics();
            if (!metrics) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "Metrics not available");
                return;
            }
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok,
                                        std::move(metrics.value()));
        });

    // Human-readable summary endpoint
    http_server_->set_http_handler<GET>(
        "/metrics/summary",
        [this](coro_http_request& req, coro_http_response& resp) {
            auto metrics = GetSummaryMetrics();
            if (!metrics) {
                resp.set_status_and_content(status_type::service_unavailable,
                                            "Metrics not available");
                return;
            }
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok,
                                        std::move(metrics.value()));
        });

    // Health check endpoint
    http_server_->set_http_handler<GET>(
        "/health", [this](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, GetHealthStatus());
        });

    RegisterRuntimeConfigHttpMethods();
}

void ClientService::RegisterRuntimeConfigHttpMethods() {
    if (!http_server_) return;

    using namespace coro_http;

    auto respond_json = [](coro_http_response& resp, const Json::Value& val) {
        Json::StreamWriterBuilder writer;
        resp.add_header("Content-Type", "application/json");
        resp.set_status_and_content(status_type::ok,
                                    Json::writeString(writer, val));
    };

    auto parse_json = [](coro_http_request& req, coro_http_response& resp,
                         Json::Value& out) -> bool {
        auto body = req.get_body();
        if (body.empty()) {
            resp.set_status_and_content(status_type::bad_request,
                                        "Empty request body");
            return false;
        }
        Json::CharReaderBuilder builder;
        auto reader =
            std::unique_ptr<Json::CharReader>(builder.newCharReader());
        std::string errors;
        if (!reader->parse(body.data(), body.data() + body.size(), &out,
                           &errors)) {
            resp.set_status_and_content(status_type::bad_request,
                                        "Invalid JSON: " + errors);
            return false;
        }
        return true;
    };

    auto parse_body = [parse_json](coro_http_request& req,
                                   coro_http_response& resp,
                                   Json::Value& out) -> bool {
        if (!parse_json(req, resp, out)) return false;
        if (!out.isObject()) {
            resp.set_status_and_content(status_type::bad_request,
                                        "Expected JSON object");
            return false;
        }
        return true;
    };

    // GET /config — all config
    http_server_->set_http_handler<GET>(
        "/config",
        [this, respond_json](coro_http_request& req, coro_http_response& resp) {
            respond_json(resp, runtime_config_store_->exportConfig());
        });

    // GET /config/write — write config
    http_server_->set_http_handler<GET>(
        "/config/write",
        [this, respond_json](coro_http_request& req, coro_http_response& resp) {
            respond_json(resp, runtime_config_store_->exportConfig()["write"]);
        });

    // GET /config/read — read config
    http_server_->set_http_handler<GET>(
        "/config/read",
        [this, respond_json](coro_http_request& req, coro_http_response& resp) {
            respond_json(resp, runtime_config_store_->exportConfig()["read"]);
        });

    // POST /config/update — full update (body: {"write":{...}, "read":{...}})
    http_server_->set_http_handler<POST>(
        "/config/update",
        [this, parse_body, respond_json](coro_http_request& req,
                                         coro_http_response& resp) {
            Json::Value json;
            if (!parse_body(req, resp, json)) return;
            if (!runtime_config_store_->loadFromJson(json)) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "invalid write config: contradictory "
                    "waterline/remote_weight extremes");
                return;
            }
            respond_json(resp, runtime_config_store_->exportConfig());
        });

    // POST /config/update_write — patch write config (body: {...})
    http_server_->set_http_handler<POST>(
        "/config/update_write",
        [this, parse_body, respond_json](coro_http_request& req,
                                         coro_http_response& resp) {
            Json::Value json;
            if (!parse_body(req, resp, json)) return;
            if (!runtime_config_store_->updateWriteConfig(json)) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "invalid write config: contradictory "
                    "waterline/remote_weight extremes");
                return;
            }
            LOG(INFO) << "Runtime write config updated via HTTP";
            respond_json(resp, runtime_config_store_->exportConfig()["write"]);
        });

    // POST /config/update_read — patch read config (body: {...})
    http_server_->set_http_handler<POST>(
        "/config/update_read",
        [this, parse_body, respond_json](coro_http_request& req,
                                         coro_http_response& resp) {
            Json::Value json;
            if (!parse_body(req, resp, json)) return;
            runtime_config_store_->updateReadConfig(json);
            LOG(INFO) << "Runtime read config updated via HTTP";
            respond_json(resp, runtime_config_store_->exportConfig()["read"]);
        });

    // GET /config/get?section=write&key=remote_weight — get single field
    http_server_->set_http_handler<GET>(
        "/config/get",
        [this, respond_json](coro_http_request& req, coro_http_response& resp) {
            auto section = req.get_query_value("section");
            auto key = req.get_query_value("key");
            if (section.empty() || key.empty()) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "Missing 'section' or 'key' query parameter");
                return;
            }
            Json::Value full = runtime_config_store_->exportConfig();
            std::string sec(section);
            std::string k(key);
            if (!full.isMember(sec) || !full[sec].isMember(k)) {
                resp.set_status_and_content(status_type::not_found,
                                            "Key not found");
                return;
            }
            respond_json(resp, full[sec][k]);
        });

    // POST /config/set?section=write&key=remote_weight — set single field
    // Body: the JSON value
    http_server_->set_http_handler<POST>(
        "/config/set", [this, parse_json, respond_json](
                           coro_http_request& req, coro_http_response& resp) {
            auto section = req.get_query_value("section");
            auto key = req.get_query_value("key");
            if (section.empty() || key.empty()) {
                resp.set_status_and_content(
                    status_type::bad_request,
                    "Missing 'section' or 'key' query parameter");
                return;
            }
            std::string sec(section);
            std::string k(key);
            if (sec != "write" && sec != "read") {
                resp.set_status_and_content(status_type::bad_request,
                                            "Unknown section: " + sec);
                return;
            }
            Json::Value current = runtime_config_store_->exportConfig();
            if (!current.isMember(sec) || !current[sec].isMember(k)) {
                resp.set_status_and_content(status_type::not_found,
                                            "Unknown key: " + sec + "." + k);
                return;
            }
            Json::Value value;
            if (!parse_json(req, resp, value)) return;
            Json::Value patch;
            patch[k] = value;
            if (sec == "write") {
                if (!runtime_config_store_->updateWriteConfig(patch)) {
                    resp.set_status_and_content(
                        status_type::bad_request,
                        "invalid write config: contradictory "
                        "waterline/remote_weight extremes");
                    return;
                }
            } else {
                runtime_config_store_->updateReadConfig(patch);
            }
            LOG(INFO) << "Runtime config field " << sec << "." << k
                      << " updated via HTTP";
            respond_json(resp, runtime_config_store_->exportConfig()[sec]);
        });
}

void ClientService::StartHttpServer() {
    if (!http_server_) return;
    try {
        RegisterHttpMethods();
        http_server_->async_start();
        http_port_ = http_server_->port();
        LOG(INFO) << "Client HTTP server started on port " << http_port_;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to start client HTTP server: " << e.what();
        http_server_.reset();
        http_port_ = 0;
    }
}

void ClientService::StopHttpServer() {
    if (http_server_) {
        LOG(INFO) << "Stopping client HTTP server on port " << http_port_;
        http_server_->stop();
        http_server_.reset();
    }
}

void ClientService::InitLocalBufferAllocator(size_t pool_size,
                                             const std::string& protocol,
                                             bool use_hugepage) {
    resources_.InitLocalBufferAllocator(pool_size, protocol, use_hugepage);
}

tl::expected<UUID, ErrorCode> ClientService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<UUID, ErrorCode> ClientService::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<QueryTaskResponse, ErrorCode> ClientService::QueryTask(
    const UUID& task_id) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> ClientService::FetchTasks(
    size_t batch_size) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

tl::expected<void, ErrorCode> ClientService::MarkTaskToComplete(
    const TaskCompleteRequest& update_request) {
    return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
}

}  // namespace mooncake
