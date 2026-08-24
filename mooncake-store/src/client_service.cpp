#include "client_service.h"

#include <glog/logging.h>

#include "centralized_client_service.h"
#include "config.h"

namespace mooncake {

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

tl::expected<void, ErrorCode> ClientService::CheckRegisterMemoryParams(
    const void* addr, size_t length) {
    if (addr == nullptr) {
        LOG(ERROR) << "addr is nullptr";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (length == 0) {
        LOG(ERROR) << "length is 0";
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Tcp is not limited by max_mr_size, but we ignore it for now.
    auto max_mr_size = globalConfig().max_mr_size;
    if (length > max_mr_size) {
        LOG(ERROR) << "length " << length
                   << " is larger than max_mr_size: " << max_mr_size;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

size_t ClientService::CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

size_t ClientService::CalculateSliceSize(std::span<const Slice> slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

}  // namespace mooncake
