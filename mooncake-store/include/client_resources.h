#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

class AutoPortBinder;
class ClientBufferAllocator;
class TransferEngine;

// Per-service transport resources. Users must stop before destruction.
class ClientResources {
   public:
    ClientResources();
    ~ClientResources();

    ClientResources(const ClientResources&) = delete;
    ClientResources& operator=(const ClientResources&) = delete;

    ErrorCode InitTransferEngine(
        uint16_t te_port, const std::string& metadata_connstring,
        const std::string& protocol,
        const std::optional<std::string>& device_names,
        const std::string& local_ip);

    // Adopt an initialized engine before creating the local pool. Do not
    // reconfigure it or explicitly stop an engine shared with another owner.
    void UseTransferEngine(const std::shared_ptr<TransferEngine>& engine) {
        transfer_engine_ = engine;
    }

    std::shared_ptr<TransferEngine> GetTransferEngine() const {
        return transfer_engine_;
    }
    uint16_t GetTransferEnginePort() const { return te_port_; }
    std::shared_ptr<ClientBufferAllocator> GetBufferAllocator() const {
        return local_buffer_allocator_;
    }

    static tl::expected<void, ErrorCode> CheckRegisterMemoryParams(
        const void* addr, size_t length);

    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    void InitLocalBufferAllocator(size_t pool_size, const std::string& protocol,
                                 bool use_hugepage = false);

    // Only the owned pool is released here. Caller-registered buffers and SHM
    // retain their existing owners and explicit registration lifecycle.
    void ReleaseLocalBuffer(bool update_metadata);

   private:
    ErrorCode InnerInitTransferEngine(
        bool auto_discover, const std::string& protocol,
        const std::optional<std::string>& device_names,
        const std::string& metadata_connstring, const std::string& local_ip);

    // Preserve teardown order: pool, port reservation, then the TE reference.
    std::shared_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<AutoPortBinder> port_binder_;
    std::shared_ptr<ClientBufferAllocator> local_buffer_allocator_;
    uint16_t te_port_ = 0;
    bool use_tent_ = false;
};

}  // namespace mooncake
