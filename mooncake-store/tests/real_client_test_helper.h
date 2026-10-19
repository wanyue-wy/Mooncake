#pragma once

#include "real_client.h"

namespace mooncake {

// Callers retain the locking used for direct member access.
struct RealClientTestHelper {
    using MappedShm = RealClient::MappedShm;
    using ShmContext = RealClient::ShmContext;

    static auto MapShm(RealClient& client, int fd, uint64_t base, size_t size,
                       bool local, const UUID& id) {
        return client.map_shm_internal(fd, base, size, local, id);
    }

    static auto FindShmContext(RealClient& client, const UUID& id) {
        return client.find_shm_context(id);
    }

    static auto& ShmMutex(RealClient& client) {
        return client.dummy_client_mutex_;
    }

    static auto& ShmContexts(RealClient& client) {
        return client.shm_contexts_;
    }

    static bool IpcThreadJoinable(const RealClient& client) {
        return client.ipc_thread_.joinable();
    }

    static auto NativeClient(const RealClient& client) {
        return client.client_service_;
    }

    static auto& DummyRpcServer(RealClient& client) {
        return client.dummy_rpc_server_;
    }
};

}  // namespace mooncake
