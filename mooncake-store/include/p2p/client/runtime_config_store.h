#pragma once

#include <json/json.h>

#include <mutex>
#include <shared_mutex>
#include <string>
#include <variant>

#include "p2p/common/p2p_rpc_types.h"
#include "p2p/common/p2p_types.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

class RuntimeConfigStore {
   public:
    using WriteConfig = std::variant<ReplicateConfig, P2PWriteRouteConfig>;

    explicit RuntimeConfigStore(DeploymentMode mode);

    WriteConfig getDefaultWriteConfig() const;
    ReadRouteConfig getDefaultReadConfig() const;
    Json::Value exportConfig() const;

    bool loadFromJson(const Json::Value& root);
    bool updateWriteConfig(const Json::Value& json);
    void updateReadConfig(const Json::Value& json);

   private:
    static void applyPatch(ReplicateConfig& config, const Json::Value& json);
    static bool applyPatch(P2PWriteRouteConfig& config,
                           const Json::Value& json);
    static void applyPatch(ReadRouteConfig& config, const Json::Value& json);

    static Json::Value toJson(const ReplicateConfig& config);
    static Json::Value toJson(const P2PWriteRouteConfig& config);
    static Json::Value toJson(const ReadRouteConfig& config);

    mutable std::shared_mutex mu_;
    WriteConfig write_;
    ReadRouteConfig read_;
};

}  // namespace mooncake
