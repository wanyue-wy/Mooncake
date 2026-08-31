#include <gflags/gflags.h>
#include <glog/logging.h>

#include "p2p/master/p2p_master.h"
#include "p2p/master/p2p_master_config_loader.h"
#include "types.h"

int main(int argc, char* argv[]) {
    mooncake::init_ylt_log_level();
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!FLAGS_log_dir.empty()) {
        google::InitGoogleLogging(argv[0]);
    }

    auto config = mooncake::LoadP2PMasterConfig();
    if (!config) {
        LOG(ERROR) << config.error();
        return 1;
    }
    return mooncake::P2PMaster(config.value()).Run();
}
