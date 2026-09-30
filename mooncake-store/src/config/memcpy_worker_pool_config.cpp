#include "memcpy_worker_pool_config.h"

#include <glog/logging.h>

#include "environ.h"
#include "environment_value_parser.h"
#include "environment_variables.h"

namespace mooncake {

MemcpyWorkerPoolConfig MemcpyWorkerPoolConfig::FromEnvironment() {
    MemcpyWorkerPoolConfig config;
    const auto raw = Environ::Read(
        TransferSubmitterEnvironmentVariables::MC_STORE_MEMCPY_WORKERS);
    if (!raw || raw->empty()) {
        return config;
    }

    const auto value = TryParseEnvironmentValue<int>(*raw);
    if (value && *value > 0) {
        config.worker_count = *value;
    } else {
        LOG(WARNING) << "Invalid value for MC_STORE_MEMCPY_WORKERS: " << *raw
                     << ", using default " << config.worker_count;
    }
    return config;
}

}  // namespace mooncake
