#pragma once

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include <ylt/metric/counter.hpp>
#include <ylt/metric/histogram.hpp>

#include "hybrid_metric.h"

namespace mooncake {

namespace p2p::client_metric {

// Latency bucket boundaries are in microseconds.
const std::vector<double> kLatencyBucket = {
    125, 150, 200, 250, 300, 400, 500, 750, 1000,
    1500, 2000, 3000, 5000, 7000, 15000, 20000,
    50000, 100000, 200000, 500000, 1000000};

template <typename BucketValueFn>
std::string FormatLatencySummaryFromBuckets(
    size_t bucket_count, BucketValueFn&& bucket_value,
    const std::string& count_key = "count") {
    int64_t total_count = 0;
    for (size_t i = 0; i < bucket_count; ++i) {
        total_count += bucket_value(i);
    }
    if (bucket_count == 0 || total_count == 0) {
        return "No data";
    }

    std::stringstream ss;
    ss << count_key << "=" << total_count;

    // Keep a single sample from matching an empty bucket with target == 0.
    int64_t p95_target = std::max<int64_t>(1, (total_count * 95) / 100);
    int64_t cumulative = 0;
    double p95_bucket = 0;
    for (size_t i = 0; i < bucket_count && i < kLatencyBucket.size(); ++i) {
        cumulative += bucket_value(i);
        if (cumulative >= p95_target) {
            p95_bucket = kLatencyBucket[i];
            break;
        }
    }
    if (p95_bucket > 0) {
        ss << ", p95<" << p95_bucket << "μs";
    }

    // Max bucket: highest bucket boundary that received at least one sample.
    double max_bucket = 0;
    for (size_t i = std::min(bucket_count, kLatencyBucket.size()); i > 0; --i) {
        if (bucket_value(i - 1) > 0) {
            max_bucket = kLatencyBucket[i - 1];
            break;
        }
    }
    if (max_bucket > 0) {
        ss << ", max<" << max_bucket << "μs";
    }
    return ss.str();
}

inline std::string FormatLatencySummary(ylt::metric::histogram_t& hist) {
    auto counts = hist.get_bucket_counts();
    return FormatLatencySummaryFromBuckets(
        counts.size(), [&](size_t i) { return counts[i]->value(); });
}

// Simple stopwatch for measuring elapsed time in microseconds.
class Stopwatch {
   public:
    Stopwatch() : start_time_(std::chrono::steady_clock::now()) {}

    int64_t elapsed_us() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   now - start_time_)
            .count();
    }

   private:
    std::chrono::steady_clock::time_point start_time_;
};

inline std::string GetEnvOrDefault(const char* env_var,
                                  const std::string& default_val = "") {
    const char* val = std::getenv(env_var);
    return val ? val : default_val;
}

// Static labels remain constant during the lifetime of the application.
const std::string kClusterID = GetEnvOrDefault("MC_STORE_CLUSTER_ID");

inline std::map<std::string, std::string> MergeLabels(
    const std::map<std::string, std::string>& labels) {
    std::map<std::string, std::string> merged_labels;
    if (!kClusterID.empty()) {
        merged_labels["cluster_id"] = kClusterID;
    }
    merged_labels.insert(labels.begin(), labels.end());
    return merged_labels;
}

}  // namespace p2p::client_metric

struct P2PTransferMetric {
    P2PTransferMetric(std::map<std::string, std::string> labels = {})
        : total_read_bytes("mooncake_transfer_read_bytes", "Total bytes read",
                           labels),
          total_write_bytes("mooncake_transfer_write_bytes",
                            "Total bytes written", labels),
          batch_put_latency_us("mooncake_transfer_batch_put_latency",
                               "Batch Put transfer latency (us)",
                               p2p::client_metric::kLatencyBucket, labels),
          batch_get_latency_us("mooncake_transfer_batch_get_latency",
                               "Batch Get transfer latency (us)",
                               p2p::client_metric::kLatencyBucket, labels),
          get_latency_us("mooncake_transfer_get_latency",
                         "Get transfer latency (us)",
                         p2p::client_metric::kLatencyBucket, labels),
          put_latency_us("mooncake_transfer_put_latency",
                         "Put transfer latency (us)",
                         p2p::client_metric::kLatencyBucket, labels) {}

    ylt::metric::counter_t total_read_bytes;
    ylt::metric::counter_t total_write_bytes;
    ylt::metric::histogram_t batch_put_latency_us;
    ylt::metric::histogram_t batch_get_latency_us;
    ylt::metric::histogram_t get_latency_us;
    ylt::metric::histogram_t put_latency_us;

    void serialize(std::string& str) {
        total_read_bytes.serialize(str);
        total_write_bytes.serialize(str);
        batch_put_latency_us.serialize(str);
        batch_get_latency_us.serialize(str);
        get_latency_us.serialize(str);
        put_latency_us.serialize(str);
    }

    std::string summary_metrics();
};

struct P2PMasterClientMetric {
    std::array<std::string, 1> rpc_names = {"rpc_name"};

    P2PMasterClientMetric(std::map<std::string, std::string> labels = {})
        : rpc_count("mooncake_client_rpc_count",
                    "Total number of RPC calls made by the client", labels,
                    rpc_names),
          rpc_latency("mooncake_client_rpc_latency",
                      "Latency of RPC calls made by the client (in us)",
                      p2p::client_metric::kLatencyBucket, labels, rpc_names) {}

    ylt::metric::hybrid_counter_1t rpc_count;
    ylt::metric::hybrid_histogram_1t rpc_latency;

    void serialize(std::string& str) {
        rpc_count.serialize(str);
        rpc_latency.serialize(str);
    }

    std::string summary_metrics();
};

}  // namespace mooncake
