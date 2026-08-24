# Mooncake Store Deployment & Operations Guide

This page summarizes useful flags, environment variables, and HTTP endpoints to help advanced users tune Mooncake Master and observe metrics.

Mooncake Store installs three executables: `mooncake_master` for centralized
deployments, `mooncake_master_p2p` for P2P deployments, and the shared
`mooncake_client`. The master deployment mode is selected by the executable;
`--deployment_mode` is a client-only option.

## Master Startup Flags (with defaults)

- RPC Related
  - `--rpc_port` (int, default 50051): RPC listen port.
  - `--rpc_thread_num` (int, default min(4, CPU cores)): RPC worker threads. If not set, uses `--max_threads` (default 4) capped by CPU cores.
  - `--rpc_address` (str, default `0.0.0.0`): RPC bind address.
  - `--rpc_conn_timeout_seconds` (int, default `0`): RPC idle connection timeout; `0` disables.
  - `--rpc_enable_tcp_no_delay` (bool, default `true`): Enable TCP_NODELAY.

- Metrics
  - `--enable_metric_reporting` (bool, default `true`): Periodically log master metrics to INFO.
  - `--metrics_port` (int, default `9003`): HTTP port for `/metrics` endpoints.

- HTTP Metadata Server For Mooncake Transfer Engine
  - `--enable_http_metadata_server` (bool, default `false`): Enable embedded HTTP metadata server.
  - `--http_metadata_server_host` (str, default `0.0.0.0`): Metadata bind host.
  - `--http_metadata_server_port` (int, default `8080`): Metadata TCP port.

- Eviction and TTLs
  - `--default_kv_lease_ttl` (uint64, default `5000` ms): Default lease TTL for KV objects.
  - `--default_kv_soft_pin_ttl` (uint64, default `1800000` ms): Soft pin TTL (30 minutes).
  - `--allow_evict_soft_pinned_objects` (bool, default `true`): Allow evicting soft-pinned objects.
  - `--eviction_ratio` (double, default `0.05`): Fraction evicted when hitting high watermark.
  - `--eviction_high_watermark_ratio` (double, default `0.95`): Usage ratio to trigger eviction.

- High Availability (optional)
  - `--enable_ha` (bool, default `false`): Enable HA.
  - `--election_backend` (str, default `etcd`): Election backend. Centralized master supports `etcd`; P2P master supports `etcd` or `redis`.
  - `--etcd_endpoints` (str, default empty unless HA config): etcd endpoints, semicolon separated.
  - `--redis_endpoint` (str, default empty): P2P-only Redis endpoint for Redis-based HA, such as `10.0.0.10:6379`.
  - `--redis_username` (str, default empty): Redis ACL username for Redis-based HA.
  - `--redis_password` (str, default empty): Redis AUTH password for Redis-based HA.
  - `--redis_db_index` (int, default `0`): Redis DB index for Redis-based HA.
  - `--redis_master_view_ttl_sec` (int, default `4`): TTL for the Redis master view key.
  - `--redis_heartbeat_interval_sec` (int, default `1`): Redis leader renewal interval. It must be smaller than `--redis_master_view_ttl_sec`.
  - `--client_ttl` (int64, default `10` s): Client alive TTL after last ping (HA mode).
  - `--cluster_id` (str, default `mooncake_cluster`): Cluster ID for persistence and HA metadata isolation.
  - `--enable_oplog` (bool, default `false`): P2P-only master metadata OpLog recording.
  - `--oplog_store_type` (str, default `localfs`): OpLog backend, for example `localfs` or `redis`.
  - `--oplog_data_dir` (str, default `/tmp/mooncake_oplog`): OpLog data path for `localfs`; Redis endpoint for `redis`.
  - `--standby_snapshot_service_port` (uint32, default `0`): Port for serving P2P Standby metadata snapshots. `0` disables the snapshot service.
  - `--standby_snapshot_service_endpoint` (str, default empty): Optional advertised snapshot endpoint. If empty, it is derived from the master endpoint and snapshot service port.
  - `--standby_snapshot_sources` (str, default empty): Optional comma-separated snapshot source override. If empty, sources are discovered from Redis master registry.
  - `--standby_snapshot_chunk_size` (uint32, default `256`): Maximum metadata records per snapshot RPC chunk.

### P2P HA OpLog Coverage

The current P2P primary master records oplog entries for explicit client and segment lifecycle changes (`REGISTER_CLIENT`, `UNREGISTER_CLIENT`, `MOUNT_SEGMENT`, `UNMOUNT_SEGMENT`) and replica mapping changes (`ADD_REPLICA`, `REMOVE_REPLICA`). Remaining failover-visible metadata mutations still need follow-up coverage, including client crash cleanup, heartbeat state transitions, replica eviction/rebalance, and task metadata.

- DFS Storage (optional)
  - `--root_fs_dir` (str, default empty): DFS mount directory for storage backend, used in Multi-layer Storage Support.
  - `--global_file_segment_size` (int64, default `int64_max`): Maximum available space for DFS segments.

Example (enable embedded HTTP metadata and metrics):

```bash
mooncake_master \
  --enable_http_metadata_server=true \
  --http_metadata_server_host=0.0.0.0 \
  --http_metadata_server_port=8080 \
  --rpc_thread_num=64 \
  --metrics_port=9003 \
  --enable_metric_reporting=true
```

## Redis-based P2P Master HA

The centralized `mooncake_master` keeps the community etcd HA protocol. Redis
election and Redis-backed OpLog are available on `mooncake_master_p2p`. They are
independent from the Transfer Engine metadata backend.

Build the P2P master with Redis support:

```bash
cmake -S . -B build -DSTORE_USE_REDIS=ON
cmake --build build --target mooncake_master_p2p
```

Run multiple P2P masters with the same Redis endpoint, cluster ID, and OpLog
configuration. Each master must advertise a reachable RPC address; do not use
`0.0.0.0` as the advertised address in a multi-node deployment.

```bash
mooncake_master_p2p \
  --enable_ha=true \
  --election_backend=redis \
  --redis_endpoint=10.0.0.10:6379 \
  --redis_username=<redis_user> \
  --redis_password=<redis_password> \
  --cluster_id=p2p_mooncake_cluster \
  --enable_oplog=true \
  --oplog_store_type=redis \
  --oplog_data_dir=10.0.0.10:6379 \
  --standby_snapshot_service_port=52051 \
  --rpc_address=10.0.0.1
```

For P2P Redis HA, set `--standby_snapshot_service_port` on every master so a Standby that falls behind the Redis OpLog trim horizon can rebootstrap from another ready Standby. Use `--standby_snapshot_service_endpoint` when the advertised host or port differs from the derived endpoint. Use `--standby_snapshot_sources` only for fixed-source deployments or tests; otherwise Redis registry discovery is preferred.

Recommended test/staging flags:

```text
--redis_heartbeat_interval_sec=1
--redis_master_view_ttl_sec=4
--standby_snapshot_service_port=<unique_port_per_master>
--standby_snapshot_chunk_size=256
```

P2P clients should also use Redis master discovery and the same cluster ID:

```text
master_server_entry = "redis://10.0.0.10:6379"
redis_cluster_id = "p2p_mooncake_cluster"
redis_username = "<redis_user>"
redis_password = "<redis_password>"
deployment_mode = "P2P"
```

For `mooncake_client` in P2P mode:

```bash
mooncake_client \
  --deployment_mode=P2P \
  --metadata_server=P2PHANDSHAKE \
  --master_server_address=redis://10.0.0.10:6379 \
  --redis_cluster_id=p2p_mooncake_cluster \
  --redis_username=<redis_user> \
  --redis_password=<redis_password>
```

**Tips:**

In addition to command-line flags, the Master also supports configuration via JSON and YAML files. For example:

```bash
mooncake_master_p2p \
  --config_path=mooncake-store/conf/master.yaml 
```

## Metrics Endpoints

The master exposes Prometheus-style metrics over HTTP on `--metrics_port`:

- `GET /metrics` — Prometheus format (`text/plain; version=0.0.4`).
- `GET /metrics/summary` — Human-readable summary.

Examples:

```bash
curl -s http://<master_host>:9003/metrics
curl -s http://<master_host>:9003/metrics/summary
```

## Client/Engine Tuning (Env Vars, with defaults)

- Topology discovery (Store Client → Transfer Engine)
  - `MC_MS_AUTO_DISC` (default `1`): Auto-discover NIC/GPU topology. Set `0` to disable and provide `rdma_devices` manually.
  - `MC_MS_FILTERS` (default empty): Optional comma-separated NIC whitelist when auto-discovery is enabled (e.g., `mlx5_0,mlx5_2`).
  - If `MC_MS_AUTO_DISC=0`, pass `rdma_devices` (comma-separated) to the Python `setup(...)` call.

- Transfer Engine metrics (disabled by default)
  - `MC_TE_METRIC` (default `0`/unset): Set to `1` to enable periodic engine metrics logging. **Note:** Not supported when using Transfer Engine TENT.
  - `MC_TE_METRIC_INTERVAL_SECONDS` (default `5`): Positive integer seconds between reports (effective only if metrics enabled).

- Client metrics (enabled by default)
  - `MC_STORE_CLIENT_METRIC` (default `1`): Client-side metrics on by default; set `0` to disable entirely.
  - `MC_STORE_CLIENT_METRIC_INTERVAL` (default `0`): Reporting interval in seconds; `0` collects but does not periodically report.

- Local memcpy optimization (Store transfer path)
  - `MC_STORE_MEMCPY` (default `0`/false): Set to `1` to prefer local memcpy when source/destination are on the same client.

## Set the Log Level for yalantinglibs coro_rpc and coro_http
By default, the log level is set to warning. You can customize it using the following environment variable:

`export MC_YLT_LOG_LEVEL=info`

This sets the log level for yalantinglibs (including coro_rpc and coro_http) to info.

Available log levels: trace, debug, info, warn (or warning), error, and critical.

## Quick Tips

- Scale `--rpc_thread_num` with available CPU cores and workload.
- Start with default eviction settings; adjust `--eviction_high_watermark_ratio` and `--eviction_ratio` based on memory pressure and object churn.
- Use `/metrics/summary` during bring-up; integrate `/metrics` with Prometheus/Grafana for production.
