#!/usr/bin/env python3
"""Diff audit script for the P2P-Mooncake-Store split (see p2p-split-plan.md).

Compares `git diff --name-status <base> <head>` over mooncake-store/ and
mooncake-integration/ against the file disposition table (§4 of the plan).

Reports:
  - VIOLATIONS: changed files not covered by any disposition category
  - MISSING:    files listed in the disposition table but not changed
  - D-entries:  deletions vs the allowed set (segment trio until Phase 3)
  - RENAMES:    rename entries (informational, classified by new path)

Exit code 0 iff there are no violations and no unexpected deletions.

Usage:
  python3 scripts/p2p_split_diff_audit.py [--base a00f757] [--head HEAD]
"""

import argparse
import fnmatch
import subprocess
import sys
from collections import defaultdict

SCOPE = ["mooncake-store", "mooncake-integration"]

# ---------------------------------------------------------------------------
# Disposition table (§4 of p2p-split-plan.md, Phase 0 confirmed edition).
# Patterns are fnmatch globs against repo-relative paths.
# ---------------------------------------------------------------------------

# §4.A — P2P-only files, pure move into mooncake-store/{include,src}/p2p/.
CAT_A = [
    # post-move layout (Phase 1+)
    "mooncake-store/include/p2p/*",
    "mooncake-store/src/p2p/*",
    # pre-move layout (kept so older refs still classify)
    # master side
    "mooncake-store/include/p2p_master_*.h",
    "mooncake-store/include/p2p_rpc_service.h",
    "mooncake-store/include/p2p_rpc_types.h",
    "mooncake-store/include/p2p_segment_manager.h",
    "mooncake-store/src/p2p_master_*.cpp",
    "mooncake-store/src/p2p_rpc_service.cpp",
    "mooncake-store/src/p2p_segment_manager.cpp",
    # client side
    "mooncake-store/include/p2p_client_*.h",
    "mooncake-store/src/p2p_client_*.cpp",
    "mooncake-store/include/peer_client.h",
    "mooncake-store/src/peer_client.cpp",
    "mooncake-store/include/client_rpc_service.h",
    "mooncake-store/src/client_rpc_service.cpp",
    "mooncake-store/include/client_rpc_types.h",
    "mooncake-store/include/data_manager.h",
    "mooncake-store/src/data_manager.cpp",
    "mooncake-store/include/route_cache.h",
    "mooncake-store/src/route_cache.cpp",
    "mooncake-store/include/inflight_tracker.h",
    "mooncake-store/include/async_memcpy_executor.h",
    "mooncake-store/src/async_memcpy_executor.cpp",
    "mooncake-store/include/async_metadata_notifier.h",
    "mooncake-store/src/async_metadata_notifier.cpp",
    "mooncake-store/include/client_metrics_aggregator.h",
    "mooncake-store/src/client_metrics_aggregator.cpp",
    "mooncake-store/include/runtime_config_store.h",
    "mooncake-store/src/runtime_config_store.cpp",
    "mooncake-store/include/task_handle.h",
    "mooncake-store/include/heartbeat_type.h",
    # HA (oplog dir + standalone redis/standby/metric files)
    "mooncake-store/include/ha/oplog/*",
    "mooncake-store/src/ha/oplog/*",
    "mooncake-store/include/redis_election_helper.h",
    "mooncake-store/src/redis_election_helper.cpp",
    "mooncake-store/include/redis_master_view_helper.h",
    "mooncake-store/src/redis_master_view_helper.cpp",
    "mooncake-store/include/redis_util.h",
    "mooncake-store/src/redis_util.cpp",
    "mooncake-store/include/standby_state_machine.h",
    "mooncake-store/src/standby_state_machine.cpp",
    "mooncake-store/include/ha_metric_manager.h",
    "mooncake-store/src/ha_metric_manager.cpp",
    "mooncake-store/include/ha_recovery_manager.h",
    "mooncake-store/src/ha_recovery_manager.cpp",
    # tiered storage
    "mooncake-store/include/tiered_cache/*",
    "mooncake-store/src/tiered_cache/*",
    # common (P2P-only after rollback); metadata_store.h moved to p2p/ha/
    # post-review (standby/oplog-specific). base64.h returned to the shared
    # utils/ layer post-review (generic helper, fork-added -> conflict-free).
    "mooncake-store/include/metadata_store.h",
    "mooncake-store/include/p2p/common/metadata_store.h",
    # config (kept in conf/ until Phase 4; attribution-only for now)
    "mooncake-store/conf/p2p_runtime_config.json",
    "mooncake-store/conf/tiered_backend*.json",
    "mooncake-store/conf/tiered_backend_config_example.md",
]

# §4.K — stable client entry/common implementation kept in fork form. The
# public ClientService interface remains shared, while protocol-specific master
# clients are split in Phase 3 route A.
CAT_K_CLIENT = [
    "mooncake-store/include/client_service.h",
    "mooncake-store/src/client_service.cpp",
    "mooncake-store/include/client_service_base.h",
    "mooncake-store/src/client_service_base.cpp",
    "mooncake-store/include/centralized_client_service.h",
    "mooncake-store/src/centralized_client_service.cpp",
    "mooncake-store/include/centralized_master_rpc_adapter.*",
    "mooncake-store/src/centralized_master_rpc_adapter.*",
    "mooncake-store/include/client_metric.h",
    "mooncake-store/src/client_metric.cpp",
    "mooncake-store/include/ha_helper.h",
    "mooncake-store/src/ha_helper.cpp",
]

# §4.B — base classes: centralized side restored to a00f757 (M files);
# P2P side merges base logic in Phase 2.
CAT_B_BASE = [
    "mooncake-store/include/master_service.h",
    "mooncake-store/src/master_service.cpp",
    "mooncake-store/include/client_service.h",
    "mooncake-store/src/client_service.cpp",
    "mooncake-store/include/master_client.h",
    "mooncake-store/src/master_client.cpp",
    "mooncake-store/include/rpc_service.h",
    "mooncake-store/src/rpc_service.cpp",
    "mooncake-store/include/master_metric_manager.h",
    "mooncake-store/src/master_metric_manager.cpp",
    "mooncake-store/include/client_metric.h",
    "mooncake-store/src/client_metric.cpp",
]

# §4.B/§4.E — fork-added centralized subclasses + base shells: deleted in
# Phase 3 (all are post-a00f757 additions, so deletion leaves no diff).
CAT_B_SUBCLASS_DELETE = [
    "mooncake-store/include/centralized_*.h",
    "mooncake-store/src/centralized_*.cpp",
    "mooncake-store/include/client_manager.h",
    "mooncake-store/src/client_manager.cpp",
    "mooncake-store/include/client_meta.h",
    "mooncake-store/src/client_meta.cpp",
    "mooncake-store/include/segment_manager.h",
    "mooncake-store/src/segment_manager.cpp",
    # their tests disappear with them (§4.E)
    "mooncake-store/tests/centralized_*_test.cpp",
    "mooncake-store/tests/centralized_segment_manager_test.cpp",
]

# §4.C — entry adaptation (diff allowed, external interfaces unchanged).
CAT_C = [
    "mooncake-store/src/real_client_main.cpp",
    "mooncake-store/include/real_client.h",
    "mooncake-store/src/real_client.cpp",
    "mooncake-store/include/dummy_client.h",
    "mooncake-store/src/dummy_client.cpp",
    "mooncake-store/include/pyclient.h",
    "mooncake-store/include/client_config_builder.h",
    "mooncake-store/include/types.h",  # minimal additive (DeploymentMode)
    "mooncake-store/src/CMakeLists.txt",
    "mooncake-store/tests/CMakeLists.txt",
    "mooncake-store/CMakeLists.txt",
    "mooncake-store/benchmarks/CMakeLists.txt",
    "mooncake-store/benchmarks/master_bench.cpp",
    "mooncake-integration/store/store_py.cpp",
    "mooncake-integration/CMakeLists.txt",
]

# §4.D — shared leaf files, converge to a00f757 file-by-file in Phase 4.
# Note: replica.cpp did NOT exist at a00f757 (replica.h was header-only);
# the fork-added .cpp stays a shared leaf (doc correction, Phase 0 audit).
CAT_D = [
    "mooncake-store/include/allocator.h",
    "mooncake-store/src/allocator.cpp",
    "mooncake-store/include/client_buffer.hpp",
    "mooncake-store/src/client_buffer.cpp",
    "mooncake-store/include/utils.h",
    "mooncake-store/src/utils.cpp",
    "mooncake-store/src/types.cpp",
    "mooncake-store/include/thread_pool.h",
    "mooncake-store/include/mutex.h",
    "mooncake-store/include/replica.h",
    "mooncake-store/src/replica.cpp",
    "mooncake-store/include/storage_backend.h",
    "mooncake-store/src/storage_backend.cpp",
    "mooncake-store/include/file_storage.h",
    "mooncake-store/src/file_storage.cpp",
    "mooncake-store/include/ha_helper.h",
    "mooncake-store/src/ha_helper.cpp",
    "mooncake-store/include/rpc_types.h",
    "mooncake-store/include/master_config.h",
    "mooncake-store/include/offset_allocator/offset_allocator.hpp",
    "mooncake-store/include/cachelib_memory_allocator/SlabAllocator.h",
    # fork-added generic helper, kept in the shared layer (post-review)
    "mooncake-store/include/utils/base64.h",
]

# §4.E — deletions of post-a00f757 files (subset of CAT_B_SUBCLASS_DELETE).
CAT_E = [
    "mooncake-store/conf/centralized_runtime_config.json",
    # eviction_strategy.h / transfer_task.{h,cpp} + their tests are unchanged
    # since a00f757, hence absent from this diff; deletion (Phase 3/4) keeps
    # them absent. Confirmed dead code in Phase 0.
]

# conf files modified for P2P keys (DoD (d): additive conf changes).
CAT_CONF = [
    "mooncake-store/conf/master.json",
    "mooncake-store/conf/master.yaml",
]

# §4.G — tests moving to tests/p2p/ (Phase 1 C4).
CAT_G_P2P = [
    # post-move layout (Phase 1+): whole tests/p2p/ subtree
    "mooncake-store/tests/p2p/*",
    "mooncake-store/tests/p2p_*",
    "mooncake-store/tests/peer_client*",
    "mooncake-store/tests/tiered_backend_test.cpp",
    "mooncake-store/tests/storage_tier_test.cpp",
    "mooncake-store/tests/ascend_tier_test.cpp",
    "mooncake-store/tests/scheduler_integration_test.cpp",
    "mooncake-store/tests/multi_lru_test.cpp",
    "mooncake-store/tests/event_driven_*",
    "mooncake-store/tests/bounded_dedup_queue_test.cpp",
    "mooncake-store/tests/tinylfu_sketch_test.cpp",
    "mooncake-store/tests/tier_role_resolver_test.cpp",
    "mooncake-store/tests/data_manager_test.cpp",
    "mooncake-store/tests/route_cache_test.cpp",
    "mooncake-store/tests/inflight_tracker_test.cpp",
    "mooncake-store/tests/client_rpc_service_test.cpp",
    "mooncake-store/tests/runtime_config_store_test.cpp",
    "mooncake-store/tests/async_metadata_notifier_test.cpp",
    "mooncake-store/tests/client_metrics_aggregator_test.cpp",
    "mooncake-store/tests/client_http_metrics_test.cpp",
    "mooncake-store/tests/redis_election_helper_test.cpp",
    "mooncake-store/tests/redis_test_utils.h",
    "mooncake-store/tests/ha/*",
    "mooncake-store/tests/ha_integration_test.cpp",
    "mooncake-store/tests/ha_recovery_manager_test.cpp",
    "mooncake-store/tests/test_p2p_server_helpers.h",
    "mooncake-store/tests/e2e/redis_chaos_test.cpp",
    "mooncake-store/tests/stress_single_workload_runner.py",
]

# §4.G — tests restored to the a00f757 version in Phase 3/4.
CAT_G_RESTORE = [
    "mooncake-store/tests/master_service_test.cpp",
    "mooncake-store/tests/master_service_ssd_test.cpp",
    "mooncake-store/tests/master_metrics_test.cpp",
    "mooncake-store/tests/client_integration_test.cpp",
    "mooncake-store/tests/allocation_strategy_test.cpp",
    "mooncake-store/tests/buffer_allocator_test.cpp",
    "mooncake-store/tests/storage_backend_test.cpp",
    "mooncake-store/tests/file_storage_test.cpp",
    "mooncake-store/tests/ipv6_client_test.cpp",
    "mooncake-store/tests/non_ha_reconnect_test.cpp",
    "mooncake-store/tests/cxl_client_integration_test.cpp",
    "mooncake-store/tests/stress_workload_test.cpp",
    "mooncake-store/tests/pybind_client_test.cpp",
    "mooncake-store/tests/utils_test.cpp",
    "mooncake-store/tests/test_server_helpers.h",
    # e2e harness: restored to a00f757 semantics except binary-name switching
    # (Phase 4).
    "mooncake-store/tests/e2e/CMakeLists.txt",
    "mooncake-store/tests/e2e/client_wrapper.cpp",
    "mooncake-store/tests/e2e/client_wrapper.h",
    "mooncake-store/tests/e2e/clientctl.cpp",
    "mooncake-store/tests/e2e/process_handler.cpp",
    "mooncake-store/tests/e2e/process_handler.h",
]

# Phase 0 confirmed: entry-layer tests added after a00f757; kept in place
# (no P2P-internal dependency), listed in the additive whitelist.
CAT_G_KEEP = [
    "mooncake-store/tests/real_client_remount_test.cpp",
    "mooncake-store/tests/shm_helper_test.cpp",
    "mooncake-store/tests/client_config_builder_test.cpp",
    "mooncake-store/tests/utils/common.h",  # additive InitTieredBackendForTest
    # Entry/e2e tests with direct includes after ClientService header narrowing.
    "mooncake-store/tests/e2e/chaos_test.cpp",
    "mooncake-store/tests/e2e/chaos_rand_test.cpp",
]

# Pending Phase 2 decisions (tracked, not violations).
CAT_PENDING = [
    # Belongs to B-9 client_metric rework; placement decided when metrics are
    # made self-contained.
    "mooncake-store/tests/client_metrics_test.cpp",
]

# §4.B-7 — deleted by the fork, restored to a00f757 in Phase 3; appears as D
# in the diff until then.
CAT_RESTORE_PENDING = [
    "mooncake-store/include/segment.h",
    "mooncake-store/src/segment.cpp",
    "mooncake-store/tests/segment_test.cpp",
]

# Deletions allowed to appear as 'D' in the diff (Phase 3 restores them).
ALLOWED_D = {
    "mooncake-store/include/segment.h",
    "mooncake-store/src/segment.cpp",
    "mooncake-store/tests/segment_test.cpp",
}

CATEGORIES = [
    ("A (move to p2p/)", CAT_A),
    ("K (stable client entry/common base)", CAT_K_CLIENT),
    ("restore-pending (segment trio, back in Phase 3)", CAT_RESTORE_PENDING),
    ("B-base (restore a00f757 + merge into P2P)", CAT_B_BASE),
    ("B-sub/E (fork subclasses, delete in Phase 3)", CAT_B_SUBCLASS_DELETE),
    ("C (entry adaptation)", CAT_C),
    ("D (leaf convergence)", CAT_D),
    ("E (delete)", CAT_E),
    ("conf additive", CAT_CONF),
    ("G-p2p (tests move to tests/p2p/)", CAT_G_P2P),
    ("G-restore (tests back to a00f757)", CAT_G_RESTORE),
    ("G-keep (entry-layer tests whitelist)", CAT_G_KEEP),
    ("PENDING (Phase 2 decision)", CAT_PENDING),
]


def match_category(path):
    for name, patterns in CATEGORIES:
        for pat in patterns:
            if fnmatch.fnmatch(path, pat):
                return name, pat
    return None, None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="a00f757")
    parser.add_argument("--head", default="HEAD")
    args = parser.parse_args()

    cmd = ["git", "diff", "--name-status", args.base, args.head, "--"] + SCOPE
    out = subprocess.check_output(cmd, text=True)

    entries = []  # (status, path, old_path_or_None)
    for line in out.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        status = parts[0]
        if status.startswith("R") or status.startswith("C"):
            entries.append((status, parts[2], parts[1]))
        else:
            entries.append((status, parts[1], None))

    violations = []
    deletions = []
    renames = []
    by_cat = defaultdict(list)
    for status, path, old in entries:
        cat, pat = match_category(path)
        if cat is None:
            violations.append((status, path))
            continue
        by_cat[cat].append((status, path))
        if status == "D":
            deletions.append(path)
        if old is not None:
            renames.append((status, old, path))

    # Missing check: explicit additive/kept files that must remain in the final
    # diff. Restored/deleted categories are intentionally absent from the diff
    # after Phase 3 and therefore are not missing expectations.
    STABLE_CATEGORIES = {
        "K (stable client entry/common base)",
        "C (entry adaptation)",
        "D (leaf convergence)",
        "conf additive",
        "G-keep (entry-layer tests whitelist)",
        "PENDING (Phase 2 decision)",
    }
    expected_changed = set()
    for name, patterns in CATEGORIES:
        if name not in STABLE_CATEGORIES:
            continue
        for pat in patterns:
            if "*" not in pat and "?" not in pat:
                expected_changed.add(pat)
    changed_paths = {p for _, p, _ in entries} | {o for _, _, o in entries if o}
    missing = sorted(p for p in expected_changed if p not in changed_paths)

    # A deletion of an old path that matches an A-category pattern is the
    # residue of a move whose rename detection fell below the similarity
    # threshold (paired with an A entry at the new p2p/ path) - acceptable.
    def moved_source(path):
        return any(fnmatch.fnmatch(path, pat) for pat in CAT_A)

    unexpected_d = [p for p in deletions
                    if p not in ALLOWED_D and not moved_source(p)]

    print("=" * 72)
    print(f"P2P split diff audit: {args.base} -> {args.head}")
    print(f"Scope: {' + '.join(SCOPE)}; changed entries: {len(entries)}")
    print("=" * 72)
    for name, _ in CATEGORIES:
        hits = by_cat.get(name, [])
        if hits:
            print(f"  [{len(hits):3d}] {name}")
    print("-" * 72)

    if renames:
        print(f"RENAMES ({len(renames)}):")
        for status, old, new in renames:
            print(f"  {status}\t{old} -> {new}")
        print("-" * 72)

    if missing:
        print(f"MISSING vs disposition table ({len(missing)}):")
        for p in missing:
            print(f"  {p}")
        print("-" * 72)

    if violations:
        print(f"VIOLATIONS ({len(violations)}):")
        for status, p in violations:
            print(f"  {status}\t{p}")
        print("-" * 72)

    if unexpected_d:
        print(f"UNEXPECTED DELETIONS ({len(unexpected_d)}):")
        for p in unexpected_d:
            print(f"  {p}")
        print("-" * 72)
    elif deletions:
        print(f"Deletions OK (allowed set): {deletions}")

    ok = not violations and not unexpected_d
    print(f"RESULT: {'PASS (0 violations)' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
