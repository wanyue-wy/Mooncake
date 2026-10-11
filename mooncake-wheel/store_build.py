"""Validate native Store artifacts before adding them to a wheel."""

import os
from pathlib import Path
import re
import subprocess


def binary_identity(binary):
    result = subprocess.run([str(binary), "--version"], check=True,
                            capture_output=True, text=True, timeout=10)
    identity = {}
    for key in ("store_version", "architecture"):
        matches = re.findall(r"(?:^| version )" + key + r"=([^\n]+)$",
                             result.stdout, re.MULTILINE)
        if len(matches) != 1:
            raise ValueError(f"Invalid {key} in {binary} --version")
        identity[key] = matches[0]
    if identity["architecture"] not in ("centralized", "p2p"):
        raise ValueError(f"Invalid Store architecture in {binary}")
    return identity


def validate_store_artifacts(package_dir):
    package_dir = Path(package_dir)
    for name in ("mooncake_master_p2p", "mooncake_client_p2p"):
        if os.path.lexists(package_dir / name):
            raise ValueError(f"Obsolete Store binary in wheel staging: {name}")
    files = [package_dir / name for name in
             ("store.so", "mooncake_master", "mooncake_client")]
    if not any(os.path.lexists(path) for path in files):
        return False
    if not all(path.is_file() and not path.is_symlink() for path in files):
        raise ValueError("Store wheel requires store.so and matching real Master and Client binaries")
    master, client = (binary_identity(path) for path in files[1:])
    if master != client:
        raise ValueError("Store Master and Client build identities do not match")
    return True
