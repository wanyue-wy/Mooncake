#!/usr/bin/env python3
"""CLI entry point for the P2P Mooncake Store master."""

import os
import subprocess
import sys


def main():
    """Run the bundled mooncake_master_p2p binary."""
    package_dir = os.path.dirname(os.path.abspath(__file__))
    bin_path = os.path.join(package_dir, "mooncake_master_p2p")
    os.chmod(bin_path, 0o755)
    return subprocess.call([bin_path] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
