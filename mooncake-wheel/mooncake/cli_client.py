#!/usr/bin/env python3
"""
Minimal CLI module for mooncake_client.
"""

import os
import sys
import subprocess


def main(binary_name="mooncake_client"):
    """
    Run the selected Client binary with all command-line arguments unchanged.
    """
    # Get the path to the selected Client binary.
    package_dir = os.path.dirname(os.path.abspath(__file__))
    bin_path = os.path.join(package_dir, binary_name)

    # Make sure the binary is executable
    os.chmod(bin_path, 0o755)

    # Run the binary with all arguments passed through
    return subprocess.call([bin_path] + sys.argv[1:])


def p2p_main():
    return main("mooncake_client_p2p")


if __name__ == "__main__":
    sys.exit(main())
