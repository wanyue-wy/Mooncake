#!/usr/bin/env python3
"""
Minimal CLI module for mooncake_master.
"""

import logging
import os
import sys


def main():
    """
    Run the selected Master binary with all command-line arguments unchanged.
    """
    # Get the path to the selected Master binary.
    package_dir = os.path.dirname(os.path.abspath(__file__))
    bin_path = os.path.join(package_dir, "mooncake_master")
    
    try:
        # Make sure the binary is executable
        os.chmod(bin_path, 0o755)

        # Replace this wrapper so exit status and signals reach the caller.
        os.execv(bin_path, [bin_path] + sys.argv[1:])
    except OSError:
        logging.exception("Failed to start %s", bin_path)
        return 1


if __name__ == "__main__":
    sys.exit(main())
