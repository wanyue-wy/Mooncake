"""Store packaging and CLI contracts, independent of native dependencies."""

import importlib.util
import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

from mooncake import cli, cli_client

_SPEC = importlib.util.spec_from_file_location(
    "store_build", Path(__file__).resolve().parents[1] / "store_build.py")
store_build = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(store_build)


class StoreArtifactTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.identity = dict(store_version="2.0.0", architecture="p2p")

    def binary(self, name, identity=None):
        path = self.root / name
        fields = identity or self.identity
        output = name + " version " + "\n".join(f"{k}={v}" for k, v in fields.items())
        path.write_text(f"#!{sys.executable}\nprint({output!r})\n")
        path.chmod(0o755)
        return path

    def pair(self):
        (self.root / "store.so").touch()
        self.binary("mooncake_master")
        self.binary("mooncake_client")

    def validate(self):
        return store_build.validate_store_artifacts(self.root)

    def test_matching_pair_for_each_architecture(self):
        for arch in ("centralized", "p2p"):
            with self.subTest(architecture=arch):
                self.identity["architecture"] = arch
                self.pair()
                self.assertTrue(self.validate())

    def test_te_only_has_no_store_commands(self):
        self.assertFalse(self.validate())

    def test_partial_artifacts_fail(self):
        self.pair()
        for name in ("store.so", "mooncake_master", "mooncake_client"):
            with self.subTest(missing=name):
                path = self.root / name
                saved = path.read_bytes()
                path.unlink()
                with self.assertRaisesRegex(ValueError, "requires"):
                    self.validate()
                path.write_bytes(saved)
                path.chmod(0o755)

    def test_legacy_files_and_symlinks_fail(self):
        self.pair()
        for name in ("mooncake_master_p2p", "mooncake_client_p2p"):
            path = self.root / name
            path.symlink_to("missing")
            with self.assertRaisesRegex(ValueError, "Obsolete"):
                self.validate()
            path.unlink()
        (self.root / "mooncake_master").unlink()
        (self.root / "mooncake_master").symlink_to("mooncake_client")
        with self.assertRaisesRegex(ValueError, "requires"):
            self.validate()

    def test_mismatched_identity_fields_fail(self):
        self.pair()
        for field, value in dict(store_version="3.0.0", architecture="centralized").items():
            with self.subTest(field=field):
                self.binary("mooncake_client", dict(self.identity, **{field: value}))
                with self.assertRaisesRegex(ValueError, "do not match"):
                    self.validate()

    def test_failed_or_malformed_version_is_rejected(self):
        self.pair()
        binary = self.root / "mooncake_master"
        binary.write_text(f"#!{sys.executable}\nraise SystemExit(17)\n")
        with self.assertRaises(subprocess.CalledProcessError):
            self.validate()
        self.binary("mooncake_master", {"architecture": "p2p"})
        with self.assertRaisesRegex(ValueError, "store_version"):
            self.validate()
        with mock.patch.object(store_build.subprocess, "run", side_effect=
                               subprocess.TimeoutExpired("version", 10)):
            with self.assertRaises(subprocess.TimeoutExpired):
                self.validate()


class StoreWrapperTest(unittest.TestCase):
    def test_arguments_output_and_exit_status_are_preserved(self):
        for module, name in ((cli, "mooncake_master"), (cli_client, "mooncake_client")):
            with self.subTest(binary=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                wrapper = root / "wrapper.py"
                shutil.copyfile(module.__file__, wrapper)
                binary = root / name
                binary.write_text(f"#!{sys.executable}\nimport json,sys\n"
                                  "print(json.dumps(sys.argv[1:]))\n"
                                  "print('native stderr', file=sys.stderr)\n"
                                  "raise SystemExit(23)\n")
                args = ["--version", "value with spaces", "x=y"]
                result = subprocess.run([sys.executable, str(wrapper)] + args,
                                        capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 23)
                self.assertEqual(json.loads(result.stdout), args)
                self.assertEqual(result.stderr.strip(), "native stderr")

    def test_native_signal_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copyfile(cli.__file__, root / "wrapper.py")
            (root / "mooncake_master").write_text(
                f"#!{sys.executable}\nimport os,signal\nos.kill(os.getpid(), signal.SIGTERM)\n")
            result = subprocess.run([sys.executable, str(root / "wrapper.py")], timeout=10)
            self.assertEqual(result.returncode, -signal.SIGTERM)

    def test_missing_binary_logs_and_fails(self):
        with mock.patch.object(cli.os, "chmod", side_effect=FileNotFoundError("missing")):
            with self.assertLogs(level="ERROR"):
                self.assertEqual(cli.main(), 1)


if __name__ == "__main__":
    unittest.main()
