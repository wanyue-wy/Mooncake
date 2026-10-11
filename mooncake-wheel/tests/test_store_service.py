"""HTTP service startup, retry and cleanup contracts for both bindings."""

import asyncio
import importlib
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

with mock.patch.dict("sys.modules", {"mooncake.store": types.SimpleNamespace(MooncakeDistributedStore=object)}):
    service_module = importlib.import_module("mooncake.mooncake_store_service")


class StoreServiceTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config_path = Path(self.temp.name) / "config.json"
        self.config = dict(local_hostname="localhost", metadata_server="P2PHANDSHAKE",
                           master_server_address="localhost:50051")
        self.config_path.write_text(json.dumps(self.config))

    def service(self, p2p=False, overrides=None):
        constructor = mock.Mock(spec=["setup_p2p_real_client"] if p2p else ["setup"])
        with mock.patch.object(service_module, "MooncakeDistributedStore", constructor):
            return service_module.MooncakeStoreService(str(self.config_path), overrides)

    async def test_centralized_setup_and_typed_cli_overrides(self):
        service = self.service(overrides={"global_segment_size": "1gb", "heartbeat_rpc_port": "5123"})
        store = mock.Mock()
        store.setup.return_value = 0
        with mock.patch.object(service_module, "MooncakeDistributedStore", return_value=store):
            self.assertTrue(await service.start_store_service())
        self.assertEqual(store.setup.call_args.args[2], 1024 ** 3)
        self.assertEqual(store.setup.call_args.kwargs["heartbeat_rpc_port"], 5123)
        await service.stop()
        await service.stop()
        store.close.assert_called_once()

    async def test_p2p_native_dict_and_nested_json(self):
        config = dict(local_hostname="localhost", metadata_server="P2PHANDSHAKE",
                      master_server_addr="localhost:50051", tiered_backend_config={"tiers": []})
        self.config_path.write_text(json.dumps(config))
        service = self.service(p2p=True, overrides={"client_rpc_port": "23456"})
        store = mock.Mock()
        store.setup_p2p_real_client.return_value = 0
        with mock.patch.object(service_module, "MooncakeDistributedStore", return_value=store):
            self.assertTrue(await service.start_store_service())
        passed = store.setup_p2p_real_client.call_args.args[0]
        self.assertEqual(passed["client_rpc_port"], "23456")
        self.assertEqual(json.loads(passed["tiered_backend_config"]), {"tiers": []})
        self.assertNotIn("global_segment_size", passed)
        store.setup.assert_not_called()
        await service.stop()

    async def test_p2p_config_path_environment(self):
        constructor = mock.Mock(spec=["setup_p2p_real_client"])
        with mock.patch.object(service_module, "MooncakeDistributedStore", constructor), \
             mock.patch.dict("os.environ", {"MOONCAKE_CONFIG_PATH": str(self.config_path)}):
            service = service_module.MooncakeStoreService()
        self.assertEqual(service.config["local_hostname"], "localhost")

    async def test_configuration_errors_are_immediate(self):
        for text in ("{invalid", "[]"):
            self.config_path.write_text(text)
            with self.assertLogs(level="ERROR"), self.assertRaises(ValueError):
                self.service(p2p=True)
        self.config_path.write_text(json.dumps(self.config))
        with self.assertLogs(level="ERROR"), self.assertRaises(ValueError):
            self.service(overrides={"heartbeat_rpc_port": "bad"})
        with self.assertLogs(level="ERROR"), self.assertRaises(ValueError):
            self.service(overrides={"unknown": "value"})

    async def test_setup_exception_closes_without_retry(self):
        service = self.service()
        store = mock.Mock()
        store.setup.side_effect = TypeError("bad interface")
        with mock.patch.object(service_module, "MooncakeDistributedStore", return_value=store) as make, \
             mock.patch.object(service_module.asyncio, "sleep", new_callable=mock.AsyncMock) as sleep:
            with self.assertLogs(level="ERROR"), self.assertRaises(TypeError):
                await service.start_store_service()
            make.assert_called_once()
            sleep.assert_not_awaited()
        store.close.assert_called_once()
        self.assertIsNone(service.store)

    async def test_returned_error_closes_before_retry(self):
        service = self.service()
        first, second = mock.Mock(), mock.Mock()
        first.setup.return_value, second.setup.return_value = -1, 0
        def create():
            if not first.setup.called:
                return first
            first.close.assert_called_once()
            return second
        with mock.patch.object(service_module, "MooncakeDistributedStore", side_effect=create), \
             mock.patch.object(service_module.asyncio, "sleep", new_callable=mock.AsyncMock):
            with self.assertLogs(level="ERROR"):
                self.assertTrue(await service.start_store_service())
        await service.stop()
        second.close.assert_called_once()

    async def test_retry_deadline_and_invalid_deadlines(self):
        service = self.service()
        store = mock.Mock()
        store.setup.return_value = -1
        with mock.patch.object(service_module, "MooncakeDistributedStore", return_value=store), \
             mock.patch.object(service_module.time, "monotonic", side_effect=[0, 0, 2, 2]):
            with self.assertLogs(level="ERROR"):
                self.assertFalse(await service.start_store_service(1))
        store.close.assert_called_once()
        for limit in (0, -1, float("nan"), float("inf")):
            with self.assertLogs(level="ERROR"), self.assertRaises(ValueError):
                await service.start_store_service(limit)

    async def test_http_failure_and_cancellation_cleanup(self):
        for error in (OSError("port occupied"), asyncio.CancelledError()):
            service = self.service()
            store = service.store = mock.Mock()
            runner = mock.Mock(setup=mock.AsyncMock(), cleanup=mock.AsyncMock())
            site = mock.Mock(start=mock.AsyncMock(side_effect=error))
            with mock.patch.object(service_module.web, "AppRunner", return_value=runner), \
                 mock.patch.object(service_module.web, "TCPSite", return_value=site):
                with self.assertLogs(level="ERROR"), self.assertRaises(type(error)):
                    await service.start_http_service()
            runner.cleanup.assert_awaited_once()
            store.close.assert_called_once()
            await service.stop()
            store.close.assert_called_once()

    async def test_cleanup_error_still_closes_store(self):
        service = self.service()
        store = service.store = mock.Mock()
        service.runner = mock.Mock(cleanup=mock.AsyncMock(side_effect=RuntimeError("cleanup")))
        with self.assertLogs(level="ERROR"), self.assertRaises(RuntimeError):
            await service.stop()
        store.close.assert_called_once()

    async def test_cancelling_run_cleans_service(self):
        service = mock.Mock(start_store_service=mock.AsyncMock(return_value=True),
                            start_http_service=mock.AsyncMock(), stop=mock.AsyncMock())
        args = types.SimpleNamespace(define=[], config=None, max_wait_time=1, port=0)
        with mock.patch.object(service_module, "MooncakeStoreService", return_value=service):
            task = asyncio.create_task(service_module._run(args))
            await asyncio.sleep(0)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        service.stop.assert_awaited_once()


class StoreConsoleTest(unittest.TestCase):
    def test_main_is_synchronous_and_reports_failure(self):
        self.assertFalse(asyncio.iscoroutinefunction(service_module.main))
        with mock.patch.object(service_module, "parse_arguments", return_value=object()), \
             mock.patch.object(service_module, "_run", new_callable=mock.AsyncMock) as run:
            self.assertEqual(service_module.main(), 0)
            run.side_effect = ValueError("invalid config")
            with self.assertLogs(level="ERROR"):
                self.assertEqual(service_module.main(), 1)


if __name__ == "__main__":
    unittest.main()
