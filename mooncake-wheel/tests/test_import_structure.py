#!/usr/bin/env python3
import os
import unittest

class TestImportStructure(unittest.TestCase):

    def test_new_import_structure(self):
        """Test that the new import structure works."""
        import mooncake.engine

        # Verify the module exists
        self.assertIsNotNone(mooncake.engine)

        # Verify direct access to TransferEngine
        self.assertIsNotNone(mooncake.engine.TransferEngine)

        # Verify direct access to TransferOpcode
        self.assertIsNotNone(mooncake.engine.TransferOpcode)

        from mooncake.store import MooncakeDistributedStore, STORE_BACKEND

        # Just verify we can create instances
        store = MooncakeDistributedStore()

        self.assertIsNotNone(store)
        self.assertEqual(STORE_BACKEND, os.environ.get("STORE_BACKEND", "centralized"))
        self.assertTrue(hasattr(store, "setup"))
        self.assertTrue(hasattr(store, "setup_p2p_real_client"))

        backend = os.environ.get("STORE_BACKEND", "centralized")
        if backend == "p2p":
            result = store.setup({})
        else:
            result = store.setup_p2p_real_client({})
        self.assertEqual(result, -600)

    def test_direct_import(self):
        """Test direct import of specific components."""
        from mooncake.engine import TransferEngine, TransferOpcode

        # Verify direct imports work
        self.assertIsNotNone(TransferEngine)
        self.assertIsNotNone(TransferOpcode)

if __name__ == '__main__':
    unittest.main()
