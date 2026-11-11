import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import train
from src.scorer import Model


class ModelTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        cls.path = Path(cls.directory.name)
        with patch.object(train, "MODELS", cls.path):
            train.train()
        cls.first = json.loads((cls.path / "latest.json").read_text())

    @classmethod
    def tearDownClass(cls):
        cls.directory.cleanup()

    def setUp(self):
        (self.path / "latest.json").write_text(json.dumps(self.first))
        self.model = Model(self.path)
        self.model.reload()

    def test_score_contract(self):
        row = {"amount": 100, "us_state": "XX", "merch": "new", "cat_id": "new"}
        score = self.model.score(row)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 1)
        self.assertEqual(score, self.model.score({**row, "target": 1}))

    def test_unchanged_model(self):
        self.assertFalse(self.model.reload())

    def test_hot_reload(self):
        with patch.object(train, "MODELS", self.path):
            train.train()
        self.assertTrue(self.model.reload())
        self.assertNotEqual(self.first["version"], self.model.version)

    def test_broken_model_keeps_previous(self):
        manifest = {"version": "broken", "file": "missing.cbm"}
        (self.path / "latest.json").write_text(json.dumps(manifest))
        with self.assertRaises(Exception):
            self.model.reload()
        self.assertEqual(self.model.version, self.first["version"])
        self.assertGreaterEqual(self.model.score({"amount": 10, "us_state": "CA", "merch": "x", "cat_id": "x"}), 0)


if __name__ == "__main__":
    unittest.main()
