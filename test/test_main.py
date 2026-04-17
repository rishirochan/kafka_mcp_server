import os
import sys
import unittest
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.main import should_auto_start_kafka


class TestShouldAutoStartKafka(unittest.TestCase):
    @patch.dict(os.environ, {}, clear=False)
    def test_defaults_to_disabled(self):
        os.environ.pop("AUTO_START_LOCAL_KAFKA", None)
        self.assertFalse(should_auto_start_kafka())

    @patch.dict(os.environ, {"AUTO_START_LOCAL_KAFKA": "true"})
    def test_accepts_true(self):
        self.assertTrue(should_auto_start_kafka())

    @patch.dict(os.environ, {"AUTO_START_LOCAL_KAFKA": "YES"})
    def test_accepts_yes_case_insensitively(self):
        self.assertTrue(should_auto_start_kafka())

    @patch.dict(os.environ, {"AUTO_START_LOCAL_KAFKA": "0"})
    def test_rejects_falsey_values(self):
        self.assertFalse(should_auto_start_kafka())


if __name__ == "__main__":
    unittest.main()
