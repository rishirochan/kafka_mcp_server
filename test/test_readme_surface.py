import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parent.parent
README_PATH = ROOT / "README.md"


TOOLS = [
    "configure_kafka",
    "disconnect_kafka",
    "get_topics",
    "describe_topic",
    "get_partitions",
    "is_topic_exists",
    "create_topic",
    "delete_topic",
    "publish",
    "consume",
    "list_schemas",
    "get_schema",
    "register_schema",
    "delete_schema",
]

RESOURCES = [
    "kafka://topics",
    "kafka://topics/{topic_name}",
    "kafka://schemas",
]

PROMPTS = [
    "inspect-topic",
    "register-and-publish",
]


class TestReadmeSurface(unittest.TestCase):
    def test_readme_mentions_all_tools_resources_and_prompts(self):
        readme = README_PATH.read_text()

        for name in TOOLS + RESOURCES + PROMPTS:
            with self.subTest(name=name):
                self.assertIn(f"`{name}`", readme)

    def test_readme_mentions_byok_setting(self):
        readme = README_PATH.read_text()
        self.assertIn("`REQUIRE_BYOK`", readme)


if __name__ == "__main__":
    unittest.main()
