import pathlib
import json
import unittest


ROOT = pathlib.Path(__file__).resolve().parent.parent
README_PATH = ROOT / "README.md"
SERVER_JSON_PATH = ROOT / "server.json"


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

    def test_server_json_mentions_byok_setting(self):
        server_manifest = json.loads(SERVER_JSON_PATH.read_text())
        env_vars = server_manifest["packages"][0]["environmentVariables"]
        env_var_names = {entry["name"] for entry in env_vars}
        self.assertIn("REQUIRE_BYOK", env_var_names)


if __name__ == "__main__":
    unittest.main()
