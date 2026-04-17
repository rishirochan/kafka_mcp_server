import json
import pathlib
import tomllib
import unittest


ROOT = pathlib.Path(__file__).resolve().parent.parent
PYPROJECT_PATH = ROOT / "pyproject.toml"
README_PATH = ROOT / "README.md"
SERVER_JSON_PATH = ROOT / "server.json"


class TestRegistryMetadata(unittest.TestCase):
    def test_server_json_version_matches_package_version(self):
        pyproject = tomllib.loads(PYPROJECT_PATH.read_text())
        server_metadata = json.loads(SERVER_JSON_PATH.read_text())

        package_version = pyproject["project"]["version"]
        self.assertEqual(server_metadata["version"], package_version)
        self.assertEqual(server_metadata["packages"][0]["version"], package_version)

    def test_readme_contains_mcp_name_marker(self):
        readme = README_PATH.read_text()
        self.assertIn(
            "<!-- mcp-name: io.github.rishirochan/kafka-mcp-server -->", readme
        )

    def test_server_json_name_matches_readme_marker(self):
        readme = README_PATH.read_text()
        server_metadata = json.loads(SERVER_JSON_PATH.read_text())

        self.assertIn(server_metadata["name"], readme)


if __name__ == "__main__":
    unittest.main()
