import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import zed_mcp


class ZedMcpTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.root_patch = patch.object(zed_mcp, "get_root", return_value=self.workspace)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.addCleanup(self.temp.cleanup)

    def test_catalog_has_28_unique_tools(self):
        names = [tool["name"] for tool in zed_mcp.TOOLS]
        self.assertEqual(len(names), 28)
        self.assertEqual(len(set(names)), 28)
        for name in ("delete_file", "apply_patch", "git_add", "git_commit"):
            self.assertIn(name, names)

    def test_zed_active_folder_reads_latest_workspace_from_sqlite(self):
        older_ws = self.root / "older-project"
        newer_ws = self.root / "newer-project"
        older_ws.mkdir()
        newer_ws.mkdir()

        db_dir = self.root / "Zed" / "db" / "0-stable"
        db_dir.mkdir(parents=True)
        db_path = db_dir / "db.sqlite"
        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "CREATE TABLE workspaces (workspace_id INTEGER PRIMARY KEY, paths TEXT, timestamp TEXT)"
            )
            conn.execute(
                "INSERT INTO workspaces (paths, timestamp) VALUES (?, ?)",
                (str(older_ws), "2026-11-18 10:00:00"),
            )
            conn.execute(
                "INSERT INTO workspaces (paths, timestamp) VALUES (?, ?)",
                (f'["{newer_ws.as_posix()}"]', "2026-11-18 11:00:00"),
            )
            conn.commit()
        finally:
            conn.close()

        with patch.object(zed_mcp, "_zed_running", return_value=True), patch.object(
            zed_mcp, "_zed_db_paths", return_value=[db_path]
        ), patch.dict("os.environ", {"ZED_WORKSPACE_PATH": ""}):
            active = zed_mcp._zed_active_folder()

        self.assertEqual(active, newer_ws)

    def test_zed_active_folder_returns_none_when_zed_not_running(self):
        with patch.object(zed_mcp, "_zed_running", return_value=False), patch.dict(
            "os.environ", {"ZED_WORKSPACE_PATH": ""}
        ):
            self.assertIsNone(zed_mcp._zed_active_folder())

    def test_zed_workspace_path_env_override(self):
        with patch.dict("os.environ", {"ZED_WORKSPACE_PATH": str(self.workspace)}):
            self.assertEqual(zed_mcp._zed_active_folder(), self.workspace)

    def test_delete_file_refuses_unsafe_targets(self):
        zed_mcp.create_file("notes.txt", "x")
        (self.workspace / "folder").mkdir()
        (self.workspace / ".git").mkdir()
        (self.workspace / ".git" / "config").write_text("x", encoding="utf-8")
        self.assertIn("Deleted", zed_mcp.delete_file("notes.txt"))
        self.assertFalse((self.workspace / "notes.txt").exists())
        with self.assertRaises(IsADirectoryError):
            zed_mcp.delete_file("folder")
        with self.assertRaises(ValueError):
            zed_mcp.delete_file(".git/config")
        with self.assertRaises(ValueError):
            zed_mcp.delete_file(".")
        with self.assertRaises(ValueError):
            zed_mcp.delete_file("../outside.txt")
        with self.assertRaises(FileNotFoundError):
            zed_mcp.delete_file("missing.txt")

    def test_apply_patch_is_all_or_nothing(self):
        zed_mcp.create_file("app.txt", "alpha beta gamma")
        zed_mcp.apply_patch(
            "app.txt",
            [
                {"old_text": "alpha", "new_text": "one"},
                {"old_text": "gamma", "new_text": "three"},
            ],
        )
        self.assertEqual(zed_mcp.read_file("app.txt"), "one beta three")
        with self.assertRaises(ValueError):
            zed_mcp.apply_patch(
                "app.txt",
                [
                    {"old_text": "one", "new_text": "1"},
                    {"old_text": "missing", "new_text": "x"},
                ],
            )
        self.assertEqual(zed_mcp.read_file("app.txt"), "one beta three")
        with self.assertRaises(ValueError):
            zed_mcp.apply_patch("app.txt", [])

    def test_file_operations_stay_in_workspace(self):
        zed_mcp.create_file("src/main.rs", "pub fn start() {}\n")
        outline = zed_mcp.get_file_outline("src/main.rs")
        self.assertEqual(outline, [{"name": "start", "line": 1}])
        zed_mcp.edit_file("src/main.rs", "start", "run")
        self.assertIn("fn run", zed_mcp.read_file("src/main.rs"))
        self.assertEqual(zed_mcp.find_files(".", "*.rs"), ["src/main.rs"])
        with self.assertRaises(ValueError):
            zed_mcp.read_file("../outside.txt")


if __name__ == "__main__":
    unittest.main()
