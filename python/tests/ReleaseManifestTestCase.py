"""Tests for the release-manifest helpers in flows/pipeline.py.

Covers the pure helpers used to build the versioned release manifest:
- _dir_size_bytes: sums file sizes over a fake directory tree, skips symlinks
- _build_release_manifest: assembles the manifest dict from collected figures

No live ArangoDB, S3, or Docker is required — these are pure functions over a
temporary directory and in-memory figures.
"""

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

# Make both python/src and python/src/flows importable (mirrors other tests).
_SRC = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(_SRC / "flows"))
sys.path.insert(0, str(_SRC))

from pipeline import (
    _build_release_manifest,
    _dir_size_bytes,
    _upload_github_release_asset,
)


def _noop_logger():
    """Silent logger that satisfies logger.info/warning calls."""
    m = MagicMock()
    m.info = lambda *a, **kw: None
    m.warning = lambda *a, **kw: None
    return m


class DirSizeBytesTestCase(unittest.TestCase):
    """Tests for the _dir_size_bytes helper."""

    def test_sums_nested_file_sizes(self):
        """Totals byte sizes of every file across nested subdirectories."""
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            (root / "a.json").write_bytes(b"x" * 10)
            sub = root / "Cell-KN-Ontologies"
            sub.mkdir()
            (sub / "b.data.json.gz").write_bytes(b"y" * 25)
            (sub / "c.structure.json").write_bytes(b"z" * 5)

            self.assertEqual(_dir_size_bytes(root), 40)

    def test_empty_directory_is_zero(self):
        """An empty tree reports zero bytes."""
        with tempfile.TemporaryDirectory() as d:
            self.assertEqual(_dir_size_bytes(Path(d)), 0)

    def test_skips_symlinks(self):
        """Symlinked files are not counted (avoids double counting / loops)."""
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            target = root / "real.bin"
            target.write_bytes(b"a" * 12)
            try:
                os.symlink(target, root / "link.bin")
            except (OSError, NotImplementedError):
                self.skipTest("symlinks not supported on this platform")
            self.assertEqual(_dir_size_bytes(root), 12)


class BuildReleaseManifestTestCase(unittest.TestCase):
    """Tests for the _build_release_manifest pure helper."""

    def _figures(self):
        return {
            "Cell-KN-Ontologies": {
                "total_bytes": 3000,
                "collections": {
                    "BS": {"documents": 100, "bytes": 2000},
                    "CL": {"documents": 50, "bytes": 1000},
                },
            },
            "Cell-KN-Phenotypes": {
                "total_bytes": 500,
                "collections": {
                    "PH": {"documents": 7, "bytes": 500},
                },
            },
        }

    def test_assembles_expected_shape(self):
        """Produces the documented manifest shape with all fields populated."""
        manifest = _build_release_manifest(
            tag="v2026-06",
            uncompressed_bytes=123456,
            compressed_bytes=7890,
            databases=self._figures(),
            created_utc="2026-06-02T00:00:00Z",
        )

        self.assertEqual(manifest["tag"], "v2026-06")
        self.assertEqual(manifest["created_utc"], "2026-06-02T00:00:00Z")
        self.assertEqual(manifest["uncompressed_bytes"], 123456)
        self.assertEqual(manifest["compressed_bytes"], 7890)

        onto = manifest["databases"]["Cell-KN-Ontologies"]
        self.assertEqual(onto["total_bytes"], 3000)
        self.assertEqual(onto["collections"]["BS"], {"documents": 100, "bytes": 2000})
        self.assertEqual(
            set(manifest["databases"]), {"Cell-KN-Ontologies", "Cell-KN-Phenotypes"}
        )

    def test_default_timestamp_is_iso8601_utc(self):
        """A created_utc is generated when not supplied (ISO-8601, Z-suffixed)."""
        manifest = _build_release_manifest(
            tag="t",
            uncompressed_bytes=0,
            compressed_bytes=0,
            databases={},
        )
        created = manifest["created_utc"]
        self.assertTrue(created.endswith("Z"))
        self.assertEqual(len(created), len("2026-06-02T00:00:00Z"))

    def test_empty_databases_allowed(self):
        """Handles the degraded case where no per-collection figures were gathered."""
        manifest = _build_release_manifest(
            tag="t",
            uncompressed_bytes=10,
            compressed_bytes=5,
            databases={},
            created_utc="2026-06-02T00:00:00Z",
        )
        self.assertEqual(manifest["databases"], {})


class UploadGithubReleaseAssetTestCase(unittest.TestCase):
    """Tests for the _upload_github_release_asset helper (urllib mocked)."""

    _ENV = {
        "GITHUB_TOKEN": "tok",
        "GITHUB_REPOSITORY": "Springbok-LLC/nlm-ckn-etl",
        "ETL_RELEASE_TAG": "v2026-06",
    }

    def _write_manifest(self, d):
        p = Path(d) / "manifest.json"
        p.write_text('{"tag": "v2026-06"}')
        return p

    def test_noop_when_env_missing(self):
        """Does nothing (no HTTP) when the required env vars are absent."""
        with tempfile.TemporaryDirectory() as d:
            p = self._write_manifest(d)
            with patch.dict(os.environ, {}, clear=True), \
                    patch("urllib.request.urlopen") as mock_open:
                _upload_github_release_asset(p, _noop_logger())
            mock_open.assert_not_called()

    def test_uploads_to_resolved_release(self):
        """Resolves the release by tag and POSTs the asset to the upload URL."""
        calls = []

        @contextmanager
        def fake_urlopen(req, timeout=None):
            calls.append((req.method, req.full_url))
            if req.method == "GET":
                body = json.dumps({"id": 42, "assets": []}).encode()
            else:
                body = b"{}"
            yield io.BytesIO(body)

        with tempfile.TemporaryDirectory() as d:
            p = self._write_manifest(d)
            with patch.dict(os.environ, self._ENV, clear=True), \
                    patch("urllib.request.urlopen", side_effect=fake_urlopen):
                _upload_github_release_asset(p, _noop_logger())

        methods = [m for m, _ in calls]
        urls = [u for _, u in calls]
        self.assertIn("GET", methods)
        self.assertTrue(any(u.endswith("/releases/tags/v2026-06") for u in urls))
        # POST goes to uploads.github.com for release 42 with the asset name.
        post_urls = [u for m, u in calls if m == "POST"]
        self.assertEqual(len(post_urls), 1)
        self.assertIn("uploads.github.com", post_urls[0])
        self.assertIn("/releases/42/assets", post_urls[0])
        self.assertIn("name=manifest.json", post_urls[0])

    def test_replaces_existing_asset(self):
        """Deletes a same-named asset before uploading the replacement."""
        calls = []

        @contextmanager
        def fake_urlopen(req, timeout=None):
            calls.append((req.method, req.full_url))
            if req.method == "GET":
                body = json.dumps(
                    {"id": 7, "assets": [{"id": 99, "name": "manifest.json"}]}
                ).encode()
            else:
                body = b"{}"
            yield io.BytesIO(body)

        with tempfile.TemporaryDirectory() as d:
            p = self._write_manifest(d)
            with patch.dict(os.environ, self._ENV, clear=True), \
                    patch("urllib.request.urlopen", side_effect=fake_urlopen):
                _upload_github_release_asset(p, _noop_logger())

        delete_urls = [u for m, u in calls if m == "DELETE"]
        self.assertEqual(len(delete_urls), 1)
        self.assertTrue(delete_urls[0].endswith("/releases/assets/99"))


if __name__ == "__main__":
    unittest.main()
