import json
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src" / "flows"))

import _common  # noqa: E402

_QUIET = lambda *args, **kwargs: None  # noqa: E731


class FetchForceTestCase(unittest.TestCase):
    """Tests for _common.should_force_fetch / _fetch_code_hash."""

    def _write_info(self, d, *, age_hours=1.0, code_hash="MATCH"):
        if code_hash == "MATCH":
            code_hash = _common._fetch_code_hash()
        ts = (datetime.now(timezone.utc) - timedelta(hours=age_hours)).isoformat()
        (d / "fetch-info.json").write_text(
            json.dumps({"fetched_at": ts, "fetch_code_hash": code_hash})
        )

    def _decide(self, d, max_age=672.0):
        with patch.object(_common, "S3_BUCKET", ""), patch.object(
            _common, "_external_dir", return_value=d
        ):
            return _common.should_force_fetch("test-run", max_age, _QUIET)

    def test_reuse_when_fresh_and_code_unchanged(self):
        with tempfile.TemporaryDirectory() as t:
            d = Path(t)
            self._write_info(d, age_hours=1.0)
            self.assertFalse(self._decide(d))

    def test_force_when_code_changed(self):
        with tempfile.TemporaryDirectory() as t:
            d = Path(t)
            self._write_info(d, age_hours=1.0, code_hash="0000deadbeef0000")
            self.assertTrue(self._decide(d))

    def test_force_when_stale(self):
        with tempfile.TemporaryDirectory() as t:
            d = Path(t)
            self._write_info(d, age_hours=1000.0)  # > 672
            self.assertTrue(self._decide(d))

    def test_force_when_missing(self):
        with tempfile.TemporaryDirectory() as t:
            self.assertTrue(self._decide(Path(t)))  # no fetch-info.json

    def test_fetch_code_hash_stable_and_16_char(self):
        h1 = _common._fetch_code_hash()
        h2 = _common._fetch_code_hash()
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 16)
        int(h1, 16)  # valid hex


if __name__ == "__main__":
    unittest.main()
