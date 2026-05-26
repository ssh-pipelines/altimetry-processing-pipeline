import gzip
import unittest
from io import BytesIO
from unittest import mock

import requests

from daily_files.fetching.downloader import HttpDownloader


def _mock_response_raw(payload: bytes) -> mock.MagicMock:
    """Mimic the stream=True response.raw shape that HttpDownloader uses."""
    resp = mock.MagicMock()
    resp.__enter__.return_value = resp
    resp.__exit__.return_value = False
    resp.raise_for_status = mock.MagicMock()
    raw = mock.MagicMock()
    raw.read.return_value = payload
    raw.decode_content = False
    resp.raw = raw
    return resp


class TestHttpDownloader(unittest.TestCase):
    def _make_downloader(self, payload: bytes) -> HttpDownloader:
        session = mock.MagicMock(spec=requests.Session)
        session.get.return_value = _mock_response_raw(payload)
        return HttpDownloader(session_fn=lambda: session)

    def test_gz_uri_is_decompressed(self):
        body = b"hello world" * 100
        compressed = gzip.compress(body)
        dl = self._make_downloader(compressed)
        buf = dl.download("https://example.com/file.nc.gz")
        self.assertIsInstance(buf, BytesIO)
        self.assertEqual(buf.read(), body)

    def test_plain_nc_uri_passes_through(self):
        body = b"\x89HDF\r\nplain-nc-bytes"
        dl = self._make_downloader(body)
        buf = dl.download("https://example.com/file.nc")
        self.assertIsInstance(buf, BytesIO)
        self.assertEqual(buf.read(), body)

    def test_download_all_returns_list_in_order(self):
        body_a = gzip.compress(b"AAA")
        body_b = gzip.compress(b"BBB")
        session = mock.MagicMock(spec=requests.Session)
        session.get.side_effect = [
            _mock_response_raw(body_a),
            _mock_response_raw(body_b),
        ]
        dl = HttpDownloader(session_fn=lambda: session)
        results = dl.download_all([
            "https://example.com/a.nc.gz",
            "https://example.com/b.nc.gz",
        ])
        self.assertEqual([r.read() for r in results], [b"AAA", b"BBB"])

    def test_http_error_propagates(self):
        session = mock.MagicMock(spec=requests.Session)
        resp = mock.MagicMock()
        resp.__enter__.return_value = resp
        resp.__exit__.return_value = False
        resp.raise_for_status.side_effect = requests.HTTPError("401 Unauthorized")
        session.get.return_value = resp
        dl = HttpDownloader(session_fn=lambda: session)
        # assertLogs captures the ERROR-level traceback HttpDownloader emits
        # before re-raising, so it doesn't pollute test output.
        with self.assertLogs(level="ERROR"), self.assertRaises(requests.HTTPError):
            dl.download("https://example.com/file.nc.gz")


if __name__ == "__main__":
    unittest.main()
