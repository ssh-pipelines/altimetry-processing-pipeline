import gzip
import io
import os
import unittest
from unittest.mock import patch

import requests
from daily_files.fetching import orbit_fetcher
from daily_files.fetching.orbit_fetcher import OrbitFetcher
from urllib3.connectionpool import HTTPConnectionPool
from urllib3.exceptions import NewConnectionError, ReadTimeoutError
from urllib3.response import HTTPResponse

NTC_GRANULE = "S6A_P4_2__LR_RED__NT_214_137_20260905T001122_20260905T010735_F09.nc"
STC_GRANULE = "S6A_P4_2__LR_RED__ST_214_137_20260905T001122_20260905T010735_F09.nc"
ORBIT_TEXT = b"E S6AN 840000000 0.0 1.0 2.0 3.0\n"


def _http(status: int, body: bytes = b"") -> HTTPResponse:
    return HTTPResponse(
        body=io.BytesIO(body), status=status, headers={}, preload_content=False,
        decode_content=False,
    )


class _Wire:
    """Replays one urllib3 response or exception per request through the real
    adapter, so the mounted Retry policy is what is under test."""

    def __init__(self, *events):
        self.events = list(events)
        self.calls = 0

    def __call__(self, pool, conn, method, url, **kwargs):
        self.calls += 1
        event = self.events.pop(0)
        if isinstance(event, Exception):
            raise event
        return event


class TestOrbitFetcherRetry(unittest.TestCase):
    def setUp(self):
        # Backoff sleeps happen in urllib3 and in the body retry; neither is under test.
        self._sleeps = []
        for target in ("urllib3.util.retry.time.sleep", "daily_files.fetching.orbit_fetcher.time.sleep"):
            p = patch(target, side_effect=self._sleeps.append)
            p.start()
            self.addCleanup(p.stop)

    def _fetch(self, wire: _Wire, granule: str = NTC_GRANULE) -> str:
        with patch.object(HTTPConnectionPool, "_make_request", autospec=True, side_effect=wire):
            path = OrbitFetcher().fetch(granule)
        self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
        return path

    def test_retries_5xx_then_succeeds(self):
        wire = _Wire(_http(503), _http(502), _http(200, gzip.compress(ORBIT_TEXT)))
        path = self._fetch(wire)
        self.assertEqual(wire.calls, 3)
        with open(path, "rb") as f:
            self.assertEqual(f.read(), ORBIT_TEXT)

    def test_backoff_is_exponential(self):
        wire = _Wire(*[_http(503) for _ in range(4)], _http(200, gzip.compress(ORBIT_TEXT)))
        self._fetch(wire)
        # 0 before the first retry, then 2, 4, 8 s, each with up to 1 s of jitter.
        self.assertEqual(len(self._sleeps), 3)
        for slept, base in zip(self._sleeps, (2, 4, 8)):
            self.assertGreaterEqual(slept, base)
            self.assertLess(slept, base + 1)

    def test_retries_connection_errors(self):
        refused = NewConnectionError(None, "connection refused")
        wire = _Wire(refused, refused, _http(200, gzip.compress(ORBIT_TEXT)))
        self._fetch(wire)
        self.assertEqual(wire.calls, 3)

    def test_gives_up_after_five_attempts(self):
        stall = ReadTimeoutError(None, "/", "read timed out")
        wire = _Wire(*[stall] * 6)
        with self.assertRaises(requests.exceptions.ConnectionError):
            self._fetch(wire)
        self.assertEqual(wire.calls, 5)

    def test_persistent_5xx_raises_http_error(self):
        wire = _Wire(*[_http(500) for _ in range(6)])
        with self.assertRaises(requests.HTTPError):
            self._fetch(wire)
        self.assertEqual(wire.calls, 5)

    def test_404_is_not_retried(self):
        # An unpublished orbit stays unpublished for the rest of the invocation.
        wire = _Wire(_http(404))
        with self.assertRaises(requests.HTTPError):
            self._fetch(wire, STC_GRANULE)
        self.assertEqual(wire.calls, 1)

    def test_truncated_gzip_is_refetched(self):
        good = gzip.compress(ORBIT_TEXT)
        wire = _Wire(_http(200, good[: len(good) // 2]), _http(200, good))
        path = self._fetch(wire)
        self.assertEqual(wire.calls, 2)
        with open(path, "rb") as f:
            self.assertEqual(f.read(), ORBIT_TEXT)

    def test_persistently_corrupt_gzip_raises(self):
        wire = _Wire(*[_http(200, b"not gzip") for _ in range(orbit_fetcher._BODY_ATTEMPTS)])
        with self.assertRaises(gzip.BadGzipFile):
            self._fetch(wire)
        self.assertEqual(wire.calls, orbit_fetcher._BODY_ATTEMPTS)

    def test_same_date_and_type_downloads_once(self):
        wire = _Wire(_http(200, gzip.compress(ORBIT_TEXT)))
        with patch.object(HTTPConnectionPool, "_make_request", autospec=True, side_effect=wire):
            fetcher = OrbitFetcher()
            first = fetcher.fetch(NTC_GRANULE)
            self.addCleanup(lambda: os.path.exists(first) and os.remove(first))
            self.assertEqual(fetcher.fetch(NTC_GRANULE.replace("_137_", "_138_")), first)
        self.assertEqual(wire.calls, 1)


if __name__ == "__main__":
    unittest.main()
