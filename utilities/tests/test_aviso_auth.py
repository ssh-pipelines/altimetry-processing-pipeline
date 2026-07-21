import unittest
from unittest import mock

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import utilities.aviso_auth as aviso_auth


class TestAvisoSessionRetry(unittest.TestCase):
    def setUp(self):
        # build_aviso_session caches the session at module level; reset between
        # tests so each builds a fresh one.
        aviso_auth._cached_session = None
        self.addCleanup(setattr, aviso_auth, "_cached_session", None)

    def _build_session(self):
        # Skip real credential resolution (Secrets Manager / netrc / env).
        with mock.patch.object(
            aviso_auth, "_resolve_credentials", return_value=(None, None)
        ):
            return aviso_auth.build_aviso_session()

    def test_retry_adapter_mounted_on_both_schemes(self):
        session = self._build_session()
        for scheme in ("https://", "http://"):
            adapter = session.get_adapter(scheme + "tds-odatis.aviso.altimetry.fr/")
            self.assertIsInstance(adapter, HTTPAdapter)
            self.assertIsInstance(adapter.max_retries, Retry)

    def test_retry_configuration(self):
        session = self._build_session()
        retry: Retry = session.get_adapter("https://example.com/").max_retries
        self.assertEqual(retry.total, 5)
        self.assertEqual(retry.connect, 5)
        self.assertEqual(retry.read, 5)
        self.assertEqual(retry.backoff_factor, 1.0)
        self.assertEqual(retry.backoff_max, 60)
        self.assertEqual(
            set(retry.status_forcelist), {429, 500, 502, 503, 504}
        )
        self.assertIn("GET", retry.allowed_methods)
        # Final non-retryable status is left to the caller's raise_for_status().
        self.assertFalse(retry.raise_on_status)

    def test_session_is_cached(self):
        first = self._build_session()
        # Second call must not rebuild — returns the cached instance.
        second = aviso_auth.build_aviso_session()
        self.assertIs(first, second)


if __name__ == "__main__":
    unittest.main()
