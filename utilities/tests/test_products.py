"""Product and published-identity resolution, plus the registry invariants that
keep the identity rule invertible and the reference line uncontested.
"""
import unittest
from datetime import date

from utilities.products import (
    along_track_product,
    contributing_sources,
    published_along_track_product,
    published_identity,
    published_simple_grid_product,
    registered_contributors,
)
from utilities.source_profile import (
    SourceCommon,
    get_published_product,
    get_registered_sources,
    get_source_profile,
    list_published_products,
)


def _profile(source: str, product_type: str) -> SourceCommon:
    return SourceCommon(
        source=source,
        product_type=product_type,
        start_date=date(2020, 1, 1),
    )


class TestPublishedIdentity(unittest.TestCase):
    def test_reference_sources_share_one_identity(self):
        for source in ("GSFC", "S6", "S6B"):
            with self.subTest(source=source):
                self.assertEqual(published_identity(get_source_profile(source)), "NASA-SSH")

    def test_high_latitude_source_gets_its_own_identity(self):
        self.assertEqual(
            published_identity(get_source_profile("S3B")), "NASA-SSH_S3B"
        )

    def test_identity_is_underscore_separated(self):
        """One string for both the products/ directory and the filename prefix."""
        self.assertEqual(published_identity(_profile("HY2B", "high_latitude")), "NASA-SSH_HY2B")

    def test_unknown_product_type_raises(self):
        with self.assertRaises(ValueError):
            published_identity(_profile("X", "bogus"))

    def test_every_registered_source_resolves(self):
        """No source may be in the registry without a published identity — there
        is no publish gate, so every source publishes somewhere."""
        for source in get_registered_sources():
            with self.subTest(source=source):
                self.assertTrue(published_identity(get_source_profile(source)))


class TestContributors(unittest.TestCase):
    def test_registered_contributors_ignore_enablement(self):
        """Resolving a name must not depend on a production fact: S6B and S3B are
        both disabled and must still resolve."""
        self.assertIn("S6B", registered_contributors("NASA-SSH"))
        self.assertEqual(registered_contributors("NASA-SSH_S3B"), ["S3B"])

    def test_contributing_sources_are_the_enabled_ones(self):
        enabled = contributing_sources("NASA-SSH")
        self.assertIn("GSFC", enabled)
        self.assertIn("S6", enabled)
        self.assertNotIn("S6B", enabled)

    def test_disabled_line_has_no_contributors(self):
        self.assertEqual(contributing_sources("NASA-SSH_S3B"), [])

    def test_unknown_identity_has_no_contributors(self):
        self.assertEqual(registered_contributors("NASA-SSH_NOPE"), [])


class TestPublishedAlongTrackProduct(unittest.TestCase):
    def test_reference_identity_resolves_its_family(self):
        self.assertEqual(
            published_along_track_product("NASA-SSH").name, "along_track_reference"
        )

    def test_high_latitude_identity_resolves_while_disabled(self):
        self.assertEqual(
            published_along_track_product("NASA-SSH_S3B").name,
            "along_track_high_latitude",
        )

    def test_identity_no_source_resolves_to_raises(self):
        """Silently returning None here would produce an empty path segment and a
        file written to an address nothing reads."""
        with self.assertRaises(ValueError):
            published_along_track_product("NASA-SSH_NOPE")

    def test_matches_the_source_keyed_resolution(self):
        for source in get_registered_sources():
            profile = get_source_profile(source)
            with self.subTest(source=source):
                self.assertEqual(
                    published_along_track_product(published_identity(profile)).name,
                    along_track_product(profile).name,
                )


class TestPublishedSimpleGridProduct(unittest.TestCase):
    def test_declared_grid_resolves(self):
        self.assertEqual(
            published_simple_grid_product("NASA-SSH").name, "simple_grid_reference"
        )

    def test_line_without_a_declared_grid_raises(self):
        with self.assertRaises(ValueError):
            published_simple_grid_product("NASA-SSH_S3B")


class TestPublishedProductRegistry(unittest.TestCase):
    def test_only_lines_needing_declarations_are_listed(self):
        """`published_products:` carries only non-derivable facts, so an
        along-track-only line must not need an entry."""
        self.assertEqual(list_published_products(), ["NASA-SSH"])

    def test_reference_line_declares_its_grid_and_indicators(self):
        entry = get_published_product("NASA-SSH")
        self.assertEqual(entry.simple_grid, "simple_grid_reference")
        self.assertTrue(entry.indicators)

    def test_unknown_published_product_raises(self):
        with self.assertRaises(ValueError):
            get_published_product("NASA-SSH_HILAT")

    def test_every_declared_grid_product_exists(self):
        for name in list_published_products():
            entry = get_published_product(name)
            if entry.simple_grid is not None:
                with self.subTest(published_product=name):
                    published_simple_grid_product(name)


class TestReferenceContributorsAreDisjoint(unittest.TestCase):
    """The static half of the contested-date guard.

    Every enabled reference source resolves to the same published key, so two
    with overlapping date ranges would both claim the same published file and
    execution order would decide what ships. This fails at review time, the
    moment someone flips `enabled: true`; the publisher's own refusal to compose
    is the runtime half.
    """

    @staticmethod
    def _range(source):
        profile = get_source_profile(source)
        return profile.start_date, profile.end_date or date.max

    def test_enabled_reference_sources_have_disjoint_ranges(self):
        enabled = [
            s
            for s in contributing_sources("NASA-SSH")
            if get_source_profile(s).product_type == "reference"
        ]
        self.assertGreater(len(enabled), 0, "no enabled reference source")
        for i, a in enumerate(enabled):
            a_start, a_end = self._range(a)
            for b in enabled[i + 1:]:
                b_start, b_end = self._range(b)
                with self.subTest(a=a, b=b):
                    self.assertTrue(
                        a_end < b_start or b_end < a_start,
                        f"{a} [{a_start}..{a_end}] overlaps {b} [{b_start}..{b_end}]; "
                        f"both would claim the same published reference dates",
                    )


if __name__ == "__main__":
    unittest.main()
