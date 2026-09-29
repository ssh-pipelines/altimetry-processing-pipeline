"""In-file identity of the along-track products.

The empty-template path is the cheapest way to build a complete attr set
offline — no granules, no S3, no upstream credentials — and it shares
`get_base_global_attrs` with the processed path, so it pins the same values.
"""

import unittest

from daily_files.daily_file_job import DailyFileJob, make_empty

_SUMMARY = (
    "This data set contains satellite based measurements of sea surface height, "
    "computed relative to the mean sea surface specified in mean_sea_surface. "
    "Data have been collected from multiple satellites, and processed to maximize "
    "compatibility and minimize bias between satellites. They are intended for use "
    "in studies and applications requiring climate-quality observations without "
    "additional adjustments or filtering."
)

# The attrs that distinguish one product from another. Everything else in the
# base set (institution, license, publisher_*, creator_*, keywords, platform,
# instrument, cdm_data_type, featureType, naming_authority, acknowledgement,
# project) is shared by every NASA-SSH product.
_IDENTITY_ATTRS = (
    "title",
    "summary",
    "id",
    "processing_level",
    "product_short_name",
    "product_version",
)


def _global_attrs(source: str) -> dict:
    ds = make_empty(DailyFileJob("2023-01-01", source))
    attrs = dict(ds.attrs)
    ds.close()
    return attrs


class TestReferenceAlongTrackIdentity(unittest.TestCase):
    """Pinned because these are the values PO.DAAC ingested: moving identity out
    of a Python literal and into `utilities/products.yaml` must not change them."""

    @classmethod
    def setUpClass(cls):
        cls.attrs = _global_attrs("GSFC")

    def test_title(self):
        self.assertEqual(
            self.attrs["title"],
            "NASA-SSH Along-Track Sea Surface Height from Standardized Reference Missions Version 1.1",
        )

    def test_summary(self):
        self.assertEqual(self.attrs["summary"], _SUMMARY)

    def test_doi(self):
        self.assertEqual(self.attrs["id"], "10.5067/NSREF-AT0V11")

    def test_product_short_name(self):
        self.assertEqual(self.attrs["product_short_name"], "NASA_SSH_REF_ALONGTRACK_V11")

    def test_processing_level_is_along_track(self):
        self.assertEqual(self.attrs["processing_level"], "Level 2")

    def test_product_version(self):
        self.assertEqual(self.attrs["product_version"], "V1.1")

    def test_project_is_shared_across_products(self):
        self.assertEqual(self.attrs["project"], "NASA-SSH")


class TestHighLatitudeAlongTrackIdentity(unittest.TestCase):
    """A high-latitude file is written by the same code path as a reference file
    and must not inherit its identity. The DOI is the placeholder `TBD` until the
    science side settles this product — an obviously-unfinished identifier rather
    than a plausible-looking fake one, in a file that is already distributed."""

    @classmethod
    def setUpClass(cls):
        cls.attrs = _global_attrs("S3B")

    def test_does_not_claim_the_reference_doi(self):
        self.assertNotEqual(self.attrs["id"], "10.5067/NSREF-AT0V11")
        self.assertEqual(self.attrs["id"], "TBD")

    def test_product_short_name(self):
        self.assertEqual(self.attrs["product_short_name"], "NASA_SSH_HILAT_ALONGTRACK_V11")

    def test_title_does_not_claim_reference_missions(self):
        self.assertNotIn("Reference Missions", self.attrs["title"])

    def test_summary_is_not_the_reference_summary(self):
        self.assertNotEqual(self.attrs["summary"], _SUMMARY)

    def test_processing_level_still_along_track(self):
        self.assertEqual(self.attrs["processing_level"], "Level 2")

    def test_shares_the_project_and_version(self):
        self.assertEqual(self.attrs["project"], "NASA-SSH")
        self.assertEqual(self.attrs["product_version"], "V1.1")

    def test_still_passes_schema_validation(self):
        """`id` and `product_short_name` are outside ALLOW_EMPTY_GLOBAL_ATTRS, so
        the placeholder has to be non-empty, not blank."""
        from daily_files.config.dataset_schema import validate_dataset

        ds = make_empty(DailyFileJob("2023-01-01", "S3B"))
        self.assertEqual(validate_dataset(ds), [])
        ds.close()


class TestIdentityIsProductOwnedNotSourceOwned(unittest.TestCase):
    """Two sources contributing to the same product publish the same identity;
    a source contributing to a different product does not inherit it."""

    def test_reference_sources_agree(self):
        gsfc = _global_attrs("GSFC")
        s6 = _global_attrs("S6")
        for attr in _IDENTITY_ATTRS:
            self.assertEqual(gsfc[attr], s6[attr], f"GSFC and S6 disagree on '{attr}'")

    def test_high_latitude_differs_from_reference(self):
        gsfc = _global_attrs("GSFC")
        s3b = _global_attrs("S3B")
        for attr in ("title", "summary", "id", "product_short_name"):
            self.assertNotEqual(gsfc[attr], s3b[attr], f"S3B inherited the reference '{attr}'")


if __name__ == "__main__":
    unittest.main()
