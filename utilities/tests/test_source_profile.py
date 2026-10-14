import unittest
from dataclasses import dataclass
from datetime import date

from utilities.source_profile import (
    CollectionConfig,
    Product,
    SourceCommon,
    _load_products_raw,
    clear_caches,
    get_product,
    get_registered_sources,
    get_source_profile,
    list_sources_for_stage,
    load_source_config,
)


@dataclass(kw_only=True, frozen=True)
class _StageA(SourceCommon):
    extra_field: str
    nested_value: int


class TestLoadSourceCommon(unittest.TestCase):
    def test_loads_gsfc_common(self):
        profile = get_source_profile("GSFC")
        self.assertIsInstance(profile, SourceCommon)
        self.assertEqual(profile.source, "GSFC")
        self.assertEqual(profile.product_type, "reference")
        self.assertEqual(profile.discovery_type, "cmr")
        self.assertTrue(profile.unify)
        self.assertEqual(profile.start_date, date(1992, 10, 25))
        self.assertEqual(profile.end_date, date(2025, 12, 31))

    def test_collections_parsed_into_dataclasses(self):
        profile = get_source_profile("S6")
        self.assertGreater(len(profile.collections), 0)
        for c in profile.collections:
            self.assertIsInstance(c, CollectionConfig)
            self.assertTrue(c.concept_id)
            self.assertTrue(c.shortname)
        self.assertEqual([c.priority for c in profile.collections], [1, 2, 3])

    def test_s3_bucket_fields_only_for_s3_sources(self):
        gsfc = get_source_profile("GSFC")
        self.assertIsNone(gsfc.source_bucket)
        self.assertIsNone(gsfc.source_filename_pattern)
        ex = get_source_profile("EXAMPLE_S3")
        self.assertEqual(ex.source_bucket, "example-source-bucket")
        self.assertEqual(ex.source_filename_pattern, "{source}_{date}.nc")


class TestLoadWithStage(unittest.TestCase):
    def test_load_with_empty_stage_section(self):
        # pipeline_init's section is `{}` for every source; loader should
        # still merge common fields successfully.
        profile = load_source_config(SourceCommon, "pipeline_init", "GSFC")
        self.assertEqual(profile.source, "GSFC")
        self.assertEqual(profile.product_type, "reference")


class TestErrors(unittest.TestCase):
    def test_missing_yaml_raises(self):
        with self.assertRaises(ValueError) as ctx:
            get_source_profile("DOES_NOT_EXIST")
        self.assertIn("not configured", str(ctx.exception))

    def test_missing_stage_required_field_raises_typeerror(self):
        # `_StageA` requires `extra_field` + `nested_value`; no source provides them
        with self.assertRaises(TypeError):
            load_source_config(_StageA, "pipeline_init", "GSFC")

    def test_unknown_field_raises_typeerror(self):
        # Construct a dataclass that doesn't accept all common fields
        @dataclass(frozen=True)
        class _NarrowDC:
            source: str

        with self.assertRaises(TypeError):
            load_source_config(_NarrowDC, None, "GSFC")


class TestProducts(unittest.TestCase):
    def test_get_along_track_reference(self):
        p = get_product("along_track_reference")
        self.assertIsInstance(p, Product)
        self.assertEqual(p.version, "v1_1")
        self.assertIn("{source}", p.filename_template)
        self.assertIn("{YYYYMMDD}", p.filename_template)

    def test_unknown_product_raises(self):
        with self.assertRaises(ValueError):
            get_product("does-not-exist")


class TestProductIdentity(unittest.TestCase):
    """The identity attrs a product writes into its own files."""

    # Families whose artifacts carry CF identity. The ENSO grids do not: the
    # gridder copies variable attrs from its input simple grid and writes no
    # title, DOI, or short name of its own.
    CF_FAMILIES = ("along_track_", "simple_grid_")

    # Empty is legal for these two — they mirror ALLOW_EMPTY_GLOBAL_ATTRS in the
    # daily_files schema, where the along-track path fills them per source.
    MAY_BE_EMPTY = {"references", "source_url"}

    def _cf_product_names(self) -> list[str]:
        names = [
            n
            for n in _load_products_raw()["products"]
            if n.startswith(self.CF_FAMILIES)
        ]
        self.assertGreater(len(names), 0)
        return names

    def test_every_cf_product_declares_its_full_identity(self):
        """A new source's product_type must not resolve to an entry that silently
        inherits an empty title or DOI — those attrs land in distributed files."""
        for name in self._cf_product_names():
            attrs = get_product(name).global_attrs()
            for attr, value in attrs.items():
                if attr in self.MAY_BE_EMPTY:
                    continue
                with self.subTest(product=name, attr=attr):
                    self.assertTrue(value, f"{name} declares no '{attr}'")

    def test_no_two_products_share_a_doi_or_short_name(self):
        """Except the deliberate `TBD` placeholders, which mark a product as not
        yet deliverable to PO.DAAC."""
        for attr in ("id", "product_short_name"):
            values = [getattr(get_product(n), attr) for n in self._cf_product_names()]
            real = [v for v in values if v != "TBD"]
            with self.subTest(attr=attr):
                self.assertEqual(len(real), len(set(real)), f"duplicate '{attr}': {real}")

    def test_product_version_is_derived_from_version(self):
        p = get_product("along_track_reference")
        self.assertEqual(p.version, "v1_1")
        self.assertEqual(p.product_version, "V1.1")
        self.assertEqual(p.global_attrs()["product_version"], "V1.1")

    def test_enso_carries_no_cf_identity(self):
        self.assertEqual(get_product("enso").global_attrs()["title"], "")


class TestListSourcesForStage(unittest.TestCase):
    def test_pipeline_init_excludes_nasa_ssh(self):
        sources = list_sources_for_stage("pipeline_init")
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)
        # NASA-SSH has no pipeline_init: section
        self.assertNotIn("NASA-SSH", sources)

    def test_unifier_excludes_s6b(self):
        sources = list_sources_for_stage("unifier")
        self.assertIn("GSFC", sources)
        self.assertIn("S6", sources)
        self.assertNotIn("S6B", sources)

    def test_none_returns_all_sources(self):
        sources = list_sources_for_stage(None)
        # All known sources should appear
        for expected in ["GSFC", "S6", "S6B", "S3B", "EXAMPLE_S3", "NASA-SSH"]:
            self.assertIn(expected, sources)

    def test_get_registered_sources(self):
        self.assertEqual(get_registered_sources(), list_sources_for_stage(None))


class TestCaching(unittest.TestCase):
    def test_clear_caches_does_not_raise(self):
        get_source_profile("GSFC")  # warm cache
        clear_caches()
        # Should still work after cache clear
        profile = get_source_profile("GSFC")
        self.assertEqual(profile.source, "GSFC")


if __name__ == "__main__":
    unittest.main()
