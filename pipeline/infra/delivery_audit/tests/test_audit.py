"""Tests for the audit module: collection resolution, S3 listing, CMR paging,
reconciliation, per-collection isolation, and the rendered report."""
import io
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone

from audit import (
    CMR_PAGE_SIZE,
    SEED_COPY_TIME,
    AuditError,
    AuditResult,
    CmrGranule,
    Collection,
    PartialCmrResults,
    ReleasedFile,
    collections_for,
    fetch_cmr_granules,
    list_released,
    main,
    reconcile,
    render_report,
    resolve_collections,
    run_audit,
)

from tests.fakes import AT, OLD, FakeS3, FakeSession, reference_cmr, reference_keys, umm_item

NOW = datetime(2026, 11, 19, 12, tzinfo=timezone.utc)
GRACE = timedelta(hours=12)
X = Collection("NASA-SSH", "X", "products/NASA-SSH/along_track/", ".nc")


def released(ur, size=100, modified=SEED_COPY_TIME - timedelta(days=1), key=None):
    return ReleasedFile(ur, key or f"products/NASA-SSH/along_track/2026/{ur}.nc", size, modified)


def granule(ur, size=100, revision=SEED_COPY_TIME):
    return CmrGranule(ur, size, revision)


class ReconcileTestCase(unittest.TestCase):
    def run_reconcile(self, released_files, granules):
        return reconcile(
            X,
            {f.granule_ur: f for f in released_files},
            {g.granule_ur: g for g in granules},
            NOW,
            GRACE,
        )

    def test_ingested_file_is_clean(self):
        r = self.run_reconcile([released("a")], [granule("a")])
        self.assertEqual(r.problems, 0)
        self.assertEqual((r.released, r.ingested), (1, 1))

    def test_missing_granule(self):
        r = self.run_reconcile([released("a"), released("b")], [granule("a")])
        self.assertEqual(r.missing, ["b"])
        self.assertEqual(r.problems, 1)

    def test_recent_missing_file_is_pending(self):
        r = self.run_reconcile([released("a", modified=NOW - timedelta(hours=10))], [])
        self.assertEqual(r.missing, [])
        self.assertEqual(r.pending, ["a"])
        self.assertEqual(r.problems, 0)

    def test_republished_after_ingest_is_stale(self):
        f = released("a", modified=NOW - timedelta(days=5))
        g = granule("a", revision=NOW - timedelta(days=9))
        r = self.run_reconcile([f], [g])
        self.assertEqual(r.stale, ["a"])

    def test_recent_republish_is_pending(self):
        f = released("a", modified=NOW - timedelta(hours=1))
        g = granule("a", revision=NOW - timedelta(days=9))
        r = self.run_reconcile([f], [g])
        self.assertEqual(r.stale, [])
        self.assertEqual(r.pending, ["a"])

    def test_seed_copy_time_is_not_a_republish(self):
        """The products/ seed reset LastModified on objects ingested long before."""
        f = released("a", modified=SEED_COPY_TIME)
        g = granule("a", revision=datetime(2026, 4, 27, tzinfo=timezone.utc))
        r = self.run_reconcile([f], [g])
        self.assertEqual(r.problems, 0)

    def test_size_mismatch(self):
        r = self.run_reconcile([released("a", size=100)], [granule("a", size=99)])
        self.assertEqual(r.size_mismatch, ["a"])

    def test_granule_without_size_is_not_a_mismatch(self):
        r = self.run_reconcile([released("a")], [granule("a", size=None)])
        self.assertEqual(r.problems, 0)

    def test_cmr_only_is_not_a_problem(self):
        r = self.run_reconcile([released("a")], [granule("a"), granule("old")])
        self.assertEqual(r.cmr_only, ["old"])
        self.assertEqual(r.problems, 0)

    def test_empty_cmr_reports_everything_missing(self):
        r = self.run_reconcile([released("a"), released("b")], [])
        self.assertEqual(r.missing, ["a", "b"])

    def test_no_released_files_is_a_problem(self):
        """An empty prefix for a delivered family means the declaration or the
        layout is wrong, not that there is nothing to deliver."""
        r = self.run_reconcile([], [granule("a")])
        self.assertTrue(r.empty)
        self.assertEqual(r.problems, 1)


class CollectionsTestCase(unittest.TestCase):
    def test_reference_line_comes_from_the_declaration(self):
        cols = {c.short_name: c for c in collections_for("NASA-SSH")}
        self.assertEqual(
            set(cols),
            {
                "NASA_SSH_REF_ALONGTRACK_V11",
                "NASA_SSH_REF_SIMPLE_GRID_V11",
                "NASA_SSH_GMSL_INDICATOR",
                "NASA_SSH_ENSO_INDICATOR",
                "NASA_SSH_IOD_INDICATOR",
                "NASA_SSH_PDO_INDICATOR",
            },
        )
        self.assertEqual(cols["NASA_SSH_REF_ALONGTRACK_V11"].prefix, "products/NASA-SSH/along_track/")
        self.assertEqual(cols["NASA_SSH_REF_SIMPLE_GRID_V11"].prefix, "products/NASA-SSH/simple_grid/")
        self.assertEqual(cols["NASA_SSH_GMSL_INDICATOR"].suffix, ".txt")
        self.assertEqual(cols["NASA_SSH_GMSL_INDICATOR"].name_prefix, "NASA_SSH_GMSL_INDICATOR")
        self.assertTrue(all(c.published_product == "NASA-SSH" for c in cols.values()))

    def test_undelivered_line_is_refused(self):
        """S3B publishes but has no PO.DAAC collection yet."""
        with self.assertRaisesRegex(ValueError, "declares no PO.DAAC delivery"):
            collections_for("NASA-SSH_S3B")

    def test_unknown_line_is_refused(self):
        with self.assertRaises(ValueError):
            collections_for("NASA-SSH_NOPE")

    def test_default_is_every_delivering_line(self):
        self.assertEqual(resolve_collections(), collections_for("NASA-SSH"))

    def test_named_lines_narrow_the_audit(self):
        self.assertEqual(resolve_collections(["NASA-SSH"]), collections_for("NASA-SSH"))


class ListReleasedTestCase(unittest.TestCase):
    T = SEED_COPY_TIME

    def test_filters_suffix_and_strips_extension(self):
        s3 = FakeS3({
            f"products/NASA-SSH/along_track/2026/{AT}20261113.nc": (5, self.T),
            f"products/NASA-SSH/along_track/2026/{AT}20261113.nc.md5": (70, self.T),
            f"products/NASA-SSH/along_track/1992/{AT}19921025.nc": (6, self.T),
        })
        files = list_released(s3, "b", X)
        self.assertEqual(sorted(files), [f"{AT}19921025", f"{AT}20261113"])
        self.assertEqual(files[f"{AT}20261113"].size, 5)

    def test_indicator_filter_includes_archive_excludes_mp_and_others(self):
        p = "products/NASA-SSH/indicators/"
        s3 = FakeS3({
            f"{p}indicators.nc": (1, self.T),
            f"{p}NASA_SSH_GMSL_INDICATOR.txt": (1, self.T),
            f"{p}NASA_SSH_GMSL_INDICATOR.mp": (1, self.T),
            f"{p}NASA_SSH_ENSO_INDICATOR.txt": (1, self.T),
            f"{p}archive/GMSL/NASA_SSH_GMSL_INDICATOR_20261116.txt": (1, self.T),
        })
        c = Collection("NASA-SSH", "NASA_SSH_GMSL_INDICATOR", p, ".txt", "NASA_SSH_GMSL_INDICATOR")
        self.assertEqual(
            sorted(list_released(s3, "b", c)),
            ["NASA_SSH_GMSL_INDICATOR", "NASA_SSH_GMSL_INDICATOR_20261116"],
        )

    def test_duplicate_name_across_directories_raises(self):
        """Delivery flattens paths, so two files with one name collide at PO.DAAC."""
        s3 = FakeS3({
            f"products/NASA-SSH/along_track/2025/{AT}20250101.nc": (1, self.T),
            f"products/NASA-SSH/along_track/2026/{AT}20250101.nc": (1, self.T),
        })
        with self.assertRaisesRegex(ValueError, "share a granule name"):
            list_released(s3, "b", X)


class FetchCmrTestCase(unittest.TestCase):
    def test_follows_search_after_until_short_page(self):
        full = [umm_item(f"g{i}") for i in range(CMR_PAGE_SIZE)]
        session = FakeSession({"X": [full, [umm_item("last")]]})
        granules = fetch_cmr_granules(session, "X")
        self.assertEqual(len(granules), CMR_PAGE_SIZE + 1)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(session.calls[1][1], {"CMR-Search-After": "1"})

    def test_size_is_data_file_not_md5(self):
        session = FakeSession({"X": [[umm_item("a", size=3381761)]]})
        g = fetch_cmr_granules(session, "X")["a"]
        self.assertEqual(g.size, 3381761)
        self.assertEqual(g.revision_date, datetime(2026, 11, 16, 16, 23, 49, 169000, tzinfo=timezone.utc))

    def test_empty_collection(self):
        self.assertEqual(fetch_cmr_granules(FakeSession({}), "X"), {})

    def test_fewer_granules_than_hits_is_refused(self):
        """A truncated listing would report real granules as missing."""
        session = FakeSession({"X": [[umm_item("a")]]}, hits={"X": 2})
        with self.assertRaisesRegex(PartialCmrResults, "reported 2 granules, returned 1"):
            fetch_cmr_granules(session, "X")

    def test_paging_stopping_early_is_refused(self):
        """A full page with no Search-After header ends paging; CMR-Hits catches it."""
        full = [umm_item(f"g{i}") for i in range(CMR_PAGE_SIZE)]
        session = FakeSession({"X": [full]}, hits={"X": CMR_PAGE_SIZE + 5})
        with self.assertRaises(PartialCmrResults):
            fetch_cmr_granules(session, "X")

    def test_duplicate_granule_urs_are_refused(self):
        """Duplicates across pages mean a shifted page, so some other granule was skipped."""
        session = FakeSession({"X": [[umm_item("a"), umm_item("a")]]})
        with self.assertRaises(PartialCmrResults):
            fetch_cmr_granules(session, "X")


class RunAuditTestCase(unittest.TestCase):
    def test_clean_reference_line(self):
        outcomes = run_audit(
            resolve_collections(), FakeS3(reference_keys()), "b", FakeSession(reference_cmr()), NOW, GRACE
        )
        self.assertEqual(len(outcomes), 6)
        self.assertTrue(all(isinstance(o, AuditResult) and o.problems == 0 for o in outcomes))

    def test_cmr_failure_on_one_collection_does_not_stop_the_rest(self):
        session = FakeSession(reference_cmr(), status={"NASA_SSH_REF_SIMPLE_GRID_V11": 503})
        outcomes = run_audit(resolve_collections(), FakeS3(reference_keys()), "b", session, NOW, GRACE)
        errors = [o for o in outcomes if isinstance(o, AuditError)]
        self.assertEqual([e.collection.short_name for e in errors], ["NASA_SSH_REF_SIMPLE_GRID_V11"])
        self.assertIn("503", errors[0].error)
        self.assertEqual(sum(isinstance(o, AuditResult) for o in outcomes), 5)

    def test_s3_failure_on_one_collection_is_reported(self):
        s3 = FakeS3(reference_keys(), fail_prefixes=("products/NASA-SSH/along_track/",))
        outcomes = run_audit(resolve_collections(), s3, "b", FakeSession(reference_cmr()), NOW, GRACE)
        errors = [o for o in outcomes if isinstance(o, AuditError)]
        self.assertEqual([e.collection.short_name for e in errors], ["NASA_SSH_REF_ALONGTRACK_V11"])
        self.assertIn("AccessDenied", errors[0].error)

    def test_partial_cmr_is_an_error_not_missing_files(self):
        session = FakeSession(reference_cmr(), hits={"NASA_SSH_REF_ALONGTRACK_V11": 2})
        outcomes = run_audit(resolve_collections(), FakeS3(reference_keys()), "b", session, NOW, GRACE)
        at = next(o for o in outcomes if o.collection.short_name == "NASA_SSH_REF_ALONGTRACK_V11")
        self.assertIsInstance(at, AuditError)
        self.assertIn("PartialCmrResults", at.error)


class RenderReportTestCase(unittest.TestCase):
    def result(self, short_name="X", **kw):
        r = AuditResult(Collection("NASA-SSH", short_name, "p/", ".nc"), released=1, ingested=1)
        for k, v in kw.items():
            setattr(r, k, v)
        return r

    def test_clean(self):
        subject, body = render_report([self.result()], "b", GRACE)
        self.assertEqual(subject, "Delivery audit clean")
        self.assertTrue(body.endswith("Audit clean"))
        self.assertIn("Published products: NASA-SSH", body)
        self.assertIn("Grace: 12 h", body)

    def test_problems_are_listed_by_granule_ur(self):
        subject, body = render_report([self.result(missing=["a"], stale=["b"])], "b", GRACE)
        self.assertEqual(subject, "Delivery audit: 2 problems")
        self.assertIn("  missing: a", body)
        self.assertIn("  stale: b", body)

    def test_long_lists_are_capped_with_a_count(self):
        urs = [f"g{i:03d}" for i in range(60)]
        _, body = render_report([self.result(missing=urs)], "b", GRACE, cap=50)
        self.assertIn("  missing: g049", body)
        self.assertNotIn("  missing: g050", body)
        self.assertIn("  missing: ... 10 more (60 total)", body)

    def test_pending_and_cmr_only_only_when_verbose(self):
        r = self.result(pending=["p"], cmr_only=["c"])
        _, quiet = render_report([r], "b", GRACE)
        _, loud = render_report([r], "b", GRACE, verbose=True)
        self.assertNotIn("pending: p", quiet)
        self.assertIn("  pending: p", loud)
        self.assertIn("  CMR only: c", loud)

    def test_errors_lead_and_name_the_subject(self):
        err = AuditError(Collection("NASA-SSH", "Y", "p/", ".nc"), "RuntimeError: 503 Server Error")
        subject, body = render_report([self.result(), err], "b", GRACE)
        self.assertEqual(subject, "Delivery audit incomplete: 1 of 2 collections not audited")
        self.assertLess(body.index("Y: could not audit"), body.index("X: released"))
        self.assertIn("1 collections could not be audited; 0 problems in the rest", body)

    def test_empty_collection_names_its_prefix(self):
        _, body = render_report([self.result(released=0)], "b", GRACE)
        self.assertIn("  no released files under p/", body)


class MainTestCase(unittest.TestCase):
    def run_main(self, keys, cmr, argv=("--bucket", "b"), **session_kw):
        out = io.StringIO()
        with redirect_stdout(out):
            code = main(list(argv), s3_client=FakeS3(keys), http_session=FakeSession(cmr, **session_kw), now=NOW)
        return code, out.getvalue()

    def test_clean_audit_exits_zero(self):
        code, out = self.run_main(reference_keys(), reference_cmr())
        self.assertEqual(code, 0, out)
        self.assertIn("Audit clean", out)

    def test_missing_granule_exits_one_and_names_it(self):
        keys = reference_keys()
        keys[f"products/NASA-SSH/along_track/2026/{AT}20261113.nc"] = (100, OLD)
        code, out = self.run_main(keys, reference_cmr())
        self.assertEqual(code, 1)
        self.assertIn(f"missing: {AT}20261113", out)

    def test_hand_runs_list_every_problem(self):
        keys = reference_keys()
        for i in range(60):
            keys[f"products/NASA-SSH/along_track/2025/{AT}2025{i:04d}.nc"] = (100, OLD)
        _, out = self.run_main(keys, reference_cmr())
        self.assertEqual(out.count("  missing: "), 60)

    def test_no_released_files_is_a_problem(self):
        keys = {k: v for k, v in reference_keys().items() if "/simple_grid/" not in k}
        code, out = self.run_main(keys, reference_cmr())
        self.assertEqual(code, 1)
        self.assertIn("no released files under products/NASA-SSH/simple_grid/", out)

    def test_collection_that_cannot_be_audited_exits_two(self):
        code, out = self.run_main(reference_keys(), reference_cmr(), hits={"NASA_SSH_IOD_INDICATOR": 9})
        self.assertEqual(code, 2)
        self.assertIn("NASA_SSH_IOD_INDICATOR: could not audit", out)

    def test_undelivered_product_exits_two(self):
        code, _ = self.run_main(reference_keys(), reference_cmr(), ("--bucket", "b", "--product", "NASA-SSH_S3B"))
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()

