import unittest
from pathlib import Path

from utilities import targets as reg
from utilities.targets import Packaging


# Expected catalog, kept here as an independent assertion of intent (the module
# derives this from the filesystem; this pins what we expect to find).
HEAVY = {"bad_pass", "daily_files", "finalizer", "oer", "xover", "enso", "indicators", "simple_grids"}
LIGHT_CONTAINER = {"pipeline_init", "unifier", "run_summary"}
BASE_IMAGE = {"pipeline_runtime"}
ZIP = {"failure_handling", "podaac_auth", "rewrite_manifest", "set_sg_jobs"}

ALL_CONTAINER = HEAVY | LIGHT_CONTAINER | BASE_IMAGE
ALL_NAMES = ALL_CONTAINER | ZIP

REPO_ROOT = Path(__file__).resolve().parents[2]


class TestCatalogConsistency(unittest.TestCase):
    def setUp(self):
        reg.clear_cache()

    def test_manifest_matches_filesystem(self):
        # targets() raises if targets.yaml and the filesystem disagree.
        names = {t.name for t in reg.targets()}
        self.assertEqual(names, ALL_NAMES)

    def test_packaging_is_derived(self):
        by_name = {t.name: t for t in reg.targets()}
        for name in ALL_CONTAINER:
            self.assertIs(by_name[name].packaging, Packaging.CONTAINER, name)
        for name in ZIP:
            self.assertIs(by_name[name].packaging, Packaging.ZIP, name)

    def test_paths_resolve(self):
        for t in reg.targets():
            d = REPO_ROOT / t.path
            self.assertTrue(d.is_dir(), f"{t.name}: {d} is not a dir")
            marker = "Dockerfile" if t.packaging is Packaging.CONTAINER else "app.py"
            self.assertTrue((d / marker).is_file(), f"{t.name}: missing {marker}")

    def test_deployable(self):
        by_name = {t.name: t for t in reg.targets()}
        self.assertFalse(by_name["pipeline_runtime"].deployable)
        for name in ALL_NAMES - {"pipeline_runtime"}:
            self.assertTrue(by_name[name].deployable, name)

    def test_heavy_matches_base_image_parameterization(self):
        # The brittle-grep contract, guarded: heavy <=> the Dockerfile
        # parameterizes its base via ${BASE_IMAGE}.
        for t in reg.targets():
            if t.packaging is not Packaging.CONTAINER:
                self.assertFalse(t.heavy, f"{t.name}: zip target marked heavy")
                continue
            text = (REPO_ROOT / t.path / "Dockerfile").read_text()
            parameterized = "${BASE_IMAGE}" in text
            self.assertEqual(
                t.heavy, parameterized,
                f"{t.name}: heavy={t.heavy} but Dockerfile parameterized={parameterized}",
            )


class TestNamingHelpers(unittest.TestCase):
    def setUp(self):
        reg.clear_cache()

    def test_ecr_repo(self):
        self.assertEqual(reg.get("oer").ecr_repo("dev"), "dev/oer")

    def test_function_name(self):
        self.assertEqual(reg.get("oer").function_name("prod"), "prod-oer")
        self.assertEqual(reg.get("failure_handling").function_name("dev"), "dev-failure_handling")

    def test_function_name_override_is_stage_agnostic(self):
        # podaac_auth is a shared singleton: one function name for every stage.
        pa = reg.get("podaac_auth")
        self.assertEqual(pa.function_name("prod"), "podaac_cred_update")
        self.assertEqual(pa.function_name("dev"), "podaac_cred_update")

    def test_deployable_in_defaults_to_all_stages(self):
        oer = reg.get("oer")
        self.assertTrue(oer.deployable_in("dev"))
        self.assertTrue(oer.deployable_in("prod"))

    def test_deploy_stages_restricts_singleton_to_prod(self):
        # The shared podaac_cred_update Lambda is only deployed from prod, so the
        # dev pipeline skips it (no dev-podaac_auth) and release.sh targets the
        # real shared name (no prod-podaac_auth).
        pa = reg.get("podaac_auth")
        self.assertTrue(pa.deployable_in("prod"))
        self.assertFalse(pa.deployable_in("dev"))

    def test_ecr_repo_rejects_zip(self):
        with self.assertRaises(ValueError):
            reg.get("failure_handling").ecr_repo("dev")

    def test_function_name_rejects_non_deployable(self):
        with self.assertRaises(ValueError):
            reg.get("pipeline_runtime").function_name("dev")


class TestChangeImpact(unittest.TestCase):
    def setUp(self):
        reg.clear_cache()

    def _names(self, changed):
        return {t.name for t in reg.dirty(changed)}

    def test_empty(self):
        self.assertEqual(self._names([]), set())

    def test_own_dir_light_container(self):
        pi = reg.get("pipeline_init").path
        self.assertEqual(self._names([f"{pi}/handler.py"]), {"pipeline_init"})

    def test_heavy_own_dir_includes_runtime(self):
        # Changing a heavy stage's own dir triggers a build; pipeline_runtime must
        # also be built so the SHA-tagged ECR image exists, even if its content
        # hasn't changed.
        oer = reg.get("oer").path
        self.assertEqual(self._names([f"{oer}/processing.py"]), {"oer", "pipeline_runtime"})

    def test_shared_utilities_dirties_all_containers(self):
        # Heavy stages are dirty → pipeline_runtime is added via the build-dep edge.
        self.assertEqual(
            self._names(["utilities/aws_utils.py"]),
            ALL_CONTAINER,
        )

    def test_setup_py_dirties_all_containers(self):
        self.assertEqual(self._names(["setup.py"]), ALL_CONTAINER)

    def test_runtime_change_dirties_heavy_and_runtime_itself(self):
        rt = reg.get("pipeline_runtime").path
        self.assertEqual(
            self._names([f"{rt}/requirements.txt"]),
            HEAVY | BASE_IMAGE,
        )

    def test_zip_is_self_contained(self):
        fh = reg.get("failure_handling").path
        self.assertEqual(self._names([f"{fh}/app.py"]), {"failure_handling"})

    def test_shared_change_does_not_dirty_zip(self):
        self.assertNotIn("failure_handling", self._names(["utilities/aws_utils.py"]))

    def test_combined(self):
        oer = reg.get("oer").path
        self.assertEqual(
            self._names([f"{oer}/x.py", "utilities/aws_utils.py"]),
            ALL_CONTAINER,
        )


if __name__ == "__main__":
    unittest.main()
