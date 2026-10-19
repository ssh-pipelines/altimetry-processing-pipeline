"""The schedule registry paired against the Source registry.

`enabled` on a Source and `enabled` on a schedule are independent facts, and
both failure modes between them are silent: an enabled Source whose published
line has no schedule simply never runs, and an enabled schedule whose line has
no enabled contributor runs to completion having done nothing. These are the
static counterparts to `start_run` raising at runtime.
"""

import unittest

from utilities import schedules
from utilities.products import (
    contributing_sources,
    published_identity,
    registered_contributors,
)
from utilities.source_profile import get_registered_sources, get_source_profile


class TestScheduleRegistry(unittest.TestCase):
    def setUp(self):
        schedules.clear_cache()
        self.schedules = schedules.list_schedules()

    def test_registry_is_not_empty(self):
        self.assertTrue(self.schedules)

    def test_every_schedule_is_product_keyed(self):
        """The whole point of the change: a trigger names a Published product,
        never a Source. A leftover `source` key would still *work* (pipeline_init
        would be handed it) while skipping contributor fan-out entirely."""
        for s in self.schedules:
            self.assertIn("published_product", s.input, s.name)
            self.assertNotIn("source", s.input, s.name)

    def test_nominal_schedules_carry_no_overrides(self):
        """`force_update` or a pinned date window on a *scheduled* rule would
        re-plan the full history every week."""
        for s in self.schedules:
            self.assertEqual(
                set(s.input) - {"published_product", "bucket"},
                set(),
                f"{s.name} carries invocation overrides",
            )

    def test_every_schedule_names_a_resolvable_published_product(self):
        """Catches a typo in an identity string, which would otherwise surface
        only as a failed run at the schedule's next firing."""
        for s in self.schedules:
            self.assertTrue(
                registered_contributors(s.published_product),
                f"{s.name} names '{s.published_product}', which no registered "
                f"source resolves to",
            )

    def test_enabled_schedules_have_an_enabled_contributor(self):
        """An enabled rule whose line has no enabled Source resolves to zero
        contributors — a run that succeeds having produced nothing."""
        for s in self.schedules:
            if not s.enabled:
                continue
            self.assertTrue(
                contributing_sources(s.published_product),
                f"{s.name} is enabled but no enabled source contributes to "
                f"'{s.published_product}'",
            )

    def test_every_enabled_source_has_a_schedule(self):
        """The complement: a Source flipped to `enabled: true` with nothing
        pointed at its published line never runs, and looks configured."""
        scheduled = {s.published_product for s in self.schedules}
        for source in get_registered_sources():
            profile = get_source_profile(source)
            if not profile.enabled:
                continue
            identity = published_identity(profile)
            self.assertIn(
                identity,
                scheduled,
                f"source '{source}' is enabled but no schedule triggers "
                f"'{identity}'",
            )

    def test_schedule_names_are_unique(self):
        names = [s.name for s in self.schedules]
        self.assertEqual(len(names), len(set(names)))


class TestScheduleFields(unittest.TestCase):
    def test_created_reflects_the_cron_placeholder(self):
        staged = schedules.Schedule("x", schedules.UNSCHEDULED, True, {})
        live = schedules.Schedule("y", "cron(0 12 ? * MON *)", True, {})
        self.assertFalse(staged.created)
        self.assertTrue(live.created)

    def test_published_product_raises_without_one(self):
        with self.assertRaises(ValueError):
            schedules.Schedule("x", "TBD", True, {}).published_product


if __name__ == "__main__":
    unittest.main()
