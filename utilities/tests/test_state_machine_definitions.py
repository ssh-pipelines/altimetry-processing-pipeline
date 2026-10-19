"""Structural checks on the ASL templates.

Nothing in the suite deploys a state machine, so a definition Step Functions
rejects at `update-state-machine` time otherwise surfaces only after the
Lambdas it fans out to are already live. These pins cover the mistakes that
are invisible in review: fields valid on one Map mode and rejected on the
other, and leaf ARNs that drift from the template placeholders.
"""

import json
import re
import unittest
from pathlib import Path

from utilities import targets

TEMPLATE_DIR = Path(__file__).resolve().parents[2] / "state_machines"

_FUNCTION_ARN = re.compile(r'"FunctionName":\s*"([^"]+)"')

# Accepted only on a DISTRIBUTED Map. Inline mode has no child-workflow
# execution to read, write, batch, or partially tolerate, so Step Functions
# rejects the whole definition rather than ignoring the field.
DISTRIBUTED_ONLY_FIELDS = (
    "ItemReader",
    "ResultWriter",
    "ItemBatcher",
    "Label",
    "ToleratedFailureCount",
    "ToleratedFailureCountPath",
    "ToleratedFailurePercentage",
    "ToleratedFailurePercentagePath",
)


def _templates() -> list[Path]:
    return sorted(TEMPLATE_DIR.glob("*.asl.json"))


def _map_states(states: dict, path: str = "") -> list[tuple[str, dict]]:
    """Every Map state in a definition, including ones nested in a Map or
    Parallel branch, labelled by its path for a legible failure message."""
    found = []
    for name, state in states.items():
        qualified = f"{path}{name}"
        if state.get("Type") == "Map":
            found.append((qualified, state))
            processor = state.get("ItemProcessor") or state.get("Iterator") or {}
            found += _map_states(processor.get("States", {}), f"{qualified}/")
        elif state.get("Type") == "Parallel":
            for index, branch in enumerate(state.get("Branches", [])):
                found += _map_states(branch.get("States", {}), f"{qualified}[{index}]/")
    return found


def _mode(map_state: dict) -> str:
    processor = map_state.get("ItemProcessor") or map_state.get("Iterator") or {}
    return processor.get("ProcessorConfig", {}).get("Mode", "INLINE")


class TestTemplatesParse(unittest.TestCase):
    def test_templates_exist(self):
        self.assertTrue(_templates(), f"no ASL templates under {TEMPLATE_DIR}")

    def test_every_template_is_valid_json_with_states(self):
        for template in _templates():
            with self.subTest(template=template.name):
                definition = json.loads(template.read_text())
                self.assertIn("States", definition)
                self.assertIn(definition["StartAt"], definition["States"])


class TestMapModeFields(unittest.TestCase):
    def test_inline_maps_carry_no_distributed_only_fields(self):
        """The parent's source tier is inline because the merge consumes its item
        results; `ToleratedFailureCount: 0` and `Label` were set on it anyway and
        made the definition unacceptable to `update-state-machine`."""
        for template in _templates():
            definition = json.loads(template.read_text())
            for name, map_state in _map_states(definition["States"]):
                if _mode(map_state) != "INLINE":
                    continue
                with self.subTest(template=template.name, state=name):
                    offenders = [f for f in DISTRIBUTED_ONLY_FIELDS if f in map_state]
                    self.assertEqual(
                        offenders,
                        [],
                        f"{template.name}:{name} is an INLINE Map carrying "
                        f"DISTRIBUTED-only field(s) {offenders}",
                    )

    def test_distributed_maps_are_still_allowed_those_fields(self):
        """Guard against the check above being tightened into a blanket ban: the
        leaf ASLs all fan out distributed and depend on ResultWriter."""
        writers = [
            name
            for template in _templates()
            for name, map_state in _map_states(json.loads(template.read_text())["States"])
            if _mode(map_state) == "DISTRIBUTED" and "ResultWriter" in map_state
        ]
        self.assertTrue(writers)


class TestLambdaArns(unittest.TestCase):
    """The templates and `utilities/targets.yaml` name the same Lambdas from two
    places, and only the registry drives the deploy. A template invoking a
    function the registry does not build deploys clean and fails at runtime."""

    def setUp(self):
        self.arns = [
            (template.name, arn)
            for template in _templates()
            for arn in _FUNCTION_ARN.findall(template.read_text())
        ]
        self.registered = {t.function_name("${STAGE}") for t in targets.targets() if t.deployable}

    def test_templates_invoke_lambdas(self):
        self.assertTrue(self.arns)

    def test_region_and_account_are_placeholders(self):
        """`render.sh` substitutes region, account, and stage. A literal in any of
        the three would point a dev deploy at prod, or vice versa."""
        for template, arn in self.arns:
            with self.subTest(template=template, arn=arn):
                self.assertTrue(arn.startswith("arn:aws:lambda:${AWS_REGION}:${AWS_ACCOUNT_ID}:"))

    def test_every_invoked_function_is_a_registered_target(self):
        """Pins both halves of a stage rename: an ASL still invoking a deleted
        stage, and a new Lambda invoked without a registry entry to build it."""
        for template, arn in self.arns:
            function = arn.split(":function:")[1].split(":")[0]
            with self.subTest(template=template, function=function):
                self.assertIn(function, self.registered)


if __name__ == "__main__":
    unittest.main()
