"""Target registry: the single source of truth for the build/deploy scripts.

A *Target* is anything the build/deploy scripts manage — a buildable image
and/or a deployable Lambda. Existence and packaging kind are derived from the
filesystem; the ``heavy`` / ``deployable`` facts are declared in ``targets.yaml``.
See CONTEXT.md -> Build & deploy and docs/adr/0004-target-registry.md.

Callers (the bash scripts) use the CLI:

    python -m utilities.targets catalog [--stage dev]   # TSV catalog
    python -m utilities.targets dirty --base main       # names to rebuild

The pure functions ``targets()`` and ``dirty()`` are the test surface.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable

import yaml


class Packaging(str, Enum):
    CONTAINER = "container"
    ZIP = "zip"


# Repo-relative paths every container target (except pipeline_runtime) depends
# on at build time (each stage Dockerfile does `COPY utilities/` + `pip install .`).
# If the shared package or build metadata moves (e.g. the planned
# utilities/ -> src/shared/), update this one constant.
_SHARED_BUILD_PATHS = ("utilities", "pyproject.toml")

_RUNTIME_NAME = "pipeline_runtime"
_MANIFEST_NAME = "targets.yaml"


@dataclass(frozen=True)
class Target:
    name: str
    path: Path  # repo-relative dir holding the Dockerfile / app.py
    packaging: Packaging
    heavy: bool
    deployable: bool
    # Explicit Lambda function name, overriding the ``${stage}-${name}`` default.
    # Used for stage-agnostic singletons whose one function is shared across
    # stages (e.g. the podaac_cred_update credential refresher).
    function_override: str | None = None
    # Stages whose deploy step updates this target's Lambda; None means every
    # stage. Restricts a shared singleton to the controlled path (e.g. prod only).
    deploy_stages: tuple[str, ...] | None = None

    def ecr_repo(self, stage: str) -> str:
        if self.packaging is not Packaging.CONTAINER:
            raise ValueError(f"{self.name!r} is not a container target; it has no ECR repo")
        return f"{stage}/{self.name}"

    def function_name(self, stage: str) -> str:
        if not self.deployable:
            raise ValueError(f"{self.name!r} is not deployable; it has no Lambda function")
        if self.function_override:
            return self.function_override
        return f"{stage}-{self.name}"

    def deployable_in(self, stage: str) -> bool:
        """Whether deploying ``stage`` updates this target's Lambda.

        Defaults to every stage; ``deploy_stages`` in targets.yaml narrows it so
        a shared singleton is only touched by the stage(s) that own it.
        """
        return self.deployable and (
            self.deploy_stages is None or stage in self.deploy_stages
        )


def _find_repo_root() -> Path:
    """Locate the repo root from either the module location (editable install)
    or the cwd (the scripts enforce running from the repo root)."""
    for start in (Path(__file__).resolve(), Path.cwd().resolve()):
        for d in (start, *start.parents):
            if (d / "pyproject.toml").is_file() and (d / "pipeline").is_dir():
                return d
    raise RuntimeError(
        "Target registry: could not locate the repo root (need pyproject.toml + pipeline/)."
    )


def _discover(repo_root: Path) -> dict[str, tuple[Path, Packaging]]:
    """name -> (repo-relative path, packaging), derived from the filesystem."""
    found: dict[str, tuple[Path, Packaging]] = {}
    pipeline = repo_root / "pipeline"

    for dockerfile in pipeline.rglob("Dockerfile"):
        d = dockerfile.parent
        found[d.name] = (d.relative_to(repo_root), Packaging.CONTAINER)

    for app in (pipeline / "infra").glob("*/app.py"):
        d = app.parent
        if (d / "Dockerfile").is_file():
            continue  # already captured as a container target
        found[d.name] = (d.relative_to(repo_root), Packaging.ZIP)

    return found


def _load_declared() -> dict[str, dict]:
    manifest = Path(__file__).resolve().parent / _MANIFEST_NAME
    with open(manifest) as f:
        data = yaml.safe_load(f) or {}
    return data.get("targets") or {}


_CACHE: list[Target] | None = None


def clear_cache() -> None:
    """Drop the memoized catalog (tests)."""
    global _CACHE
    _CACHE = None


def targets() -> list[Target]:
    """The full catalog, sorted by name.

    Raises if ``targets.yaml`` and the filesystem disagree, so every caller is
    protected from drift, not just the consistency test.
    """
    global _CACHE
    if _CACHE is not None:
        return _CACHE

    repo_root = _find_repo_root()
    discovered = _discover(repo_root)
    declared = _load_declared()

    fs_names, yaml_names = set(discovered), set(declared)
    if fs_names != yaml_names:
        lines = ["Target registry: targets.yaml is out of sync with the filesystem."]
        if fs_names - yaml_names:
            lines.append(f"  on disk but not declared: {sorted(fs_names - yaml_names)}")
        if yaml_names - fs_names:
            lines.append(f"  declared but not on disk: {sorted(yaml_names - fs_names)}")
        raise RuntimeError("\n".join(lines))

    result: list[Target] = []
    for name, (path, packaging) in sorted(discovered.items()):
        facts = declared.get(name) or {}
        heavy = bool(facts.get("heavy", False))
        deployable = bool(facts.get("deployable", True))
        function_override = facts.get("function")
        raw_stages = facts.get("deploy_stages")
        deploy_stages = tuple(raw_stages) if raw_stages is not None else None
        if packaging is Packaging.ZIP and heavy:
            raise RuntimeError(f"Target registry: zip target {name!r} cannot be heavy.")
        if not deployable and (function_override or deploy_stages is not None):
            raise RuntimeError(
                f"Target registry: {name!r} is not deployable but declares "
                f"function/deploy_stages."
            )
        result.append(
            Target(
                name,
                path,
                packaging,
                heavy,
                deployable,
                function_override=function_override,
                deploy_stages=deploy_stages,
            )
        )

    _CACHE = result
    return result


def get(name: str) -> Target:
    for t in targets():
        if t.name == name:
            return t
    raise KeyError(name)


def _under(path: str, root: str) -> bool:
    p = path.replace("\\", "/").strip("/")
    r = root.replace("\\", "/").strip("/")
    return p == r or p.startswith(r + "/")


def dirty(changed_paths: Iterable[str]) -> list[Target]:
    """The Targets that must be rebuilt/redeployed for a set of changed
    repo-relative paths. Pure over (changed_paths, catalog).

    Edges (see CONTEXT.md -> Change-impact):
      - own dir changed                     -> any target
      - utilities/ or pyproject.toml changed -> every container target except pipeline_runtime
      - pipeline_runtime/ changed           -> every heavy target + pipeline_runtime itself
      - any heavy target in result          -> pipeline_runtime (must exist in ECR at the
                                              target SHA even when its content is unchanged)
    """
    changed = [c for c in changed_paths if c and c.strip()]

    shared_changed = any(
        any(_under(c, sp) for sp in _SHARED_BUILD_PATHS) for c in changed
    )
    runtime_path = str(get(_RUNTIME_NAME).path)
    runtime_changed = any(_under(c, runtime_path) for c in changed)

    result: list[Target] = []
    for t in targets():
        own = any(_under(c, str(t.path)) for c in changed)
        shared_edge = (
            t.packaging is Packaging.CONTAINER
            and t.name != _RUNTIME_NAME
            and shared_changed
        )
        runtime_edge = t.heavy and runtime_changed
        if own or shared_edge or runtime_edge:
            result.append(t)

    # If any heavy stage needs to be built, pipeline_runtime must also be
    # available at the target SHA — the SHA-tagged ECR image won't exist even
    # when its own content is unchanged.
    if any(t.heavy for t in result):
        runtime = get(_RUNTIME_NAME)
        if runtime not in result:
            result.insert(0, runtime)

    return result


# ─── CLI ─────────────────────────────────────────────────────────────────

def _cmd_catalog(stage: str | None) -> int:
    for t in targets():
        ecr = t.ecr_repo(stage) if (stage and t.packaging is Packaging.CONTAINER) else ""
        # With a stage, the deployable column is stage-specific so the deploy
        # core skips targets this stage doesn't own (e.g. the prod-only
        # podaac_cred_update singleton is not deployable in dev). Without a
        # stage it reports the static fact.
        deployable = t.deployable_in(stage) if stage else t.deployable
        fn = t.function_name(stage) if (stage and deployable) else ""
        print(
            "\t".join(
                [
                    t.name,
                    str(t.path),
                    t.packaging.value,
                    "true" if t.heavy else "false",
                    "true" if deployable else "false",
                    ecr,
                    fn,
                ]
            )
        )
    return 0


def _cmd_dirty(base: str) -> int:
    import subprocess

    proc = subprocess.run(
        ["git", "diff", "--name-only", f"{base}...HEAD"],
        capture_output=True,
        text=True,
        check=True,
    )
    for t in dirty(proc.stdout.splitlines()):
        print(t.name)
    return 0


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(prog="python -m utilities.targets")
    sub = parser.add_subparsers(dest="cmd", required=True)

    cat = sub.add_parser("catalog", help="emit the TSV catalog of all targets")
    cat.add_argument("--stage", help="fill ecr_repo/function columns for this stage")

    dty = sub.add_parser("dirty", help="emit names of targets changed vs a git ref")
    dty.add_argument("--base", default="main", help="base git ref (default: main)")

    args = parser.parse_args(argv)
    if args.cmd == "catalog":
        return _cmd_catalog(args.stage)
    if args.cmd == "dirty":
        return _cmd_dirty(args.base)
    return 2


if __name__ == "__main__":
    sys.exit(main())
