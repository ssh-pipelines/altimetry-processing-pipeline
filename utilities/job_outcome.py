"""Job-outcome contract for the altimetry pipeline's deliverable stages.

A **deliverable stage** (`finalizer`, `unifier`, `simple_grids`, `enso`) returns a
``JobOutcome`` from its Lambda handler instead of a thin ``{status, data}`` dict. The
Distributed Map's ``ResultWriter`` persists each return verbatim into ``SUCCEEDED_n.json``,
so a Job outcome is the success-side analog of the structured ``FAILED_n.json`` entry
(ADR 0003): same mechanism, same prefixes, opposite outcome.

The producing stage already computes its output key via ``utilities.pipeline_layout``; it
now *reports* that key (an :class:`Output`) rather than leaving the success path to infer it
from live S3 listings. The ``run_summary`` Lambda reconciles these declared outputs against
the jobs manifest (see ADR 0005).

Consumers read plain JSON (``to_dict()``); only producers depend on these dataclasses.
``schema_version`` guards evolution of the wire shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field

SCHEMA_VERSION = 1

# status values
SUCCESS = "success"
SKIPPED = "skipped"


@dataclass(frozen=True)
class Output:
    """One artifact a stage declares it wrote.

    ``key`` is a bucket-relative S3 key (from ``pipeline_layout``); ``kind`` is a stable
    label for the artifact family (e.g. ``daily_file_p3``, ``simple_grid``).
    """

    key: str
    kind: str

    def to_dict(self) -> dict:
        return {"key": self.key, "kind": self.kind}


@dataclass
class JobOutcome:
    """What a deliverable stage produced (or intentionally skipped) for one job.

    ``status`` is :data:`SUCCESS` or :data:`SKIPPED`. A skipped outcome carries no outputs
    and explains itself via ``metadata['skip_reason']``, distinguishing an *intentionally*
    absent deliverable (e.g. a low-coverage gridding date) from a silent gap.
    """

    stage: str
    status: str
    date: str
    source: str
    outputs: list[Output] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION

    @classmethod
    def success(
        cls,
        *,
        stage: str,
        date: str,
        source: str,
        outputs: list[Output],
        metadata: dict | None = None,
    ) -> "JobOutcome":
        return cls(
            stage=stage,
            status=SUCCESS,
            date=date,
            source=source,
            outputs=outputs,
            metadata=metadata or {},
        )

    @classmethod
    def skipped(
        cls,
        *,
        stage: str,
        date: str,
        source: str,
        reason: str,
        metadata: dict | None = None,
    ) -> "JobOutcome":
        meta = {"skip_reason": reason}
        if metadata:
            meta.update(metadata)
        return cls(
            stage=stage,
            status=SKIPPED,
            date=date,
            source=source,
            outputs=[],
            metadata=meta,
        )

    def to_dict(self) -> dict:
        return {
            "schema_version": self.schema_version,
            "stage": self.stage,
            "status": self.status,
            "date": self.date,
            "source": self.source,
            "outputs": [o.to_dict() for o in self.outputs],
            "metadata": self.metadata,
        }
