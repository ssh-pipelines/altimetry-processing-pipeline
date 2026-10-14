"""Promote an internal staging artifact to its released address under `products/`.

One Lambda for both product families, with `family` as an argument. The two jobs
are not quite symmetric — along-track fans out over the along-track manifest and
will eventually compose contested dates, grid promotion fans out over the
gridded manifest and stays a copy — but neither difference justifies two
near-identical copy Lambdas, two registry rows, two READMEs and two deploy paths.

Still a byte-level `copy_object` with no NetCDF engine, so this stage stays off
`pipeline_runtime` (ADR 0001). The copy preserves the file's in-file
`processing_history` lineage without opening it.
"""

import json
import logging
from datetime import datetime

import boto3

from utilities.errors import PipelineError
from utilities.job_outcome import JobOutcome, Output
from utilities.pipeline_layout import (
    daily_file_key,
    published_along_track_key,
    published_simple_grid_key,
    simple_grid_key,
)
from utilities.source_profile import get_source_profile

ALONG_TRACK = "along_track"
SIMPLE_GRID = "simple_grid"
FAMILIES = (ALONG_TRACK, SIMPLE_GRID)

# Job-outcome `kind` per family: what the run summary reconciles the promotion as.
OUTPUT_KINDS = {
    ALONG_TRACK: "published_along_track",
    SIMPLE_GRID: "published_simple_grid",
}


def resolve_keys(
    family: str,
    publish_as: str,
    contributors: list[str],
    date_obj,
) -> tuple[str, str]:
    """(internal source key, published destination key) for one date.

    The interface is one-date-many-contributors even though today's along-track
    implementation copies from exactly one: once a reference date can be claimed
    by two overlapping missions this becomes a composer, and that change should
    replace the body without reshaping the event or the Job outcome.
    """
    if family == ALONG_TRACK:
        if not contributors:
            raise ValueError(
                f"family '{ALONG_TRACK}' requires contributors — there is no "
                f"internal along-track file for a published product itself."
            )
        if len(contributors) > 1:
            raise ValueError(
                f"{len(contributors)} contributors claim {publish_as} on "
                f"{date_obj.isoformat()} ({contributors}); composing a contested "
                f"date is not implemented, and picking one silently would let "
                f"execution order decide what ships."
            )
        profile = get_source_profile(contributors[0])
        return (
            daily_file_key(profile, date_obj, "p3"),
            published_along_track_key(publish_as, date_obj),
        )

    if family == SIMPLE_GRID:
        # The staged grid is keyed on its own output identity, so the contributor
        # list that produced it is not needed to find it.
        return (
            simple_grid_key(publish_as, date_obj),
            published_simple_grid_key(publish_as, date_obj),
        )

    raise ValueError(f"Unknown product family '{family}'. Available: {list(FAMILIES)}")


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event.get("bucket")
    proc_date = event.get("date")
    family = event.get("family")
    publish_as = event.get("publish_as")

    if None in [bucket, proc_date, family, publish_as]:
        raise ValueError(
            "One of date, bucket, family, or publish_as job parameters missing."
        )

    contributors = event.get("contributors") or []

    try:
        date_obj = datetime.strptime(proc_date, "%Y-%m-%d").date()
        src_key, dst_key = resolve_keys(family, publish_as, contributors, date_obj)

        logging.info(f"Publishing s3://{bucket}/{src_key} -> s3://{bucket}/{dst_key}")

        s3 = boto3.client("s3")
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": src_key},
            Key=dst_key,
        )

        logging.info(f"Published {publish_as} {family} for {proc_date}")
        # No `provenance_complete`: this stage never opens the file, and the
        # byte-identical internal artifact's own outcome already carries the
        # authoritative flag. run_summary treats absence as "unknown" rather than
        # failing reconciliation (ADR 0005).
        return JobOutcome.success(
            stage="publisher",
            date=date_obj.isoformat(),
            source=publish_as,
            outputs=[Output(key=dst_key, kind=OUTPUT_KINDS[family])],
            metadata={
                "family": family,
                "copied_from": src_key,
                "contributors": contributors,
            },
        ).to_dict()
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise PipelineError(json.dumps(error_response))
