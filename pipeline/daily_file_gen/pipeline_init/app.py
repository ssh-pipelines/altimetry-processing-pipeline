import logging
import os
from datetime import date

from config.source_config import get_available_sources, get_source_config
from planning import daily_file_end_date, plan_jobs, write_manifest

from utilities.products import published_identity


def _parse_date_range(event: dict, start_default: date, end_default: date) -> tuple[date, date]:
    raw_start = event.get("start")
    raw_end = event.get("end")
    if raw_start and raw_end:
        start = max(date.fromisoformat(raw_start), start_default)
        end = date.fromisoformat(raw_end)
        logging.info(f"Using manual date range: {start} to {end}")
    else:
        start = start_default
        end = end_default
        logging.info(f"Using default range: {start} to {end}")
    return start, end


def handler(event, context):
    """Plan jobs for one source and write a manifest to S3.

    Event parameters:
        - bucket (required): S3 bucket name
        - source (required): Data source name
        - run_id (required): the run this Source's leg belongs to, minted by start_run
        - force_update (optional): regenerate all dates
        - start, end (optional): ISO date range
    """
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event.get("bucket") or os.environ.get("BUCKET_NAME")
    if bucket is None:
        raise ValueError("bucket not provided in event or BUCKET_NAME env var.")

    source = event.get("source")
    if source is None:
        raise ValueError("source job parameter missing.")

    # Minted once per run by `start_run` and handed to every contributor, so the
    # source tier's manifests and the product tier's share one identity. Required
    # rather than defaulted: a locally-generated run_id would put this Source's
    # leg under a run path nothing else in the execution writes to, and the
    # product tier would silently see one fewer contributor.
    run_id = event.get("run_id")
    if run_id is None:
        raise ValueError("run_id job parameter missing; it is minted by start_run.")

    available = get_available_sources()
    if source not in available:
        raise ValueError(f"Invalid source: {source}. Must be one of {available}")

    config = get_source_config(source)
    force_update = event.get("force_update", False)

    end_default = daily_file_end_date()
    if config.end_date is not None and end_default > config.end_date:
        end_default = config.end_date

    start, end = _parse_date_range(event, config.start_date, end_default)

    if config.end_date is not None and end > config.end_date:
        end = config.end_date
        logging.info(f"Capped end date to source end: {end}")

    if start > end:
        logging.info(f"start {start} > end {end}; nothing to plan")
        jobs: list[dict] = []
    else:
        jobs = plan_jobs(config, bucket, start, end, force_update)

    logging.info(f"Generated {len(jobs)} jobs for processing")

    # What the window resolved to *for this Source* after defaulting and capping
    # to its date range. The given overrides are recorded too, but the run-level
    # sidecar `start_run` writes is the one the Run summary reports from; this
    # one exists because `resolved_*` differs per contributor and a run over
    # several of them has no single answer.
    run_params = {
        "start": event.get("start"),
        "end": event.get("end"),
        "force_update": force_update,
        "defaults_used": not (event.get("start") or event.get("end")),
        "resolved_start": start.isoformat(),
        "resolved_end": end.isoformat(),
    }

    jobs_key = write_manifest(bucket, source, run_id, jobs, run_params)

    # `published_product` rather than a publish gate: every source publishes, and
    # which line it publishes to follows from its product_type. Echoed back
    # although the parent already knows it, so the publisher's Map item can be
    # built from this Source's own leg and a mismatch would show up as a
    # publication shortfall rather than a wrong destination.
    return {
        "jobs_key": jobs_key,
        "bucket": bucket,
        "source": source,
        "run_id": run_id,
        "published_product": published_identity(config),
        "job_count": len(jobs),
    }
