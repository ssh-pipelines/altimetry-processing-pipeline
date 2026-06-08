from datetime import date, datetime
import logging
import os

from config.source_config import get_source_config, get_available_sources
from planning import daily_file_end_date, plan_jobs, write_manifest


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

    # Record how this run was invoked. `start`/`end`/`force_update` are the *given*
    # overrides (null/false on a nominal scheduled run); `resolved_*` capture the range
    # actually planned after defaulting/capping, for unambiguous provenance.
    run_params = {
        "start": event.get("start"),
        "end": event.get("end"),
        "force_update": force_update,
        "defaults_used": not (event.get("start") or event.get("end")),
        "resolved_start": start.isoformat(),
        "resolved_end": end.isoformat(),
    }

    jobs_key = write_manifest(bucket, source, jobs, run_params)

    return {
        "jobs_key": jobs_key,
        "bucket": bucket,
        "source": source,
        "unify": config.unify,
    }
