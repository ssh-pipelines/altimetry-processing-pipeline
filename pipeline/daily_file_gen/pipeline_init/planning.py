from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import json
import logging
import re

import boto3

from config.source_config import PipelineInitSourceConfig
from enumeration import build_enumerator
from enumeration.base import GranuleRef


_s3 = boto3.client("s3")


def plan_jobs(
    source_config: PipelineInitSourceConfig,
    bucket: str,
    start: date,
    end: date,
    force_update: bool,
) -> list[dict]:
    """Build the list of jobs needing processing for a single source over [start, end]."""
    enumerator = build_enumerator(source_config, bucket=bucket)
    granules = enumerator.enumerate(start, end)

    by_date: dict[date, list[GranuleRef]] = defaultdict(list)
    for g in granules:
        by_date[g.date].append(g)

    if force_update:
        existing: dict[date, datetime] = {}
    else:
        existing = scan_existing_p3_mod_times(bucket, source_config, start, end)

    jobs: list[dict] = []
    for d, grs in sorted(by_date.items()):
        latest_upstream = max(g.mod_time for g in grs)
        existing_mod = existing.get(d)

        if existing_mod is not None and not _is_newer(latest_upstream, existing_mod) and not force_update:
            continue

        sorted_grs = sorted(grs, key=lambda g: (g.sort_key, g.uri))
        jobs.append(
            {
                "date": d.isoformat(),
                "source": source_config.source,
                "bucket": bucket,
                "granules": [g.uri for g in sorted_grs],
            }
        )
    return jobs


def _is_newer(upstream: datetime, existing: datetime) -> bool:
    """Compare two datetimes that may differ in tzinfo."""
    if upstream.tzinfo is None and existing.tzinfo is not None:
        upstream = upstream.replace(tzinfo=existing.tzinfo)
    elif existing.tzinfo is None and upstream.tzinfo is not None:
        existing = existing.replace(tzinfo=upstream.tzinfo)
    return upstream > existing


def scan_existing_p3_mod_times(
    bucket: str,
    source_config: PipelineInitSourceConfig,
    start: date,
    end: date,
) -> dict[date, datetime]:
    """Return mod-times of existing P3 daily files in [start, end]."""
    pattern = source_config.filename_pattern.replace("{date8}", r"(\d{8})")
    pattern = pattern.replace(".", r"\.")
    regex = re.compile(pattern)

    paginator = _s3.get_paginator("list_objects_v2")

    results: dict[date, datetime] = {}
    for year in range(start.year, end.year + 1):
        prefix = f"{source_config.s3_prefix}/{year}/"
        logging.info(f"Querying S3 for {source_config.source} daily files in {year}")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in pages:
            for obj in page.get("Contents", []):
                match = regex.search(obj["Key"])
                if not match:
                    continue
                file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
                if start <= file_date <= end:
                    results[file_date] = obj["LastModified"]
    return results


def write_manifest(bucket: str, source: str, jobs: list[dict]) -> str:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    key = f"pipeline_runs/{source}/{run_id}/jobs.json"
    _s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(jobs),
        ContentType="application/json",
    )
    logging.info(f"Wrote manifest to s3://{bucket}/{key}")
    return key


def daily_file_end_date() -> date:
    """Return the date of the most recent Monday for which a full 10-day window is available."""
    today = datetime.today().date()
    latest_simple_grid_date = today - timedelta(days=today.weekday())
    while latest_simple_grid_date + timedelta(days=4) >= today:
        latest_simple_grid_date -= timedelta(weeks=1)
    return latest_simple_grid_date + timedelta(days=4)
