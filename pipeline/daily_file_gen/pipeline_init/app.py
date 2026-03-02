from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging
import re

import boto3
from cmr import GranuleQuery

from config.source_config import get_source_config, get_available_sources, PipelineInitSourceConfig

session = boto3.Session()
s3 = session.client("s3")


def daily_file_end_date() -> datetime:
    """
    Returns the date of the most recent Monday for which a full 10-day window is available.
    The pipeline runs on a Monday cadence and simple grids are generated for Mondays.
    """
    today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
    latest_simple_grid_date = today - timedelta(days=today.weekday())
    while latest_simple_grid_date + timedelta(days=4) >= today:
        latest_simple_grid_date -= timedelta(weeks=1)
    return latest_simple_grid_date + timedelta(days=4)


def chunk_dates_by_year(dates: list[datetime]) -> dict[int, list[datetime]]:
    """
    Group a list of dates by year.
    """
    grouped_by_year = defaultdict(list)
    for date in dates:
        grouped_by_year[date.year].append(date)
    return grouped_by_year


def query_daily_files_for_year(
    year: int,
    start_date: datetime,
    end_date: datetime,
    bucket: str,
    config: PipelineInitSourceConfig,
) -> dict[datetime, datetime]:
    """
    Query S3 for modified times of daily files for a specific year.
    """
    print(f"Querying S3 for {config.source} daily files in {year}")
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"{config.s3_prefix}/{year}/"
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    # Build regex from the config filename pattern
    # e.g. "NASA-SSH_alt_ref_at_v1_{date8}.nc" -> "NASA-SSH_alt_ref_at_v1_(\d{8})\.nc"
    pattern = config.filename_pattern.replace("{date8}", r"(\d{8})")
    pattern = pattern.replace(".", r"\.")

    timestamps = {}
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            match = re.search(pattern, key)
            if match:
                file_date = datetime.strptime(match.group(1), "%Y%m%d")
                if start_date <= file_date <= end_date:
                    timestamps[file_date.date()] = obj["LastModified"]
    return timestamps


def _load_cycle_index(bucket: str, key: str) -> dict:
    """Load and parse a cycle index JSON from S3."""
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read())


def query_source_bucket(
    start_date: datetime,
    end_date: datetime,
    config: PipelineInitSourceConfig,
    source_bucket: str,
) -> dict[datetime, datetime]:
    """
    Query an S3 bucket for source files and their modification times.
    Used for sources with discovery_type='s3_bucket'.
    """
    if config.cycle_index_key:
        return _query_source_bucket_cycle_index(start_date, end_date, config, source_bucket)

    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    yearly_dates = chunk_dates_by_year(dates)

    # Build regex from the source_filename_pattern
    # e.g. "{source}_{date8}.nc" -> "EXAMPLE_S3_(\d{8})\.nc"
    fname_pattern = config.source_filename_pattern.replace("{source}", config.source)
    fname_pattern = fname_pattern.replace("{date8}", r"(\d{8})")
    fname_pattern = fname_pattern.replace(".", r"\.")

    timestamps = {}
    paginator = s3.get_paginator("list_objects_v2")

    for year, year_dates in yearly_dates.items():
        prefix = config.source_prefix_pattern.format(source=config.source, year=year)
        if not prefix.endswith("/"):
            prefix += "/"

        print(f"Querying source bucket s3://{source_bucket}/{prefix} for {config.source} in {year}")
        pages = paginator.paginate(Bucket=source_bucket, Prefix=prefix)

        year_start = year_dates[0].date()
        year_end = year_dates[-1].date()

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                match = re.search(fname_pattern, key)
                if match:
                    file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
                    if year_start <= file_date <= year_end:
                        timestamps[file_date] = obj["LastModified"]

    return timestamps


def _query_source_bucket_cycle_index(
    start_date: datetime,
    end_date: datetime,
    config: PipelineInitSourceConfig,
    source_bucket: str,
) -> dict[datetime, datetime]:
    """
    Query source bucket using a cycle index for sources where files span
    multiple days (e.g. one file per ~10-day cycle).
    """
    from datetime import date

    cycle_index = _load_cycle_index(source_bucket, config.cycle_index_key)

    # Parse cycle date ranges
    cycles = []
    for filename, span in cycle_index.items():
        cycles.append({
            "filename": filename,
            "start": date.fromisoformat(span["start"]),
            "end": date.fromisoformat(span["end"]),
        })

    # List the prefix to get LastModified for cycle files
    # We need to scan all years in the date range
    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    yearly_dates = chunk_dates_by_year(dates)

    file_mod_times = {}
    paginator = s3.get_paginator("list_objects_v2")

    for year in yearly_dates:
        prefix = config.source_prefix_pattern.format(source=config.source, year=year)
        if not prefix.endswith("/"):
            prefix += "/"

        print(f"Querying source bucket s3://{source_bucket}/{prefix} for cycle files in {year}")
        pages = paginator.paginate(Bucket=source_bucket, Prefix=prefix)

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.rsplit("/", 1)[-1]
                file_mod_times[filename] = obj["LastModified"]

    # For each date in the range, find which cycle(s) cover it and use the
    # latest LastModified among covering cycle files
    timestamps = {}
    for d in dates:
        d_date = d.date()
        latest_mod = None
        for cycle in cycles:
            if cycle["start"] <= d_date <= cycle["end"]:
                mod_time = file_mod_times.get(cycle["filename"])
                if mod_time and (latest_mod is None or mod_time > latest_mod):
                    latest_mod = mod_time
        if latest_mod is not None:
            timestamps[d_date] = latest_mod

    return timestamps


def query_cmr(
    start_date: datetime,
    end_date: datetime,
    config: PipelineInitSourceConfig,
) -> dict[datetime, datetime]:
    """
    Unified CMR query function. Uses single-collection logic (max mod time)
    or multi-collection priority resolution depending on config.
    """
    concept_ids = [c.concept_id for c in config.collections]
    if not concept_ids:
        return {}

    print(f"Querying CMR for {config.source} granules from {start_date.date()} to {end_date.date()}")
    api = GranuleQuery().concept_id(concept_ids).provider("POCLOUD").temporal(start_date, end_date)
    query_results = api.get_all()

    query_results_by_date = defaultdict(list)
    for granule in query_results:
        granule_start = datetime.fromisoformat(granule.get("time_start").replace("Z", ""))
        granule_end = datetime.fromisoformat(granule.get("time_end").replace("Z", ""))

        for date in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]:
            if granule_end > date > granule_start:
                query_results_by_date[date.date()].append(granule)

    # Build priority map from config collections
    priority_map = {c.concept_id: c.priority for c in config.collections}
    use_priority = len(config.collections) > 1

    granule_mod_times = {}
    for date, granules in query_results_by_date.items():
        if use_priority:
            # Multi-collection: cycle_pass priority resolution
            priority_granules = {}
            max_mod_time = None

            for granule in granules:
                granule_id = granule.get("title")
                match = re.search(r"\d{3}_\d{3}", granule_id)
                if not match:
                    continue

                cycle_pass = match.group(0)
                concept_id = granule.get("collection_concept_id")
                collection_priority = priority_map[concept_id]
                modified_time = datetime.fromisoformat(granule.get("updated"))

                prior_priority = priority_granules.get(cycle_pass, float("inf"))
                if collection_priority <= prior_priority:
                    priority_granules[cycle_pass] = collection_priority
                    if max_mod_time is None or modified_time > max_mod_time:
                        max_mod_time = modified_time
            granule_mod_times[date] = max_mod_time
        else:
            # Single collection: simple max mod time
            max_mod_time = None
            for granule in granules:
                modified_time = datetime.fromisoformat(granule.get("updated"))
                if max_mod_time is None or modified_time > max_mod_time:
                    max_mod_time = modified_time
            granule_mod_times[date] = max_mod_time
    return granule_mod_times


def handler(event, context):
    """
    Check if certain dates need processing for a specific source.

    Event parameters:
        - bucket (required): S3 bucket name
        - source (required): Data source name (e.g. 'GSFC', 'S6', 'S6B')
        - force_update (optional): Skip modification time checks, regenerate all dates
        - start (optional): Start date (ISO format)
        - end (optional): End date (ISO format)
    """
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event.get("bucket")
    if bucket is None:
        raise ValueError("bucket job parameter missing.")

    source = event.get("source")
    if source is None:
        raise ValueError("source job parameter missing.")

    available = get_available_sources()
    if source not in available:
        raise ValueError(f"Invalid source: {source}. Must be one of {available}")

    config = get_source_config(source)

    force_update = event.get("force_update", False)

    # Determine date range
    if event.get("start") and event.get("end"):
        start_date = max(
            datetime.fromisoformat(event.get("start")),
            datetime.combine(config.start_date, datetime.min.time()),
        )
        end_date = datetime.fromisoformat(event.get("end"))
        logging.info(f"Using manual date range: {start_date.date()} to {end_date.date()}")
    else:
        start_date = datetime.combine(config.start_date, datetime.min.time())
        end_date = daily_file_end_date()
        logging.info(f"Using default range: {start_date.date()} to {end_date.date()}")

    # Cap end date to source's collection end date if configured
    if config.end_date is not None:
        source_end = datetime.combine(config.end_date, datetime.min.time())
        if end_date > source_end:
            end_date = source_end
            logging.info(f"Capped end date to source end: {end_date.date()}")

    # Generate the list of dates
    lookback_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    logging.info(f"Checking {len(lookback_dates)} dates between {start_date.date()} and {end_date.date()}")

    # Query modification times
    df_mod_times = {}
    granule_mod_times = {}

    if not force_update:
        yearly_dates = chunk_dates_by_year(lookback_dates)
        for year, dates in yearly_dates.items():
            year_start, year_end = dates[0], dates[-1]
            df_mod_times.update(query_daily_files_for_year(year, year_start, year_end, bucket, config))

        # Query source modification times (CMR or S3 bucket)
        if config.discovery_type == "s3_bucket":
            if source_bucket is None:
                raise ValueError("source_bucket event parameter required for s3_bucket discovery type.")
            granule_mod_times.update(query_source_bucket(lookback_dates[0], lookback_dates[-1], config, source_bucket))
        else:
            for year, dates in yearly_dates.items():
                year_start, year_end = dates[0], dates[-1]
                granule_mod_times.update(query_cmr(year_start, year_end, config))

    # Build jobs list
    jobs = []
    for date in lookback_dates:
        df_mod_time = df_mod_times.get(date.date())
        granule_mod_time = granule_mod_times.get(date.date())

        needs_processing = force_update or (
            not df_mod_time
            or (not granule_mod_time and not df_mod_time)
            or (df_mod_time and granule_mod_time and df_mod_time < granule_mod_time)
        )

        if needs_processing:
            jobs.append(
                {
                    "date": date.date().isoformat(),
                    "source": source,
                    "bucket": bucket,
                }
            )

    logging.info(f"Generated {len(jobs)} jobs for processing")

    # Write jobs manifest to S3
    run_id = datetime.now().strftime("%Y%m%dT%H%M%S")
    jobs_key = f"pipeline_runs/{source}/{run_id}/jobs.json"
    s3.put_object(
        Bucket=bucket,
        Key=jobs_key,
        Body=json.dumps(jobs),
        ContentType="application/json",
    )
    logging.info(f"Wrote manifest to s3://{bucket}/{jobs_key}")

    return {
        "jobs_key": jobs_key,
        "bucket": bucket,
        "source": source,
        "unify": config.unify,
    }
