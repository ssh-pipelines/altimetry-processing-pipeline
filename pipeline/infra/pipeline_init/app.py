from collections import defaultdict
from datetime import datetime, timedelta
import logging
import re

import boto3
from cmr import GranuleQuery

from pipeline_init.config.source_config import get_source_config, get_available_sources, PipelineInitSourceConfig

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
    year: int, start_date: datetime, end_date: datetime, bucket: str,
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


def query_cmr(
    start_date: datetime, end_date: datetime, config: PipelineInitSourceConfig,
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
            df_mod_times.update(
                query_daily_files_for_year(year, year_start, year_end, bucket, config)
            )

        # Query granules by year chunks
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
            jobs.append({
                "date": date.date().isoformat(),
                "source": source,
                "satellite": config.satellite,
                "bucket": bucket,
            })

    logging.info(f"Generated {len(jobs)} jobs for processing")
    return {"jobs": jobs}
