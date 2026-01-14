from collections import defaultdict
from datetime import datetime, timedelta
import logging
import re
from typing import Optional

import boto3
from cmr import GranuleQuery

session = boto3.Session()
s3 = session.client("s3")

S6_COLLECTIONS = {
    "C3332203845-POCLOUD": 1,  # Highest priority
    "C3332203819-POCLOUD": 2,
    "C1968979561-POCLOUD": 3,  # Lowest priority
}

GSFC_COLLECTION = "C2901523432-POCLOUD"

# Default switchover date from GSFC to S6
SWITCHOVER_DATE = datetime(2024, 1, 21)


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
    year: int, start_date: datetime, end_date: datetime, bucket: str
) -> dict[datetime, datetime]:
    """
    Query S3 for modified times of daily files for a specific year.
    """
    print(f"Querying S3 for daily files in {year}")
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"daily_files/p3/{year}/"
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    timestamps = {}
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            match = re.search(r"NASA-SSH_alt_ref_at_v1_(\d{8})\.nc", key)
            if match:
                file_date = datetime.strptime(match.group(1), "%Y%m%d")
                if start_date <= file_date <= end_date:
                    timestamps[file_date.date()] = obj["LastModified"]
    return timestamps


def query_gsfc(start_date: datetime, end_date: datetime) -> dict[datetime, datetime]:
    print(f"Querying CMR for GSFC granules from {start_date.date()} to {end_date.date()}")

    api = GranuleQuery().concept_id(GSFC_COLLECTION).provider("POCLOUD").temporal(start_date, end_date)
    query_results = api.get_all()

    query_results_by_date = defaultdict(list)
    for granule in query_results:
        granule_start = datetime.fromisoformat(granule.get("time_start").replace("Z", ""))
        granule_end = datetime.fromisoformat(granule.get("time_end").replace("Z", ""))

        for date in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]:
            if granule_end > date > granule_start:
                query_results_by_date[date.date()].append(granule)

    granule_mod_times = {}
    for date, granules in query_results_by_date.items():
        max_mod_time = None
        for granule in granules:
            modified_time = datetime.fromisoformat(granule.get("updated"))
            if max_mod_time is None or modified_time > max_mod_time:
                max_mod_time = modified_time
        granule_mod_times[date] = max_mod_time
    return granule_mod_times


def query_s6(start_date: datetime, end_date: datetime) -> dict[datetime, datetime]:
    print(f"Querying CMR for S6 granules from {start_date.date()} to {end_date.date()}")
    api = GranuleQuery().concept_id(list(S6_COLLECTIONS.keys())).provider("POCLOUD").temporal(start_date, end_date)
    query_results = api.get_all()

    query_results_by_date = defaultdict(list)
    for granule in query_results:
        granule_start = datetime.fromisoformat(granule.get("time_start").replace("Z", ""))
        granule_end = datetime.fromisoformat(granule.get("time_end").replace("Z", ""))

        for date in [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]:
            if granule_end > date > granule_start:
                query_results_by_date[date.date()].append(granule)

    granule_mod_times = {}
    for date, granules in query_results_by_date.items():
        priority_granules = {}
        max_mod_time = None

        for granule in granules:
            granule_id = granule.get("title")
            match = re.search(r"\d{3}_\d{3}", granule_id)
            if not match:
                continue

            cycle_pass = match.group(0)
            concept_id = granule.get("collection_concept_id")
            collection_priority = S6_COLLECTIONS[concept_id]
            modified_time = datetime.fromisoformat(granule.get("updated"))

            # Get the prior priority for this cycle_pass
            prior_priority = priority_granules.get(cycle_pass, float("inf"))

            # Update max_mod_time only if the current priority is better or equal
            if collection_priority <= prior_priority:
                priority_granules[cycle_pass] = collection_priority
                if max_mod_time is None or modified_time > max_mod_time:
                    max_mod_time = modified_time
        granule_mod_times[date] = max_mod_time
    return granule_mod_times


def query_granules_with_source_logic(
    dates: list[datetime], source_override: Optional[str] = None
) -> dict[datetime, datetime]:
    """
    Query granules using either manual source specification or default switchover logic.

    Args:
        dates: List of dates to query
        source_override: Optional source override ('GSFC' or 'S6'). If None, uses switchover logic.

    Returns:
        Dictionary mapping dates to their modification times
    """
    granule_mod_times = {}

    # Scenario 1: Manual source specified - query all dates with that source
    if source_override:
        if source_override not in ["GSFC", "S6"]:
            raise ValueError(f"Invalid source: {source_override}. Must be 'GSFC' or 'S6'")

        logging.info(f"Using manual source: {source_override}")
        yearly_dates = chunk_dates_by_year(dates)

        for year, year_dates in yearly_dates.items():
            start_date = year_dates[0]
            end_date = year_dates[-1]

            if source_override == "GSFC":
                granule_mod_times.update(query_gsfc(start_date, end_date))
            else:  # S6
                granule_mod_times.update(query_s6(start_date, end_date))

    # Scenario 2: Default behavior - use switchover logic
    else:
        logging.info(f"Using default switchover logic (GSFC before {SWITCHOVER_DATE.date()}, S6 after)")

        # Separate dates by source based on switchover date
        gsfc_dates = [d for d in dates if d < SWITCHOVER_DATE]
        s6_dates = [d for d in dates if d >= SWITCHOVER_DATE]

        # Query GSFC dates
        if gsfc_dates:
            gsfc_by_year = chunk_dates_by_year(gsfc_dates)
            for year, year_dates in gsfc_by_year.items():
                start_date = year_dates[0]
                end_date = year_dates[-1]
                granule_mod_times.update(query_gsfc(start_date, end_date))

        # Query S6 dates
        if s6_dates:
            s6_by_year = chunk_dates_by_year(s6_dates)
            for year, year_dates in s6_by_year.items():
                start_date = year_dates[0]
                end_date = year_dates[-1]
                granule_mod_times.update(query_s6(start_date, end_date))

    return granule_mod_times


def determine_source_for_date(date: datetime, source_override: Optional[str] = None) -> str:
    """
    Determine which data source to use for a given date.

    Args:
        date: The date to check
        source_override: Optional source override ('GSFC' or 'S6')

    Returns:
        'GSFC' or 'S6'
    """
    if source_override:
        return source_override

    # Default switchover logic
    return "GSFC" if date < SWITCHOVER_DATE else "S6"


def handler(event, context):
    """
    Check if certain dates need processing. Supports both GSFC and S6 data.

    Event parameters:
        - bucket (required): S3 bucket name
        - force_update (optional): Skip modification time checks, regenerate all dates
        - source (optional): Manually specify 'GSFC' or 'S6' for all dates
        - start (optional): Start date (ISO format)
        - end (optional): End date (ISO format)
        - lookback (optional): 'full' to check everything from 1992-10-25

    Scenarios:
        1. Default with switchover: No 'source' param, uses SWITCHOVER_DATE logic
        2. Manual source: 'source' param specified, uses that source for entire range
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

    # Force an update (skips modification time checks)
    force_update = event.get("force_update", False)

    # Manual source override
    source_override = event.get("source")
    if source_override and source_override not in ["GSFC", "S6"]:
        raise ValueError(f"Invalid source: {source_override}. Must be 'GSFC' or 'S6'")

    # Determine date range
    if event.get("start") and event.get("end"):
        # Manual date range
        start_date = max(datetime.fromisoformat(event.get("start")), datetime(1992, 10, 25))
        end_date = datetime.fromisoformat(event.get("end"))
        logging.info(f"Using manual date range: {start_date.date()} to {end_date.date()}")
    elif event.get("lookback") == "full":
        # Full lookback checks everything starting at 1992-10-25
        start_date = datetime(1992, 10, 25)
        end_date = daily_file_end_date()
        logging.info(f"Using full lookback: {start_date.date()} to {end_date.date()}")
    else:
        # Default: check S6 data starting on 2024-01-01
        start_date = datetime(2024, 1, 1)
        end_date = daily_file_end_date()
        logging.info(f"Using default range: {start_date.date()} to {end_date.date()}")

    # Generate the list of dates
    lookback_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    logging.info(f"Checking {len(lookback_dates)} dates between {start_date.date()} and {end_date.date()}")

    # Query modification times
    df_mod_times = {}
    granule_mod_times = {}

    if not force_update:
        # Query daily files
        yearly_dates = chunk_dates_by_year(lookback_dates)
        for year, dates in yearly_dates.items():
            year_start, year_end = dates[0], dates[-1]
            df_mod_times.update(query_daily_files_for_year(year, year_start, year_end, bucket))

        # Query granules with appropriate source logic
        granule_mod_times = query_granules_with_source_logic(lookback_dates, source_override)

    # Build jobs list
    jobs = []
    for date in lookback_dates:
        df_mod_time = df_mod_times.get(date.date())
        granule_mod_time = granule_mod_times.get(date.date())

        # Determine if this date needs processing
        needs_processing = force_update or (
            not df_mod_time
            or (not granule_mod_time and not df_mod_time)
            or (df_mod_time and granule_mod_time and df_mod_time < granule_mod_time)
        )

        if needs_processing:
            source = determine_source_for_date(date, source_override)
            jobs.append({"date": date.date().isoformat(), "source": source, "satellite": source, "bucket": bucket})

    logging.info(f"Generated {len(jobs)} jobs for processing")
    return {"jobs": jobs}
