from collections import defaultdict
from datetime import datetime, timedelta
import logging
import re
from typing import Dict, Iterable, List

import boto3
from cmr import GranuleQuery

session = boto3.Session()
s3 = session.client("s3")

S6_COLLECTIONS = {
    "C2619443998-POCLOUD": 1,  # Highest priority
    "C2619444006-POCLOUD": 2,
    "C1968979561-POCLOUD": 3,  # Lowest priority
}

GSFC_COLLECTION = "C2901523432-POCLOUD"


def chunk_dates_by_year(dates: List[datetime]) -> Dict[int, List[datetime]]:
    """
    Group a list of dates by year.
    """
    grouped_by_year = defaultdict(list)
    for date in dates:
        grouped_by_year[date.year].append(date)
    return grouped_by_year


def query_daily_files_for_year(year: int, start_date: datetime, end_date: datetime) -> Dict[datetime, datetime]:
    """
    Query S3 for modified times of daily files for a specific year.
    """
    print(f"Querying S3 for daily files in {year}")
    paginator = s3.get_paginator("list_objects_v2")
    prefix = f"daily_files/p3/{year}/"
    pages = paginator.paginate(Bucket="example-bucket", Prefix=prefix)

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


def query_gsfc(start_date: datetime, end_date: datetime) -> Dict[datetime, List[dict]]:
    print(f"Querying CMR for GSFC granules in {start_date.year}")

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


def query_s6(start_date: datetime, end_date: datetime) -> Dict[datetime, datetime]:
    print(f"Querying CMR for S6 granules in {start_date.year}")
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


def query_granules_for_year(year: int, start_date: datetime, end_date: datetime) -> Dict[datetime, datetime]:
    """
    Query CMR for granules across multiple collections for a specific year.
    """
    if year == 2024:
        granule_mod_times = query_gsfc(datetime(2024, 1, 1), datetime(2024, 1, 20))
        granule_mod_times.update(query_s6(datetime(2024, 1, 21), end_date))
    elif year < 2024:
        granule_mod_times = query_gsfc(start_date, end_date)
    else:
        granule_mod_times = query_s6(start_date, end_date)
    return granule_mod_times


def handler(event, context):
    """
    Look back dates
    """
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    if event.get("lookback") == "full":
        start_date = datetime(1992, 10, 25)
    else:
        start_date = datetime(2024, 1, 1)

    end_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    # Generate the list of dates
    lookback_dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    logging.info(f"Determining jobs between {start_date} and {end_date} by looking at {len(lookback_dates)} dates")

    # Chunk dates by year
    yearly_dates = chunk_dates_by_year(lookback_dates)

    df_mod_times = {}
    granule_mod_times = {}

    for year, dates in yearly_dates.items():
        start_date, end_date = dates[0], dates[-1]

        # Query daily files and granules for the year
        df_mod_times.update(query_daily_files_for_year(year, start_date, end_date))
        granule_mod_times.update(query_granules_for_year(year, start_date, end_date))

    jobs = []
    for date in lookback_dates:
        df_mod_time = df_mod_times.get(date.date())
        granule_mod_time = granule_mod_times.get(date.date())

        if (
            not df_mod_time
            or (not granule_mod_time and not df_mod_time)
            or (df_mod_time and granule_mod_time and df_mod_time < granule_mod_time)
        ):
            if date.date() < datetime(2024, 1, 21).date():
                source = "GSFC"
            else:
                source = "S6"
            jobs.append({"date": date.date().isoformat(), "source": source})
    for job in jobs:
        print(job.items())
    return jobs