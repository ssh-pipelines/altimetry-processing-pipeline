import json
from datetime import date, timedelta
from typing import Tuple

import boto3

s3 = boto3.client("s3")


def last_sg_date(today = date.today()) -> date:
    """
    Returns the date of the most recent Monday for which a full 10-day window is available.
    The pipeline runs on a Monday cadence and simple grids are generated for Mondays.
    """
    latest_simple_grid_date = today - timedelta(days=today.weekday())
    while latest_simple_grid_date + timedelta(days=4) >= today:
        latest_simple_grid_date -= timedelta(weeks=1)
    return latest_simple_grid_date


def surrounding_mondays(d: date) -> Tuple[date, date]:
    weekday = d.weekday()  # Monday=0, Sunday=6
    prev_monday = d - timedelta(days=weekday)
    next_monday = prev_monday + timedelta(days=7)

    return prev_monday, next_monday


def lambda_handler(event, context):
    bucket = event["bucket"]
    jobs_key = event["jobs_key"]
    source = event["source"]

    # Read manifest from S3
    resp = s3.get_object(Bucket=bucket, Key=jobs_key)
    jobs = json.loads(resp["Body"].read())

    end_date = last_sg_date()
    sg_jobs = set()
    for job in jobs:
        job_date_dt = date.fromisoformat(job["date"])
        prev_monday, next_monday = surrounding_mondays(job_date_dt)
        if prev_monday <= end_date:
            sg_jobs.add(prev_monday)
        if next_monday <= end_date:
            sg_jobs.add(next_monday)

    # Simple grids uses a 10-day window: [monday - 5, monday + 4].
    # Discard any Monday whose window has no overlap with the actual daily file dates,
    # otherwise simple grids silently skips the date and downstream stages (ENSO) fail.
    min_job_date = min(date.fromisoformat(job["date"]) for job in jobs)
    max_job_date = max(date.fromisoformat(job["date"]) for job in jobs)
    sg_jobs = {
        m for m in sg_jobs
        if m + timedelta(days=4) >= min_job_date
        and m - timedelta(days=5) <= max_job_date
    }

    filtered_jobs = [{"date": d.isoformat(), "bucket": bucket, "source": source} for d in sorted(sg_jobs)]

    # Write filtered manifest to S3
    new_key = jobs_key.replace("/jobs.json", "/sg_jobs.json")
    s3.put_object(
        Bucket=bucket,
        Key=new_key,
        Body=json.dumps(filtered_jobs),
        ContentType="application/json",
    )

    return {"jobs_key": new_key, "bucket": bucket, "source": source}
