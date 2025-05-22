from datetime import date, timedelta
from typing import Tuple


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
    job_dates_dt = [date.fromisoformat(jd["date"]) for jd in event]

    end_date = last_sg_date()

    sg_jobs = set()
    for job_date in job_dates_dt:
        prev_monday, next_monday = surrounding_mondays(job_date)

        if prev_monday <= end_date:
            sg_jobs.add(prev_monday)
        if next_monday <= end_date:
            sg_jobs.add(next_monday)

    intersection = [job.isoformat() for job in sg_jobs]
    
    return intersection