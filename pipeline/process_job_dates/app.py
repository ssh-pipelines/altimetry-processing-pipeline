from datetime import date, timedelta


def simple_grid_date_generator():
    d = date(1992, 10, 12)
    while d <= date.today():
        yield d
        d += timedelta(days=7)


def lambda_handler(event, context):
    job_dates_dt = [date.fromisoformat(jd["date"]) for jd in event]

    sg_dates = [i for i in simple_grid_date_generator()]

    sg_dates_with_jobs = sorted(sg_dates + job_dates_dt)

    sg_jobs = set()
    for job_date in job_dates_dt:
        job_date_idx = sg_dates_with_jobs.index(job_date)
        left = sg_dates_with_jobs[job_date_idx - 1]
        sg_jobs.add(left)

        try:
            right = sg_dates_with_jobs[job_date_idx + 1]
            sg_jobs.add(right)
        except IndexError:
            pass

    intersection = [sg_job.isoformat() for sg_job in list(sg_jobs & set(sg_dates))]

    return intersection
