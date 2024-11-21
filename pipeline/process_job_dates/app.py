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
        except:
            pass

    intersection = [sg_job.isoformat() for sg_job in list(sg_jobs & set(sg_dates))]

    return intersection

if __name__ == '__main__':
    dates = [{"date": "1995-11-27"},{"date": "1995-11-28"},{"date": "1995-11-29"},{"date": "1995-11-30"},{"date": "1995-12-01"},{"date": "1995-12-02"},{"date": "1995-12-03"},{"date": "1995-12-04"},{"date": "1995-12-05"},{"date": "1995-12-06"},{"date": "1999-02-10"},{"date": "2003-11-20"},{"date": "2003-11-21"},{"date": "2003-11-22"},{"date": "2003-11-23"},{"date": "2003-11-24"},{"date": "2003-11-25"},{"date": "2003-11-26"},{"date": "2003-11-27"},{"date": "2004-02-16"},{"date": "2004-02-17"},{"date": "2004-02-18"},{"date": "2004-02-19"},{"date": "2004-02-20"},{"date": "2005-09-21"},{"date": "2005-09-22"},{"date": "2005-09-23"},{"date": "2005-09-24"},{"date": "2005-09-25"},{"date": "2005-09-26"},{"date": "2005-09-27"},{"date": "2006-10-31"},{"date": "2006-11-01"},{"date": "2006-11-02"},{"date": "2006-11-03"},{"date": "2006-11-04"},{"date": "2006-11-05"},{"date": "2006-11-06"},{"date": "2006-11-07"},{"date": "2006-11-08"},{"date": "2006-11-09"},{"date": "2006-11-10"},{"date": "2006-11-11"},{"date": "2006-11-12"},{"date": "2006-11-13"},{"date": "2006-11-14"},{"date": "2006-11-15"},{"date": "2013-03-31"},{"date": "2013-04-01"},{"date": "2013-04-02"},{"date": "2013-04-03"},{"date": "2013-04-04"},{"date": "2013-09-06"},{"date": "2013-09-07"},{"date": "2013-09-08"},{"date": "2013-09-09"},{"date": "2013-09-10"},{"date": "2013-09-11"},{"date": "2019-02-25"},{"date": "2019-02-26"},{"date": "2019-02-27"},{"date": "2019-02-28"},{"date": "2019-03-01"},{"date": "2019-03-02"},{"date": "2019-03-03"},{"date": "2019-03-04"},{"date": "2019-03-05"},{"date": "2020-02-01"},{"date": "2020-02-02"},{"date": "2020-02-03"},{"date": "2020-02-04"},{"date": "2020-10-28"},{"date": "2024-11-21"}]
    
    sg_jobs = lambda_handler(dates, {})
    print(len(sg_jobs))
    print(sg_jobs)