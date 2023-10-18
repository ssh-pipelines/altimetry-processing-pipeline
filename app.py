from daily_files import daily_file_job

def handler(event, context):
    daily_file_job.start_job(event)