import json
from daily_files import daily_file_job

def handler(event, context):
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            daily_file_job.start_job(message_body)
    else:
        daily_file_job.start_job(event)