import json
from daily_files import daily_file_job

def handler(event, context):
    # Handle messages from SQS
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            daily_file_job.start_job(message_body)
    # Handle direct Lambda invocations
    else:
        daily_file_job.start_job(event)