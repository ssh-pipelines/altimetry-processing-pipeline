import json
from daily_files import daily_file_job

def handler(event, context):
    # Handle messages from SQS
    response = {"batchItemFailures": []}
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            try:
                daily_file_job.start_job(message_body)
            except Exception as e:
                print(e)
                response["batchItemFailures"].append({"itemIdentifier": record['messageId']})
    # Handle direct Lambda invocations
    else:
        try:
            daily_file_job.start_job(event)
        except Exception as e:
            print(e)
            response["batchItemFailures"].append({"itemIdentifier": record['messageId']})