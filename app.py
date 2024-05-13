from datetime import datetime
import json
from oer.oer import oer

def handler(event, context):
    # Handle messages from SQS
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            date = message_body.get(date)
            satellite = message_body.get(satellite)
            try:
                date = datetime.strptime(date, '%Y-%m-%d')
            except:
                raise ValueError(f'Unable to parse date.')
            oer(date, satellite)
    # Handle direct Lambda invocations
    else:
        date = event.get(date)
        satellite = event.get(satellite)
        try:
            date = datetime.strptime(date, '%Y-%m-%d')
        except:
            raise ValueError(f'Unable to parse date.')
        oer(date, satellite)