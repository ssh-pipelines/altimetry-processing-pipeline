from datetime import datetime
import json
import logging
from typing import Tuple

from oer.oer import OerCorrection

def parse_params(params: dict) -> Tuple[datetime, str]:
    try:
        date = datetime.strptime(params.get('date'), '%Y-%m-%d')
    except:
        raise ValueError(f'Unable to parse date.')
    satellite = params.get('satellite')
    if None in [date, satellite]:
        raise ValueError(f'Unable to get date {date} or satellite {satellite} from message.')
    return date, satellite

def handler(event, context):
    # Handle messages from SQS
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            try:
                date, satellite = parse_params(message_body)
            except ValueError as e:
                logging.error(e)
                continue
            oer_job = OerCorrection(satellite, date)
            oer_job.run()

    # Handle direct Lambda invocations
    else:
        date, satellite = parse_params(event)
        oer_job = OerCorrection(satellite, date)
        oer_job.run()