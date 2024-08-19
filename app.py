from datetime import datetime
import json
import logging
from typing import Tuple

from oer.oer import OerCorrection


def setup_logging(log_level: str):
    logging.root.handlers = []
    logging.basicConfig(
        level=log_level,
        format='[%(levelname)s] %(asctime)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )

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
    setup_logging('INFO')
    # Handle messages from SQS
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            try:
                date, satellite = parse_params(message_body)
            except ValueError as e:
                logging.exception(f'Error processing {satellite} {date}: {e}')
                continue
            try:
                oer_job = OerCorrection(satellite, date)
                oer_job.run()
            except Exception as e:
                logging.exception(f'Error processing {satellite} {date}: {e}')
                continue
    # Handle direct Lambda invocations
    else:
        date, satellite = parse_params(event)
        setup_logging(event.get('log_level', 'INFO'))
        oer_job = OerCorrection(satellite, date)
        oer_job.run()