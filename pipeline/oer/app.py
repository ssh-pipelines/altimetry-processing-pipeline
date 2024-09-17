from datetime import datetime
import json
import logging
from typing import Tuple

from oer.oer import OerCorrection


def parse_params(params: dict) -> Tuple[datetime, str]:
    try:
        date = datetime.strptime(params.get("date"), "%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Unable to parse date: {e}")
    source = params.get("source")
    if None in [date, source]:
        raise ValueError(f"Unable to get date {date} or source {source} from message.")
    return date, source


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Handle messages from SQS
    if "Records" in event:
        for record in event["Records"]:
            message_body = json.loads(record["body"])
            try:
                date, source = parse_params(message_body)
            except ValueError as e:
                logging.exception(f"Error processing params: {e}")
                continue
            try:
                oer_job = OerCorrection(source, date)
                oer_job.run()
            except Exception as e:
                logging.exception(f"Error processing params: {e}")
                continue
    # Handle direct Lambda invocations
    else:
        date, source = parse_params(event)
        oer_job = OerCorrection(source, date)
        oer_job.run()
