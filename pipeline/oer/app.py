from datetime import datetime
import json
import logging
from typing import Tuple

from oer.oer import OerCorrection


def process_records(event):
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


def parse_params(params: dict) -> Tuple[datetime, str, str]:
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
    
    # Bulk processing via SQS
    if "Records" in event:
        process_records(event)
        return

    try:
        date, source = parse_params(event)
        oer_job = OerCorrection(source, date)
        oer_job.run()
        result = {"status": "success", "data": event}
        return result
    except Exception as e:
        error_response = {"status": "error", "errorType": type(e).__name__, "errorMessage": str(e), "input": event}
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
