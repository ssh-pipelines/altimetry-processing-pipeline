from datetime import datetime
import json
import logging
from typing import Tuple

from oer.oer import OerCorrection
from utilities.aws_utils import aws_manager


def parse_params(params: dict) -> Tuple[datetime, str, str]:
    try:
        date = datetime.strptime(params.get("date"), "%Y-%m-%d")
    except Exception as e:
        raise ValueError(f"Unable to parse date: {e}")
    source = params.get("source")
    processing = params.get("processing", "update")

    if None in [date, source]:
        raise ValueError(f"Unable to get date {date} or source {source} from message.")
    return date, source, processing


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Handle direct Lambda invocations
    if "Records" not in event:
        date, source, processing = parse_params(event)
        oer_job = OerCorrection(source, date)
        oer_job.run()
        return

    # Handle messages from SQS
    response = {"batchItemFailures": []}
    for record in event["Records"]:
        message_body = json.loads(record["body"])
        try:
            date, source, processing = parse_params(message_body)
        except ValueError as e:
            logging.exception(f"Error processing params: {e}")
            continue
        try:
            oer_job = OerCorrection(source, date)
            oer_job.run()
            if processing == "update":
                aws_manager.update_stage("oer", f"{source}_{date}", "Complete")
        except Exception as e:
            logging.exception(f"Error processing params: {e}")
            if processing == "update":
                aws_manager.update_stage("oer", f"{source}_{date}", "Failed")
            response["batchItemFailures"].append({"itemIdentifier": record["messageId"]})
