import json
import logging
from typing import Tuple
import numpy as np
from crossover.parallel_crossovers import Crossover


def parse_params(params: dict) -> Tuple[np.datetime64, str, str, str]:
    try:
        date = np.datetime64(params.get("date"))
    except Exception as e:
        raise ValueError(f"Unable to parse date: {e}")
    source = params.get("source")
    df_version = params.get("df_version")

    if None in [date, source, df_version]:
        raise ValueError(
            f"Missing job parameters: {df_version = },{source = },{date = }"
        )
    return date, source, df_version


def process_records(event):
    for record in event["Records"]:
        message_body = json.loads(record["body"])
        try:
            date, source, daily_file_version = parse_params(message_body)
        except ValueError as e:
            logging.error(e)
            continue
        try:
            processor = Crossover(date, source, daily_file_version)
            processor.run()
        except Exception as e:
            logging.exception(e)


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

    # Step Function processing
    try:
        date, source, daily_file_version = parse_params(event)
        processor = Crossover(date, source, daily_file_version)
        processor.run()
        result = {"status": "success", "data": event}
        return result

    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
