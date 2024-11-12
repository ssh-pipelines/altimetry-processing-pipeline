import json
import logging
from typing import Tuple
import numpy as np
from crossover.parallel_crossovers import Crossover
from utilities.aws_utils import aws_manager


def parse_params(params: dict) -> Tuple[np.datetime64, str, str, str]:
    try:
        date = np.datetime64(params.get("date"))
    except Exception as e:
        raise ValueError(f"Unable to parse date: {e}")
    source = params.get("source")
    df_version = params.get("df_version")
    processing = params.get("processing", "update")

    if None in [date, source, df_version]:
        raise ValueError(f"Missing job parameters: {df_version = },{source = },{date = }")
    return date, source, df_version, processing


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    if "Records" not in event:
        date, source, daily_file_version, processing = parse_params(event)
        processor = Crossover(date, source, daily_file_version)
        processor.run()
        return

    response = {"batchItemFailures": []}
    for record in event["Records"]:
        message_body = json.loads(record["body"])
        try:
            date, source, daily_file_version, processing = parse_params(message_body)
        except ValueError as e:
            logging.error(e)
            continue
        try:
            processor = Crossover(date, source, daily_file_version)
            processor.run()
            if processing == "update":
                aws_manager.update_stage(f"xover_{daily_file_version}", f"{source}_{date}", "Complete")
        except Exception as e:
            logging.exception(e)
            if processing == "update":
                aws_manager.update_stage(f"xover_{daily_file_version}", f"{source}_{date}", "Failed")
            response["batchItemFailures"].append({"itemIdentifier": record["messageId"]})
