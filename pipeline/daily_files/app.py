import json
import logging
from daily_files import daily_file_job
from utilities.aws_utils import aws_manager


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Handle direct Lambda invocations ie: from testing
    # Will circumvent db tracking
    if "Records" not in event:
        date = event.get("date")
        source: str = event.get("source")
        satellite = event.get("satellite")

        try:
            if None in [date, source, satellite]:
                raise RuntimeError("One of date, source, or satellite job parameters missing. Job failure.")
            daily_file_job.start_job(date, source, satellite)
        except Exception as e:
            logging.exception(e)
        return

    # Handle messages from SQS
    response = {"batchItemFailures": []}
    for record in event["Records"]:
        message_body = json.loads(record["body"])

        date = message_body.get("date")
        source: str = message_body.get("source")
        satellite = message_body.get("satellite")

        # Either "bulk" or "update"
        # Bulk circumvents db
        processing = message_body.get("processing", "update")

        try:
            if None in [date, source, satellite]:
                raise RuntimeError("One of date, source, or satellite job parameters missing. Job failure.")

            daily_file_job.start_job(date, source, satellite)
            if processing == "update":
                aws_manager.update_stage("daily_files", f"{source}_{date}", "Complete")
        except Exception as e:
            logging.exception(e)
            if processing == "update":
                aws_manager.update_stage("daily_files", f"{source}_{date}", "Failed")
            response["batchItemFailures"].append({"itemIdentifier": record["messageId"]})
    return response
