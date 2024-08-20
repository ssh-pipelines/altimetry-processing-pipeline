import json
import logging
from daily_files import daily_file_job


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Handle messages from SQS
    response = {"batchItemFailures": []}
    if "Records" in event:
        for record in event["Records"]:
            message_body = json.loads(record["body"])

            date = message_body.get("date")
            source: str = message_body.get("source")
            satellite = message_body.get("satellite")

            try:
                if None in [date, source, satellite]:
                    raise RuntimeError(
                        "One of date, source, or satellite job parameters missing. Job failure."
                    )
                daily_file_job.start_job(date, source, satellite)
            except Exception as e:
                logging.exception(e)
                response["batchItemFailures"].append(
                    {"itemIdentifier": record["messageId"]}
                )
    # Handle direct Lambda invocations
    else:
        date = message_body.get("date")
        source: str = message_body.get("source")
        satellite = message_body.get("satellite")

        try:
            if None in [date, source, satellite]:
                raise RuntimeError(
                    "One of date, source, or satellite job parameters missing. Job failure."
                )
            daily_file_job.start_job(date, source, satellite)
        except Exception as e:
            logging.exception(e)
            response["batchItemFailures"].append(
                {"itemIdentifier": record["messageId"]}
            )

    return response
