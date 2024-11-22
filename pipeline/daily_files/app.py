import json
import logging
from daily_files import daily_file_job


def process_records(event):
    for record in event["Records"]:
        message_body = json.loads(record["body"])

        date = message_body.get("date")
        source: str = message_body.get("source")
        satellite = message_body.get("satellite")

        try:
            if None in [date, source, satellite]:
                raise RuntimeError("One of date, source, or satellite job parameters missing. Job failure.")
            daily_file_job.start_job(date, source, satellite)
        except Exception as e:
            logging.exception(e)


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    # Handle messages from SQS
    if "Records" in event:
        process_records(event)
        return

    # Step Function processing
    try:
        date = event.get("date")
        source: str = event.get("source")
        satellite = event.get("satellite")

        if None in [date, source, satellite]:
            raise RuntimeError("One of date, source, or satellite job parameters missing. Job failure.")
        daily_file_job.start_job(date, source, satellite)
        result = {"status": "success", "data": event}
        return result
    except Exception as e:
        error_response = {"status": "error", "errorType": type(e).__name__, "errorMessage": str(e), "input": event}
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))