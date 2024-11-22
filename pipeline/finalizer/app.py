from datetime import datetime
import json
import logging
from finalization.finalizer import Finalizer


def process_records(event):
    for record in event["Records"]:
        message_body = json.loads(record["body"])

        date = datetime.strptime(message_body.get("date"), "%Y-%m-%d").date()

        logging.info(f"Finalizing daily file for {date.isoformat()}")
        try:
            finalizer = Finalizer(date)
            finalizer.process()
        except Exception as e:
            logging.exception(e)


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    if "Records" in event:
        process_records(event)
        return

    try:
        date = datetime.strptime(event.get("date"), "%Y-%m-%d").date()

        logging.info(f"Finalizing daily file for {date.isoformat()}")
        finalizer = Finalizer(date)
        finalizer.process()
        result = {"status": "success", "data": event}
        return result
    except Exception as e:
        error_response = {"status": "error", "errorType": type(e).__name__, "errorMessage": str(e), "input": event}
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
