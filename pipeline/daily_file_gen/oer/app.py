from datetime import datetime
import json
import logging

from oer.oer import OerCorrection


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event.get("bucket")
    proc_date = event.get("date")
    source = event.get("source")
    if None in [proc_date, source, bucket]:
        raise ValueError("One of date, source, or bucket job parameters missing.")

    try:
        date = datetime.strptime(proc_date, "%Y-%m-%d")
        oer_job = OerCorrection(source, date)
        oer_job.run(bucket)
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
