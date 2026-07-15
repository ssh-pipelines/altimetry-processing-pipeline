import json
import logging
from datetime import datetime

from oer.oer import OerCorrection

from utilities.errors import PipelineError


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

    date = datetime.strptime(proc_date, "%Y-%m-%d")

    try:
        oer_job = OerCorrection(source, date, bucket)
        oer_job.run()
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
        raise PipelineError(json.dumps(error_response))
