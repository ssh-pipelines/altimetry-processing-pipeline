from datetime import datetime
import json
import logging
from finalization.finalizer import Finalizer
from finalization.config.source_config import get_available_sources


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

    if None in [bucket, proc_date, source]:
        raise ValueError("One of date, source, or bucket job parameters missing.")

    available = get_available_sources()
    if source not in available:
        raise ValueError(f"Source '{source}' is not configured. Available sources: {available}")

    try:
        date = datetime.strptime(proc_date, "%Y-%m-%d").date()

        logging.info(f"Finalizing daily file for {date.isoformat()} (source={source})")
        finalizer = Finalizer(date, source, bucket)
        finalizer.process(bucket)
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
