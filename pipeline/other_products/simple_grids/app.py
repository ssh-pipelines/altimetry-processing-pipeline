import json
import logging
from simple_gridder.gridder import start_job


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO", format="[%(levelname)s] %(asctime)s - %(message)s", handlers=[logging.StreamHandler()]
    )

    bucket = event.get("bucket")
    date = event.get("date")
    source = event.get("source")
    resolution = event.get("resolution")

    if None in [date, source, bucket]:
        raise ValueError("One of date, or bucket job parameters missing.")
    try:
        start_job(date, source, resolution, bucket)
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
