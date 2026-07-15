import json
import logging

from daily_files import daily_file_job
from daily_files.config.source_config import get_available_sources

from utilities.errors import PipelineError


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event.get("bucket")
    date = event.get("date")
    source: str = event.get("source")
    granules = event.get("granules")

    if None in [date, source, bucket]:
        raise RuntimeError("One of date, source, or bucket job parameters missing. Job failure.")
    if granules is None:
        raise RuntimeError("granules job parameter missing. Job failure.")

    available = get_available_sources()
    if source not in available:
        raise RuntimeError(f"Source '{source}' is not configured. Available sources: {available}")

    try:
        daily_file_job.start_job(date, source, bucket, granules)
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
