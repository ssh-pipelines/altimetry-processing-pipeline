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

    bucket = event.get("bucket")
    date = event.get("date")
    source: str = event.get("source")
    satellite = event.get("satellite")

    if None in [date, source, satellite, bucket]:
        raise RuntimeError("One of date, source, satellite, or bucket job parameters missing. Job failure.")

    try:
        daily_file_job.start_job(date, source, satellite, bucket)
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
