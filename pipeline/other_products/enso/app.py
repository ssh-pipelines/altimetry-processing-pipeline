from datetime import datetime
import json
from enso_jobs import enso_processing


def handler(event, context):
    bucket = event.get("bucket")
    date = event.get("date")
    source = event.get("source")

    if None in [bucket, date, source]:
        raise ValueError("One of date, source, or bucket job parameters missing.")

    try:
        date = datetime.fromisoformat(date)
        enso_processing.start_job(date, bucket, source)
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
