import json
from datetime import datetime

from enso_jobs import enso_processing

from utilities.errors import PipelineError
from utilities.job_outcome import JobOutcome, Output


def handler(event, context):
    bucket = event.get("bucket")
    date = event.get("date")
    published_product = event.get("published_product")

    if None in [bucket, date, published_product]:
        raise ValueError("One of date, published_product, or bucket job parameters missing.")

    try:
        date_obj = datetime.fromisoformat(date)
        key = enso_processing.start_job(date_obj, bucket, published_product)

        return JobOutcome.success(
            stage="enso",
            date=date_obj.date().isoformat(),
            source=published_product,
            outputs=[Output(key=key, kind="enso_grid")],
        ).to_dict()
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise PipelineError(json.dumps(error_response))
