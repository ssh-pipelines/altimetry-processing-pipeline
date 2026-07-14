import json
import logging

from simple_gridder.gridder import start_job

from utilities.errors import PipelineError
from utilities.job_outcome import JobOutcome, Output


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO", format="[%(levelname)s] %(asctime)s - %(message)s", handlers=[logging.StreamHandler()]
    )

    bucket = event.get("bucket")
    date = event.get("date")
    source = event.get("source")
    resolution = event.get("resolution")

    if None in [date, bucket]:
        raise ValueError("One of date, or bucket job parameters missing.")
    try:
        key = start_job(date, source, resolution, bucket)

        if key is None:
            return JobOutcome.skipped(
                stage="simple_grids",
                date=date,
                source=source,
                reason="no daily files available in window",
            ).to_dict()

        return JobOutcome.success(
            stage="simple_grids",
            date=date,
            source=source,
            outputs=[Output(key=key, kind="simple_grid")],
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
