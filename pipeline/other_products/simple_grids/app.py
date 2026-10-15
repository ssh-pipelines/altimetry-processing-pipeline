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
    contributors = event.get("contributors")
    # Absent and null are different: a null `publish_as` is the deliberate
    # "development output" choice, so it cannot be defaulted to anything.
    if "publish_as" not in event:
        raise ValueError("publish_as job parameter missing (use null for a development grid).")
    publish_as = event.get("publish_as")
    resolution = event.get("resolution")

    if None in [date, bucket] or not contributors:
        raise ValueError("One of date, bucket, or contributors job parameters missing.")
    try:
        key = start_job(date, contributors, publish_as, resolution, bucket)

        if key is None:
            return JobOutcome.skipped(
                stage="simple_grids",
                date=date,
                source=publish_as or "development",
                reason="no daily files available in window",
            ).to_dict()

        return JobOutcome.success(
            stage="simple_grids",
            date=date,
            source=publish_as or "development",
            outputs=[Output(key=key, kind="simple_grid")],
            metadata={"contributors": contributors},
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
