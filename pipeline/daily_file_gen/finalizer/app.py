import json
import logging
from datetime import datetime

from finalization.config.source_config import get_available_sources, get_source_config
from finalization.finalizer import Finalizer

from utilities.errors import PipelineError
from utilities.job_outcome import JobOutcome, Output
from utilities.provenance import processing_complete


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
        result = finalizer.process(bucket)

        config = get_source_config(source)
        return JobOutcome.success(
            stage="finalizer",
            date=date.isoformat(),
            source=source,
            outputs=[Output(key=result.key, kind="daily_file_p3")],
            metadata={
                "processing_history": result.processing_history,
                "provenance_complete": processing_complete(result.processing_history, 3),
                "product_type": config.product_type,
                "unify": config.unify,
            },
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
