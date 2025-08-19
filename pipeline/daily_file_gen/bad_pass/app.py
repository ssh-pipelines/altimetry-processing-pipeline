from datetime import datetime
import json
import logging
from bad_passes.bad_pass_flag import XoverProcessor


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

    try:
        xover_processor = XoverProcessor(source, datetime.fromisoformat(proc_date))
        bad_pass_results = xover_processor.process(bucket)
        return bad_pass_results
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
