import json
import logging
from typing import Tuple
import numpy as np
from crossover.parallel_crossovers import Crossover


def parse_params(params: dict) -> Tuple[np.datetime64, str, str, str]:
    try:
        date = np.datetime64(params.get("date"))
    except Exception as e:
        raise ValueError(f"Unable to parse date: {e}")
    source = params.get("source")
    df_version = params.get("df_version")

    if None in [date, source, df_version]:
        raise ValueError(f"Missing job parameters: {df_version = },{source = },{date = }")
    return date, source, df_version


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
    df_version = event.get("df_version")
    if None in [proc_date, source, df_version]:
        raise ValueError("One of date, source, df_version, or bucket job parameters missing.")

    # Step Function processing
    try:
        date = np.datetime64(proc_date)
        processor = Crossover(date, source, df_version)
        processor.run(bucket)
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
