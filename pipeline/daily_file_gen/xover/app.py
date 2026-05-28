import json
import logging
from typing import Tuple
import numpy as np
from crossover.parallel_crossovers import Crossover
from crossover.config.source_config import get_source_config
from utilities.errors import PipelineError


_CROSSOVER_PROCESSORS = {
    "self": Crossover,
}


def get_processor(day: np.datetime64, source: str, df_version: str) -> Crossover:
    config = get_source_config(source)
    crossover_type = config.crossover_type
    if crossover_type not in _CROSSOVER_PROCESSORS:
        raise ValueError(
            f"No processor for crossover_type '{crossover_type}' "
            f"(source '{source}'). Available types: {list(_CROSSOVER_PROCESSORS.keys())}"
        )
    return _CROSSOVER_PROCESSORS[crossover_type](day, source, df_version)


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
        processor = get_processor(date, source, df_version)
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
        raise PipelineError(json.dumps(error_response))
