from datetime import datetime
import json
import logging
from indicators.compute_indicators import IndicatorProcessor


def build_sg_key(date_str: str, bucket: str, source: str) -> str:
    """
    Construct the deterministic S3 key for a simple grid file.
    Mirrors the pattern in simple_grids/simple_gridder/gridder.py:28.
    """
    date = datetime.strptime(date_str, "%Y-%m-%d")
    year = str(date.year)
    filename = f'{source}_alt_ref_simple_grid_v1_{date.strftime("%Y%m%d")}.nc'
    return f"s3://{bucket}/simple_grids/{source}/{year}/{filename}"


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    if isinstance(event, list):
        jobs = event
    else:
        jobs = event.get("jobs")
    if not jobs:
        raise ValueError("'jobs' list is missing or empty.")

    for i, job in enumerate(jobs):
        for field in ("date", "bucket", "source"):
            if field not in job:
                raise ValueError(f"jobs[{i}] missing required field '{field}'.")

    source = jobs[0]["source"]
    bucket = jobs[0]["bucket"]

    sg_keys = [build_sg_key(j["date"], j["bucket"], j["source"]) for j in jobs]
    logging.info(f"{len(sg_keys)} simple grids to process for source={source}")

    try:
        IndicatorProcessor(sg_keys, source).run(bucket)
        return {"status": "success", "data": event}
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
