import json
import logging
from datetime import datetime

import boto3
from indicators.compute_indicators import IndicatorProcessor

from utilities.errors import PipelineError
from utilities.pipeline_layout import s3_uri, simple_grid_key

s3 = boto3.client("s3")


def build_sg_key(date_str: str, bucket: str, published_product: str) -> str:
    """Construct the deterministic S3 URI for a staged simple grid file.

    Reads the internal staging copy rather than the published one: the grid has
    to be staged for ENSO anyway, so there is no reason for indicators to take
    the long way round through `products/`.
    """
    date = datetime.strptime(date_str, "%Y-%m-%d")
    return s3_uri(bucket, simple_grid_key(published_product, date))


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event["bucket"]
    jobs_key = event["jobs_key"]
    published_product = event["published_product"]

    # Read jobs from S3 manifest
    resp = s3.get_object(Bucket=bucket, Key=jobs_key)
    jobs = json.loads(resp["Body"].read())

    if not jobs:
        logging.info("No jobs to process, returning early.")
        return {"status": "success"}

    sg_keys = [build_sg_key(j["date"], bucket, published_product) for j in jobs]
    logging.info(
        f"{len(sg_keys)} simple grids to process for published_product={published_product}"
    )

    try:
        IndicatorProcessor(sg_keys, published_product).run(bucket)
        return {"status": "success"}
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise PipelineError(json.dumps(error_response))
