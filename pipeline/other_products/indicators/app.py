from datetime import datetime
import json
import logging
from typing import List
from indicators.compute_indicators import IndicatorProcessor
from utilities.aws_utils import aws_manager


def get_indicators_modtime() -> datetime:
    """
    Get modified time of indicators file. If it doesn't exist return epoch time.

    Currently a placeholder as we are processing ALL dates every time
    """
    return datetime(1970, 1, 1)
    indicators_meta = aws_manager.get_all_obj_meta(
        "s3://example-bucket/aux_files/indicators.nc"
    )
    if "LastModified" in indicators_meta["example-bucket/aux_files/indicators.nc"]:
        return indicators_meta["example-bucket/aux_files/indicators.nc"][
            "LastModified"
        ]
    return datetime(1970, 1, 1)


def get_keys_to_process(base_mod_time: datetime) -> List[str]:
    sg_modtimes = aws_manager.get_all_obj_meta(
        "s3://example-bucket/simple_grids/p3/*/*.nc"
    )
    return [
        k
        for k, v in sg_modtimes.items()
        if v["LastModified"].replace(tzinfo=None) > base_mod_time
    ]


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    try:
        # Get existing indicators file mod time
        base_mod_time = get_indicators_modtime()
        logging.info(f"Indicators file mod time: {base_mod_time}")

        keys_to_process = get_keys_to_process(base_mod_time)
        logging.info(f"{len(keys_to_process)} simple grids require processing")

        if keys_to_process:
            # process simple grids and update indicators file
            IndicatorProcessor(keys_to_process).run()

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
