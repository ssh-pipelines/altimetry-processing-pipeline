from datetime import datetime, timedelta
import logging
from bad_passes.bad_pass_flag import update_bad_passes


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO", format="[%(levelname)s] %(asctime)s - %(message)s", handlers=[logging.StreamHandler()]
    )

    gsfc_start = event.get("gsfc_start", "")
    gsfc_end = event.get("gsfc_end", "")
    s6_start = event.get("s6_start", (datetime.today() - timedelta(days=60)).date().isoformat())
    s6_end = event.get("s6_end", datetime(2024, 7, 29).date().isoformat())

    try:
        update_bad_passes(gsfc_start, gsfc_end, s6_start, s6_end)
    except Exception as e:
        logging.exception(e)
