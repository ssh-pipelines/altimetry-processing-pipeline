from datetime import date, timedelta, datetime
import logging
from finalization.finalizer import Finalizer


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    start_str = event.get("start_date", (date.today() - timedelta(days=60)).isoformat())
    end_str = event.get("end_date", date.today().isoformat())

    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()

    logging.info(f"Finalizing daily files between {start} and {end}")

    try:
        finalizer = Finalizer(start, end)
        finalizer.process()
    except Exception as e:
        logging.exception(e)
