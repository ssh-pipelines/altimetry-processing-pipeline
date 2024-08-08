from datetime import date, timedelta
import logging
from finalization.finalizer import Finalizer


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO", format="[%(levelname)s] %(asctime)s - %(message)s", handlers=[logging.StreamHandler()]
    )
    
    start = event.get("start_date", date.today() - timedelta(days=60))
    end = event.get("end_date", date.today())
    
    try:
        finalizer = Finalizer(start, end)
        finalizer.process()
    except Exception as e:
        logging.exception(e)
