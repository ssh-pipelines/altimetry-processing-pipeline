from datetime import date, timedelta, datetime
import json
import logging
from finalization.finalizer import Finalizer
from utilities.aws_utils import aws_manager


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    if "Records" not in event:
        start_str = event.get("start_date", (date.today() - timedelta(days=60)).isoformat())
        end_str = event.get("end_date", date.today().isoformat())

        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()

        logging.info(f"Finalizing daily files between {start} and {end}")
        finalizer = Finalizer(start, end)
        finalizer.process()
        return
    
    response = {"batchItemFailures": []}
    for record in event["Records"]:
        message_body = json.loads(record["body"])
        
        start_str = message_body.get("start_date", (date.today() - timedelta(days=60)).isoformat())
        end_str = message_body.get("end_date", date.today().isoformat())
        start = datetime.strptime(start_str, "%Y-%m-%d").date()
        end = datetime.strptime(end_str, "%Y-%m-%d").date()
        logging.info(f"Finalizing daily files between {start} and {end}")
        
        processing = message_body.get("processing", "update")

        try:
            finalizer = Finalizer(start, end)
            finalizer.process()
            if processing == "update":
                aws_manager.update_stage("finalizer", f"p3_{start_str}_{end_str}", "Complete")
        except Exception as e:
            logging.exception(e)
            if processing == "update":
                aws_manager.update_stage("finalizer", f"p3_{start_str}_{end_str}", "Failed")
            response["batchItemFailures"].append({"itemIdentifier": record["messageId"]})