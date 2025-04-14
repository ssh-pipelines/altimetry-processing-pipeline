from datetime import datetime
from enso_jobs import enso_processing


def handler(event, context):
    date = event.get("date")
    enso_processing.start_job(datetime.fromisoformat(date))

