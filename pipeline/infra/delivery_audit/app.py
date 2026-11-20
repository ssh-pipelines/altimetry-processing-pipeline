"""``delivery_audit`` Lambda: weekly audit of released products against CMR.

Invoked by an EventBridge Scheduler schedule, not by the pipeline: ingest lags
delivery by hours, so an audit at the end of a run would see its own files as
pending. Emails on every outcome, so a missing email means the schedule or the
Lambda stopped. When the audit cannot run, or a collection cannot be audited,
the email goes out first and the error is then raised for the Lambda-errors
alarm.

Event (every key optional except ``grace_hours``)::

    {"grace_hours": 192, "published_products": ["NASA-SSH"], "redeliver": false,
     "bucket": "nasa-ssh-staging-prod", "verbose": false}
"""

import logging
import os
from datetime import datetime, timedelta, timezone

import audit
import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]

EVENT_KEYS = {"grace_hours", "published_products", "redeliver", "bucket", "verbose"}


class AuditIncomplete(RuntimeError):
    """One or more collections could not be audited. Raised after the email."""


def parse_event(event: dict) -> dict:
    """Validate the schedule's input. Unknown keys are refused so a typo in the
    schedule (`grace_hour`) fails the run rather than silently taking a default."""
    unknown = set(event) - EVENT_KEYS
    if unknown:
        raise ValueError(f"Unknown event keys: {sorted(unknown)}")
    if "grace_hours" not in event:
        # No default: the right grace changes at cutover (8 days, then 12 h), and a
        # default would silently survive that edit being forgotten.
        raise ValueError("grace_hours is required")
    grace_hours = event["grace_hours"]
    if isinstance(grace_hours, bool) or not isinstance(grace_hours, (int, float)) or grace_hours < 0:
        raise ValueError(f"grace_hours must be a non-negative number, got {grace_hours!r}")
    if event.get("redeliver", False) is not False:
        # Redelivery goes through the delivery Lambda, which does not exist yet.
        raise ValueError("redeliver is not available until the delivery Lambda exists")
    products = event.get("published_products")
    if products is not None and (not isinstance(products, list) or not products):
        raise ValueError("published_products, if given, must be a non-empty list")
    bucket = event.get("bucket") or os.environ.get("BUCKET_NAME")
    if not bucket:
        raise ValueError("bucket not provided in event or BUCKET_NAME env var")
    return {
        "grace": timedelta(hours=grace_hours),
        "published_products": products,
        "bucket": bucket,
        "verbose": bool(event.get("verbose", False)),
    }


def _publish(subject: str, message: str) -> None:
    sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject[:100], Message=message)


def handler(event, context, http_session=None, now: datetime | None = None):
    try:
        params = parse_event(event or {})
        collections = audit.resolve_collections(params["published_products"])
    except Exception as e:
        logger.exception("Delivery audit could not run")
        _publish("Delivery audit could not run", f"{type(e).__name__}: {e}\n\nEvent: {event}")
        raise

    session = http_session or requests.Session()
    now = now or datetime.now(timezone.utc)
    outcomes = audit.run_audit(collections, s3, params["bucket"], session, now, params["grace"])
    subject, message = audit.render_report(outcomes, params["bucket"], params["grace"], params["verbose"])
    logger.info("%s\n%s", subject, message)
    _publish(subject, message)

    errors = [o for o in outcomes if isinstance(o, audit.AuditError)]
    if errors:
        raise AuditIncomplete("; ".join(f"{e.collection.short_name}: {e.error}" for e in errors))
    return {
        "status": "clean" if not any(o.problems for o in outcomes) else "problems",
        "collections": len(outcomes),
        "problems": sum(o.problems for o in outcomes),
    }
