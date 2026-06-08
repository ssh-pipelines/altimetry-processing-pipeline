"""``run_summary`` Lambda — success-path reconciliation (ADR 0005).

Replaces the ``Notify Success`` invocation of ``failure_handling``. Runs once at the top of
``pipeline.asl`` after the gridded pipeline succeeds, given the same input the old success
path received: ``{jobs_key, bucket, source}``. It reconciles expected (manifests) vs produced
(ResultWriter Job outcomes), writes the **Run summary** artifact, and publishes the success
SNS. Like ``failure_handling``, it never raises — a summarizer fault must not fail an
otherwise-successful run (the ASL also guards this with a Catch → Succeed).
"""

import json
import logging
import os

import boto3

import summarizer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def handler(event, context):
    jobs_key = event.get("jobs_key", "")
    bucket = event.get("bucket", "")

    if not jobs_key or not bucket:
        logger.error("run_summary requires jobs_key and bucket; got %s", event)
        return {"status": "skipped", "reason": "missing jobs_key or bucket"}

    manifests, outcomes, run_params = summarizer.gather(s3, bucket, jobs_key)
    summary = summarizer.build_summary(jobs_key, manifests, outcomes, run_params=run_params)

    key = summarizer.summary_key(jobs_key)
    try:
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(summary, indent=2),
            ContentType="application/json",
        )
        logger.info("Wrote run summary to s3://%s/%s", bucket, key)
    except Exception as e:
        logger.error("Could not write run summary %s: %s", key, e)

    subject, message = summarizer.render_notification(summary)
    try:
        sns.publish(TopicArn=SNS_TOPIC_ARN, Subject=subject, Message=message)
    except Exception as e:
        logger.error("Could not publish success SNS for %s: %s", jobs_key, e)

    return {"status": "summarized", "source": summary["source"], "run_id": summary["run_id"]}
