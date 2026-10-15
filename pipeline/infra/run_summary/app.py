"""``run_summary`` Lambda — success-path reconciliation (ADR 0005).

Replaces the ``Notify Success`` invocation of ``failure_handling``. Runs once at the end of
``pipeline.asl``, given the run envelope
``{bucket, published_product, run_id, contributors}``. It reconciles expected (manifests)
vs produced (ResultWriter Job outcomes) across *every* contributor on the source tier and
the one product-tier manifest, writes the **Run summary** artifact, and publishes the
success SNS. Like ``failure_handling``, it never raises — a summarizer fault must not fail
an otherwise-successful run (the ASL also guards this with a Catch → Succeed).
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
    bucket = event.get("bucket", "")
    published_product = event.get("published_product", "")
    run_id = event.get("run_id", "")
    # A product-tier-only run (the deferred composite grid) will have none, which
    # reconciles the gridded section and reports an empty source tier — correct
    # rather than degraded, so this is not defaulted away into an error.
    contributors = event.get("contributors") or []

    if not bucket or not published_product or not run_id:
        logger.error(
            "run_summary requires bucket, published_product and run_id; got %s", event
        )
        return {"status": "skipped", "reason": "missing run identity"}

    manifests, outcomes, run_params, bad_pass_results = summarizer.gather(
        s3, bucket, published_product, run_id, contributors
    )
    summary = summarizer.build_summary(
        published_product,
        run_id,
        contributors,
        manifests,
        outcomes,
        run_params=run_params,
        bad_pass_results=bad_pass_results,
    )

    key = summarizer.summary_key(published_product, run_id)
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
        logger.error("Could not publish success SNS for %s/%s: %s", published_product, run_id, e)

    return {
        "status": "summarized",
        "published_product": summary["published_product"],
        "run_id": summary["run_id"],
    }
