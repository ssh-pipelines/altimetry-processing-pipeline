"""Rewrite an along-track jobs manifest into the gridded tier's manifest.

Stamps the published product the gridded stages work on, under a nested run
path so the two manifests of one run never collide. The gridded stages are
keyed on a published product, not a source — several sources can contribute to
one line, and the gridded output belongs to the line.
"""
import json

import boto3

s3 = boto3.client("s3")


def lambda_handler(event, context):
    bucket = event["bucket"]
    jobs_key = event["jobs_key"]
    published_product = event["published_product"]

    resp = s3.get_object(Bucket=bucket, Key=jobs_key)
    jobs = json.loads(resp["Body"].read())

    for job in jobs:
        job["published_product"] = published_product
        job.pop("source", None)

    new_key = jobs_key.replace("/jobs.json", f"/{published_product}/jobs.json")
    s3.put_object(
        Bucket=bucket,
        Key=new_key,
        Body=json.dumps(jobs),
        ContentType="application/json",
    )

    return {
        "jobs_key": new_key,
        "bucket": bucket,
        "published_product": published_product,
    }
