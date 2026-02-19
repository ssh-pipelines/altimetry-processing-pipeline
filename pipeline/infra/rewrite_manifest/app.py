import json

import boto3

s3 = boto3.client("s3")


def lambda_handler(event, context):
    bucket = event["bucket"]
    jobs_key = event["jobs_key"]
    new_source = event["new_source"]

    resp = s3.get_object(Bucket=bucket, Key=jobs_key)
    jobs = json.loads(resp["Body"].read())

    for job in jobs:
        job["source"] = new_source

    new_key = jobs_key.replace("/jobs.json", f"/{new_source}/jobs.json")
    s3.put_object(
        Bucket=bucket,
        Key=new_key,
        Body=json.dumps(jobs),
        ContentType="application/json",
    )

    return {"jobs_key": new_key, "bucket": bucket, "source": new_source}
