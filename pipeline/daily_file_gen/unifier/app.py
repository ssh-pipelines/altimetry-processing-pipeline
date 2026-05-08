from datetime import datetime
import logging

import boto3

from config.source_config import get_source_config


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )

    bucket = event.get("bucket")
    proc_date = event.get("date")
    source = event.get("source")

    if None in [bucket, proc_date, source]:
        raise ValueError("One of date, source, or bucket job parameters missing.")

    config = get_source_config(source)

    date_obj = datetime.strptime(proc_date, "%Y-%m-%d").date()
    date8 = date_obj.strftime("%Y%m%d")
    year = str(date_obj.year)

    src_filename = config.src_filename_template.format(source=source, date=date8)
    src_key = f"daily_files/p3/{source}/{year}/{src_filename}"

    dst_filename = config.dst_filename_template.format(source=source, date=date8)
    dst_key = f"{config.dst_prefix}/{year}/{dst_filename}"

    logging.info(f"Copying s3://{bucket}/{src_key} -> s3://{bucket}/{dst_key}")

    s3 = boto3.client("s3")
    s3.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": src_key},
        Key=dst_key,
    )

    logging.info(f"Unification complete for {source} {proc_date}")
    return {"status": "success", "data": {**event, "source": "NASA-SSH"}}
