import json
import logging
from datetime import datetime

import boto3
from config.source_config import get_available_sources, get_source_config

from utilities.errors import PipelineError
from utilities.job_outcome import JobOutcome, Output
from utilities.pipeline_layout import daily_file_key
from utilities.source_profile import get_source_profile


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

    available = get_available_sources()
    if source not in available:
        raise ValueError(
            f"Source '{source}' is not configured for unification. Available: {available}"
        )

    try:
        src_config = get_source_config(source)
        dst_profile = get_source_profile("NASA-SSH")

        date_obj = datetime.strptime(proc_date, "%Y-%m-%d").date()

        src_key = daily_file_key(src_config, date_obj, "p3")
        dst_key = daily_file_key(dst_profile, date_obj, "p3")

        logging.info(f"Copying s3://{bucket}/{src_key} -> s3://{bucket}/{dst_key}")

        s3 = boto3.client("s3")
        s3.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": src_key},
            Key=dst_key,
        )

        logging.info(f"Unification complete for {source} {proc_date}")
        # The unifier is a deliberately lightweight byte-copy Lambda (no NetCDF engine), so
        # it does not re-read `processing_history` from the file. The copy preserves that
        # lineage in-file; the along-track P3 (finalizer) Job outcome already carries the
        # authoritative `provenance_complete`. We omit the flag here — run_summary treats its
        # absence as "unknown" rather than failing reconciliation (see ADR 0005).
        return JobOutcome.success(
            stage="unifier",
            date=date_obj.isoformat(),
            source="NASA-SSH",
            outputs=[Output(key=dst_key, kind="nasa_ssh_p3")],
            metadata={"copied_from": src_key},
        ).to_dict()
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise PipelineError(json.dumps(error_response))
