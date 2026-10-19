"""Union the contributors' source-tier manifests into one product-tier manifest.

The tier boundary: the along-track manifests are keyed on a **Source**, the
gridded manifest on the **Published product** those Sources feed. Several
Sources can contribute to one line, and the gridded output belongs to the line,
so what the product tier needs is the set of dates the line gained — not any one
contributor's view of them.

**A date claimed by two contributors fails the run.** The reference line's
contributors are not guaranteed disjoint: S6B starts 2025-11-26, overlapping
GSFC by five weeks, and under the derived-identity rule both resolve to the same
published key. Composing a contested date is a science decision that has not
been made, so the alternative to failing here is letting Map completion order
pick which mission's data ships under a PO.DAAC-facing filename.
`test_enabled_reference_sources_have_disjoint_ranges` in
`utilities/tests/test_products.py` catches the same contention at review time;
this is the runtime half, and it has to exist because the registry test sees
only declared date ranges, not what a hand-invoked window actually planned.

Deliberately layout-free: it is handed every key it reads and the one key it
writes, so it needs no `utilities` and stays a self-contained zip target
(ADR 0004). `start_run` owns the addresses.
"""
import json
import logging

import boto3

s3 = boto3.client("s3")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _read_manifest(bucket: str, key: str) -> list[dict]:
    """A contributor's Job specs.

    A missing manifest is an error, not an empty contribution: every contributor
    in the fan-out ran `pipeline_init`, which writes a manifest even when it
    plans nothing. An absent one means the source tier did not get that far, and
    treating it as "no dates" would grid a window with a contributor's data
    silently missing.
    """
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    jobs = json.loads(body)
    if not isinstance(jobs, list):
        raise ValueError(f"Manifest {key} is not a list of job specs.")
    return jobs


def merge(manifests: dict[str, list[dict]], published_product: str, bucket: str) -> list[dict]:
    """One product-tier Job spec per date, from `{source: job_specs}`.

    Job specs lose `source` entirely rather than keeping the last writer's: a
    gridded job is not per-source, and a stale `source` invites a downstream
    stage to key on the wrong thing.
    """
    claimed_by: dict[str, str] = {}
    for source, jobs in manifests.items():
        for job in jobs:
            date = job.get("date")
            if not date:
                continue
            prior = claimed_by.get(date)
            if prior is not None and prior != source:
                raise ValueError(
                    f"Contributors '{prior}' and '{source}' both claim "
                    f"{date} for published product '{published_product}'. "
                    f"Composing a contested date is not implemented, and "
                    f"letting execution order decide would pick which mission's "
                    f"data ships under a released filename."
                )
            claimed_by[date] = source

    return [
        {"date": date, "bucket": bucket, "published_product": published_product}
        for date in sorted(claimed_by)
    ]


def lambda_handler(event, context):
    bucket = event["bucket"]
    published_product = event["published_product"]
    product_jobs_key = event["product_jobs_key"]

    # Each entry is one contributor's source-tier result, as returned by its
    # branch of the Map: at minimum `{source, jobs_key}`.
    source_results = event.get("source_results") or []

    manifests = {}
    for result in source_results:
        source = result["source"]
        jobs_key = result["jobs_key"]
        manifests[source] = _read_manifest(bucket, jobs_key)
        logger.info("Read %d job specs from %s (%s)", len(manifests[source]), jobs_key, source)

    jobs = merge(manifests, published_product, bucket)
    logger.info(
        "Merged %d contributor manifests into %d dates for %s",
        len(manifests), len(jobs), published_product,
    )

    s3.put_object(
        Bucket=bucket,
        Key=product_jobs_key,
        Body=json.dumps(jobs),
        ContentType="application/json",
    )

    return {
        "jobs_key": product_jobs_key,
        "bucket": bucket,
        "published_product": published_product,
        "date_count": len(jobs),
    }
