import argparse
import json
import re
from datetime import datetime
from pathlib import Path

import boto3
import yaml

_SOURCES_YAML = Path(__file__).parent.parent.parent / "utilities" / "sources.yaml"


def _load_unifiable_sources() -> set[str]:
    with open(_SOURCES_YAML) as f:
        raw = yaml.safe_load(f)
    return {name for name, cfg in raw["sources"].items() if cfg.get("unify")}


def validate_sources(sources: list[str]) -> None:
    unifiable = _load_unifiable_sources()
    invalid = [s for s in sources if s not in unifiable]
    if invalid:
        raise SystemExit(
            f"The following sources are not configured for unification: {invalid}\n"
            f"Sources with unify=true: {sorted(unifiable)}"
        )


def list_p3_dates(s3, bucket, source):
    prefix = f"daily_files/p3/{source}/"
    pattern = re.compile(r"(\d{8})\.nc$")
    paginator = s3.get_paginator("list_objects_v2")
    dates = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            m = pattern.search(obj["Key"])
            if m:
                dates.append(datetime.strptime(m.group(1), "%Y%m%d").date().isoformat())
    return dates


def main():
    parser = argparse.ArgumentParser(
        description="Generate a unifier jobs manifest from existing P3 along-track files."
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--profile", required=True, help="AWS CLI profile name")
    parser.add_argument("--sources", required=True, nargs="+", help="Source names to include (e.g. GSFC_6.1 S6)")
    args = parser.parse_args()

    validate_sources(args.sources)

    session = boto3.Session(profile_name=args.profile)
    s3 = session.client("s3")

    jobs = []
    for source in args.sources:
        dates = list_p3_dates(s3, args.bucket, source)
        jobs += [{"date": d, "source": source, "bucket": args.bucket} for d in dates]
        print(f"Found {len(dates)} dates for {source}")

    jobs_key = "pipeline_runs/backfill/NASA-SSH-30yr/jobs.json"
    s3.put_object(Bucket=args.bucket, Key=jobs_key, Body=json.dumps(jobs), ContentType="application/json")
    print(f"Wrote {len(jobs)} jobs to s3://{args.bucket}/{jobs_key}")


if __name__ == "__main__":
    main()
