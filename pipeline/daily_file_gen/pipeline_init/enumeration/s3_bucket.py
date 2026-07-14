import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

import boto3
from config.source_config import PipelineInitSourceConfig

from enumeration.base import GranuleRef


def _chunk_dates_by_year(dates: list[date]) -> dict[int, list[date]]:
    grouped = defaultdict(list)
    for d in dates:
        grouped[d.year].append(d)
    return grouped


class S3BucketEnumerator:
    """Enumerates source files from an S3 bucket. Supports two modes:

    - Filename-regex: one file per date, matched via source_filename_pattern.
    - Cycle-index: a JSON map of filename → date span. Used when files cover
      multi-day cycles (currently only the EXAMPLE_S3 fixture).
    """

    def __init__(self, source_config: PipelineInitSourceConfig, bucket: str | None = None):
        self.source_config = source_config
        self.bucket = source_config.source_bucket or bucket
        if not self.bucket:
            raise ValueError(
                f"No source bucket configured for {source_config.source} "
                "and none provided at runtime"
            )
        self.s3 = boto3.client("s3")

    def enumerate(self, start: date, end: date) -> list[GranuleRef]:
        if self.source_config.cycle_index_key:
            return self._enumerate_with_cycle_index(start, end)
        return self._enumerate_with_filename_regex(start, end)

    def _enumerate_with_filename_regex(self, start: date, end: date) -> list[GranuleRef]:
        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        yearly = _chunk_dates_by_year(dates)

        fname_pattern = self.source_config.source_filename_pattern or ""
        fname_pattern = fname_pattern.replace("{source}", self.source_config.source)
        fname_pattern = fname_pattern.replace("{date}", r"(\d{8})")
        fname_pattern = fname_pattern.replace(".", r"\.")

        refs: list[GranuleRef] = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for year, year_dates in yearly.items():
            prefix = (self.source_config.source_prefix_pattern or "").format(
                source=self.source_config.source,
                year=year,
            )
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            logging.info(
                f"Querying source bucket s3://{self.bucket}/{prefix} "
                f"for {self.source_config.source} in {year}"
            )
            pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

            year_start = year_dates[0]
            year_end = year_dates[-1]

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    match = re.search(fname_pattern, key)
                    if not match:
                        continue
                    file_date = datetime.strptime(match.group(1), "%Y%m%d").date()
                    if not (year_start <= file_date <= year_end):
                        continue
                    refs.append(
                        GranuleRef(
                            date=file_date,
                            uri=f"s3://{self.bucket}/{key}",
                            mod_time=obj["LastModified"],
                            sort_key=(file_date.toordinal(), key),
                        )
                    )

        refs.sort(key=lambda r: (r.date, r.uri))
        return refs

    def _enumerate_with_cycle_index(self, start: date, end: date) -> list[GranuleRef]:
        cycle_index = self._load_cycle_index()

        cycles = []
        for filename, span in cycle_index.items():
            cycles.append(
                {
                    "filename": filename,
                    "start": date.fromisoformat(span["start"]),
                    "end": date.fromisoformat(span["end"]),
                }
            )

        dates = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        yearly = _chunk_dates_by_year(dates)

        file_mod_times: dict[str, datetime] = {}
        file_keys: dict[str, str] = {}
        paginator = self.s3.get_paginator("list_objects_v2")

        for year in yearly:
            prefix = (self.source_config.source_prefix_pattern or "").format(
                source=self.source_config.source,
                year=year,
            )
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            logging.info(
                f"Querying source bucket s3://{self.bucket}/{prefix} for cycle files in {year}"
            )
            pages = paginator.paginate(Bucket=self.bucket, Prefix=prefix)

            for page in pages:
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    filename = key.rsplit("/", 1)[-1]
                    file_mod_times[filename] = obj["LastModified"]
                    file_keys[filename] = key

        refs: list[GranuleRef] = []
        for d in dates:
            for cycle in cycles:
                if not (cycle["start"] <= d <= cycle["end"]):
                    continue
                filename = cycle["filename"]
                mod_time = file_mod_times.get(filename)
                key = file_keys.get(filename)
                if mod_time is None or key is None:
                    continue
                refs.append(
                    GranuleRef(
                        date=d,
                        uri=f"s3://{self.bucket}/{key}",
                        mod_time=mod_time,
                        sort_key=(d.toordinal(), filename),
                    )
                )

        refs.sort(key=lambda r: (r.date, r.uri))
        return refs

    def _load_cycle_index(self) -> dict:
        logging.info(
            f"Loading cycle index from s3://{self.bucket}/{self.source_config.cycle_index_key}"
        )
        resp = self.s3.get_object(Bucket=self.bucket, Key=self.source_config.cycle_index_key)
        return json.loads(resp["Body"].read())
