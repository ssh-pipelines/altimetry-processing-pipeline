import json
import logging
import re
from datetime import date, datetime

import boto3

from daily_files.fetching.enumerator import Enumerator, FileRef


class S3BucketEnumerator(Enumerator):
    """Enumerates source files from an internal S3 bucket instead of CMR."""

    def __init__(self, date, source_config, bucket: str | None = None):
        super().__init__(date, source_config, bucket)
        self.bucket = source_config.source_bucket or bucket
        if not self.bucket:
            raise ValueError(f"No source bucket configured for {source_config.source} and none provided at runtime")
        self.s3 = boto3.client("s3")

    def _build_prefix(self) -> str:
        """Interpolate source_prefix_pattern with source, year, month, day."""
        return self.source_config.source_prefix_pattern.format(
            source=self.source_config.source,
            year=self.date.year,
            month=f"{self.date.month:02d}",
            day=f"{self.date.day:02d}",
        )

    def _build_filename_regex(self) -> str:
        """Convert source_filename_pattern into a regex with a date8 capture group."""
        pattern = self.source_config.source_filename_pattern.replace("{source}", self.source_config.source)
        pattern = pattern.replace("{date8}", r"(\d{8})")
        pattern = pattern.replace(".", r"\.")
        return pattern

    def _enumerate_with_cycle_index(self) -> list[FileRef]:
        """Enumerate files using a cycle index JSON that maps filenames to date ranges."""
        bucket = self.bucket
        cycle_index_key = self.source_config.cycle_index_key

        logging.info(f"Loading cycle index from s3://{bucket}/{cycle_index_key}")
        resp = self.s3.get_object(Bucket=bucket, Key=cycle_index_key)
        cycle_index = json.loads(resp["Body"].read())

        target_date = self.date.date() if isinstance(self.date, datetime) else self.date

        # Find cycle files whose date range overlaps the target date
        matching_filenames = set()
        for filename, span in cycle_index.items():
            start = date.fromisoformat(span["start"])
            end = date.fromisoformat(span["end"])
            if start <= target_date <= end:
                matching_filenames.add(filename)

        if not matching_filenames:
            logging.info("No cycle files cover the target date")
            return []

        # List the S3 prefix to get LastModified for matched files
        prefix = self.source_config.source_prefix_pattern.format(
            source=self.source_config.source,
            year=target_date.year,
            month=f"{target_date.month:02d}",
            day=f"{target_date.day:02d}",
        )
        if not prefix.endswith("/"):
            prefix += "/"

        logging.info(f"Listing s3://{bucket}/{prefix} for cycle files matching {target_date}")
        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        file_refs = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                filename = key.rsplit("/", 1)[-1]
                if filename in matching_filenames:
                    # Look up the cycle's time range for this file
                    span = cycle_index[filename]
                    file_refs.append(
                        FileRef(
                            id=key,
                            title=filename,
                            access_url=f"s3://{bucket}/{key}",
                            time_start=f"{span['start']}T00:00:00Z",
                            time_end=f"{span['end']}T23:59:59Z",
                            modified_time=obj["LastModified"].isoformat(),
                            collection_id="",
                        )
                    )

        logging.info(f"Found {len(file_refs)} cycle file(s) from S3 bucket listing")
        return file_refs

    def enumerate(self) -> list[FileRef]:
        if self.source_config.cycle_index_key:
            return self._enumerate_with_cycle_index()

        bucket = self.bucket
        prefix = self._build_prefix()
        if not prefix.endswith("/"):
            prefix += "/"

        filename_regex = self._build_filename_regex()
        target_date_str = self.date.strftime("%Y%m%d")

        logging.info(f"Listing s3://{bucket}/{prefix} for {self.source_config.source} on {self.date.date()}")

        paginator = self.s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        file_refs = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                match = re.search(filename_regex, key)
                if match and match.group(1) == target_date_str:
                    filename = key.rsplit("/", 1)[-1]
                    file_refs.append(
                        FileRef(
                            id=key,
                            title=filename,
                            access_url=f"s3://{bucket}/{key}",
                            time_start=self.date.strftime("%Y-%m-%dT00:00:00Z"),
                            time_end=self.date.strftime("%Y-%m-%dT23:59:59Z"),
                            modified_time=obj["LastModified"].isoformat(),
                            collection_id="",
                        )
                    )

        logging.info(f"Found {len(file_refs)} file(s) from S3 bucket listing")
        return file_refs
