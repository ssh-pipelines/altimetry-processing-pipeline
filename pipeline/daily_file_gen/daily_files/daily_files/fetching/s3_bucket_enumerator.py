import logging
import re

import boto3

from daily_files.fetching.enumerator import Enumerator, FileRef


class S3BucketEnumerator(Enumerator):
    """Enumerates source files from an internal S3 bucket instead of CMR."""

    def __init__(self, date, source_config):
        super().__init__(date, source_config)
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
        pattern = self.source_config.source_filename_pattern.replace("{source}", re.escape(self.source_config.source))
        pattern = pattern.replace("{date8}", r"(\d{8})")
        pattern = pattern.replace(".", r"\.")
        return pattern

    def enumerate(self) -> list[FileRef]:
        bucket = self.source_config.source_bucket
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
