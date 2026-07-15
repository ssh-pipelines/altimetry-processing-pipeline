from enumeration.base import Enumerator, GranuleRef
from enumeration.cmr import CMREnumerator
from enumeration.s3_bucket import S3BucketEnumerator
from enumeration.thredds import ThreddsEnumerator

__all__ = [
    "GranuleRef",
    "Enumerator",
    "CMREnumerator",
    "ThreddsEnumerator",
    "S3BucketEnumerator",
    "build_enumerator",
]


def build_enumerator(source_config, bucket: str | None = None) -> Enumerator:
    discovery_type = source_config.discovery_type
    if discovery_type == "cmr":
        return CMREnumerator(source_config)
    if discovery_type == "thredds":
        return ThreddsEnumerator(source_config)
    if discovery_type == "s3_bucket":
        return S3BucketEnumerator(source_config, bucket)
    raise ValueError(f"Unknown discovery_type: {discovery_type}")
