import boto3
import urllib.parse

s3 = boto3.client("s3")

SOURCE_BUCKET = "example-bucket"
DEST_BUCKET = "nasa-ssh-products"

# Map source prefix to target prefix
PREFIX_MAP = {
    "daily_files/p3/": "NASA_SSH_REF_ALONGTRACK_V1/",
    "simple_grids/p3/": "NASA_SSH_REF_SIMPLE_GRID_V1/",
    "indicators/NASA_SSH_ENSO_INDICATOR": "NASA_SSH_ENSO_INDICATOR/",
    "indicators/archive/ENSO/": "NASA_SSH_ENSO_INDICATOR/",
    "indicators/NASA_SSH_GMSL_INDICATOR": "NASA_SSH_GMSL_INDICATOR/",
    "indicators/archive/GMSL/": "NASA_SSH_GMSL_INDICATOR/",
    "indicators/NASA_SSH_IOD_INDICATOR": "NASA_SSH_IOD_INDICATOR/",
    "indicators/archive/IOD/": "NASA_SSH_IOD_INDICATOR/",
    "indicators/NASA_SSH_PDO_INDICATOR": "NASA_SSH_PDO_INDICATOR/",
    "indicators/archive/PDO/": "NASA_SSH_PDO_INDICATOR/",
}

def lambda_handler(event, context):
    for record in event["Records"]:
        source_bucket = record["s3"]["bucket"]["name"]
        source_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])

        # Match against PREFIX_MAP
        matched = False
        for src_prefix, dest_prefix in PREFIX_MAP.items():
            if source_key.startswith(src_prefix):
                filename = source_key.split("/")[-1]
                dest_key = f"{dest_prefix}{filename}"

                print(f"Copying {source_key} → {dest_key}")
                s3.copy({"Bucket": source_bucket, "Key": source_key}, DEST_BUCKET, dest_key)
                matched = True
                break

        if not matched:
            print(f"No mapping found for key: {source_key}, skipping.")

    return {"status": "done"}