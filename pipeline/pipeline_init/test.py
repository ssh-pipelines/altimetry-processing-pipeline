import boto3

def find_small_files(bucket_name: str, prefix: str, size_threshold: int) -> list:
    """
    Query the contents of an S3 bucket under a specific prefix and identify files below a size threshold.

    :param bucket_name: Name of the S3 bucket.
    :param prefix: Prefix path in the bucket to search.
    :param size_threshold: Size threshold in bytes.
    :return: List of files below the size threshold with their sizes.
    """
    session = boto3.Session(profile_name="s6")
    s3 = session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    
    small_files = []
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            
            if size < size_threshold:
                small_files.append({"Key": key, "Size": size})
    
    return small_files

# Example usage:
bucket_name = "example-bucket"
prefix = "daily_files/p3/"
size_threshold = 90000  # 60 KB

small_files = find_small_files(bucket_name, prefix, size_threshold)
for file in small_files:
    print(f"File: {file['Key']}, Size: {file['Size']} bytes")