import boto3


def upload_s3(src: str, bucket: str, dest:str):
    s3 = boto3.resource('s3')
    s3.upload_file(src, bucket, dest)