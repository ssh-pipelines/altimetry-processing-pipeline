import boto3
import os


def upload_s3(src: str, bucket: str, dest:str):
    session = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                            aws_session_token=os.environ['AWS_SESSION_TOKEN'])
    s3 = session.client('s3')
    s3.upload_file(src, bucket, dest)