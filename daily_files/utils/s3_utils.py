import boto3
import json
from botocore.exceptions import ClientError
import os


def upload_s3(src: str, bucket: str, dest:str):
    session = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                            aws_session_token=os.environ['AWS_SESSION_TOKEN'])
    s3 = session.client('s3')
    s3.upload_file(src, bucket, dest)
    
def get_secret(secret_name: str) -> dict:
    session = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                            aws_session_token=os.environ['AWS_SESSION_TOKEN'])
    sm = session.client(service_name='secretsmanager')
    
    try:
        secret_str = sm.get_secret_value(SecretId=secret_name)['SecretString']
    except ClientError as e:
        raise e
    
    try:
        secret = json.loads(secret_str)
    except:
        raise RuntimeError('Error converting secret string to dict')
    
    return secret


def put_secret(secret_name: str, secret_string: str):
    session = boto3.Session(aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
                            aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
                            aws_session_token=os.environ['AWS_SESSION_TOKEN'])
    sm = session.client(service_name='secretsmanager')
    
    try:
        sm.put_secret_value(SecretId=secret_name, SecretString=secret_string)
    except ClientError as e:
        raise e