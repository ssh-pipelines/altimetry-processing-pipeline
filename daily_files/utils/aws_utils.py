from io import TextIOWrapper
import logging
import boto3
import s3fs
import json
from botocore.exceptions import ClientError
import os


class AWSManager:
    
    def __init__(self) -> None:
        self._access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        self._secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self._session_token = os.environ.get('AWS_SESSION_TOKEN')
        
        try:
            if not all([self._access_key, self._secret_key, self._session_token]):
                raise RuntimeWarning('Valid AWS credentials not found.')
        except RuntimeWarning as e:
            logging.warning(e)

        try:
            self._session = boto3.Session(aws_access_key_id=self._access_key,
                                        aws_secret_access_key=self._secret_key,
                                        aws_session_token=self._session_token)
        except Exception as e:
            logging.error(e)
            
        self.fs = s3fs.S3FileSystem(anon=False,
                                    key=self._access_key,
                                    secret=self._secret_key,
                                    token=self._session_token)
            
    def stream_s3(self, src: str) -> TextIOWrapper:
        return self.fs.open(src)
        
    
    def upload_s3(self, src: str, bucket: str, dest:str):
        s3_client = self._session.client('s3')
        logging.info('Uploading daily file to S3')
        s3_client.upload_file(src, bucket, dest)
        
    def get_secret(self, secret_name: str) -> dict:
        sm_client = self._session.client(service_name='secretsmanager')
        try:
            secret_str = sm_client.get_secret_value(SecretId=secret_name)['SecretString']
        except ClientError as e:
            raise e
        try:
            secret = json.loads(secret_str)
        except:
            raise RuntimeError('Error converting secret string to dict')
        return secret
    
    def put_secret(self, secret_name: str, secret_string: str):
        sm_client = self._session.client(service_name='secretsmanager')
        try:
            sm_client.put_secret_value(SecretId=secret_name, SecretString=secret_string)
        except ClientError as e:
            raise e

aws_manager = AWSManager()