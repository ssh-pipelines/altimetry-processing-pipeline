from io import TextIOWrapper
import logging
from typing import Iterable
import boto3
import os
import s3fs

class AWSManager:
    DAILY_FILE_BUCKET = "example-bucket"
    
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
            self.fs = s3fs.S3FileSystem(anon=False,
                                    key=self._access_key,
                                    secret=self._secret_key,
                                    token=self._session_token)
        except Exception as e:
            logging.error(e)
            
        self.s3_client = self._session.client('s3')
            
    def get_filepaths(self, prefix: str) -> Iterable[str]:
        objs = self.s3_client.list_objects_v2(Bucket=self.DAILY_FILE_BUCKET, Prefix=prefix)
        return [obj['Key'] for obj in objs.get('Contents', []) if 'Key' in obj and obj['Key'].endswith('.nc')]
            
    def stream_s3(self, src: str) -> TextIOWrapper:
        if self.fs.exists(os.path.join(self.DAILY_FILE_BUCKET, src)):
            return self.fs.open(os.path.join(self.DAILY_FILE_BUCKET, src))
        raise RuntimeError(f'{src} does not exist!')
    
    def upload_s3(self, src: str, bucket: str, dest:str):
        s3_client = self.s3_client
        logging.info(f'Uploading {src} to {dest}')
        s3_client.upload_file(src, bucket, dest)

aws_manager = AWSManager()