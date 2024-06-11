from io import TextIOWrapper
import logging
from typing import Iterable
import boto3
import s3fs
from s3fs import S3FileSystem
import os


class S3Utils():
    def __init__(self):
        self.bucket = 'example-bucket'
        self.daily_file_pre = 'daily_files'
        self.simple_grid_pre = 'simple_grids'
        
        self._access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        self._secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
        self._session_token = os.environ.get('AWS_SESSION_TOKEN')
        
        if all([self._access_key, self._secret_key, self._session_token]):
            self._session = boto3.Session(aws_access_key_id=self._access_key,
                                        aws_secret_access_key=self._secret_key,
                                        aws_session_token=self._session_token)
            self.fs = s3fs.S3FileSystem(anon=False,
                                    key=self._access_key,
                                    secret=self._secret_key,
                                    token=self._session_token)
        else:
            self._session = boto3.Session(profile_name='s6')
            credentials = self._session.get_credentials()
            self.fs = s3fs.S3FileSystem(anon=False,
                                    key=credentials.access_key,
                                    secret=credentials.secret_key,
                                    token=credentials.token)
    
    def key_exists(self, key: str) -> bool:
        return self.fs.exists(key)
    
    def stream_s3(self, src: str) -> TextIOWrapper:
        return self.fs.open(src)

    def upload_s3(self, src: str, dest:str):
        self.fs.put(src, dest)