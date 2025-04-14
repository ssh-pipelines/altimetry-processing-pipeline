from io import TextIOWrapper
import json
import os
import s3fs
import boto3
from botocore.exceptions import ClientError


class AWSManager:
    """
    Class to support all AWS actions used throughout pipeline.
    """

    def __init__(self) -> None:
        self._access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        self._secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self._session_token = os.environ.get("AWS_SESSION_TOKEN")

        self.fs = s3fs.S3FileSystem(
            anon=False,
            key=self._access_key,
            secret=self._secret_key,
            token=self._session_token,
        )

        self._session = boto3.Session(
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            aws_session_token=self._session_token,
        )

        self._dynamodb = self._session.resource("dynamodb")
        self._msg_table = self._dynamodb.Table("nasa-ssh-pipeline")
        self._job_table = self._dynamodb.Table("nasa-ssh-pipeline-jobs")

    def key_exists(self, key: str) -> bool:
        return self.fs.exists(key)

    def stream_obj(self, src: str) -> TextIOWrapper:
        return self.fs.open(src)

    def download_obj(self, src: str, dst: str):
        self.fs.download(src, dst)

    def upload_obj(self, src: str, dest: str):
        self.fs.put(src, dest)

    def get_all_obj_meta(self, prefix) -> dict:
        return self.fs.glob(prefix, detail=True)

    def get_secret(self, secret_name: str) -> dict:
        """
        Retrieves secret from SecretsManager
        """
        sm_client = self._session.client(service_name="secretsmanager")
        try:
            secret_str = sm_client.get_secret_value(SecretId=secret_name)[
                "SecretString"
            ]
        except ClientError as e:
            raise e
        try:
            secret = json.loads(secret_str)
        except Exception as e:
            raise RuntimeError(f"Error converting secret string to dict: {e}")
        return secret


aws_manager = AWSManager()
