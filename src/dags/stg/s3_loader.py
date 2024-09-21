import logging
from airflow.hooks.base import BaseHook
import boto3
from typing import List, Any

log = logging.getLogger(__name__)
ENDPOINT = "https://storage.yandexcloud.net"


class S3loader(BaseHook):

    def __init__(self, conn_id: str) -> None:
        super().__init__()
        self.conn_id = conn_id

    def _get_aws_access_key_id(self, conn) -> str:
        return conn.extra_dejson.get('aws_access_key_id')

    def _aws_secret_access_key(self, conn) -> str:
        return conn.extra_dejson.get('aws_secret_access_key')

    def get_key(self, bucket_name: str) -> List:
        keys = []
        session = boto3.session.Session()
        conn = self.get_connection(self.conn_id)
        aws_access_key_id = self._get_aws_access_key_id(conn)
        aws_secret_access_key = self._aws_secret_access_key(conn)

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="ru-central1",
        )

        s3 = session.client("s3", endpoint_url=ENDPOINT)
        for key in s3.list_objects(Bucket=bucket_name)['Contents']:
            keys.append(key["Key"])
        return keys

    def download_from_s3(self, bucket_name: str, key: str, local_path: str) -> Any:
        session = boto3.session.Session()
        conn = self.get_connection(self.conn_id)
        aws_access_key_id = self._get_aws_access_key_id(conn)
        aws_secret_access_key = self._aws_secret_access_key(conn)

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="ru-central1",
        )

        s3 = session.client("s3", endpoint_url=ENDPOINT)
        file_name = f'{local_path}/{key}'
        s3.download_file(Bucket=bucket_name,
                         Key=key,
                         Filename=file_name)
        return file_name


