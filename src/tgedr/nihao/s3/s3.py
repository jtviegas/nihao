import logging
import os
import boto3

logger = logging.getLogger(__name__)


class S3Connector:
    def __init__(self):
        self.__resource = None
        self.__session = None
        self.__client = None

    @property
    def _session(self):
        if self.__session is None:
            self.__session = boto3.Session(
                aws_access_key_id=os.environ["AWS_KEY_ID"],
                aws_secret_access_key=os.environ["AWS_SECRET_KEY"],
                region_name=os.environ["AWS_REGION"],
            )
        return self.__session

    @property
    def _resource(self):
        if self.__resource is None:
            self.__resource = self._session.resource("s3")
        return self.__resource

    @property
    def _client(self):
        if self.__client is None:
            self.__client = self._session.client("s3")
        return self.__client
