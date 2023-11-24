import io
import logging
from typing import Any, Dict, Optional
import pandas as pd
from tgedr.nihao.s3.s3_connector import S3Connector
from tgedr.nihao.source.source import Source

logger = logging.getLogger(__name__)


class PdDataFrameFromParquetSource(S3Connector, Source):
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        logger.info(f"[__init__|in] ({config})")
        S3Connector.__init__(self)
        Source.__init__(self, config)
        logger.info("[__init__|out]")

    def __read_parquet(self, key: str, bucket: str):
        logger.info(f"[__read_parquet|in] ({key}, {bucket})")
        obj = self._client.get_object(Bucket=bucket, Key=key)
        result = pd.read_parquet(io.BytesIO(obj["Body"].read()))
        logger.info(f"[__read_parquet|out] => {result}")
        return result

    def get(self, key: str, **kwargs) -> pd.DataFrame:
        logger.info(f"[get|in] ({key})")
        result: pd.DataFrame = None
        bucket_name = key.split("://")[1].split("/")[0]
        file_path = key.replace("s3://" + bucket_name + "/", "")
        bucket = self._resource.Bucket(bucket_name)

        bucket_paths = [item.key for item in bucket.objects.filter(Prefix=file_path) if item.key.endswith(".parquet")]

        dfs = [self.__read_parquet(path, bucket=bucket_name) for path in bucket_paths]
        result = pd.concat(dfs, ignore_index=True)
        logger.info(f"[get|out] => {result}")
        return result
