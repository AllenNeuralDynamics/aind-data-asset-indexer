from datetime import datetime
from typing import Any, Dict, List, Optional

from mypy_boto3_s3 import S3Client
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings


class BucketIndexJobConfigs(BaseSettings):
    docdb_host: str
    docdb_port: int
    docdb_username: str
    docdb_password: SecretStr
    docdb_conn_options: Dict[str, Any] = {}
    docdb_name: str
    docdb_collection_name: str
    s3_bucket: str
    n_partitions: int = Field(
        default=20, description="Number of partitions to use for dask job."
    )
    lookback_days: Optional[int] = Field(
        default=None,
        description=(
            "Records from S3 and DocDB will be filtered by this date time. If "
            "set to None, then all records will be processed."
        ),
    )


class CodeOceanBucketIndexJobConfigs(BucketIndexJobConfigs):
    codeocean_host: str
    codeocean_api_token: SecretStr
