"""Module to hold job settings models"""

from typing import Optional

import boto3
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings


class IndexJobSettings(BaseSettings):
    """Basic Index Job Settings"""

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
    metadata_nd_overwrite: bool = Field(
        default=False,
        description=(
            "If set to True, will ignore the metadata.nd.json file and use "
            "the core schemas to build a new one. If set to False, then use"
            "the metadata.nd.json file if it exists in S3."
        ),
    )

    @classmethod
    def from_param_store(cls, param_store_name: str):
        """
        Construct class from aws param store
        Parameters
        ----------
        param_store_name : str
        """
        param_store_client = boto3.client("ssm")
        response = param_store_client.get_parameter(
            Name=param_store_name, WithDecryption=True
        )
        param_store_client.close()
        parameters = response["Parameter"]["Value"]
        return cls.model_validate_json(parameters)


class AindIndexBucketJobSettings(IndexJobSettings):
    doc_db_host: str
    doc_db_port: int
    doc_db_user_name: str
    doc_db_password: SecretStr
    doc_db_db_name: str
    doc_db_collection_name: str
