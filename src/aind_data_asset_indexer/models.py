"""Module to hold job settings models"""

import json
from typing import List, Optional

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
    copy_original_md_subdir: str = Field(
        default="original_metadata",
        description=(
            "Subdirectory to copy original core schema json files to."
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
    """Aind Index Bucket Job Settings"""

    doc_db_host: str
    doc_db_db_name: str
    doc_db_collection_name: str


class PopulateAindBucketsJobSettings(IndexJobSettings):
    """Job Settings to populate a list of aind managed buckets with
    metadata.nd.json files"""

    # Set individual bucket off
    s3_bucket: type(None) = None
    s3_buckets: List[str]


class AindIndexBucketsJobSettings(AindIndexBucketJobSettings):
    """Job Settings to sync docdb with list of aind managed buckets."""

    # Set individual bucket off
    s3_bucket: type(None) = None
    s3_buckets: List[str]


class CodeOceanIndexBucketJobSettings(IndexJobSettings):
    """Aind Index Bucket Job Settings"""

    doc_db_host: str
    doc_db_db_name: str
    doc_db_collection_name: str
    codeocean_domain: str
    codeocean_token: SecretStr

    @classmethod
    def from_param_store(cls, param_store_name: str):
        """
        Construct class from aws param store and secrets manager

        Parameters
        ----------
        param_store_name : str
        """
        param_store_client = boto3.client("ssm")
        response = param_store_client.get_parameter(
            Name=param_store_name, WithDecryption=True
        )
        param_store_client.close()
        parameters: str = response["Parameter"]["Value"]
        parameters_json: dict = json.loads(parameters)
        if "codeocean_secret_name" not in parameters_json.keys():
            raise ValueError("codeocean_secret_name not found in parameters.")
        secrets_client = boto3.client("secretsmanager")
        codeocean_secret = secrets_client.get_secret_value(
            SecretId=parameters_json["codeocean_secret_name"]
        )
        secrets_client.close()
        codeocean_secret_json: dict = json.loads(
            codeocean_secret["SecretString"]
        )
        codeocean_domain = codeocean_secret_json["domain"]
        codeocean_token = codeocean_secret_json["token"]
        parameters_json["codeocean_domain"] = codeocean_domain
        parameters_json["codeocean_token"] = codeocean_token
        del parameters_json["codeocean_secret_name"]
        return cls.model_validate_json(json.dumps(parameters_json))
