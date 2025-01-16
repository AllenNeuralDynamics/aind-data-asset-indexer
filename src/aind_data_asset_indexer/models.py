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
    doc_db_port: int
    doc_db_user_name: str
    doc_db_password: SecretStr
    doc_db_db_name: str
    doc_db_collection_name: str

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
        if "doc_db_secret_name" not in parameters_json.keys():
            raise ValueError("doc_db_secret_name not found in parameters.")
        secrets_client = boto3.client("secretsmanager")
        docdb_secret = secrets_client.get_secret_value(
            SecretId=parameters_json["doc_db_secret_name"]
        )
        secrets_client.close()
        docdb_secret_json: dict = json.loads(docdb_secret["SecretString"])
        del parameters_json["doc_db_secret_name"]
        secret_to_job_settings_map = {
            "host": "doc_db_host",
            "port": "doc_db_port",
            "username": "doc_db_user_name",
            "password": "doc_db_password",
        }

        for secret_key, job_setting in secret_to_job_settings_map.items():
            if secret_key not in docdb_secret_json.keys():
                raise ValueError(f"{secret_key} not found in docdb secret.")
            parameters_json[job_setting] = docdb_secret_json[secret_key]
        return cls.model_validate_json(json.dumps(parameters_json))


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
    doc_db_port: int
    doc_db_user_name: str
    doc_db_password: SecretStr
    doc_db_db_name: str
    doc_db_collection_name: str
    codeocean_domain: str
    codeocean_token: SecretStr
    temp_codeocean_endpoint: Optional[str] = Field(
        default=None,
        description=(
            "(deprecated) Temp proxy to access code ocean information from "
            "their analytics databases. Will be removed in a future release."
        ),
    )

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
        if "doc_db_secret_name" not in parameters_json.keys():
            raise ValueError("doc_db_secret_name not found in parameters.")
        if "codeocean_secret_name" not in parameters_json.keys():
            raise ValueError("codeocean_secret_name not found in parameters.")
        secrets_client = boto3.client("secretsmanager")
        docdb_secret = secrets_client.get_secret_value(
            SecretId=parameters_json["doc_db_secret_name"]
        )
        codeocean_secret = secrets_client.get_secret_value(
            SecretId=parameters_json["codeocean_secret_name"]
        )
        secrets_client.close()
        docdb_secret_json: dict = json.loads(docdb_secret["SecretString"])
        codeocean_secret_json: dict = json.loads(
            codeocean_secret["SecretString"]
        )
        codeocean_domain = codeocean_secret_json["domain"]
        codeocean_token = codeocean_secret_json["token"]
        parameters_json["codeocean_domain"] = codeocean_domain
        parameters_json["codeocean_token"] = codeocean_token
        del parameters_json["doc_db_secret_name"]
        del parameters_json["codeocean_secret_name"]
        secret_to_job_settings_map = {
            "host": "doc_db_host",
            "port": "doc_db_port",
            "username": "doc_db_user_name",
            "password": "doc_db_password",
        }

        for secret_key, job_setting in secret_to_job_settings_map.items():
            if secret_key not in docdb_secret_json.keys():
                raise ValueError(f"{secret_key} not found in docdb secret.")
            parameters_json[job_setting] = docdb_secret_json[secret_key]
        return cls.model_validate_json(json.dumps(parameters_json))
