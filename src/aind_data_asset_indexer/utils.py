"""Package for common methods used such as interfacing with S3."""
import json
from typing import Dict, Iterator, List, Optional

from aind_data_schema.core.metadata import Metadata
from aind_data_schema.utils.json_writer import SchemaWriter
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from pydantic import ValidationError

core_schema_file_names = [s.default_filename() for s in SchemaWriter.get_schemas()]


def upload_metadata_json_str_to_s3(
    bucket: str, metadata_json: str, prefix: str, s3_client: S3Client
):
    object_key = f"{prefix}{Metadata.default_filename()}"
    contents = json.dumps(
        json.loads(metadata_json), indent=3, ensure_ascii=False
    ).encode("utf-8")
    response = s3_client.put_object(
        Bucket=bucket, Key=object_key, Body=contents
    )
    return response


def does_file_exist(s3_client: S3Client, bucket: str, key: str) -> bool:
    """
    Check that a file exists inside a bucket. Uses the head_object operation,
    which is cheaper compared to the list_objects operation.
    Parameters
    ----------
    s3_client : S3Client
      The opening and closing of the s3_client is handled outside this method.
    bucket : str
    key : str
      For example, behavior_655019_2020-10-10_01-00-23/subject.json

    Returns
    -------
    bool
      True if the file exists, otherwise False.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise e


def get_dict_of_file_info(
    s3_client: S3Client, bucket: str, keys: List[str]
) -> Dict[str, Optional[dict]]:
    """
    For a list of object keys, returns a list of metadata info for each object
    that exists in the bucket.
    Parameters
    ----------
    s3_client : S3Client
      The opening and closing of the s3_client is handled outside this method.
    bucket : str
    keys : List[str]

    Returns
    -------
    Dict[str, Optional[dict]]
      Shape of dict is
      {"last_modified": datetime, "e_tag": str, "version_id": str}

    """
    responses = dict()
    for key in keys:
        try:
            response = s3_client.head_object(Bucket=bucket, Key=key)
            last_modified = response.get("LastModified")
            e_tag = response.get("ETag")
            version_id = response.get("VersionId")
            responses[key] = {
                "last_modified": last_modified,
                "e_tag": e_tag,
                "version_id": version_id,
            }
        except ClientError as e:
            if e.response["Error"]["Code"] != "404":
                raise e
            else:
                responses[key] = None
    return responses


def iterate_through_top_level(
    s3_client: S3Client, bucket: str
) -> Iterator[List[str]]:
    """
    Returns an iterator of s3 responses. If prefix is None, then will return
    an iterator of top-level prefixes of a bucket. Otherwise, will return an
    iterator of the top level items under a prefix.
    Parameters
    ----------
    s3_client : S3Client
      The opening and closing of the s3_client is handled outside this method.
    bucket : str

    Returns
    -------
    Iterator[List[str]]
      Returns an iterator. Each object in the iterator is a list of up to 1000
      prefixes in a bucket.

    """
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(
        Bucket=bucket,
        Delimiter="/",
    )
    for page in pages:
        yield [
            p.get("Prefix")
            for p in page["CommonPrefixes"]
            if p.get("Prefix") is not None
        ]


def is_dict_corrupt(input_dict: dict) -> bool:
    """Checks that all the keys don't contain '$' or '.'"""
    for key in input_dict.keys():
        if "$" in key or "." in key:
            return True
        elif isinstance(input_dict[key], dict):
            return is_dict_corrupt(input_dict[key])
    return False


def download_json_file_from_s3(
    s3_client: S3Client, bucket: str, object_key: str
) -> Optional[dict]:
    result = s3_client.get_object(Bucket=bucket, Key=object_key)
    try:
        content_string = result["Body"].read().decode()
        return json.loads(content_string)
    except json.decoder.JSONDecodeError:
        return None


def build_metadata_record_from_prefix(
    bucket: str,
    metadata_nd_overwrite: bool,
    prefix: str,
    s3_client: S3Client,
) -> Optional[str]:
    metadata_nd_file_key = prefix + Metadata.default_filename()
    if metadata_nd_overwrite or not does_file_exist(
        s3_client=s3_client, bucket=bucket, key=metadata_nd_file_key
    ):
        file_keys = [
            prefix + file_name
            for file_name in core_schema_file_names
            if file_name != Metadata.default_filename()
        ]
        s3_file_responses = get_dict_of_file_info(
            s3_client=s3_client, bucket=bucket, keys=file_keys
        )
        # Strip the trailing slash from the prefix
        metadata_dict = {
            "name": prefix[:-1],
            "location": f"s3://{bucket}/{prefix[:-1]}",
        }
        for object_key, response_data in s3_file_responses.items():
            if response_data is not None:
                field_name = object_key.split("/")[-1].replace(".json", "")
                json_contents = download_json_file_from_s3(
                    s3_client=s3_client, bucket=bucket, object_key=object_key
                )
                if json_contents is not None:
                    is_corrupt = is_dict_corrupt(input_dict=json_contents)
                    if not is_corrupt:
                        metadata_dict[field_name] = json_contents
        try:
            return Metadata.model_validate(metadata_dict).model_dump_json(
                warnings=False
            )
        except (ValidationError, AttributeError, ValueError):
            return Metadata.model_construct(**metadata_dict).model_dump_json(
                warnings=False
            )
    else:
        return None
