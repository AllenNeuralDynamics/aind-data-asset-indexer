"""Package for common methods used such as interfacing with S3 and DocDB."""

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from json.decoder import JSONDecodeError
from typing import Dict, Iterator, List, Optional
from urllib.parse import urlparse

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_schema.core.data_description import DataLevel, DataRegex
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.utils.json_writer import SchemaWriter
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import (
    PaginatorConfigTypeDef,
    PutObjectOutputTypeDef,
)
from pymongo import MongoClient

metadata_filename = Metadata.default_filename()

# TODO: This would be better if it was available in aind-data-schema
core_schema_file_names = [
    s.default_filename()
    for s in SchemaWriter.get_schemas()
    if s.default_filename() != metadata_filename
]


def _log_message(
    message: str, log_level: int = logging.INFO, log_flag: bool = True
) -> None:
    """
    Log a message using the given log level. If log_flag is False,
    then it will not log anything.

    Parameters
    ----------
    message : str
    log_level : int
        Default is logging.INFO
    log_flag : bool
        Default is True

    Returns
    -------
    None
    """
    if not log_flag:
        return
    if log_level not in [
        logging.DEBUG,
        logging.INFO,
        logging.WARNING,
        logging.ERROR,
        logging.CRITICAL,
    ]:
        raise ValueError("Invalid log level")
    logging.log(log_level, message)


def create_object_key(prefix: str, filename: str) -> str:
    """
    For a given s3 prefix and filename, create the expected
    object key for the file.
    Parameters
    ----------
    prefix : str
      For example, ecephys_123456_2020-10-10_01-02-03
    filename : str
      For example, 'metadata.nd.json'

    Returns
    -------
    str
      For example,
      ecephys_123456_2020-10-10_01-02-03/metadata.nd.json

    """
    stripped_prefix = prefix.strip("/")
    return f"{stripped_prefix}/{filename}"


def create_metadata_object_key(prefix: str) -> str:
    """
    For a given s3 prefix, create the expected object key for the
    metadata.nd.json file.
    Parameters
    ----------
    prefix : str
      For example, ecephys_123456_2020-10-10_01-02-03

    Returns
    -------
    str
      For example, ecephys_123456_2020-10-10_01-02-03/metadata.nd.json

    """
    return create_object_key(prefix=prefix, filename=metadata_filename)


def is_prefix_valid(prefix: str) -> bool:
    """
    Check if a given prefix is valid. A valid prefix conforms to a regex
    pattern defined in aind-data-schema.

    Parameters
    ----------
    prefix : str
        For example, 'ecephys_123456_2020-10-10_01-02-03'

    Returns
    -------
    bool
        True if the prefix is valid, otherwise False.
    """
    return re.match(DataRegex.DATA.value, prefix.strip("/")) is not None


def is_record_location_valid(
    record: dict, expected_bucket: str, expected_prefix: Optional[str] = None
) -> bool:
    """
    Check if a given record has a valid location url.
    Parameters
    ----------
    record : dict
      Metadata record as a dictionary
    expected_bucket : str
      The expected s3 bucket the location should have.
    expected_prefix: Optional[str]
      If provided, also check that the record location matches the expected
      s3_prefix. Default is None, which won't perform the check.

    Returns
    -------
    bool
      True if there is a location field and the url in the field has a form
      like 's3://{expected_bucket}/prefix'
      Will return False if there is no s3 scheme, the bucket does not match
      the expected bucket, the prefix contains forward slashes, or the prefix
      is invalid, or doesn't match the record name or expected prefix.

    """
    expected_stripped_prefix = (
        None if expected_prefix is None else expected_prefix.strip("/")
    )
    if record.get("location") is None:
        return False
    else:
        parts = urlparse(record.get("location"), allow_fragments=False)
        if parts.scheme != "s3":
            return False
        elif parts.netloc != expected_bucket:
            return False
        else:
            stripped_prefix = parts.path.strip("/")
            if (
                stripped_prefix == ""
                or len(stripped_prefix.split("/")) > 1
                or not is_prefix_valid(stripped_prefix)
                or record.get("name") != stripped_prefix
                or (
                    expected_prefix is not None
                    and stripped_prefix != expected_stripped_prefix
                )
            ):
                return False
            else:
                return True


def get_s3_bucket_and_prefix(s3_location: str) -> Dict[str, str]:
    """
    For a location url like s3://bucket/prefix, it will return the bucket
    and prefix. It doesn't check the scheme is s3. It will strip the leading
    and trailing forward slashes from the prefix.
    Parameters
    ----------
    s3_location : str
      For example, 's3://some_bucket/some_prefix'

    Returns
    -------
    Dict[str, str]
      For example, {'bucket': 'some_bucket', 'prefix': 'some_prefix'}

    """
    parts = urlparse(s3_location, allow_fragments=False)
    stripped_prefix = parts.path.strip("/")
    return {"bucket": parts.netloc, "prefix": stripped_prefix}


def get_s3_location(bucket: str, prefix: str) -> str:
    """
    For a given bucket and prefix, return a location url in format
    s3://{bucket}/{prefix}
    Parameters
    ----------
    bucket : str
    prefix : str

    Returns
    -------
    str
      For example, 's3://some_bucket/some_prefix'

    """
    stripped_prefix = prefix.strip("/")
    return f"s3://{bucket}/{stripped_prefix}"


def compute_md5_hash(json_contents: str) -> str:
    """
    Computes the md5 hash of the object as it would be stored in S3. Useful
    for comparing against the S3 object e-tag to check if they are the same.
    Parameters
    ----------
    json_contents : str
      JSON string representation of an object.

    Returns
    -------
    str
      The md5 hash of the object as it would be uploaded to S3.

    """
    contents = json.dumps(
        json.loads(json_contents), indent=3, ensure_ascii=False, sort_keys=True
    ).encode("utf-8")
    return hashlib.md5(contents).hexdigest()


def upload_json_str_to_s3(
    bucket: str, object_key: str, json_str: str, s3_client: S3Client
) -> PutObjectOutputTypeDef:
    """
    Upload JSON string contents to a location in S3.
    Parameters
    ----------
    bucket : str
        For example, 'aind-open-data'
    object_key : str
        For example, 'prefix/original_metadata/subject.json'
    json_str : str
        JSON string to upload as JSON file.
    s3_client : S3Client

    Returns
    -------
    PutObjectOutputTypeDef
      Response of the put object operation.

    """
    contents = json.dumps(
        json.loads(json_str),
        indent=3,
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    response = s3_client.put_object(
        Bucket=bucket, Key=object_key, Body=contents
    )
    return response


def upload_metadata_json_str_to_s3(
    bucket: str, metadata_json: str, prefix: str, s3_client: S3Client
) -> PutObjectOutputTypeDef:
    """
    Upload JSON string representation of the contents of the metadata.nd.json
    file to a location in S3.
    Parameters
    ----------
    bucket : str
    metadata_json : str
    prefix : str
    s3_client : S3Client

    Returns
    -------
    PutObjectOutputTypeDef
      Response of the put object operation.

    """
    object_key = create_metadata_object_key(prefix)
    response = upload_json_str_to_s3(
        bucket=bucket,
        object_key=object_key,
        json_str=metadata_json,
        s3_client=s3_client,
    )
    return response


def does_s3_object_exist(s3_client: S3Client, bucket: str, key: str) -> bool:
    """
    Check that a file exists inside a bucket. Uses the head_object operation,
    which is cheaper compared to the list_objects operation.
    Parameters
    ----------
    s3_client : S3Client
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


def does_s3_metadata_copy_exist(
    s3_client: S3Client, bucket: str, prefix: str, copy_subdir: str
):
    """
    For a given bucket and prefix, check if there are any original core schema
    jsons in the copy_subdir. Uses the list_objects operation.

    Parameters
    ----------
    s3_client : S3Client
    bucket : str
    prefix : str
      For example, ecephys_123456_2020-10-10_01-02-03
    copy_subdir : str
      For example, original_metadata

    Returns
    -------
    bool
      True if any of the core schema jsons exists in the copy_subdir,
      otherwise False.
    """
    # Use trailing slash and delimiter to get top-level objects in copy_subdir
    copy_prefix = create_object_key(prefix, copy_subdir.strip("/") + "/")
    response = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=copy_prefix, Delimiter="/"
    )
    if "Contents" in response:
        core_schemas = [s.replace(".json", "") for s in core_schema_file_names]
        pattern = re.escape(copy_prefix) + r"([a-zA-Z0-9_]+)\.\d{8}\.json$"
        for obj in response["Contents"]:
            m = re.match(pattern, obj["Key"])
            if m is not None and m.group(1) in core_schemas:
                return True
    return False


def list_metadata_copies(
    s3_client: S3Client,
    bucket: str,
    prefix: str,
    copy_subdir: str,
) -> List[str]:
    """
    For a given bucket and prefix, return a list of the core schemas in the
    copy_subdir.

    Parameters
    ----------
    s3_client : S3Client
    bucket : str
    prefix : str
      For example, ecephys_123456_2020-10-10_01-02-03
    copy_subdir : str
      For example, original_metadata

    Returns
    -------
    List[str]
      A list of the core schemas in the copy_subdir without timestamp, e..g,
      ["subject.json", "procedures.json", "processing.json"]
    """
    # Use trailing slash and delimiter to get top-level objects in copy_subdir
    copy_prefix = create_object_key(prefix, copy_subdir.strip("/") + "/")
    response = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=copy_prefix, Delimiter="/"
    )
    files = []
    if "Contents" in response:
        core_schemas = [s.replace(".json", "") for s in core_schema_file_names]
        pattern = re.escape(copy_prefix) + r"([a-zA-Z0-9_]+)\.\d{8}\.json$"
        for obj in response["Contents"]:
            m = re.match(pattern, obj["Key"])
            if m is not None and m.group(1) in core_schemas:
                files.append(f"{m.group(1)}.json")
    return files


def get_dict_of_file_info(
    s3_client: S3Client, bucket: str, keys: List[str]
) -> Dict[str, Optional[dict]]:
    """
    For a list of object keys, returns a list of metadata info for each object
    that exists in the bucket.
    Parameters
    ----------
    s3_client : S3Client
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


def get_dict_of_core_schema_file_info(
    s3_client: S3Client, bucket: str, prefix: str
) -> Dict[str, Optional[dict]]:
    """
    For a bucket and prefix get list of core schema file info.
    Parameters
    ----------
    s3_client : S3Client
    bucket : str
    prefix : str

    Returns
    -------
    Dict[str, Optional[dict]]
      {"subject.json":
         {"last_modified": datetime, "e_tag": str, "version_id": str},
       "procedures.json":
         {"last_modified": datetime, "e_tag": str, "version_id": str},
       ...
      }
    """
    key_map = dict(
        [
            (create_object_key(prefix=prefix, filename=s), s)
            for s in core_schema_file_names
        ]
    )
    file_info = get_dict_of_file_info(
        s3_client=s3_client, bucket=bucket, keys=list(key_map.keys())
    )
    remapped_info = dict([(key_map[k], v) for (k, v) in file_info.items()])

    return remapped_info


def iterate_through_top_level(
    s3_client: S3Client, bucket: str, max_pages: Optional[int] = None
) -> Iterator[List[str]]:
    """
    Returns an iterator of s3 responses. If prefix is None, then will return
    an iterator of top-level prefixes of a bucket. Otherwise, will return an
    iterator of the top level items under a prefix.
    Parameters
    ----------
    s3_client : S3Client
    bucket : str
    max_pages : Optional[int]
      Number of pages to return. None returns maximum number allowed.

    Returns
    -------
    Iterator[List[str]]
      Returns an iterator. Each object in the iterator is a list of up to 1000
      prefixes in a bucket.

    """
    paginator = s3_client.get_paginator("list_objects_v2")
    optional_page_config = PaginatorConfigTypeDef(
        PageSize=max_pages, MaxItems=None, StartingToken=None
    )
    pages = paginator.paginate(
        Bucket=bucket, Delimiter="/", PaginationConfig=optional_page_config
    )
    for page in pages:
        yield [
            p.get("Prefix")
            for p in page["CommonPrefixes"]
            if p.get("Prefix") is not None
        ]


def is_dict_corrupt(input_dict: dict) -> bool:
    """
    Checks that all the keys, included nested keys, don't contain '$' or '.'
    Parameters
    ----------
    input_dict : dict

    Returns
    -------
    bool
      True if input_dict is not a dict, or if nested dictionary keys contain
      forbidden characters. False otherwise.

    """
    if not isinstance(input_dict, dict):
        return True
    for key, value in input_dict.items():
        if "$" in key or "." in key:
            return True
        elif isinstance(value, dict):
            if is_dict_corrupt(value):
                return True
    return False


def download_json_file_from_s3(
    s3_client: S3Client, bucket: str, object_key: str
) -> Optional[dict]:
    """
    Downloads json file contents from S3. Will return None if object is not
    a valid json file.
    Parameters
    ----------
    s3_client : S3Client
    bucket : str
    object_key : str

    Returns
    -------
    dict | None

    """
    result = s3_client.get_object(Bucket=bucket, Key=object_key)
    try:
        content_string = result["Body"].read().decode(encoding="utf-8")
        return json.loads(content_string)
    except JSONDecodeError:
        return None


def build_metadata_record_from_prefix(
    bucket: str,
    prefix: str,
    s3_client: S3Client,
    optional_name: Optional[str] = None,
    optional_created: Optional[datetime] = None,
    optional_external_links: Optional[List[dict]] = None,
) -> Optional[str]:
    """
    For a given bucket and prefix, this method will return a JSON string
    representation of a Metadata record. The Metadata record will be
    constructed from any non-corrupt core schema json files found under the
    prefix. If there are issues with Metadata construction, then it will
    return None.
    Parameters
    ----------
    bucket : str
    prefix : str
    s3_client : S3Client
    optional_name : Optional[str]
      If optional_name is None, then a name will be constructed from the
      s3_prefix. Default is None.
    optional_created: Optional[datetime]
      User can override created datetime. Default is None.
    optional_external_links: Optional[List[dict]]
      User can provide external_links. Default is None.

    Returns
    -------
    Optional[str]
      The constructed Metadata record as a json string. Will return None if
      there are issues with Metadata construction.

    """
    file_keys = [
        create_object_key(prefix=prefix, filename=file_name)
        for file_name in core_schema_file_names
    ]
    s3_file_responses = get_dict_of_file_info(
        s3_client=s3_client, bucket=bucket, keys=file_keys
    )
    record_name = prefix.strip("/") if optional_name is None else optional_name
    try:
        metadata_dict = {
            "name": record_name,
            "location": get_s3_location(bucket=bucket, prefix=prefix),
        }
        if optional_created is not None:
            metadata_dict["created"] = optional_created
        if optional_external_links is not None:
            metadata_dict["external_links"] = optional_external_links
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
        # TODO: We should handle constructing the Metadata file in a better way
        #  in aind-data-schema. By using model_validate, a lot of info from the
        #  original files get removed. For now, we can use model_construct
        #  until a better method is implemented in aind-data-schema. This will
        #  mark all the initial files as metadata_status=Unknown
        metadata_dict = Metadata.model_construct(
            **metadata_dict
        ).model_dump_json(warnings=False, by_alias=True)
    except Exception:
        metadata_dict = None
    return metadata_dict


def cond_copy_then_sync_core_json_files(
    metadata_json: str,
    bucket: str,
    prefix: str,
    s3_client: S3Client,
    copy_original_md_subdir: str = "original_metadata",
    log_flag: bool = False,
) -> None:
    """
    For a given bucket and prefix
    1) Copy the core schema files if a copy does not already exist.
    2) Sync the core schema files with core fields from the metadata record.
    If the original core schema json was corrupt, then it will be deleted after
    its original contents are copied.

    Parameters
    ----------
    metadata_json : str
        The JSON string representation of the Metadata record.
    bucket : str
        The name of the S3 bucket.
    prefix : str
        The prefix for the S3 object keys.
    s3_client : S3Client
        The S3 client object.
    log_flag: bool
        Flag indicating whether to log operations. Default is False.
    copy_original_md_subdir : str
        Subdirectory to copy original core schema json files to.
        Default is 'original_metadata'.

    Returns
    -------
    None

    """
    if does_s3_metadata_copy_exist(
        s3_client=s3_client,
        bucket=bucket,
        prefix=prefix,
        copy_subdir=copy_original_md_subdir,
    ):
        _log_message(
            message=(
                "Copy of original metadata already exists at "
                f"s3://{bucket}/{prefix}/{copy_original_md_subdir}"
            ),
            log_flag=log_flag,
        )
    else:
        copy_core_json_files(
            bucket=bucket,
            prefix=prefix,
            s3_client=s3_client,
            copy_original_md_subdir=copy_original_md_subdir,
            log_flag=log_flag,
        )
    sync_core_json_files(
        metadata_json=metadata_json,
        bucket=bucket,
        prefix=prefix,
        s3_client=s3_client,
        log_flag=log_flag,
    )


def copy_core_json_files(
    bucket: str,
    prefix: str,
    s3_client: S3Client,
    copy_original_md_subdir: str,
    log_flag: bool = False,
) -> None:
    """
    For a given bucket and prefix, copy the core schema files to a
    sub-directory.

    Parameters
    ----------
    bucket : str
        The name of the S3 bucket.
    prefix : str
        The prefix for the S3 object keys.
    s3_client : S3Client
        The S3 client object.
    log_flag: bool
        Flag indicating whether to log operations. Default is False.
    copy_original_md_subdir : str
        Subdirectory to copy original core schema json files to.
        For example, 'original_metadata'.

    Returns
    -------
    None

    """
    tgt_copy_subdir = copy_original_md_subdir.strip("/")
    tgt_copy_prefix = create_object_key(prefix, tgt_copy_subdir)
    core_files_keys = [
        create_object_key(prefix=prefix, filename=s)
        for s in core_schema_file_names
    ]
    core_files_infos = get_dict_of_file_info(
        s3_client=s3_client, bucket=bucket, keys=core_files_keys
    )
    for file_name in core_schema_file_names:
        source = create_object_key(prefix, file_name)
        source_location = get_s3_location(bucket=bucket, prefix=source)
        source_file_info = core_files_infos[source]
        if source_file_info is not None:
            date_stamp = source_file_info["last_modified"].strftime("%Y%m%d")
            target = create_object_key(
                prefix=tgt_copy_prefix,
                filename=file_name.replace(".json", f".{date_stamp}.json"),
            )
            # Copy original core json files to /original_metadata
            _log_message(
                message=f"Copying {source} to {target} in s3://{bucket}",
                log_flag=log_flag,
            )
            response = s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": source},
                Key=target,
            )
            _log_message(message=response, log_flag=log_flag)
        else:
            _log_message(
                message=(
                    f"Source file {source_location} does not exist. "
                    f"Skipping copy."
                ),
                log_flag=log_flag,
            )


def sync_core_json_files(
    metadata_json: str,
    bucket: str,
    prefix: str,
    s3_client: S3Client,
    log_flag: bool = False,
) -> None:
    """
    Sync the core schema files with the core fields from metadata.nd.json.
    Core schema jsons are only updated if their contents are outdated.
    Core schema jsons are created if they don't already exist.
    If a core field is None in metadata.nd.json but the core schema json
    exists in s3, then the core schema json will be deleted.

    Parameters
    ----------
    metadata_json : str
        The JSON string representation of the Metadata record.
    bucket : str
        The name of the S3 bucket.
    prefix : str
        The prefix for the S3 object keys.
    s3_client : S3Client
        The S3 client object.
    log_flag: bool
        Flag indicating whether to log operations. Default is False.

    Returns
    -------
    None
    """
    md_record_json = json.loads(metadata_json)
    core_files_keys = [
        create_object_key(prefix=prefix, filename=s)
        for s in core_schema_file_names
    ]
    core_files_infos = get_dict_of_file_info(
        s3_client=s3_client, bucket=bucket, keys=core_files_keys
    )
    for file_name in core_schema_file_names:
        object_key = create_object_key(prefix, file_name)
        field_name = file_name.replace(".json", "")
        location = get_s3_location(bucket=bucket, prefix=object_key)
        if (
            field_name in md_record_json
            and md_record_json[field_name] is not None
        ):
            field_contents = md_record_json[field_name]
            field_contents_str = json.dumps(field_contents)
            # Core schema jsons are created if they don't already exist.
            # Otherwise, they are only updated if their contents are outdated.
            if core_files_infos[object_key] is None:
                _log_message(
                    message=(f"Uploading new {field_name} to {location}"),
                    log_flag=log_flag,
                )
                response = upload_json_str_to_s3(
                    bucket=bucket,
                    object_key=object_key,
                    json_str=field_contents_str,
                    s3_client=s3_client,
                )
                _log_message(message=response, log_flag=log_flag)
            else:
                s3_object_hash = core_files_infos[object_key]["e_tag"].strip(
                    '"'
                )
                core_field_md5_hash = compute_md5_hash(field_contents_str)
                if core_field_md5_hash != s3_object_hash:
                    _log_message(
                        message=(
                            f"Uploading updated {field_name} to {location}"
                        ),
                        log_flag=log_flag,
                    )
                    response = upload_json_str_to_s3(
                        bucket=bucket,
                        object_key=object_key,
                        json_str=field_contents_str,
                        s3_client=s3_client,
                    )
                    _log_message(message=response, log_flag=log_flag)
                else:
                    _log_message(
                        message=(
                            f"{field_name} is up-to-date in {location}. "
                            f"Skipping."
                        ),
                        log_flag=log_flag,
                    )
        else:
            # If a core field is None but the core json exists,
            # delete the core json.
            if core_files_infos[object_key] is not None:
                _log_message(
                    message=(
                        f"{field_name} not found in metadata.nd.json for "
                        f"{prefix} but {location} exists! Deleting."
                    ),
                    log_flag=log_flag,
                )
                response = s3_client.delete_object(
                    Bucket=bucket, Key=object_key
                )
                _log_message(message=response, log_flag=log_flag)
            else:
                _log_message(
                    message=(
                        f"{field_name} not found in metadata.nd.json for "
                        f"{prefix} nor in {location}! Skipping."
                    ),
                    log_flag=log_flag,
                )


def does_metadata_record_exist_in_docdb(
    docdb_client: MongoClient,
    db_name: str,
    collection_name: str,
    bucket: str,
    prefix: str,
) -> bool:
    """
    For a given bucket and prefix, check if there is already a record in DocDb
    Parameters
    ----------
    docdb_client : MongoClient
    db_name : str
    collection_name : str
    bucket : str
    prefix : str

    Returns
    -------
    True if there is a record in DocDb. Otherwise, False.

    """
    location = get_s3_location(bucket=bucket, prefix=prefix)
    db = docdb_client[db_name]
    collection = db[collection_name]
    records = list(
        collection.find(
            filter={"location": location}, projection={"_id": 1}, limit=1
        )
    )
    if len(records) == 0:
        return False
    else:
        return True


def get_record_from_docdb(
    docdb_client: MongoClient,
    db_name: str,
    collection_name: str,
    record_id: str,
) -> Optional[dict]:
    """
    Download a record from docdb using the record _id.
    Parameters
    ----------
    docdb_client : MongoClient
    db_name : str
    collection_name : str
    record_id : str

    Returns
    -------
    Optional[dict]
        None if record does not exist. Otherwise, it will return the record as
        a dict.

    """
    db = docdb_client[db_name]
    collection = db[collection_name]
    records = list(collection.find(filter={"_id": record_id}, limit=1))
    if len(records) > 0:
        return records[0]
    else:
        return None


def paginate_docdb(
    db_name: str,
    collection_name: str,
    docdb_client: MongoClient,
    page_size: int = 1000,
    filter_query: Optional[dict] = None,
    projection: Optional[dict] = None,
) -> Iterator[List[dict]]:
    """
    Paginate through records in DocDb.
    Parameters
    ----------
    db_name : str
    collection_name : str
    docdb_client : MongoClient
    page_size : int
      Default is 1000
    filter_query : Optional[dict]
    projection : Optional[dict]

    Returns
    -------
    Iterator[List[dict]]

    """
    if filter_query is None:
        filter_query = {}
    if projection is None:
        projection = {}
    db = docdb_client[db_name]
    collection = db[collection_name]
    cursor = collection.find(filter=filter_query, projection=projection)
    obj = next(cursor, None)
    while obj:
        page = []
        while len(page) < page_size and obj:
            page.append(obj)
            obj = next(cursor, None)
        yield page


def build_docdb_location_to_id_map(
    db_name: str,
    collection_name: str,
    docdb_client: MongoClient,
    bucket: str,
    prefixes: List[str],
) -> Dict[str, str]:
    """
    For a given s3 bucket and list of prefixes, return a dictionary that looks
    like {'s3://bucket/prefix': 'abc-1234'} where the value is the id of the
    record in DocDb. If the record does not exist, then there will be no key
    in the dictionary.
    Parameters
    ----------
    db_name : str
    collection_name : ste
    docdb_client : MongoClient
    bucket : str
    prefixes : List[str]

    Returns
    -------
    Dict[str, str]

    """
    locations = [get_s3_location(bucket=bucket, prefix=p) for p in prefixes]
    filter_query = {"location": {"$in": locations}}
    projection = {"_id": 1, "location": 1}
    db = docdb_client[db_name]
    collection = db[collection_name]
    results = collection.find(filter=filter_query, projection=projection)
    location_to_id_map = {r["location"]: r["_id"] for r in results}
    return location_to_id_map


def get_all_processed_codeocean_asset_records(
    co_client: CodeOceanClient, co_data_asset_bucket: str
) -> Dict[str, dict]:
    """
    Gets all the data asset records we're interested in indexing. The location
    field in the output is the expected location of the data asset. It may
    still require double-checking that the s3 location is valid.
    Parameters
    ----------
    co_client : CodeOceanClient
    co_data_asset_bucket : str
      Name of Code Ocean's data asset bucket
    Returns
    -------
    Dict[str, dict]
      {data_asset_location:
      {"name": data_asset_name,
      "location": data_asset_location,
      "created": data_asset_created,
      "external_links": {"Code Ocean": data_asset_id}
      }
      }

    """

    # We need to break up the search query until Code Ocean fixes a bug in
    # their search API. This may still break if the number of records in an
    # individual response exceeds 10,000

    all_responses = dict()

    for tag in {DataLevel.DERIVED.value, "processed"}:
        response = co_client.search_all_data_assets(
            type="result", query=f"tag:{tag}"
        )
        # There is a bug with the codeocean api that caps the number of
        # results in a single request to 10000.
        if len(response.json()["results"]) >= 10000:
            logging.warning(
                "Number of records exceeds 10,000! This can lead to "
                "possible data loss."
            )
        # Extract relevant information
        extracted_info = dict()
        for data_asset_info in response.json()["results"]:
            data_asset_id = data_asset_info["id"]
            data_asset_name = data_asset_info["name"]
            created_timestamp = data_asset_info["created"]
            created_datetime = datetime.fromtimestamp(
                created_timestamp, tz=timezone.utc
            )
            # Results hosted externally have a source_bucket field
            is_external = (
                data_asset_info.get("sourceBucket") is not None
                or data_asset_info.get("source_bucket") is not None
            )
            if not is_external and data_asset_info.get("state") == "ready":
                location = f"s3://{co_data_asset_bucket}/{data_asset_id}"
                extracted_info[location] = {
                    "name": data_asset_name,
                    "location": location,
                    "created": created_datetime,
                    "external_links": {"Code Ocean": data_asset_id},
                }
        # Occasionally, there are duplicate items returned. This is one
        # way to remove the duplicates.
        all_responses.update(extracted_info)
    return all_responses
