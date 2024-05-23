"""Package for common methods used such as interfacing with S3."""
import hashlib
import json
import logging
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict, Iterator, List, Optional
from urllib.parse import urlparse

from aind_data_schema.core.metadata import Metadata
from aind_data_schema.utils.json_writer import SchemaWriter
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import (
    PaginatorConfigTypeDef,
    PutObjectOutputTypeDef,
)
from pymongo import MongoClient

COPY_ORIGINAL_MD_SUBDIR = "original_metadata"

# TODO: This would be better if it was available in aind-data-schema
core_schema_file_names = [
    s.default_filename()
    for s in SchemaWriter.get_schemas()
    if s.default_filename() != Metadata.default_filename()
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
    return create_object_key(
        prefix=prefix, filename=Metadata.default_filename()
    )


def create_core_schema_object_keys_map(
    prefix: str,
    target_prefix: str,
) -> Dict[str, Dict[str, str]]:
    """
    For a given s3 prefix, return a dictionary of { core_schema_file_name:
    { source: source_object_key, target: target_object_key } } for all possible core schema files in s3.
    The source is the original core schema object key.
    The target is in the target_prefix and has a date stamp appended.
    Parameters
    ----------
    prefix : str
      The source prefix. For example, ecephys_123456_2020-10-10_01-02-03
    target_prefix : str
      The target prefix for target files.
      For example, ecephys_123456_2020-10-10_01-02-03/original_metadata.

    Returns
    -------
    Dict[str, Dict[str, str]]
      Returns a dictionary of all possible core schema file names and their corresponding
      source and target core schema object keys.
      For example, {
        'subject.json': {
            'source': 'ecephys_123456_2020-10-10_01-02-03/subject.json',
            'target': 'ecephys_123456_2020-10-10_01-02-03/original_metadata/subject.20240520.json'
        },
        ...
    }

    """
    date_stamp = datetime.now().strftime("%Y%m%d")
    object_keys = dict()
    for s in core_schema_file_names:
        source = create_object_key(prefix=prefix, filename=s)
        target = create_object_key(
            prefix=target_prefix,
            filename=s.replace(".json", f".{date_stamp}.json"),
        )
        object_keys[s] = {"source": source, "target": target}
    return object_keys


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
      doesn't match the expected prefix.

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
        For example, 'ecephys_123456_2020-10-10_01-02-03/original_metadata/subject.json'
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


def does_s3_prefix_exist(s3_client: S3Client, bucket: str, prefix: str) -> bool:
    """
    Check if a prefix (folder) exists in a bucket. Uses the list_objects
    operation with MaxKeys of 1 to check if the prefix exists.
    
    Parameters
    ----------
    s3_client : S3Client
    bucket : str
    prefix : str
      For example, behavior_655019_2020-10-10_01-00-23

    Returns
    -------
    bool
      True if the prefix (folder) exists, otherwise False.

    """
    # Add a trailing slash so that we do not match s3 objects
    prefix_to_check = prefix if prefix.endswith("/") else f"{prefix}/"
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix_to_check, MaxKeys=1)
    if "Contents" in response:
        if len(response["Contents"]) != 1:
            raise ValueError("Unexpected number of objects returned")
        return True
    else:
        return False

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
      True if nested dictionary keys contain forbidden characters.
      False otherwise.

    """
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
    Optional[dict]

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
      If optional is None, then a name will be constructed from the s3_prefix.
      Default is None.

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
    record_name = (
        prefix.strip("/") if optional_name is None else optional_name
    )
    metadata_dict = {
        "name": record_name,
        "location": get_s3_location(bucket=bucket, prefix=prefix),
    }
    for object_key, response_data in s3_file_responses.items():
        if response_data is not None:
            field_name = object_key.split("/")[-1].replace(".json", "")
            json_contents = download_json_file_from_s3(
                s3_client=s3_client, bucket=bucket, object_key=object_key
            )
            if json_contents is not None:
                # noinspection PyTypeChecker
                is_corrupt = is_dict_corrupt(input_dict=json_contents)
                if not is_corrupt:
                    metadata_dict[field_name] = json_contents
    try:
        # TODO: We should handle constructing the Metadata file in a better way
        #  in aind-data-schema. By using model_validate, a lot of info from the
        #  original files get removed. For now, we can use model_construct
        #  until a better method is implemented in aind-data-schema. This will
        #  mark all the initial files as metadata_status=Unknown
        metadata_dict = Metadata.model_construct(**metadata_dict).model_dump_json(
            warnings=False, by_alias=True
        )
    except Exception:
        metadata_dict = None
    return metadata_dict


def copy_then_overwrite_core_json_files(
    metadata_json: str,
    bucket: str,
    prefix: str,
    s3_client: S3Client,
    log_flag: bool = False,
) -> None:
    """
    For a given Metadata record, copy the core schema files to a sub-directory,
    and then overwrite the core schema file with the new core fields. If the
    original core schema json was corrupt, then it will be deleted after its
    original contents are copied.
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
    tgt_copy_prefix = create_object_key(prefix, COPY_ORIGINAL_MD_SUBDIR)
    if does_s3_prefix_exist(
        s3_client=s3_client,
        bucket=bucket,
        prefix=tgt_copy_prefix
    ):
        _log_message(
            message=(
                f"Target copy folder s3://{bucket}/{tgt_copy_prefix} already exists."
            ),
            log_flag=log_flag,
        )
    object_keys = create_core_schema_object_keys_map(prefix, tgt_copy_prefix)
    for core_schema_filename, key_mapping in object_keys.items():
        source = key_mapping["source"]
        target = key_mapping["target"]
        if does_s3_object_exist(
            s3_client=s3_client, bucket=bucket, key=source
        ):
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
            # Overwrite core schema fields from metadata.nd.json to the core json files.
            core_field = core_schema_filename.replace(".json", "")
            if (
                core_field in md_record_json
                and md_record_json[core_field] is not None
            ):
                core_json = md_record_json[core_field]
                core_json_str = json.dumps(core_json)
                _log_message(
                    message=f"Uploading new {core_field} to s3://{bucket}/{source}",
                    log_flag=log_flag,
                )
                response = upload_json_str_to_s3(
                    bucket=bucket,
                    object_key=source,
                    json_str=core_json_str,
                    s3_client=s3_client,
                )
                _log_message(message=response, log_flag=log_flag)
            else:
                # If a core json was corrupt, it would exist in the metadata.nd.json
                # Since a copy has been made already, we can delete it from the top level
                _log_message(
                    message=(
                        f"{core_field} not found in metadata.nd.json for {prefix} but "
                        f"s3://{bucket}/{source} exists! Deleting."
                    ),
                    log_level=logging.WARNING,
                    log_flag=log_flag,
                )
                response = s3_client.delete_object(Bucket=bucket, Key=source)
                _log_message(message=response, log_flag=log_flag)
        else:
            _log_message(
                message=f"Source file s3://{bucket}/{source} does not exist. Skipping copy.",
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
