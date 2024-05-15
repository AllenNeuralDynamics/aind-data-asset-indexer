"""Package for common methods used such as interfacing with S3."""
import hashlib
import json
from json.decoder import JSONDecodeError
from typing import Dict, Iterator, List, Optional
from urllib.parse import urlparse

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_schema.core.metadata import Metadata
from aind_data_schema.utils.json_writer import SchemaWriter
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import (
    PaginatorConfigTypeDef,
    PutObjectOutputTypeDef,
)

# TODO: This would be better if it was available in aind-data-schema
from pymongo import MongoClient

core_schema_file_names = [
    s.default_filename()
    for s in SchemaWriter.get_schemas()
    if s.default_filename() != Metadata.default_filename()
]


def create_metadata_object_key(prefix: str) -> str:
    stripped_prefix = prefix[:-1] if prefix.endswith("/") else prefix
    return f"{stripped_prefix}/{Metadata.default_filename()}"


def is_record_location_valid(record: dict, expected_bucket: str) -> bool:
    if record.get("location") is None:
        return False
    else:
        parts = urlparse(record.get("location"), allow_fragments=False)
        if parts.scheme != "s3":
            return False
        elif parts.netloc != expected_bucket:
            return False
        else:
            stripped_prefix = (
                parts[1:-1] if parts.path.endswith("/") else parts[1:]
            )
            if stripped_prefix == "" or len(stripped_prefix.split("/")) > 1:
                return False
            else:
                return True


def get_s3_bucket_and_prefix(s3_location: str) -> Dict[str, str]:
    parts = urlparse(s3_location, allow_fragments=False)

    return {"bucket": parts.netloc, "prefix": parts.path[1:]}


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
        json.loads(json_contents), indent=3, ensure_ascii=False
    ).encode("utf-8")
    return hashlib.md5(contents).hexdigest()


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
    stripped_prefix = prefix[:-1] if prefix.endswith("/") else prefix
    object_key = f"{stripped_prefix}/{Metadata.default_filename()}"
    contents = json.dumps(
        json.loads(metadata_json), indent=3, ensure_ascii=False
    ).encode("utf-8")
    response = s3_client.put_object(
        Bucket=bucket, Key=object_key, Body=contents
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
    for key in input_dict.keys():
        if "$" in key or "." in key:
            return True
        elif isinstance(input_dict[key], dict):
            return is_dict_corrupt(input_dict[key])
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
    metadata_nd_overwrite: bool,
    prefix: str,
    s3_client: S3Client,
    optional_name: Optional[str] = None,
) -> Optional[str]:
    """
    For a given bucket and prefix, this method will return a JSON string
    representation of a Metadata record. If metadata_nd_overwrite is True or
    a metadata.nd.json file does not exist, then a Metadata record will be
    constructed from any core schema json files found under the prefix.
    Otherwise, the method will return the metadata.nd.json file found in S3
    as a JSON string if it is valid json. If not valid json, then it will
    return None.
    Parameters
    ----------
    bucket : str
    metadata_nd_overwrite : bool
    prefix : str
    s3_client : S3Client
    optional_name : Optional[str]
      If optional is None, then a name will be constructed from the s3_prefix.
      Default is None.

    Returns
    -------
    Optional[str]
      The Metadata record as a json string. Will return None if
      metadata_nd_overwrite is set to false, or there is a
      metadata.nd.json file and the file is corrupt.

    """
    stripped_prefix = prefix[:-1] if prefix.endswith("/") else prefix
    metadata_nd_file_key = stripped_prefix + "/" + Metadata.default_filename()
    does_metadata_nd_file_exist = does_s3_object_exist(
        s3_client=s3_client, bucket=bucket, key=metadata_nd_file_key
    )
    if metadata_nd_overwrite or not does_metadata_nd_file_exist:
        file_keys = [
            stripped_prefix + "/" + file_name
            for file_name in core_schema_file_names
        ]
        s3_file_responses = get_dict_of_file_info(
            s3_client=s3_client, bucket=bucket, keys=file_keys
        )
        record_name = (
            stripped_prefix if optional_name is None else optional_name
        )
        metadata_dict = {
            "name": record_name,
            "location": f"s3://{bucket}/{stripped_prefix}",
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
        # TODO: We should handle constructing the Metadata file in a better way
        #  in aind-data-schema. By using model_validate, a lot of info from the
        #  original files get removed. For now, we can use model_construct
        #  until a better method is implemented in aind-data-schema. This will
        #  mark all the initial files as metadata_status=Unknown
        return Metadata.model_construct(**metadata_dict).model_dump_json(
            warnings=False, by_alias=True
        )
    else:
        metadata_contents = download_json_file_from_s3(
            s3_client=s3_client, bucket=bucket, object_key=metadata_nd_file_key
        )
        return (
            None
            if metadata_contents is None
            else json.dumps(metadata_contents)
        )


def does_metadata_record_exist_in_docdb(
    docdb_client: MongoClient,
    db_name: str,
    collection_name: str,
    location: str,
) -> bool:
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
    collection_name,
    docdb_client,
    page_size: int = 1000,
    filter_query: Optional[dict] = None,
) -> Iterator[List[dict]]:
    if filter_query is None:
        filter_query = {}
    db = docdb_client[db_name]
    collection = db[collection_name]
    cursor = collection.find(filter=filter_query)
    obj = next(cursor, None)
    while obj:
        page = [obj]
        while len(page) < page_size and obj:
            obj = next(cursor, None)
            if obj:
                page.append(obj)
        yield page


def build_codeocean_id_to_name_map(
    codeocean_client: CodeOceanClient,
) -> Dict[str, Optional[str]]:
    id_to_name_map = dict()
    co_response = codeocean_client.search_data_assets(type="dataset")
    start_index = 0
    for r in co_response.json().get("results", []):
        id_to_name_map[r["id"]] = r["name"]
    while (
        co_response.json().get("has_more", False)
        and len(co_response.json().get("results")) > 0
    ):
        start_index += len(co_response.json().get("results"))
        co_response = codeocean_client.search_data_assets(
            type="dataset", start=start_index
        )
        for r in co_response.json().get("results", []):
            id_to_name_map[r["id"]] = r.get("name")
    return id_to_name_map
