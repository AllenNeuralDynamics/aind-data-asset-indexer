"""Package for common methods used such as interfacing with S3 and DocDB."""

import hashlib
import json
import logging
import re
from datetime import datetime, timezone
from json.decoder import JSONDecodeError
from typing import Dict, Iterator, List, Optional
from urllib.parse import urlparse

from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.utils import get_s3_location
from aind_data_schema.core.data_description import DataLevel, DataRegex
from aind_data_schema.core.metadata import CORE_FILES as CORE_SCHEMAS
from aind_data_schema.core.metadata import (
    ExternalPlatforms,
    Metadata,
    create_metadata_json,
)
from botocore.exceptions import ClientError
from codeocean import CodeOcean
from codeocean.data_asset import (
    DataAssetSearchOrigin,
    DataAssetSearchParams,
    DataAssetState,
    DataAssetType,
)
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import (
    PaginatorConfigTypeDef,
    PutObjectOutputTypeDef,
)

metadata_filename = Metadata.default_filename()

core_schema_file_names = {
    field_name: f"{field_name}.json" for field_name in CORE_SCHEMAS
}


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
      is invalid or does not match the expected prefix.
      If the record name does not match the prefix, a warning is logged,
      but the method will still return True.

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
                or (
                    expected_prefix is not None
                    and stripped_prefix != expected_stripped_prefix
                )
            ):
                return False
            elif record.get("name") != stripped_prefix:
                logging.warning(
                    f"Record name {record.get('name')} does not match "
                    f"prefix {stripped_prefix}."
                )
            return True


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
        pattern = re.escape(copy_prefix) + r"([a-zA-Z0-9_]+)\.\d{8}\.json$"
        for obj in response["Contents"]:
            m = re.match(pattern, obj["Key"])
            if m is not None and m.group(1) in CORE_SCHEMAS:
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
      A list of the core schemas in the copy_subdir without timestamp, e.g,
      ["subject", "procedures", "processing"]
    """
    # Use trailing slash and delimiter to get top-level objects in copy_subdir
    copy_prefix = create_object_key(prefix, copy_subdir.strip("/") + "/")
    response = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=copy_prefix, Delimiter="/"
    )
    files = []
    if "Contents" in response:
        pattern = re.escape(copy_prefix) + r"([a-zA-Z0-9_]+)\.\d{8}\.json$"
        for obj in response["Contents"]:
            m = re.match(pattern, obj["Key"])
            if m is not None and m.group(1) in CORE_SCHEMAS:
                files.append(m.group(1))
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
    key_map = {
        create_object_key(prefix=prefix, filename=file_name): file_name
        for file_name in core_schema_file_names.values()
    }
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
    optional_external_links: Optional[Dict[str, List[str]]] = None,
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
    optional_external_links: Optional[Dict[str, List[str]]]
      User can provide external_links. Default is None.

    Returns
    -------
    Optional[str]
      The constructed Metadata record as a json string. Will return None if
      there are issues with Metadata construction.

    """
    core_files_infos = get_dict_of_core_schema_file_info(
        s3_client=s3_client, bucket=bucket, prefix=prefix
    )
    record_name = prefix.strip("/") if optional_name is None else optional_name
    try:
        core_jsons = dict()
        for field_name, file_name in core_schema_file_names.items():
            response_data = core_files_infos.get(file_name)
            if response_data is not None:
                object_key = create_object_key(prefix, file_name)
                json_contents = download_json_file_from_s3(
                    s3_client=s3_client, bucket=bucket, object_key=object_key
                )
                if json_contents is not None:
                    core_jsons[field_name] = json_contents
        # Construct Metadata file using core schema jsons
        # Validation and de/serialization are handled in aind-data-schema
        metadata_dict = create_metadata_json(
            name=record_name,
            location=get_s3_location(bucket=bucket, prefix=prefix),
            core_jsons=core_jsons,
            optional_created=optional_created,
            optional_external_links=optional_external_links,
        )
        metadata_str = json.dumps(metadata_dict)
    except Exception:
        metadata_str = None
    return metadata_str


def cond_copy_then_sync_core_json_files(
    metadata_json: str,
    bucket: str,
    prefix: str,
    s3_client: S3Client,
    copy_original_md_subdir: str,
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
        logging.warning(
            "Copy of original metadata already exists at "
            f"s3://{bucket}/{prefix}/{copy_original_md_subdir}"
        )
    else:
        copy_core_json_files(
            bucket=bucket,
            prefix=prefix,
            s3_client=s3_client,
            copy_original_md_subdir=copy_original_md_subdir,
        )
    sync_core_json_files(
        metadata_json=metadata_json,
        bucket=bucket,
        prefix=prefix,
        s3_client=s3_client,
    )


def copy_core_json_files(
    bucket: str,
    prefix: str,
    s3_client: S3Client,
    copy_original_md_subdir: str,
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
    copy_original_md_subdir : str
        Subdirectory to copy original core schema json files to.
        For example, 'original_metadata'.

    Returns
    -------
    None

    """
    tgt_copy_subdir = copy_original_md_subdir.strip("/")
    tgt_copy_prefix = create_object_key(prefix, tgt_copy_subdir)
    core_files_infos = get_dict_of_core_schema_file_info(
        s3_client=s3_client, bucket=bucket, prefix=prefix
    )
    for file_name in core_schema_file_names.values():
        source = create_object_key(prefix, file_name)
        source_location = get_s3_location(bucket=bucket, prefix=source)
        source_file_info = core_files_infos[file_name]
        if source_file_info is not None:
            date_stamp = source_file_info["last_modified"].strftime("%Y%m%d")
            target = create_object_key(
                prefix=tgt_copy_prefix,
                filename=file_name.replace(".json", f".{date_stamp}.json"),
            )
            # Copy original core json files to /original_metadata
            logging.info(f"Copying {source} to {target} in s3://{bucket}")
            response = s3_client.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": source},
                Key=target,
            )
            logging.debug(response)
        else:
            logging.info(
                f"Source file {source_location} does not exist. "
                f"Skipping copy."
            )


def sync_core_json_files(
    metadata_json: str,
    bucket: str,
    prefix: str,
    s3_client: S3Client,
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

    Returns
    -------
    None
    """
    md_record_json = json.loads(metadata_json)
    core_files_infos = get_dict_of_core_schema_file_info(
        s3_client=s3_client, bucket=bucket, prefix=prefix
    )
    for field_name, file_name in core_schema_file_names.items():
        object_key = create_object_key(prefix, file_name)
        location = get_s3_location(bucket=bucket, prefix=object_key)
        if (
            field_name in md_record_json
            and md_record_json[field_name] is not None
        ):
            field_contents = md_record_json[field_name]
            field_contents_str = json.dumps(field_contents)
            # Core schema jsons are created if they don't already exist.
            # Otherwise, they are only updated if their contents are outdated.
            if core_files_infos[file_name] is None:
                logging.info(f"Uploading new {field_name} to {location}")
                response = upload_json_str_to_s3(
                    bucket=bucket,
                    object_key=object_key,
                    json_str=field_contents_str,
                    s3_client=s3_client,
                )
                logging.debug(response)
            else:
                s3_object_hash = core_files_infos[file_name]["e_tag"].strip(
                    '"'
                )
                core_field_md5_hash = compute_md5_hash(field_contents_str)
                if core_field_md5_hash != s3_object_hash:
                    logging.info(
                        f"Uploading updated {field_name} to {location}"
                    )
                    response = upload_json_str_to_s3(
                        bucket=bucket,
                        object_key=object_key,
                        json_str=field_contents_str,
                        s3_client=s3_client,
                    )
                    logging.debug(response)
                else:
                    logging.info(
                        f"{field_name} is up-to-date in {location}. "
                        f"Skipping."
                    )
        else:
            # If a core field is None but the core json exists,
            # delete the core json.
            if core_files_infos[file_name] is not None:
                logging.info(
                    f"{field_name} not found in metadata.nd.json for "
                    f"{prefix} but {location} exists! Deleting."
                )
                response = s3_client.delete_object(
                    Bucket=bucket, Key=object_key
                )
                logging.debug(response)
            else:
                logging.info(
                    f"{field_name} not found in metadata.nd.json for "
                    f"{prefix} nor in {location}! Skipping."
                )


# TODO: replace with method from aind_data_access_api.utils once available
def build_docdb_location_to_id_map(
    docdb_api_client: MetadataDbClient,
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
    docdb_api_client : MetadataDbClient
    bucket : str
    prefixes : List[str]

    Returns
    -------
    Dict[str, str]

    """
    locations = [get_s3_location(bucket=bucket, prefix=p) for p in prefixes]
    # NOTE: use aggregation since filter too large for retrieve_docdb_records
    agg_pipeline = [
        {"$match": {"location": {"$in": locations}}},
        {"$project": {"location": 1, "_id": 1}},
    ]
    results = docdb_api_client.aggregate_docdb_records(pipeline=agg_pipeline)
    location_to_id_map = {r["location"]: r["_id"] for r in results}
    return location_to_id_map


def get_all_processed_codeocean_asset_records(
    co_client: CodeOcean, co_data_asset_bucket: str
) -> Dict[str, dict]:
    """
    Gets all the data asset records we're interested in indexing. The location
    field in the output is the expected location of the data asset. It may
    still require double-checking that the s3 location is valid.

    Parameters
    ----------
    co_client : CodeOcean
    co_data_asset_bucket : str
      Name of Code Ocean's data asset bucket
    Returns
    -------
    Dict[str, dict]
      {data_asset_location:
      {"name": data_asset_name,
      "location": data_asset_location,
      "created": data_asset_created,
      "external_links": {"Code Ocean": [data_asset_id]}
      }
      }

    """

    all_responses = dict()

    for tag in {DataLevel.DERIVED.value, "processed"}:
        search_params = DataAssetSearchParams(
            type=DataAssetType.Result,
            query=f"tag:{tag}",
            archived=False,
            origin=DataAssetSearchOrigin.Internal,
            limit=1000,
        )
        iter_response = co_client.data_assets.search_data_assets_iterator(
            search_params=search_params
        )
        # Extract relevant information
        extracted_info = dict()
        for data_asset_info in iter_response:
            data_asset_id = data_asset_info.id
            data_asset_name = data_asset_info.name
            created_timestamp = data_asset_info.created
            created_datetime = datetime.fromtimestamp(
                created_timestamp, tz=timezone.utc
            )
            # Results hosted externally have a source_bucket field
            is_external = data_asset_info.source_bucket is not None
            if (
                not is_external
                and data_asset_info.state == DataAssetState.Ready
            ):
                location = f"s3://{co_data_asset_bucket}/{data_asset_id}"
                extracted_info[location] = {
                    "name": data_asset_name,
                    "location": location,
                    "created": created_datetime,
                    "external_links": {
                        ExternalPlatforms.CODEOCEAN.value: [data_asset_id]
                    },
                }
        # Occasionally, there are duplicate items returned. This is one
        # way to remove the duplicates.
        all_responses.update(extracted_info)
    return all_responses
