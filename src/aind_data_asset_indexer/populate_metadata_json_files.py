import json
import warnings
from typing import List

import boto3
import dask.bag as db
from aind_data_schema.core.metadata import Metadata
from mypy_boto3_s3 import S3Client

from aind_data_asset_indexer.utils import (
    build_metadata_record_from_prefix,
    iterate_through_top_level,
    list_of_core_schema_file_names,
)

# pydantic raises too many serialization warnings
warnings.filterwarnings("ignore", category=UserWarning)


class AindPopulateMetadataJsonJob:
    """This job will:
    1) Crawl through an S3 bucket
    2) Look inside each prefix that adheres to data asset naming convention
    3) If the name is a data asset name, then it will look inside the prefix
    4.0) If there is no metadata.nd.json file, then it will create one by using
    any of the core json files it finds.
    4.1) If the metadata_nd_overwrite option is set to False, then it will pass
    a data asset if there is already a metadata.nd.json in that folder. If set
    to True, then it will write a new metadata.nd.json file even if one already
    exists.
    """

    def __init__(
        self, bucket: str, metadata_nd_overwrite: bool = False, n_partitions=5
    ):
        self.bucket = bucket
        self.metadata_nd_overwrite = metadata_nd_overwrite
        self.core_schema_file_names = list_of_core_schema_file_names()
        self.metadata_nd_file_name = Metadata.default_filename()
        self.n_partitions = n_partitions

    def upload_metadata_file_to_s3(
        self, metadata_json: str, object_key: str, s3_client: S3Client
    ):
        contents = json.dumps(
            json.loads(metadata_json), indent=3, ensure_ascii=False
        ).encode("utf-8")
        response = s3_client.put_object(
            Bucket=self.bucket, Key=object_key, Body=contents
        )
        return response

    def process_prefix(self, prefix: str, s3_client: S3Client):
        md_record = build_metadata_record_from_prefix(
            prefix=prefix,
            s3_client=s3_client,
            bucket=self.bucket,
            core_schema_file_names=self.core_schema_file_names,
            metadata_nd_file_name=self.metadata_nd_file_name,
            metadata_nd_overwrite=self.metadata_nd_overwrite,
        )
        if md_record is not None:
            object_key = prefix + self.metadata_nd_file_name
            response = self.upload_metadata_file_to_s3(
                metadata_json=md_record,
                object_key=object_key,
                s3_client=s3_client,
            )
            print(response)

    def dask_task_to_process_prefix_list(self, prefix_list: List[str]):
        # create a s3_client here since dask doesn't serialize it
        s3_client = boto3.client("s3")
        for prefix in prefix_list:
            self.process_prefix(prefix=prefix, s3_client=s3_client)
        s3_client.close()

    def process_prefixes(self, prefixes: List[str]):
        prefix_bag = db.from_sequence(prefixes, npartitions=self.n_partitions)
        db.map_partitions(
            self.dask_task_to_process_prefix_list, prefix_bag
        ).compute()

    def run_job(self):
        iterator_s3_client = boto3.client("s3")
        prefix_iterator = iterate_through_top_level(
            s3_client=iterator_s3_client, bucket=self.bucket
        )
        for prefix_list in prefix_iterator:
            self.process_prefixes(prefix_list)
        iterator_s3_client.close()


# class AindBucketIndexJob:
#
#     def __init__(self, job_settings: IndexJobConfigs):
#         self.job_settings = job_settings
