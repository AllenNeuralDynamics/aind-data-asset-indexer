from typing import Dict, List

import boto3
import dask.bag as dask_bag
from aind_data_schema.core.metadata import Metadata
from pymongo import MongoClient

from aind_data_asset_indexer.models import BucketIndexJobConfigs
from aind_data_asset_indexer.utils import (
    build_metadata_record_from_prefix,
    copy_metadata_json_to_docdb,
    copy_record_from_docdb_to_s3,
    download_json_file_from_s3,
    get_dict_of_file_info,
    iterate_through_mongo_db_records,
    iterate_through_top_level,
    list_of_core_schema_file_names,
    upload_metadata_json_str_to_s3,
)


class AindBucketIndexJob:
    def __init__(self, job_settings: BucketIndexJobConfigs):
        self.job_settings = job_settings

    def get_docdb_records(self) -> Dict[str, dict]:
        mongo_client = MongoClient(
            host=self.job_settings.docdb_host,
            port=self.job_settings.docdb_port,
            username=self.job_settings.docdb_username,
            password=self.job_settings.docdb_password.get_secret_value(),
            **self.job_settings.docdb_conn_options,
        )
        records = dict()
        counter = 0
        for record in iterate_through_mongo_db_records(
            mongo_client=mongo_client,
            db_name=self.job_settings.docdb_name,
            collection_name=self.job_settings.docdb_collection_name,
            bucket=self.job_settings.s3_bucket,
            lookback_days=self.job_settings.lookback_days,
        ):
            counter += 1
            record_location = record.get("location", f"UNKNOWN_{counter}")
            records[record_location] = record
        mongo_client.close()
        return records

    def process_s3_and_metadata_info(
        self,
        s3_object_key,
        s3_object_info,
        docdb_record_info,
        s3_client,
        mongo_client,
    ):
        # First situation. There is a record in DocDB and S3. Compare the two
        # records and update S3 if needed.
        # Second situation. There is a record in S3 not found in DocDb.
        # Copy the record to DocDb. The lambda function must have missed it.
        # Third situation. There is no record in S3 or DocDB. Build the record
        # and save it to S3. Let the Lambda function update DocDB.
        # Fourth situation. There is a record in DocDB and not in S3. Copy
        # the record from DocDB to S3. This shouldn't happen though.
        if docdb_record_info is not None and s3_object_info is not None:
            record_id = docdb_record_info["_id"]
            copy_record_from_docdb_to_s3(
                s3_client=s3_client,
                mongo_client=mongo_client,
                bucket=self.job_settings.s3_bucket,
                db_name=self.job_settings.docdb_name,
                collection_name=self.job_settings.docdb_collection_name,
                e_tag=s3_object_info.get("ETag")[1:-1],
                record_id=record_id,
                metadata_filename=Metadata.default_filename(),
            )
        elif docdb_record_info is None and s3_object_info is not None:
            metadata_contents = download_json_file_from_s3(
                s3_client=s3_client,
                bucket=self.job_settings.s3_bucket,
                object_key=s3_object_key,
            )
            if metadata_contents is not None:
                copy_metadata_json_to_docdb(
                    mongo_client=mongo_client,
                    db_name=self.job_settings.docdb_name,
                    collection_name=self.job_settings.docdb_collection_name,
                    metadata_contents=metadata_contents,
                    bucket=self.job_settings.s3_bucket,
                    prefix=s3_object_key.replace(
                        Metadata.default_filename(), ""
                    ),
                )
        elif docdb_record_info is None and s3_object_info is None:
            md_record = build_metadata_record_from_prefix(
                bucket=self.job_settings.s3_bucket,
                prefix=s3_object_key.replace(Metadata.default_filename(), ""),
                metadata_nd_overwrite=True,
                metadata_nd_file_name=Metadata.default_filename(),
                s3_client=s3_client,
                core_schema_file_names=list_of_core_schema_file_names(),
            )
            if md_record is not None:
                upload_metadata_json_str_to_s3(
                    s3_client=s3_client,
                    bucket=self.job_settings.s3_bucket,
                    metadata_json=md_record,
                    object_key=s3_object_key,
                )
        elif docdb_record_info is not None and s3_object_info is None:
            record_id = docdb_record_info["_id"]
            copy_record_from_docdb_to_s3(
                s3_client=s3_client,
                mongo_client=mongo_client,
                bucket=self.job_settings.s3_bucket,
                db_name=self.job_settings.docdb_name,
                collection_name=self.job_settings.docdb_collection_name,
                e_tag=None,
                record_id=record_id,
                metadata_filename=Metadata.default_filename(),
            )
        else:
            pass

    def dask_task_to_process_prefix_list(
        self, prefix_list: List[str], docdb_records: Dict[str, dict]
    ):
        # create a s3_client here since dask doesn't serialize it
        s3_client = boto3.client("s3")
        mongo_client = MongoClient(
            host=self.job_settings.docdb_host,
            port=self.job_settings.docdb_port,
            username=self.job_settings.docdb_username,
            password=self.job_settings.docdb_password.get_secret_value(),
            **self.job_settings.docdb_conn_options,
        )
        object_keys = [
            s3p + Metadata.default_filename() for s3p in prefix_list
        ]
        s3_prefix_info = get_dict_of_file_info(
            s3_client=s3_client,
            bucket=self.job_settings.s3_bucket,
            keys=object_keys,
        )
        for s3_object_key, s3_object_info in s3_prefix_info:
            docdb_record_info = docdb_records.get(
                f"{self.job_settings.s3_bucket}/{s3_object_key[:-1]}"
            )
            self.process_s3_and_metadata_info(
                s3_object_key=s3_object_key,
                s3_object_info=s3_object_info,
                docdb_record_info=docdb_record_info,
                s3_client=s3_client,
                mongo_client=mongo_client,
            )
        s3_client.close()
        mongo_client.close()

    def process_prefixes(
        self, prefixes: List[str], docdb_records: Dict[str, dict]
    ):
        prefix_bag = dask_bag.from_sequence(
            prefixes, npartitions=self.job_settings.n_partitions
        )
        dask_bag.map_partitions(
            self.dask_task_to_process_prefix_list,
            prefix_bag,
            docdb_records=docdb_records,
        ).compute()

    def run_job(self):
        docdb_records = self.get_docdb_records()
        iterator_s3_client = boto3.client("s3")
        prefix_iterator = iterate_through_top_level(
            s3_client=iterator_s3_client, bucket=self.job_settings.bucket
        )
        for prefix_list in prefix_iterator:
            self.process_prefixes(prefix_list, docdb_records=docdb_records)
        iterator_s3_client.close()
