import argparse
import json
import logging
import sys
import warnings
from typing import List

import boto3
import dask.bag as dask_bag
from aind_codeocean_api.codeocean import CodeOceanClient
from mypy_boto3_s3 import S3Client
from pymongo import MongoClient

from aind_data_asset_indexer.models import CodeOceanIndexBucketJobSettings
from aind_data_asset_indexer.utils import (
    build_codeocean_id_to_name_map,
    build_metadata_record_from_prefix,
    compute_md5_hash,
    create_metadata_object_key,
    does_metadata_record_exist_in_docdb,
    does_s3_object_exist,
    download_json_file_from_s3,
    get_dict_of_file_info,
    get_s3_bucket_and_prefix,
    is_record_location_valid,
    iterate_through_top_level,
    paginate_docdb,
    upload_metadata_json_str_to_s3,
)

# pydantic raises too many serialization warnings
warnings.filterwarnings("ignore", category=UserWarning)


class CodeOceanIndexBucketJob:
    """This job will:
    1) Loop through the records in DocDb filtered by bucket.
    2.0) For each record, check if the S3 location exists. If the S3 location
    does not exist, then remove the record from DocDB.
    2.1) If the S3 location exists, check if there is a metadata.nd.json file.
    2.1.0) If there is no file, log a warning and remove the record from DocDb.
    2.1.1) If there is a file, compare the md5 hashes. If they are different,
    overwrite the record in S3 with the record from DocDb.
    2.1.2) If they are the same, then do nothing.
    3) Scan through each prefix in the CodeOcean bucket.
    4) For each prefix, check if a metadata record exists in S3.
    4.0) If a metadata record exists, check if it is in DocDB.
    4.1) If already in DocDb, then don't do anything.
    Otherwise, copy record to DocDB.
    4.2) If a metadata record does not exist, then build one. Save it to the
    CodeOcean folder and upload it to DocDB.
    """

    def __init__(self, job_settings: CodeOceanIndexBucketJobSettings):
        """Class constructor."""
        self.job_settings = job_settings

    def _process_docdb_record(
        self,
        docdb_record: dict,
        docdb_client: MongoClient,
        s3_client: S3Client,
    ):
        if not is_record_location_valid(
            docdb_record, self.job_settings.s3_bucket
        ):
            logging.warning(
                f"Record location {docdb_record.get('location')} is not valid "
                f"for bucket {self.job_settings.s3_bucket}!"
            )
        else:
            s3_parts = get_s3_bucket_and_prefix(docdb_record["location"])
            s3_bucket = s3_parts["bucket"]
            prefix = s3_parts["prefix"]
            object_key = create_metadata_object_key(prefix=prefix)
            does_file_exist_in_s3 = does_s3_object_exist(
                s3_client=s3_client, bucket=s3_bucket, key=object_key
            )
            if not does_file_exist_in_s3:
                logging.warning(
                    f"File not found in S3 at s3://{s3_bucket}/{object_key}! "
                    f"Removing metadata record from DocDb."
                )
                db = docdb_client[self.job_settings.doc_db_db_name]
                collection = db[self.job_settings.doc_db_collection_name]
                response = collection.delete_one(
                    filter={"_id": docdb_record["_id"]}
                )
                logging.info(response)
            else:
                record_as_json_str = json.dumps(docdb_record, default=str)
                record_md5_hash = compute_md5_hash(record_as_json_str)
                s3_object_info = get_dict_of_file_info(
                    s3_client=s3_client, bucket=s3_bucket, keys=[object_key]
                )[object_key]
                s3_object_hash = (
                    None
                    if s3_object_info is None
                    else s3_object_info["e_tag"][1:-1]
                )
                if record_md5_hash != s3_object_hash:
                    response = upload_metadata_json_str_to_s3(
                        bucket=s3_bucket,
                        prefix=prefix,
                        metadata_json=record_as_json_str,
                        s3_client=s3_client,
                    )
                    logging.info(response)

    def _dask_task_to_process_record_list(
        self, record_list: List[dict]
    ) -> None:
        """
        The task to perform within a partition. If n_partitions is set to 20
        and the outer prefix list had length 1000, then this should process
        50 prefixes.
        Parameters
        ----------
        record_list : List[dict]

        Returns
        -------

        """
        # create a s3_client here since dask doesn't serialize it
        s3_client = boto3.client("s3")
        doc_db_client = MongoClient(
            host=self.job_settings.doc_db_host,
            port=self.job_settings.doc_db_port,
            retryWrites=False,
            directConnection=True,
            username=self.job_settings.doc_db_user_name,
            password=self.job_settings.doc_db_password.get_secret_value(),
            authSource="admin",
        )
        for record in record_list:
            self._process_docdb_record(
                docdb_record=record,
                docdb_client=doc_db_client,
                s3_client=s3_client,
            )
        s3_client.close()
        doc_db_client.close()

    def _process_records(self, records: List[dict]):
        """
        For a list of records (up to a 1000 in the list), divvy up the list
        across n_partitions. Process the set of records in each partition.
        Parameters
        ----------
        records : List[str]

        Returns
        -------
        None

        """
        record_bag = dask_bag.from_sequence(
            records, npartitions=self.job_settings.n_partitions
        )
        mapped_partitions = dask_bag.map_partitions(
            self._dask_task_to_process_record_list, record_bag
        )
        mapped_partitions.compute()

    def _process_prefix(
        self,
        s3_prefix: str,
        docdb_client: MongoClient,
        s3_client: S3Client,
        codeocean_id_to_name_map: dict,
    ):
        # Check if metadata record exits
        stripped_prefix = (
            s3_prefix[:-1] if s3_prefix.endswith("/") else s3_prefix
        )
        if codeocean_id_to_name_map.get(stripped_prefix) is not None:
            codeocean_name = codeocean_id_to_name_map.get(stripped_prefix)
        else:
            codeocean_name = None
        object_key = create_metadata_object_key(prefix=s3_prefix)
        does_metadata_file_exist = does_s3_object_exist(
            s3_client=s3_client,
            bucket=self.job_settings.s3_bucket,
            key=object_key,
        )
        if does_metadata_file_exist:
            # Check if it is in DocDb
            does_record_exist_in_docdb = does_metadata_record_exist_in_docdb(
                docdb_client=docdb_client,
                db_name=self.job_settings.doc_db_db_name,
                collection_name=self.job_settings.doc_db_collection_name,
                location=f"s3://{self.job_settings.s3_bucket}/{s3_prefix}",
            )
            if not does_record_exist_in_docdb:
                json_contents = download_json_file_from_s3(
                    s3_client=s3_client,
                    bucket=self.job_settings.s3_bucket,
                    object_key=object_key,
                )
                if json_contents:
                    db = docdb_client[self.job_settings.doc_db_db_name]
                    collection = db[self.job_settings.doc_db_collection_name]
                    response = collection.update_one(
                        {"_id": json_contents["_id"]},
                        {"$set": json_contents},
                        upsert=True,
                    )
                    logging.info(response)
                else:
                    logging.warning(
                        f"Unable to download file from S3!"
                        f" s3://{self.job_settings.s3_bucket}/{object_key}"
                    )
            else:
                logging.info("Record already exists in DocDb. Skipping.")
        elif codeocean_name is not None:
            # Build a new metadata file, save it to S3 and save it to DocDb.
            new_metadata_contents = build_metadata_record_from_prefix(
                bucket=self.job_settings.s3_bucket,
                prefix=s3_prefix,
                metadata_nd_overwrite=True,
                s3_client=s3_client,
                optional_name=codeocean_id_to_name_map.get(
                    stripped_prefix, {}
                ).get("name"),
            )
            if new_metadata_contents is not None:
                # noinspection PyTypeChecker
                s3_response = upload_metadata_json_str_to_s3(
                    metadata_json=new_metadata_contents,
                    bucket=self.job_settings.s3_bucket,
                    prefix=s3_prefix,
                    s3_client=s3_client,
                )
                logging.info(s3_response)
                # noinspection PyTypeChecker
                new_metadata_contents_json = json.loads(new_metadata_contents)
                db = docdb_client[self.job_settings.doc_db_db_name]
                collection = db[self.job_settings.doc_db_collection_name]
                response = collection.update_one(
                    {"_id": new_metadata_contents_json["_id"]},
                    {"$set": new_metadata_contents_json},
                    upsert=True,
                )
                logging.info(response)
        else:
            logging.warning(
                f"Unable to pull information about the data asset from code "
                f"ocean index!"
                f"Skipping s3://{self.job_settings.s3_bucket}/{s3_prefix}"
            )

    def _dask_task_to_process_prefix_list(
        self, prefix_list: List[str], codeocean_id_to_name_map: dict
    ) -> None:
        """
        The task to perform within a partition. If n_partitions is set to 20
        and the outer prefix list had length 1000, then this should process
        50 prefixes.
        Parameters
        ----------
        prefix_list : List[str]
        codeocean_id_to_name_map : dict

        Returns
        -------

        """
        # create a s3_client here since dask doesn't serialize it
        s3_client = boto3.client("s3")
        filtered_map = {
            prefix[:-1]: codeocean_id_to_name_map.get(prefix[:-1])
            for prefix in prefix_list
        }
        doc_db_client = MongoClient(
            host=self.job_settings.doc_db_host,
            port=self.job_settings.doc_db_port,
            retryWrites=False,
            directConnection=True,
            username=self.job_settings.doc_db_user_name,
            password=self.job_settings.doc_db_password.get_secret_value(),
            authSource="admin",
        )
        for prefix in prefix_list:
            self._process_prefix(
                s3_prefix=prefix,
                s3_client=s3_client,
                codeocean_id_to_name_map=filtered_map,
                docdb_client=doc_db_client,
            )
        s3_client.close()
        doc_db_client.close()

    def _process_prefixes(
        self, prefixes: List[str], codeocean_id_to_name_map: dict
    ):
        """
        For a list of prefixes (up to a 1000 in the list), divvy up the list
        across n_partitions. Process the set of prefixes in each partition.
        Parameters
        ----------
        prefixes : List[str]

        Returns
        -------
        None

        """
        prefix_bag = dask_bag.from_sequence(
            prefixes, npartitions=self.job_settings.n_partitions
        )
        mapped_partitions = dask_bag.map_partitions(
            self._dask_task_to_process_prefix_list,
            prefix_bag,
            codeocean_id_to_name_map=codeocean_id_to_name_map,
        )
        mapped_partitions.compute()

    def run_job(self):
        """Main method to run."""
        iterator_docdb_client = MongoClient(
            host=self.job_settings.doc_db_host,
            port=self.job_settings.doc_db_port,
            retryWrites=False,
            directConnection=True,
            username=self.job_settings.doc_db_user_name,
            password=self.job_settings.doc_db_password.get_secret_value(),
            authSource="admin",
        )

        docdb_pages = paginate_docdb(
            db_name=self.job_settings.doc_db_db_name,
            docdb_client=iterator_docdb_client,
            collection_name=self.job_settings.doc_db_collection_name,
            page_size=500,
            filter_query={
                "location": f"^s3://{self.job_settings.s3_bucket}.*"
            },
        )
        for page in docdb_pages:
            self._process_records(records=page)
        iterator_docdb_client.close()
        co_client = CodeOceanClient(
            domain=self.job_settings.codeocean_host,
            token=self.job_settings.codeocean_token.get_secret_value(),
        )
        asset_id_to_name_map = build_codeocean_id_to_name_map(
            codeocean_client=co_client
        )
        # co_client doesn't have a close method
        iterator_s3_client = boto3.client("s3")
        prefix_iterator = iterate_through_top_level(
            s3_client=iterator_s3_client, bucket=self.job_settings.s3_bucket
        )
        for prefix_list in prefix_iterator:
            self._process_prefixes(
                prefixes=prefix_list,
                codeocean_id_to_name_map=asset_id_to_name_map,
            )
        iterator_s3_client.close()


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job-settings",
        required=False,
        type=str,
        help=(
            r"""
            Instead of init args the job settings can optionally be passed in
            as a json string in the command line.
            """
        ),
    )
    parser.add_argument(
        "-p",
        "--param-store-name",
        required=False,
        type=str,
        help=(
            r"""
            Instead of init args the job settings can optionally be pulled from
            the aws param store.
            """
        ),
    )
    cli_args = parser.parse_args(sys_args)
    if cli_args.job_settings is None and cli_args.param_store_name is None:
        raise ValueError(
            "At least one of job-settings or param-store-name needs to be set"
        )
    if cli_args.job_settings is not None:
        main_job_settings = (
            CodeOceanIndexBucketJobSettings.model_validate_json(
                cli_args.job_settings
            )
        )
    else:
        main_job_settings = CodeOceanIndexBucketJobSettings.from_param_store(
            param_store_name=cli_args.param_store_name
        )
    main_job = CodeOceanIndexBucketJob(job_settings=main_job_settings)
    main_job.run_job()
