"""Module to handle syncing changes from DocDb to S3."""

import argparse
import json
import logging
import os
import sys
import warnings
from datetime import datetime
from typing import Dict, List

import boto3
import dask.bag as dask_bag
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import CopySourceTypeDef
from pymongo import MongoClient

from aind_data_asset_indexer.models import AindIndexBucketJobSettings
from aind_data_asset_indexer.utils import (
    build_docdb_location_to_id_map,
    build_metadata_record_from_prefix,
    compute_md5_hash,
    cond_copy_then_sync_core_json_files,
    core_schema_file_names,
    create_metadata_object_key,
    create_object_key,
    does_s3_object_exist,
    download_json_file_from_s3,
    get_dict_of_core_schema_file_info,
    get_s3_bucket_and_prefix,
    get_s3_location,
    is_dict_corrupt,
    is_prefix_valid,
    is_record_location_valid,
    iterate_through_top_level,
    list_metadata_copies,
    paginate_docdb,
    upload_json_str_to_s3,
    upload_metadata_json_str_to_s3,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
# pydantic raises too many serialization warnings
warnings.filterwarnings("ignore", category=UserWarning)


class AindIndexBucketJob:
    """This job will:
    1) Loop through the records in DocDb filtered by bucket. If the record does
    not have valid location, it will log a warning and not process it further.
    2.0) For each record, check if the S3 location exists. If the S3 location
    does not exist, then remove the record from DocDB.
    2.1) If the S3 location exists, check if there is a metadata.nd.json file.
    2.1.0) If there is no file, log a warning and remove the record from DocDb.
    2.1.1) If there is a file, compare the md5 hashes. If they are different,
    overwrite the record in S3 with the record from DocDb.
    2.1.2) If they are the same, then do nothing.
    3) Scan through each prefix in S3.
    4) For each prefix, check if a metadata record exists in S3.
    4.0) If a metadata record exists, check if it is in DocDB.
    4.1) If already in DocDb, then don't do anything.
    Otherwise, copy record to DocDB.
    4.2) If a metadata record does not exist, then build one and save it S3.
    Assume a lambda function will move it over to DocDb.
    """

    def __init__(self, job_settings: AindIndexBucketJobSettings):
        """Class constructor."""
        self.job_settings = job_settings

    def _write_root_file_with_record_info(
        self,
        s3_client: S3Client,
        core_schema_file_name: str,
        core_schema_info_in_root: dict,
        prefix: str,
        docdb_record: dict,
    ) -> None:
        """
        Write a core schema file in the s3 prefix root folder using the docdb
        record info. To avoid unnecessary s3 calls, the md5 hashes will be
        compared first.
        Parameters
        ----------
        s3_client : S3Client
        core_schema_file_name : str
          For example: 'subject.json', 'procedures.json', etc.
        core_schema_info_in_root : dict
          For example: {'subject.json', {'e_tag': ...}}
        prefix : str
        docdb_record : dict

        Returns
        -------
        None
          Logs responses of client calls.

        """
        bucket = self.job_settings.s3_bucket
        field_name = core_schema_file_name.replace(".json", "")
        record_info_str = json.dumps(docdb_record.get(field_name))
        record_info_md5_hash = compute_md5_hash(record_info_str)
        if core_schema_info_in_root.get(core_schema_file_name) is None:
            root_file_md5_hash = None
        else:
            root_file_md5_hash = core_schema_info_in_root.get(
                core_schema_file_name
            )["e_tag"].strip('"')
        object_key = create_object_key(
            prefix=prefix, filename=core_schema_file_name
        )
        if record_info_md5_hash != root_file_md5_hash:
            logging.info(
                f"Writing docdb record info to s3://{bucket}/{object_key}"
            )
            response = upload_json_str_to_s3(
                bucket=bucket,
                object_key=object_key,
                json_str=record_info_str,
                s3_client=s3_client,
            )
            logging.debug(f"{response}")
        else:
            logging.debug(
                f"DocDB record and s3://{bucket}/{object_key} are the same. "
                f"Skipped writing."
            )

    def _copy_file_from_root_to_subdir(
        self,
        s3_client: S3Client,
        core_schema_file_name: str,
        core_schema_info_in_root: dict,
        prefix: str,
    ):
        """
        Write a core schema file in the s3 prefix root folder using the docdb
        record info. To avoid unnecessary s3 calls, the md5 hashes will be
        compared first.
        Parameters
        ----------
        s3_client : S3Client
        core_schema_file_name : str
          For example: 'subject.json', 'procedures.json', etc.
        core_schema_info_in_root : dict
          For example: {'subject.json', {'e_tag': ...}}
        prefix : str

        Returns
        -------
        None
          Logs responses of client calls.

        """
        copy_subdir = self.job_settings.copy_original_md_subdir
        bucket = self.job_settings.s3_bucket
        date_stamp = core_schema_info_in_root["last_modified"].strftime(
            "%Y%m%d"
        )
        source = create_object_key(prefix, core_schema_file_name)
        target = create_object_key(
            prefix=create_object_key(prefix, copy_subdir.strip("/")),
            filename=core_schema_file_name.replace(
                ".json", f".{date_stamp}.json"
            ),
        )
        logging.info(
            f"Copying s3://{bucket}/{source} to s3://{bucket}/{target}"
        )
        response = s3_client.copy_object(
            Bucket=bucket,
            # noinspection PyMethodParameters
            CopySource=CopySourceTypeDef(Bucket=bucket, Key=source),
            Key=target,
        )
        logging.debug(f"{response}")

    def _resolve_schema_information(
        self,
        prefix: str,
        s3_client: S3Client,
        core_schema_info_in_root: dict,
        list_of_schemas_in_copy_subdir: List[str],
        docdb_record: dict,
    ) -> dict:
        """
        Uses the DocDb record, a dictionary of information about the core
        schema json files under the prefix folder, and a list of core schema
        json files in the original_metadata folder to figure out what to do.
        For each core schema, there are 8 possible scenarios based on:
         - Is the field null in the DocDB record?
         - Is there a file in the root prefix?
         - Is there a file in the original_metadata folder?
        In the case that the DocDB field is null, there is a file in the root
        prefix, and there is no file in the original_metadata folder, then
        the field in the DocDB record will require updating. This method
        will return a dictionary of updates needed to the DocDB record.
        Parameters
        ----------
        prefix : str
        s3_client : S3Client
        core_schema_info_in_root : dict
        list_of_schemas_in_copy_subdir : List[str]
        docdb_record : dict

        Returns
        -------
        dict
          The fields in the DocDb record that will require updating.
        """
        docdb_record_fields_to_update = dict()
        for core_schema_file_name in core_schema_file_names:
            field_name = core_schema_file_name.replace(".json", "")
            is_in_record = docdb_record.get(field_name) is not None
            is_in_root = (
                core_schema_info_in_root.get(core_schema_file_name) is not None
            )
            is_in_copy_subdir = (
                core_schema_file_name in list_of_schemas_in_copy_subdir
            )
            # To avoid copying and pasting the same arguments, we'll keep it
            # them in a dict
            common_kwargs = {
                "s3_client": s3_client,
                "prefix": prefix,
                "core_schema_file_name": core_schema_file_name,
                "core_schema_info_in_root": core_schema_info_in_root,
            }
            # If field is not null, a file exists in the root folder, and
            # a file exists in copy_subdir, then overwrite root folder file
            # with record info if they are different
            if is_in_record and is_in_root and is_in_copy_subdir:
                self._write_root_file_with_record_info(
                    docdb_record=docdb_record, **common_kwargs
                )
            # If field is not null, a file exists in the root folder, and
            # no file exists in copy_subdir, then copy root folder file to
            # copy subdir, and then overwrite root folder file with record info
            # if they are different
            elif is_in_record and is_in_root and not is_in_copy_subdir:
                self._copy_file_from_root_to_subdir(**common_kwargs)
                self._write_root_file_with_record_info(
                    docdb_record=docdb_record, **common_kwargs
                )
            # If field is not null, no file exists in the root folder, and
            # a file exists in copy_subdir, then create a file in the root
            # folder with the record info
            elif is_in_record and not is_in_root and is_in_copy_subdir:
                self._write_root_file_with_record_info(
                    docdb_record=docdb_record, **common_kwargs
                )
            # If field is not null, no file exists in the root folder, and
            # no file exists in copy_subdir, then create a file in the root
            # folder with the record info and then copy it to the copy subdir
            elif is_in_record and not is_in_root and not is_in_copy_subdir:
                self._write_root_file_with_record_info(
                    docdb_record=docdb_record, **common_kwargs
                )
                self._copy_file_from_root_to_subdir(**common_kwargs)
            # If field is null, a file exists in the root folder, and
            # a file exists in copy_subdir, then delete file from root folder
            elif not is_in_record and is_in_root and is_in_copy_subdir:
                object_key = create_object_key(
                    prefix=prefix, filename=core_schema_file_name
                )
                logging.info(
                    f"DocDb field is null. Deleting file "
                    f"s3://{self.job_settings.s3_bucket}/{object_key}"
                )
                response = s3_client.delete_object(
                    Bucket=self.job_settings.s3_bucket, Key=object_key
                )
                logging.debug(f"{response}")
            # If field is null, a file exists in the root folder, and
            # no file exists in copy_subdir, then copy file from root folder
            # to copy subdir and update the record info with the root folder
            # file if it is not corrupt
            elif not is_in_record and is_in_root and not is_in_copy_subdir:
                self._copy_file_from_root_to_subdir(**common_kwargs)
                object_key = create_object_key(
                    prefix=prefix, filename=core_schema_file_name
                )
                file_contents = download_json_file_from_s3(
                    s3_client=s3_client,
                    bucket=self.job_settings.s3_bucket,
                    object_key=object_key,
                )
                if file_contents is None:
                    is_file_corrupt = True
                else:
                    is_file_corrupt = is_dict_corrupt(file_contents)
                if not is_file_corrupt:
                    docdb_record_fields_to_update[field_name] = file_contents
                else:
                    logging.warning(
                        f"Something went wrong downloading or parsing "
                        f"s3://{self.job_settings.s3_bucket}/{object_key}"
                    )

            # If field is null, no file exists in the root folder, and
            # a file exists in copy_subdir, then do nothing
            # If field is null, no file exists in the root folder, and no
            # file exists in the copy subdir, then do nothing
            else:
                logging.info(
                    f"Field is null in docdb record and no file in root "
                    f"folder at s3://{self.job_settings.s3_bucket}/{prefix}."
                )
        return docdb_record_fields_to_update

    def _process_docdb_record(
        self,
        docdb_record: dict,
        docdb_client: MongoClient,
        s3_client: S3Client,
    ):
        """
        For a given record,
        1. Check if it needs to be deleted (no s3 object found)
        2. If there is an s3 object, then overwrite the s3 object if the docdb
        is different.
        Parameters
        ----------
        docdb_record : dict
        docdb_client : MongoClient
        s3_client : S3Client

        Returns
        -------
        None

        """
        if not is_record_location_valid(
            docdb_record, self.job_settings.s3_bucket
        ):
            logging.warning(
                f"Record location {docdb_record.get('location')} or name "
                f"{docdb_record.get('name')} not valid for bucket "
                f"{self.job_settings.s3_bucket}!"
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
                    f"File not found in S3 at "
                    f"{get_s3_location(s3_bucket, object_key)}! "
                    f"Removing metadata record from DocDb."
                )
                db = docdb_client[self.job_settings.doc_db_db_name]
                collection = db[self.job_settings.doc_db_collection_name]
                response = collection.delete_one(
                    filter={"_id": docdb_record["_id"]}
                )
                logging.info(response.raw_result)
            else:  # There is a metadata.nd.json file in S3.
                # Schema info in root level directory
                s3_core_schema_info = get_dict_of_core_schema_file_info(
                    s3_client=s3_client,
                    bucket=self.job_settings.s3_bucket,
                    prefix=prefix,
                )
                # List of files in original_metadata folder
                files_in_og_folder = list_metadata_copies(
                    s3_client=s3_client,
                    bucket=self.job_settings.s3_bucket,
                    prefix=prefix,
                    copy_subdir=self.job_settings.copy_original_md_subdir,
                )
                fields_to_update = self._resolve_schema_information(
                    s3_client=s3_client,
                    prefix=prefix,
                    core_schema_info_in_root=s3_core_schema_info,
                    list_of_schemas_in_copy_subdir=files_in_og_folder,
                    docdb_record=docdb_record,
                )
                if fields_to_update:
                    logging.info(
                        f"New files found in root folder but not in "
                        f"{self.job_settings.copy_original_md_subdir}. "
                        f"Updating DocDb record with new info."
                    )
                    db = docdb_client[self.job_settings.doc_db_db_name]
                    collection = db[self.job_settings.doc_db_collection_name]
                    fields_to_update[
                        "last_modified"
                    ] = datetime.utcnow().isoformat()
                    response = collection.update_one(
                        {"_id": docdb_record["_id"]},
                        {"$set": fields_to_update},
                        upsert=True,
                    )
                    logging.debug(response.raw_result)

    def _dask_task_to_process_record_list(
        self, record_list: List[dict]
    ) -> None:
        """
        The task to perform within a partition. If n_partitions is set to 20
        and the outer record list had length 1000, then this should process
        50 records.
        Parameters
        ----------
        record_list : List[dict]

        Returns
        -------

        """
        # create a clients here since dask doesn't serialize them
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
            try:
                self._process_docdb_record(
                    docdb_record=record,
                    docdb_client=doc_db_client,
                    s3_client=s3_client,
                )
            except Exception as e:
                logging.error(
                    f'Error processing docdb {record.get("_id")}, '
                    f'{record.get("location")}: {repr(e)}'
                )
        s3_client.close()
        doc_db_client.close()

    def _process_records(self, records: List[dict]):
        """
        For a list of records (up to a 1000 in the list), divvy up the list
        across n_partitions. Process the set of records in each partition.
        Parameters
        ----------
        records : List[dict]

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
        location_to_id_map: Dict[str, str],
    ):
        """
        Processes a prefix in S3
        1) If the prefix is not valid (does not adhere to data asset naming
        convention), log a warning and do not process it further.
        2) If metadata record exists in S3 and DocDB, do nothing.
        3) If record is in S3 but not DocDb, then copy it to DocDb if the
        location in the metadata record matches the actual location and
        the record has an _id field. Otherwise, log a warning.
        4) If record does not exist in both DocDB and S3, build a new metadata
        file and save it to S3 (assume Lambda function will save to DocDB).
        5) In both cases above, we also copy the original core json files to a
        subfolder and ensure the top level core jsons are in sync with the
        metadata.nd.json in S3.

        Parameters
        ----------
        s3_prefix : str
        docdb_client : MongoClient
        s3_client : S3Client
        location_to_id_map : Dict[str, str]
          A map created by looping through DocDb records and creating a dict
          of {record['location']: record['_id']}. Can be used to check if a
          record already exists in DocDb for a given s3 bucket, prefix

        """
        bucket = self.job_settings.s3_bucket
        if not is_prefix_valid(s3_prefix):
            logging.warning(
                f"Prefix {s3_prefix} not valid in bucket {bucket}! Skipping."
            )
            return
        # Check if metadata record exists
        location = get_s3_location(bucket=bucket, prefix=s3_prefix)
        if location_to_id_map.get(location) is not None:
            record_id = location_to_id_map.get(location)
        else:
            record_id = None
        object_key = create_metadata_object_key(prefix=s3_prefix)
        does_metadata_file_exist = does_s3_object_exist(
            s3_client=s3_client,
            bucket=bucket,
            key=object_key,
        )
        if does_metadata_file_exist:
            # If record not in DocDb, then copy it to DocDb if the location
            # in the metadata record matches the location the record lives in
            # Otherwise, log a warning that the metadata record location does
            # not make sense.
            s3_full_location = get_s3_location(bucket, object_key)
            if record_id is None:
                json_contents = download_json_file_from_s3(
                    s3_client=s3_client,
                    bucket=bucket,
                    object_key=object_key,
                )
                if json_contents:
                    # noinspection PyTypeChecker
                    if is_record_location_valid(
                        json_contents,
                        expected_bucket=bucket,
                        expected_prefix=s3_prefix,
                    ):
                        db = docdb_client[self.job_settings.doc_db_db_name]
                        collection = db[
                            self.job_settings.doc_db_collection_name
                        ]
                        if "_id" in json_contents:
                            # TODO: check is_dict_corrupt(json_contents)
                            response = collection.update_one(
                                {"_id": json_contents["_id"]},
                                {"$set": json_contents},
                                upsert=True,
                            )
                            logging.info(response.raw_result)
                            cond_copy_then_sync_core_json_files(
                                metadata_json=json.dumps(
                                    json_contents, default=str
                                ),
                                bucket=bucket,
                                prefix=s3_prefix,
                                s3_client=s3_client,
                                log_flag=True,
                                copy_original_md_subdir=(
                                    self.job_settings.copy_original_md_subdir
                                ),
                            )
                        else:
                            logging.warning(
                                f"Metadata record for {location} "
                                f"does not have an _id field!"
                            )
                    else:
                        logging.warning(
                            f"Location field {json_contents.get('location')} "
                            f"or name field {json_contents.get('name')} does "
                            f"not match actual location of record {location}!"
                        )
                else:
                    logging.warning(
                        f"Unable to download file from S3 for: "
                        f"{s3_full_location}!"
                    )
            else:
                logging.info(
                    f"Metadata record for {s3_full_location} "
                    f"already exists in DocDb. Skipping."
                )
        else:  # metadata.nd.json file does not exist in S3. Create a new one.
            # Build a new metadata file, save it to S3 and save it to DocDb.
            # Also copy the original core json files to a subfolder and then
            # overwrite them with the new fields from metadata.nd.json.
            new_metadata_contents = build_metadata_record_from_prefix(
                bucket=bucket,
                prefix=s3_prefix,
                s3_client=s3_client,
            )
            if new_metadata_contents is not None:
                # noinspection PyTypeChecker
                cond_copy_then_sync_core_json_files(
                    metadata_json=new_metadata_contents,
                    bucket=bucket,
                    prefix=s3_prefix,
                    s3_client=s3_client,
                    log_flag=True,
                    copy_original_md_subdir=(
                        self.job_settings.copy_original_md_subdir
                    ),
                )
                logging.info(f"Uploading metadata record for: {location}")
                # noinspection PyTypeChecker
                s3_response = upload_metadata_json_str_to_s3(
                    metadata_json=new_metadata_contents,
                    bucket=bucket,
                    prefix=s3_prefix,
                    s3_client=s3_client,
                )
                logging.info(s3_response)
                # Assume Lambda function will move it to DocDb. If it doesn't,
                # then next index job will pick it up.
            else:
                logging.warning(
                    f"Unable to build metadata record for: {location}!"
                )

    def _dask_task_to_process_prefix_list(self, prefix_list: List[str]):
        """
        The task to perform within a partition. If n_partitions is set to 20
        and the outer prefix list had length 1000, then this should process
        50 prefixes.
        Parameters
        ----------
        prefix_list : List[str]

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
        # For the given prefix list, download all the records from docdb
        # with those locations.
        location_to_id_map = build_docdb_location_to_id_map(
            bucket=self.job_settings.s3_bucket,
            prefixes=prefix_list,
            db_name=self.job_settings.doc_db_db_name,
            collection_name=self.job_settings.doc_db_collection_name,
            docdb_client=doc_db_client,
        )
        for prefix in prefix_list:
            try:
                self._process_prefix(
                    s3_prefix=prefix,
                    s3_client=s3_client,
                    location_to_id_map=location_to_id_map,
                    docdb_client=doc_db_client,
                )
            except Exception as e:
                logging.error(
                    f"Error processing "
                    f"{get_s3_location(self.job_settings.s3_bucket, prefix)}: "
                    f"{repr(e)}"
                )
        s3_client.close()
        doc_db_client.close()

    def _process_prefixes(self, prefixes: List[str]):
        """
        For a list of prefixes (up to a 1000 in the list), divvy up the list
        across n_partitions. Process the set of prefixes in each partition.
        Parameters
        ----------
        prefixes : List[str]

        """
        prefix_bag = dask_bag.from_sequence(
            prefixes, npartitions=self.job_settings.n_partitions
        )
        mapped_partitions = dask_bag.map_partitions(
            self._dask_task_to_process_prefix_list, prefix_bag
        )
        mapped_partitions.compute()

    def run_job(self):
        """Main method to run."""
        logging.info("Starting to scan through DocDb.")
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
                "location": {
                    "$regex": f"^s3://{self.job_settings.s3_bucket}.*"
                }
            },
        )
        for page in docdb_pages:
            self._process_records(records=page)
        iterator_docdb_client.close()
        logging.info("Finished scanning through DocDb.")
        logging.info("Starting to scan through S3.")
        iterator_s3_client = boto3.client("s3")
        prefix_iterator = iterate_through_top_level(
            s3_client=iterator_s3_client, bucket=self.job_settings.s3_bucket
        )
        for prefix_list in prefix_iterator:
            self._process_prefixes(
                prefixes=prefix_list,
            )
        iterator_s3_client.close()
        logging.info("Finished scanning through S3.")


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
        main_job_settings = AindIndexBucketJobSettings.model_validate_json(
            cli_args.job_settings
        )
    else:
        main_job_settings = AindIndexBucketJobSettings.from_param_store(
            param_store_name=cli_args.param_store_name
        )
    main_job = AindIndexBucketJob(job_settings=main_job_settings)
    main_job.run_job()
