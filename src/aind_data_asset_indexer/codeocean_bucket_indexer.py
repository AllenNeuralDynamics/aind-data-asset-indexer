"""Module to index Code Ocean processed results and update external links in
DocDB."""

import argparse
import json
import logging
import os
import sys
import warnings
from datetime import datetime
from typing import List, Optional, Union

import boto3
import dask.bag as dask_bag
import requests
from aind_data_schema.core.metadata import ExternalPlatforms
from codeocean import CodeOcean
from mypy_boto3_s3 import S3Client
from pymongo import MongoClient
from pymongo.operations import UpdateOne
from requests.exceptions import ReadTimeout
from urllib3.util import Retry

from aind_data_asset_indexer.models import CodeOceanIndexBucketJobSettings
from aind_data_asset_indexer.utils import (
    build_metadata_record_from_prefix,
    get_all_processed_codeocean_asset_records,
    get_s3_bucket_and_prefix,
    paginate_docdb,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
# pydantic raises too many serialization warnings
warnings.filterwarnings("ignore", category=UserWarning)


class CodeOceanIndexBucketJob:
    """This job will:
    1) For records in AIND buckets, update the external links with Code
    Ocean data asset ids if needed.
    2) Download all processed results records from the Code Ocean index
    3) Download all the records in DocDB for the Code Ocean bucket. The
    response is projected to just the {_id, location} fields.
    4) Creates a list of locations found in Code Ocean and a list of
    locations found in DocDB.
    5) For locations found in Code Ocean not in DocDB, a new record will be
    created from the aind-data-schema json files in S3.
    6) For locations in DocDB not found in Code Ocean, the records will be
    removed from DocDB.
    """

    def __init__(self, job_settings: CodeOceanIndexBucketJobSettings):
        """Class constructor."""
        self.job_settings = job_settings

    def _get_external_data_asset_records(self) -> Optional[List[dict]]:
        """
        Retrieves list of code ocean ids and locations for external data
        assets. The timeout is set to 600 seconds.
        Returns
        -------
        List[dict] | None
          List items have shape {"id": str, "location": str}. If error occurs,
          return None.
        """
        try:
            response = requests.get(
                self.job_settings.temp_codeocean_endpoint,
                timeout=600,
            )
            if response.status_code == 200:
                return response.json()
            else:
                return None
        except ReadTimeout:
            logging.error(
                f"Read timed out at "
                f"{self.job_settings.temp_codeocean_endpoint}"
            )
            return None

    @staticmethod
    def _map_external_list_to_dict(external_recs: List[dict]) -> dict:
        """
        Maps the response received from Code Ocean into a dict. For example,
        [{"id": "abc", "location": "s3://bucket/prefix},
        {"id": "def", "location": "s3://bucket/prefix"}]
        will be mapped to {"s3://bucket/prefix": ["abc", "def"]}

        Parameters
        ----------
        external_recs : List[dict]

        Returns
        -------
        dict

        """
        new_records = dict()
        for r in external_recs:
            location = r.get("source")
            rec_id = r["id"]
            if location is not None and new_records.get(location) is not None:
                old_id_set = new_records.get(location)
                old_id_set.add(rec_id)
                new_records[location] = old_id_set
            else:
                new_records[location] = {rec_id}
        return new_records

    @staticmethod
    def _get_co_links_from_record(
        docdb_record: Union[dict, list]
    ) -> List[str]:
        """
        Small utility to parse the external_links field of the docdb record.
        Supports the legacy type.

        Parameters
        ----------
        docdb_record : dict | list
          The legacy type was a list, while the current version is a dict.

        Returns
        -------
        List[str]

        """
        external_links = docdb_record.get("external_links", [])

        # Hopefully, ExternalPlatforms.CODEOCEAN doesn't change
        if isinstance(external_links, dict):
            external_links = external_links.get(
                ExternalPlatforms.CODEOCEAN.value, []
            )
        else:
            external_links = [
                r.get(ExternalPlatforms.CODEOCEAN.value)
                for r in external_links
            ]
        return external_links

    def _update_external_links_in_docdb(
        self, docdb_client: MongoClient
    ) -> None:
        """
        This method will:
        1) Retrieve a list of codeocean data asset ids and locations from CO
        2) Paginate through the docdb records where the location doesn't match
        the internal co bucket.
        3) Add or remove the external_links from the docdb record if needed.

        Parameters
        ----------
        docdb_client : MongoClient

        Returns
        -------
        None

        """
        # Should return a list like [{"id": co_id, "location": "s3://..."},]
        list_of_co_ids_and_locations = self._get_external_data_asset_records()
        db = docdb_client[self.job_settings.doc_db_db_name]
        collection = db[self.job_settings.doc_db_collection_name]
        if list_of_co_ids_and_locations is not None:
            co_loc_to_id_map = self._map_external_list_to_dict(
                list_of_co_ids_and_locations
            )
            pages = paginate_docdb(
                docdb_client=docdb_client,
                db_name=self.job_settings.doc_db_db_name,
                collection_name=self.job_settings.doc_db_collection_name,
                filter_query={
                    "location": {
                        "$not": {
                            "$regex": f"^s3://{self.job_settings.s3_bucket}.*"
                        }
                    }
                },
                projection={"_id": 1, "location": 1, "external_links": 1},
                page_size=500,
            )
            for page in pages:
                records_to_update = []
                for record in page:
                    location = record.get("location")
                    external_links = self._get_co_links_from_record(record)
                    code_ocean_ids = (
                        None
                        if location is None
                        else co_loc_to_id_map.get(location)
                    )
                    docdb_rec_id = record["_id"]
                    if (
                        external_links is not None
                        and code_ocean_ids is not None
                        and code_ocean_ids != set(external_links)
                    ):
                        new_external_links = code_ocean_ids
                    elif external_links is not None and not code_ocean_ids:
                        logging.info(
                            f"No code ocean data asset ids found for "
                            f"{location}. Removing external links from record."
                        )
                        new_external_links = dict()
                    else:
                        new_external_links = None
                    if new_external_links is not None:
                        record_links = {
                            ExternalPlatforms.CODEOCEAN.value: sorted(
                                list(new_external_links)
                            )
                        }
                        last_modified = datetime.utcnow().isoformat()
                        records_to_update.append(
                            UpdateOne(
                                filter={"_id": docdb_rec_id},
                                update={
                                    "$set": {
                                        "external_links": record_links,
                                        "last_modified": last_modified,
                                    }
                                },
                                upsert=False,
                            )
                        )
                if len(records_to_update) > 0:
                    logging.info(f"Updating {len(records_to_update)} records")
                    write_response = collection.bulk_write(
                        requests=records_to_update
                    )
                    logging.debug(write_response)
        else:
            logging.error("There was an error retrieving external links!")

    def _process_codeocean_record(
        self,
        codeocean_record: dict,
        docdb_client: MongoClient,
        s3_client: S3Client,
    ):
        """
        Processes a code ocean record. It's assumed that the check to verify
        the record is not in DocDB is done upstream.
        1) Using the s3 location in the codeocean record, build metadata file.
        2) Save metadata record to DocDB if no issue

        Parameters
        ----------
        codeocean_record : dict
        docdb_client : MongoClient
        s3_client : S3Client

        """
        location = codeocean_record["location"]
        created = codeocean_record["created"]
        external_links = codeocean_record["external_links"]
        name = codeocean_record["name"]
        url_parts = get_s3_bucket_and_prefix(location)
        bucket = url_parts["bucket"]
        s3_prefix = url_parts["prefix"]
        new_metadata_contents = build_metadata_record_from_prefix(
            bucket=bucket,
            prefix=s3_prefix,
            s3_client=s3_client,
            optional_name=name,
            optional_created=created,
            optional_external_links=external_links,
        )
        if new_metadata_contents is not None:
            logging.info(f"Uploading metadata record for: {location}")
            db = docdb_client[self.job_settings.doc_db_db_name]
            collection = db[self.job_settings.doc_db_collection_name]
            # noinspection PyTypeChecker
            json_contents = json.loads(new_metadata_contents)
            x = collection.update_one(
                {"_id": json_contents["_id"]},
                {"$set": json_contents},
                upsert=True,
            )
            logging.debug(x.raw_result)
        else:
            logging.warning(
                f"Unable to build metadata record for: {location}!"
            )

    def _dask_task_to_process_record_list(self, record_list: List[dict]):
        """
        The task to perform within a partition. If n_partitions is set to 20
        and the outer prefix list had length 1000, then this should process
        50 code ocean records.

        Parameters
        ----------
        record_list : List[dict]

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
            try:
                self._process_codeocean_record(
                    codeocean_record=record,
                    docdb_client=doc_db_client,
                    s3_client=s3_client,
                )
            except Exception as e:
                logging.error(
                    f'Error processing {record.get("location")}: {repr(e)}'
                )
        s3_client.close()
        doc_db_client.close()

    def _process_codeocean_records(self, records: List[dict]):
        """
        For a list of codeocean records, divvy up the list across
        n_partitions. Process the set of records in each partition.

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

    def _dask_task_to_delete_record_list(self, record_list: List[str]):
        """
        The task to perform within a partition. If n_partitions is set to 20
        and the outer prefix list had length 1000, then this should process
        50 ids.

        Parameters
        ----------
        record_list : List[str]

        """
        # create a s3_client here since dask doesn't serialize it
        docdb_client = MongoClient(
            host=self.job_settings.doc_db_host,
            port=self.job_settings.doc_db_port,
            retryWrites=False,
            directConnection=True,
            username=self.job_settings.doc_db_user_name,
            password=self.job_settings.doc_db_password.get_secret_value(),
            authSource="admin",
        )
        db = docdb_client[self.job_settings.doc_db_db_name]
        collection = db[self.job_settings.doc_db_collection_name]
        try:
            logging.info(f"Removing {len(record_list)} records")
            response = collection.delete_many(
                filter={"_id": {"$in": record_list}}
            )
            logging.debug(response.raw_result)
        except Exception as e:
            logging.error(f"Error deleting records: {repr(e)}")
        docdb_client.close()

    def _delete_records_from_docdb(self, record_list: List[str]):
        """
        Uses dask to partition the record_list. Each record will be removed
        from DocDB.

        Parameters
        ----------
        record_list : List[str]
          List of record ids to remove from DocDB

        """
        record_bag = dask_bag.from_sequence(
            record_list, npartitions=self.job_settings.n_partitions
        )
        mapped_partitions = dask_bag.map_partitions(
            self._dask_task_to_delete_record_list, record_bag
        )
        mapped_partitions.compute()

    def run_job(self):
        """Main method to run."""
        logging.info("Starting to scan through CodeOcean.")
        retry = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
        )
        co_client = CodeOcean(
            domain=self.job_settings.codeocean_domain,
            token=self.job_settings.codeocean_token.get_secret_value(),
            retries=retry,
        )
        code_ocean_records = get_all_processed_codeocean_asset_records(
            co_client=co_client,
            co_data_asset_bucket=self.job_settings.s3_bucket,
        )
        logging.info("Finished scanning through CodeOcean.")
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
        # Use existing client to add external links to fields
        logging.info("Adding links to records.")
        self._update_external_links_in_docdb(
            docdb_client=iterator_docdb_client
        )
        logging.info("Finished adding links to records")
        all_docdb_records = dict()
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
            projection={"location": 1, "_id": 1},
        )
        for page in docdb_pages:
            for record in page:
                all_docdb_records[record["location"]] = record["_id"]
        iterator_docdb_client.close()
        logging.info("Finished scanning through DocDB.")
        codeocean_locations = set(code_ocean_records.keys())
        docdb_locations = set(all_docdb_records.keys())
        records_to_add = []
        records_to_delete = []
        for location in codeocean_locations - docdb_locations:
            records_to_add.append(code_ocean_records[location])
        for location in docdb_locations - codeocean_locations:
            records_to_delete.append(all_docdb_records[location])

        logging.info("Starting to add records to DocDB.")
        self._process_codeocean_records(records=records_to_add)
        logging.info("Finished adding records to DocDB.")
        logging.info("Starting to delete records from DocDB.")
        self._delete_records_from_docdb(record_list=records_to_delete)
        logging.info("Finished deleting records from DocDB.")


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
