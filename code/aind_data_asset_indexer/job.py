"""Module to facilitate running a capsule job to populate a doc store"""

import json
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict, Union
from urllib.parse import urlparse

import boto3
import pytz
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.models import DataAssetRecord
from aind_data_schema.core.data_description import DataLevel
from botocore.exceptions import ClientError


class JobRunner:
    """Class to handle populating a document store"""

    def __init__(
        self,
        doc_db_client: MetadataDbClient,
        codeocean_client: CodeOceanClient,
        data_asset_bucket: str,
    ):
        """Class Constructor"""
        self.codeocean_client = codeocean_client
        self.doc_db_client = doc_db_client
        self.data_asset_bucket = data_asset_bucket

    @staticmethod
    def _map_raw_asset_to_record(
        co_response: Dict,
    ) -> Union[DataAssetRecord, None]:
        """
        Maps a raw dataasset type to a DataAssetRecord type
        Parameters
        ----------
        co_response : Dict
          The response from code ocean when requesting a data asset

        Returns
        -------
        Union[DataAssetRecord, None]
          The mapped response to internal model if possible. Otherwise, None.

        """

        if "sourceBucket" in co_response.keys():
            source_bucket_key = "sourceBucket"
        else:
            source_bucket_key = "source_bucket"
        source_bucket = co_response.get(source_bucket_key)
        bucket = source_bucket.get("bucket") if source_bucket else None
        prefix = source_bucket.get("prefix") if source_bucket else None
        origin = source_bucket.get("origin") if source_bucket else None
        if (bucket == "") or (origin != "aws"):
            return None
        else:
            created_timestamp = co_response["created"]
            created_datetime = datetime.fromtimestamp(
                created_timestamp, tz=pytz.UTC
            )
            name = co_response["name"]
            data_asset_id = co_response["id"]
            if prefix.endswith("/"):
                unslashed_prefix = prefix[:-1]
            else:
                unslashed_prefix = prefix
            location = f"s3://{bucket}/{unslashed_prefix}"
            base_record = DataAssetRecord(
                _id=data_asset_id,
                _name=name,
                _created=created_datetime,
                _location=location,
            )

            return base_record

    def _map_processed_result_to_record(
        self, co_response: Dict
    ) -> Union[DataAssetRecord, None]:
        """
        Maps a processed type to a DataAssetRecord type
        Parameters
        ----------
        co_response : Dict
          The response from code ocean when requesting a result

        Returns
        -------
        Union[DataAssetRecord, None]
          The mapped response to internal model if possible. Otherwise, None.

        """
        name = co_response["name"]
        data_asset_id = co_response["id"]
        prefix = data_asset_id
        bucket = self.data_asset_bucket
        created_timestamp = co_response["created"]
        created_datetime = datetime.fromtimestamp(
            created_timestamp, tz=pytz.UTC
        )
        location = f"s3://{bucket}/{prefix}"
        base_record = DataAssetRecord(
            _id=data_asset_id,
            _name=name,
            _created=created_datetime,
            _location=location,
        )
        return base_record

    def _map_co_response_to_record(
        self, co_response: Dict
    ) -> Union[DataAssetRecord, None]:
        """
        Maps a response to a DataAssetRecord type
        Parameters
        ----------
        co_response : Dict
          The response from code ocean when requesting a result

        Returns
        -------
        Union[DataAssetRecord, None]

        """

        tags = co_response.get("tags")
        data_type = co_response.get("type")

        # For legacy purposes, we can check for both processed and derived in
        # the tags. We can phase out processed.
        if (
            tags is not None
            and ("processed" in tags or DataLevel.DERIVED.value in tags)
            and data_type == "result"
        ):
            record = self._map_processed_result_to_record(co_response)
        elif (
            tags is not None
            and (
                "raw" in tags
                or DataLevel.RAW.value in tags
                or "processed" in tags
                or DataLevel.DERIVED.value in tags
            )
            and data_type == "dataset"
        ):
            record = self._map_raw_asset_to_record(co_response)
        else:
            record = None
        return record

    def _sanitize_keys(self, old_dict: dict) -> dict:
        """
        DocumentDB doesn't support '$' or '.' in field names. This method
        replaces those characters with '_'
        Parameters
        ----------
        old_dict : dict

        Returns
        -------
        dict

        """
        new_dict = {}
        for key in old_dict.keys():
            new_key = key.replace("$", "_").replace(".", "_")
            if isinstance(old_dict[key], dict):
                new_dict[new_key] = self._sanitize_keys(old_dict[key])
            else:
                new_dict[new_key] = old_dict[key]
        return new_dict

    def _update_record_with_s3_json_files(
        self,
        s3_client: boto3.session.Session.client,
        s3_response: Dict,
        base_record: DataAssetRecord,
    ) -> None:
        """

        Parameters
        ----------
        s3_client : boto3.session.Session.client
        s3_response : Dict
        base_record : DataAssetRecord

        Returns
        -------
        None

        """
        url_parts = urlparse(base_record.location)
        bucket = url_parts.netloc

        json_files_in_s3_folder = [
            r["Key"]
            for r in s3_response["Contents"]
            if r["Key"].endswith(".json")
        ]
        for json_file_key in json_files_in_s3_folder:
            try:
                json_file_name = json_file_key.split("/")[-1].replace(
                    ".json", ""
                )
                result = s3_client.get_object(Bucket=bucket, Key=json_file_key)
                contents = result["Body"].read().decode("utf-8")
                json_contents = json.loads(contents)
                if json_contents is not None and isinstance(
                    json_contents, dict
                ):
                    sanitized_contents = self._sanitize_keys(json_contents)
                    setattr(base_record, json_file_name, sanitized_contents)
                elif json_contents is not None and isinstance(
                    json_contents, list
                ):
                    setattr(base_record, json_file_name, json_contents)
            except (ClientError, JSONDecodeError):
                pass

    def _update_base_record_with_s3_info(
        self,
        s3_client: boto3.session.Session.client,
        base_record: DataAssetRecord,
    ) -> None:
        """
        Checks the s3 location for json files to attach to the base record
        Parameters
        ----------
        s3_client : boto3.session.Session.client
        base_record : DataAssetRecord

        Returns
        -------
        None

        """
        url_parts = urlparse(base_record.location)
        bucket = url_parts.netloc
        prefix = url_parts.path[1:]
        s3_response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=f"{prefix}/",
            Delimiter="/",
        )
        contents_from_s3 = s3_response.get("Contents")
        if contents_from_s3 is not None:
            self._update_record_with_s3_json_files(
                s3_client, s3_response, base_record
            )

    def run_job(self) -> None:
        """Method to run the doc store populate job."""
        print("Starting job")

        # Get data asset information from code ocean
        code_ocean_response = self.codeocean_client.search_all_data_assets()
        base_records = [
            self._map_co_response_to_record(response)
            for response in list(code_ocean_response.json()["results"])
            if self._map_co_response_to_record(response) is not None
        ]

        print(f"Number of codeocean records found: {len(base_records)}")

        base_record_ids = [r.id for r in base_records]
        doc_db_records = self.doc_db_client.retrieve_data_asset_records(
            filter_query={},
            projection={"_id": 1, "_name": 1, "_created": 1, "_location": 1},
            paginate=False,
        )
        doc_store_ids = [r.id for r in doc_db_records]
        record_ids_to_remove = set(doc_store_ids) - set(base_record_ids)

        print(
            f"Number of records in doc db not found in code ocean:"
            f" {len(record_ids_to_remove)}"
        )
        print(f"Removing {len(record_ids_to_remove)} entries from docdb")
        del_rec_response = self.doc_db_client.delete_many_records(
            list(record_ids_to_remove)
        )
        # TODO: add stream handler to use logging in capsule instead of print
        print(
            f"Finished removing records. Delete Response:"
            f" {del_rec_response.status_code},"
            f" {del_rec_response.json()}"
        )

        # Attach top-level json files to base_records
        s3_client = boto3.client("s3")
        for record in base_records:
            self._update_base_record_with_s3_info(s3_client, record)
        s3_client.close()
        print("Finished downloading from s3.")
        print("Updating docdb")
        docdb_response = self.doc_db_client.upsert_list_of_records(
            base_records
        )
        print(f"Number of responses: {len(docdb_response)}")
        bad_responses = [
            r for r in docdb_response if getattr(r, "status_code", None) != 200
        ]
        print(f"Number of bad responses: {len(bad_responses)}")
        for response in bad_responses:
            print(response)

        return None
