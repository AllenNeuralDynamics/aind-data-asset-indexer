"""Module to facilitate running a capsule job to populate a doc store"""

import json
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict, Union
from urllib.parse import urlparse

import boto3
import pytz
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_access_api.document_store import Client as DsClient
from aind_data_access_api.document_store import DocumentStoreCredentials
from aind_data_access_api.models import DataAssetRecord
from botocore.exceptions import ClientError


class JobRunner:
    """Class to handle populating a document store"""

    def __init__(
        self,
        doc_store_credentials: DocumentStoreCredentials,
        collection_name,
        codeocean_token,
        codeocean_domain,
        data_asset_bucket,
    ):
        """Class Constructor"""
        self.codeocean_client = CodeOceanClient(
            domain=codeocean_domain, token=codeocean_token
        )
        self.doc_store_client = DsClient(
            credentials=doc_store_credentials,
            collection_name=collection_name,
        )
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

        source_bucket = co_response.get("sourceBucket")
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

        if tags is not None and "processed" in tags and data_type == "result":
            record = self._map_processed_result_to_record(co_response)
        elif (
            tags is not None
            and ("raw" in tags or "processed" in tags)
            and data_type == "dataset"
        ):
            record = self._map_raw_asset_to_record(co_response)
        else:
            record = None
        return record

    @staticmethod
    def _update_record_with_s3_json_files(
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

        # Get data asset information from code ocean
        code_ocean_response = self.codeocean_client.search_all_data_assets()
        base_records = [
            self._map_co_response_to_record(response)
            for response in list(code_ocean_response.json()["results"])
            if self._map_co_response_to_record(response) is not None
        ]

        # Remove records in doc store that are no longer in code ocean
        base_record_ids = [r.id for r in base_records]
        doc_store_ids = [
            r["_id"]
            for r in self.doc_store_client.collection.find({}, {"_id": 1})
        ]
        record_ids_to_remove = set(doc_store_ids) - set(base_record_ids)
        for rec_id in record_ids_to_remove:
            self.doc_store_client.collection.delete_one({"_id": rec_id})

        # Attach top-level json files to base_records
        s3_client = boto3.client("s3")
        for record in base_records:
            self._update_base_record_with_s3_info(s3_client, record)
        s3_client.close()
        self.doc_store_client.upsert_list_of_records(base_records)

        self.doc_store_client.close()

        return None
