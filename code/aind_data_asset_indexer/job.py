import json
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict, Optional
from urllib.parse import urlparse

import boto3
import pytz
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_access_api.document_store import Client as DsClient
from aind_data_access_api.document_store import (
    DataAssetRecord,
    DocumentStoreCredentials,
)
from botocore.exceptions import ClientError


class JobRunner:
    def __init__(
        self,
        doc_store_secrets_name,
        collection_name,
        codeocean_token,
        codeocean_domain,
        data_asset_bucket,
    ):
        self.codeocean_client = CodeOceanClient(
            domain=codeocean_domain, token=codeocean_token
        )
        self.doc_store_client = DsClient(
            credentials=DocumentStoreCredentials(
                aws_secrets_name=doc_store_secrets_name
            ),
            collection_name=collection_name,
        )
        self.data_asset_bucket = data_asset_bucket

    @staticmethod
    def _map_raw_asset_to_record(
        co_response: Dict,
    ) -> Optional[DataAssetRecord]:
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
                id=data_asset_id,
                name=name,
                created=created_datetime,
                location=location,
            )

            return base_record

    def _map_processed_result_to_record(
        self, co_response: Dict
    ) -> Optional[DataAssetRecord]:
        name = co_response["name"]
        data_asset_id = co_response["id"]
        prefix = data_asset_id
        bucket = self.data_asset_bucket
        created_timestamp = co_response["created"]
        created_datetime = datetime.fromtimestamp(
            created_timestamp, tz=pytz.UTC
        )
        if prefix.endswith("/"):
            unslashed_prefix = prefix[:-1]
        else:
            unslashed_prefix = prefix
        location = f"s3://{bucket}/{unslashed_prefix}"
        base_record = DataAssetRecord(
            id=data_asset_id,
            name=name,
            created=created_datetime,
            location=location,
        )
        return base_record

    def _map_co_response_to_record(
        self, co_response
    ) -> Optional[DataAssetRecord]:
        tags = co_response["tags"]
        data_type = co_response["type"]

        if "processed" in tags and data_type == "result":
            record = self._map_processed_result_to_record(co_response)
        elif "raw" in tags and data_type == "dataset":
            record = self._map_raw_asset_to_record(co_response)
        else:
            record = None
        return record

    @staticmethod
    def _update_record_with_s3_json_files(
        s3_client, bucket, s3_response, base_record: DataAssetRecord
    ) -> None:
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

    def _attach_json_files_to_base_record(
        self, s3_client, base_record: DataAssetRecord
    ) -> None:
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
                s3_client, bucket, s3_response, base_record
            )

    def run_job(self) -> None:
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
            self._attach_json_files_to_base_record(s3_client, record)
        s3_client.close()
        self.doc_store_client.upsert_list_of_records(base_records)

        self.doc_store_client.close()

        return None
