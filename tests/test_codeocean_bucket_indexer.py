"""Tests methods in codeocean_bucket_indexer module"""

import json
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from pymongo.operations import UpdateOne
from requests import Response
from requests.exceptions import ReadTimeout

from aind_data_asset_indexer.codeocean_bucket_indexer import (
    CodeOceanIndexBucketJob,
)
from aind_data_asset_indexer.models import CodeOceanIndexBucketJobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
TEST_UTILS_DIR = TEST_DIR / "resources" / "utils"


class TestCodeOceanIndexBucketJob(unittest.TestCase):
    """Class to test methods in CodeOceanIndexBucketJob class."""

    @classmethod
    def setUpClass(cls) -> None:
        """Load json files into memory."""

        basic_job_configs = CodeOceanIndexBucketJobSettings(
            s3_bucket="some_bucket",
            doc_db_host="some_docdb_host",
            doc_db_port=12345,
            doc_db_password="some_docdb_password",
            doc_db_user_name="some_docdb_username",
            doc_db_db_name="some_docdb_dbname",
            doc_db_collection_name="some_docdb_collection_name",
            codeocean_domain="some_co_domain",
            codeocean_token="some_co_token",
            temp_codeocean_endpoint="http://some_url:8080/created_after/0",
        )
        cls.basic_job_configs = basic_job_configs
        cls.basic_job = CodeOceanIndexBucketJob(job_settings=basic_job_configs)
        cls.example_codeocean_records = [
            {
                "name": (
                    "ecephys_712141_2024-06-06_10-44-36_"
                    "sorted_2024-06-12_21-21-28"
                ),
                "location": (
                    "s3://some_co_bucket/11ee1e1e-11e1-1111-1111-e11eeeee1e11"
                ),
                "created": datetime(
                    2024, 6, 12, 21, 21, 28, tzinfo=timezone.utc
                ),
                "external_links": {
                    "Code Ocean": ["11ee1e1e-11e1-1111-1111-e11eeeee1e11"]
                },
            },
            {
                "name": (
                    "ecephys_712815_2024-05-22_12-26-32_"
                    "sorted_2024-06-12_19-45-59"
                ),
                "location": (
                    "s3://some_co_bucket/666666cc-66cc-6c66-666c-6c66c6666666"
                ),
                "created": datetime(
                    2024, 6, 12, 19, 45, 59, tzinfo=timezone.utc
                ),
                "external_links": {
                    "Code Ocean": ["666666cc-66cc-6c66-666c-6c66c6666666"]
                },
            },
        ]
        cls.example_dict_of_file_info = {
            "ecephys_642478_2023-01-17_13-56-29/acquisition.json": None,
            "ecephys_642478_2023-01-17_13-56-29/data_description.json": None,
            "ecephys_642478_2023-01-17_13-56-29/instrument.json": None,
            "ecephys_642478_2023-01-17_13-56-29/procedures.json": None,
            "ecephys_642478_2023-01-17_13-56-29/processing.json": None,
            "ecephys_642478_2023-01-17_13-56-29/quality_control.json": None,
            "ecephys_642478_2023-01-17_13-56-29/rig.json": None,
            "ecephys_642478_2023-01-17_13-56-29/session.json": None,
            "ecephys_642478_2023-01-17_13-56-29/subject.json": None,
        }
        cls.example_docdb_records = [
            {
                "location": (
                    "s3://some_co_bucket/"
                    "666666cc-66cc-6c66-666c-6c66c6666666"
                ),
                "_id": "abc-123",
            },
            {
                "location": (
                    "s3://some_co_bucket/"
                    "22ee2e2e-22e2-2222-2222-e22eeeee2e22"
                ),
                "_id": "efg-456",
            },
        ]

        cls.example_temp_endpoint_response = [
            {"id": "abc-123", "source": "s3://bucket/prefix1"},
            {"id": "def-456", "source": "s3://bucket/prefix1"},
            {"id": "ghi-789", "source": "s3://bucket/prefix2"},
        ]

    @patch("requests.get")
    def test_get_external_data_asset_records(self, mock_get: MagicMock):
        """Tests the _get_external_data_asset_records method"""
        example_response = self.example_temp_endpoint_response
        mock_get_response = Response()
        mock_get_response.status_code = 200
        mock_get_response._content = json.dumps(example_response).encode(
            "utf-8"
        )
        mock_get.return_value = mock_get_response
        response = self.basic_job._get_external_data_asset_records()
        self.assertEqual(example_response, response)

    @patch("requests.get")
    def test_get_external_data_asset_records_error(self, mock_get: MagicMock):
        """Tests the _get_external_data_asset_records method when an error
        response is returned"""
        mock_get_response = Response()
        mock_get_response.status_code = 500
        mock_get.return_value = mock_get_response
        response = self.basic_job._get_external_data_asset_records()
        self.assertIsNone(response)

    @patch("requests.get")
    @patch("logging.error")
    def test_get_external_data_asset_records_read_timeout(
        self, mock_error: MagicMock, mock_get: MagicMock
    ):
        """Tests the _get_external_data_asset_records method when the read
        times out."""
        mock_get.side_effect = ReadTimeout()
        response = self.basic_job._get_external_data_asset_records()
        self.assertIsNone(response)
        mock_error.assert_called_once_with(
            "Read timed out at http://some_url:8080/created_after/0"
        )

    def test_map_external_list_to_dict(self):
        """Tests _map_external_list_to_dict method"""
        mapped_response = self.basic_job._map_external_list_to_dict(
            self.example_temp_endpoint_response
        )
        expected_response = {
            "s3://bucket/prefix1": {"abc-123", "def-456"},
            "s3://bucket/prefix2": {"ghi-789"},
        }
        self.assertEqual(expected_response, mapped_response)

    def test_get_co_links_from_record(self):
        """Tests _get_co_links_from_record method"""
        docdb_record = {
            "_id": "12345",
            "location": "s3://bucket/prefix",
            "external_links": {"Code Ocean": ["abc-123", "def-456"]},
        }
        output = self.basic_job._get_co_links_from_record(
            docdb_record=docdb_record
        )
        self.assertEqual(["abc-123", "def-456"], output)

    def test_get_co_links_from_record_legacy(self):
        """Tests _get_co_links_from_record method with legacy format"""
        docdb_record = {
            "_id": "12345",
            "location": "s3://bucket/prefix",
            "external_links": [
                {"Code Ocean": "abc-123"},
                {"Code Ocean": "def-456"},
            ],
        }
        output = self.basic_job._get_co_links_from_record(
            docdb_record=docdb_record
        )
        self.assertEqual(["abc-123", "def-456"], output)

    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    @patch("requests.get")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.paginate_docdb")
    @patch("logging.info")
    @patch("logging.debug")
    @patch("logging.error")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.datetime")
    def test_update_external_links_in_docdb(
        self,
        mock_datetime: MagicMock,
        mock_error: MagicMock,
        mock_debug: MagicMock,
        mock_info: MagicMock,
        mock_paginate: MagicMock,
        mock_get: MagicMock,
        mock_docdb_client: MagicMock,
    ):
        """Tests _update_external_links_in_docdb method."""
        mock_datetime.utcnow.return_value = datetime(2024, 9, 5)

        # Mock requests get response
        example_response = self.example_temp_endpoint_response
        mock_get_response = Response()
        mock_get_response.status_code = 200
        mock_get_response._content = json.dumps(example_response).encode(
            "utf-8"
        )
        mock_get.return_value = mock_get_response

        # Mock bulk_write
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.bulk_write.return_value = {"message": "success"}

        # Mock paginate
        mock_paginate.return_value = [
            [
                {
                    "_id": "0000",
                    "location": "s3://bucket/prefix1",
                    "external_links": {"Code Ocean": ["abc-123"]},
                },
                {
                    "_id": "0001",
                    "location": "s3://bucket/prefix2",
                    "external_links": {"Code Ocean": ["ghi-789"]},
                },
                {
                    "_id": "0002",
                    "location": "s3://bucket2/prefix3",
                    "external_links": [{"Code Ocean": "xyz-789"}],
                },
            ]
        ]

        self.basic_job._update_external_links_in_docdb(
            docdb_client=mock_docdb_client
        )
        expected_bulk_write_calls = [
            call(
                requests=[
                    UpdateOne(
                        {"_id": "0000"},
                        {
                            "$set": {
                                "external_links": {
                                    "Code Ocean": ["abc-123", "def-456"]
                                },
                                "last_modified": "2024-09-05T00:00:00",
                            }
                        },
                        False,
                        None,
                        None,
                        None,
                    ),
                    UpdateOne(
                        {"_id": "0002"},
                        {
                            "$set": {
                                "external_links": {"Code Ocean": []},
                                "last_modified": "2024-09-05T00:00:00",
                            }
                        },
                        False,
                        None,
                        None,
                        None,
                    ),
                ]
            )
        ]

        mock_error.assert_not_called()
        mock_info.assert_has_calls(
            [
                call(
                    "No code ocean data asset ids found for "
                    "s3://bucket2/prefix3. "
                    "Removing external links from record."
                ),
                call("Updating 2 records"),
            ]
        )
        mock_debug.assert_called_once_with({"message": "success"})
        mock_collection.bulk_write.assert_has_calls(expected_bulk_write_calls)

    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    @patch("requests.get")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.paginate_docdb")
    @patch("logging.info")
    @patch("logging.debug")
    @patch("logging.error")
    def test_update_external_links_in_docdb_error(
        self,
        mock_error: MagicMock,
        mock_debug: MagicMock,
        mock_info: MagicMock,
        mock_paginate: MagicMock,
        mock_get: MagicMock,
        mock_docdb_client: MagicMock,
    ):
        """Tests _update_external_links_in_docdb method when there is an
        error retrieving info from the temp endpoint."""
        # Mock requests get response
        mock_get_response = Response()
        mock_get_response.status_code = 500
        mock_get.return_value = mock_get_response

        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db

        self.basic_job._update_external_links_in_docdb(
            docdb_client=mock_docdb_client
        )

        mock_error.assert_called_once_with(
            "There was an error retrieving external links!"
        )
        mock_info.assert_not_called()
        mock_debug.assert_not_called()
        mock_paginate.assert_not_called()

    @patch("logging.info")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils.get_dict_of_file_info")
    @patch("aind_data_asset_indexer.utils.download_json_file_from_s3")
    def test_process_codeocean_record(
        self,
        mock_download_json_file: MagicMock,
        mock_get_dict_of_file_info: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_log_info: MagicMock,
    ):
        """Tests _process_codeocean_record method"""
        # Assume user didn't attach any metadata files
        mock_get_dict_of_file_info.return_value = (
            self.example_dict_of_file_info
        )
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.update_one.return_value.raw_result = {
            "message": "success"
        }

        self.basic_job._process_codeocean_record(
            codeocean_record=self.example_codeocean_records[1],
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
        )
        mock_log_info.assert_has_calls(
            [
                call(
                    "Uploading metadata record for: "
                    "s3://some_co_bucket/666666cc-66cc-6c66-666c-6c66c6666666"
                ),
                call({"message": "success"}),
            ]
        )
        mock_download_json_file.assert_not_called()
        self.assertEqual(
            "ecephys_712815_2024-05-22_12-26-32_sorted_2024-06-12_19-45-59",
            mock_collection.update_one.mock_calls[0].args[1]["$set"]["name"],
        )
        self.assertEqual(
            "s3://some_co_bucket/666666cc-66cc-6c66-666c-6c66c6666666",
            mock_collection.update_one.mock_calls[0].args[1]["$set"][
                "location"
            ],
        )

    @patch("aind_data_schema.core.metadata.Metadata.model_construct")
    @patch("logging.warning")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils.get_dict_of_file_info")
    @patch("aind_data_asset_indexer.utils.download_json_file_from_s3")
    def test_process_codeocean_record_warning(
        self,
        mock_download_json_file: MagicMock,
        mock_get_dict_of_file_info: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_log_warn: MagicMock,
        mock_model_construct: MagicMock,
    ):
        """Tests _process_codeocean_record when there is an issue building the
        record"""
        # Assume user didn't attach any metadata files
        mock_get_dict_of_file_info.return_value = (
            self.example_dict_of_file_info
        )

        # Suppose there is an error using model_construct
        mock_model_construct.side_effect = Exception("Something went wrong")

        self.basic_job._process_codeocean_record(
            codeocean_record=self.example_codeocean_records[1],
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
        )
        mock_download_json_file.assert_not_called()
        mock_log_warn.assert_called_once_with(
            "Unable to build metadata record for: "
            "s3://some_co_bucket/666666cc-66cc-6c66-666c-6c66c6666666!"
        )

    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "CodeOceanIndexBucketJob._process_codeocean_record"
    )
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    @patch("boto3.client")
    def test_dask_task_to_process_record_list(
        self,
        mock_boto3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_process_codeocean_record: MagicMock,
    ):
        """Tests _dask_task_to_process_record_list"""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        records = self.example_codeocean_records
        self.basic_job._dask_task_to_process_record_list(record_list=records)
        mock_process_codeocean_record.assert_has_calls(
            [
                call(
                    codeocean_record=records[0],
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    codeocean_record=records[1],
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
            ]
        )
        mock_s3_client.close.assert_called_once_with()
        mock_mongo_client.close.assert_called_once_with()

    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "CodeOceanIndexBucketJob._process_codeocean_record"
    )
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.error")
    def test_dask_task_to_process_record_list_error(
        self,
        mock_log_error: MagicMock,
        mock_boto3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_process_codeocean_record: MagicMock,
    ):
        """Tests _dask_task_to_process_record_list when there is an error in 1
        record"""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        records = self.example_codeocean_records
        mock_process_codeocean_record.side_effect = [
            Exception("Error processing record"),
            None,
        ]
        self.basic_job._dask_task_to_process_record_list(record_list=records)
        mock_process_codeocean_record.assert_has_calls(
            [
                call(
                    codeocean_record=records[0],
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    codeocean_record=records[1],
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
            ]
        )
        mock_log_error.assert_called_once_with(
            "Error processing s3://some_co_bucket/"
            "11ee1e1e-11e1-1111-1111-e11eeeee1e11: "
            "Exception('Error processing record')"
        )
        mock_s3_client.close.assert_called_once_with()
        mock_mongo_client.close.assert_called_once_with()

    @patch("dask.bag.map_partitions")
    def test_process_codeocean_records(
        self, mock_dask_bag_map_parts: MagicMock
    ):
        """Test _process_codeocean_records method."""
        example_records = self.example_codeocean_records
        self.basic_job._process_codeocean_records(example_records)
        mock_dask_bag_map_parts.assert_called()

    @patch("logging.info")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    def test_dask_task_to_delete_record_list(
        self, mock_docdb_client: MagicMock, mock_log_info: MagicMock
    ):
        """Tests _dask_task_to_delete_record_list"""
        mock_db = MagicMock()
        mock_docdb_client.return_value.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.delete_many.return_value.raw_result = {
            "message": "success"
        }
        records_to_delete = [r["_id"] for r in self.example_docdb_records]
        self.basic_job._dask_task_to_delete_record_list(
            record_list=records_to_delete
        )
        mock_log_info.assert_called_once_with({"message": "success"})

    @patch("logging.error")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    def test_dask_task_to_delete_record_list_error(
        self, mock_docdb_client: MagicMock, mock_log_error: MagicMock
    ):
        """Tests _dask_task_to_delete_record_list"""
        mock_db = MagicMock()
        mock_docdb_client.return_value.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.delete_many.side_effect = Exception(
            "Error deleting records"
        )
        records_to_delete = [r["_id"] for r in self.example_docdb_records]
        self.basic_job._dask_task_to_delete_record_list(
            record_list=records_to_delete
        )
        mock_log_error.assert_called_once_with(
            "Error deleting records: Exception('Error deleting records')"
        )

    @patch("dask.bag.map_partitions")
    def test_delete_records_from_docdb(
        self, mock_dask_bag_map_parts: MagicMock
    ):
        """Test _delete_records_from_docdb method."""
        records_to_delete = [r["_id"] for r in self.example_docdb_records]
        self.basic_job._delete_records_from_docdb(
            record_list=records_to_delete
        )
        mock_dask_bag_map_parts.assert_called()

    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "CodeOceanIndexBucketJob._update_external_links_in_docdb"
    )
    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "CodeOceanIndexBucketJob._delete_records_from_docdb"
    )
    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "CodeOceanIndexBucketJob._process_codeocean_records"
    )
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.paginate_docdb")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MongoClient")
    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "get_all_processed_codeocean_asset_records"
    )
    @patch("logging.info")
    def test_run_job(
        self,
        mock_log_info: MagicMock,
        mock_get_all_co_records: MagicMock,
        mock_docdb_client: MagicMock,
        mock_paginate_docdb: MagicMock,
        mock_process_codeocean_records: MagicMock,
        mock_delete_records_from_docdb: MagicMock,
        mock_update_external_links_in_docdb: MagicMock,
    ):
        """Tests run_job method. Given the example responses, should ignore
        one record, add one record, and delete one record."""
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        mock_get_all_co_records.return_value = dict(
            [(r["location"], r) for r in self.example_codeocean_records]
        )
        mock_paginate_docdb.return_value = [self.example_docdb_records]
        self.basic_job.run_job()

        mock_update_external_links_in_docdb.assert_called_once_with(
            docdb_client=mock_mongo_client
        )
        mock_process_codeocean_records.assert_called_once_with(
            records=[self.example_codeocean_records[0]]
        )
        mock_delete_records_from_docdb.assert_called_once_with(
            record_list=["efg-456"]
        )
        mock_log_info.assert_has_calls(
            [
                call("Starting to scan through CodeOcean."),
                call("Finished scanning through CodeOcean."),
                call("Starting to scan through DocDb."),
                call("Adding links to records."),
                call("Finished adding links to records"),
                call("Finished scanning through DocDB."),
                call("Starting to add records to DocDB."),
                call("Finished adding records to DocDB."),
                call("Starting to delete records from DocDB."),
                call("Finished deleting records from DocDB."),
            ]
        )
        mock_mongo_client.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
