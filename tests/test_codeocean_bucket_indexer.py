"""Tests methods in codeocean_bucket_indexer module"""

import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytz

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
                "created": datetime(2024, 6, 12, 21, 21, 28, tzinfo=pytz.UTC),
                "external_links": {
                    "Code Ocean": "11ee1e1e-11e1-1111-1111-e11eeeee1e11"
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
                "created": datetime(2024, 6, 12, 19, 45, 59, tzinfo=pytz.UTC),
                "external_links": {
                    "Code Ocean": "666666cc-66cc-6c66-666c-6c66c6666666"
                },
            },
        ]
        cls.example_dict_of_file_info = {
            "ecephys_642478_2023-01-17_13-56-29/acquisition.json": None,
            "ecephys_642478_2023-01-17_13-56-29/data_description.json": None,
            "ecephys_642478_2023-01-17_13-56-29/instrument.json": None,
            "ecephys_642478_2023-01-17_13-56-29/procedures.json": None,
            "ecephys_642478_2023-01-17_13-56-29/processing.json": None,
            "ecephys_642478_2023-01-17_13-56-29/rig.json": None,
            "ecephys_642478_2023-01-17_13-56-29/session.json": None,
            "ecephys_642478_2023-01-17_13-56-29/subject.json": None,
            "ecephys_642478_2023-01-17_13-56-29/mri_session.json": None,
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
