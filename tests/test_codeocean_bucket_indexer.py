"""Tests methods in codeocean_bucket_indexer module"""

import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from codeocean import CodeOcean
from codeocean.data_asset import (
    DataAsset,
    DataAssetOrigin,
    DataAssetState,
    DataAssetType,
    SourceBucket,
)
from requests import Response

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
        # corresponds to cls.example_codeocean_records[1]
        cls.example_dict_of_file_info = {
            "666666cc-66cc-6c66-666c-6c66c6666666/acquisition.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/data_description.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/instrument.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/procedures.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/processing.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/quality_control.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/rig.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/session.json": None,
            "666666cc-66cc-6c66-666c-6c66c6666666/subject.json": None,
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

        cls.example_search_iterator_response = [
            DataAsset(
                id="abc-123",
                created=0,
                name="prefix1",
                mount="prefix1",
                state=DataAssetState.Ready,
                type=DataAssetType.Dataset,
                last_used=0,
                source_bucket=SourceBucket(
                    bucket="bucket",
                    prefix="prefix1",
                    origin=DataAssetOrigin.AWS,
                ),
            ),
            DataAsset(
                id="def-456",
                created=0,
                name="prefix1",
                mount="prefix1",
                state=DataAssetState.Ready,
                type=DataAssetType.Dataset,
                last_used=0,
                source_bucket=SourceBucket(
                    bucket="bucket",
                    prefix="prefix1",
                    origin=DataAssetOrigin.AWS,
                ),
            ),
            DataAsset(
                id="ghi-789",
                created=0,
                name="prefix2",
                mount="prefix2",
                state=DataAssetState.Ready,
                type=DataAssetType.Dataset,
                last_used=0,
                source_bucket=SourceBucket(
                    bucket="bucket",
                    prefix="prefix2",
                    origin=DataAssetOrigin.AWS,
                ),
            ),
        ]

    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    def test_get_external_data_asset_records(self, mock_search: MagicMock):
        """Tests the _get_external_data_asset_records method"""
        mock_search.return_value = self.example_search_iterator_response
        response = self.basic_job._get_external_data_asset_records(
            co_client=CodeOcean(domain="www.example.com", token="")
        )
        expected_response = [
            {"id": "abc-123", "location": "s3://bucket/prefix1"},
            {"id": "def-456", "location": "s3://bucket/prefix1"},
            {"id": "ghi-789", "location": "s3://bucket/prefix2"},
        ]
        self.assertEqual(expected_response, response)

    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    def test_get_external_data_asset_records_err(self, mock_search: MagicMock):
        """Tests the _get_external_data_asset_records method when an error
        response is returned"""
        mock_search.side_effect = Exception("Something went wrong!")
        with self.assertLogs(level="DEBUG") as captured:
            response = self.basic_job._get_external_data_asset_records(
                co_client=CodeOcean(domain="www.example.com", token="")
            )
        self.assertIsNone(response)
        self.assertIsNotNone(captured.output)

    def test_map_external_list_to_dict(self):
        """Tests _map_external_list_to_dict method"""
        mapped_response = self.basic_job._map_external_list_to_dict(
            [
                {"id": "abc-123", "location": "s3://bucket/prefix1"},
                {"id": "def-456", "location": "s3://bucket/prefix1"},
                {"id": "ghi-789", "location": "s3://bucket/prefix2"},
            ]
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

    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.paginate_docdb")
    def test_update_external_links_in_docdb(
        self,
        mock_paginate: MagicMock,
        mock_search: MagicMock,
        mock_docdb_client: MagicMock,
    ):
        """Tests _update_external_links_in_docdb method."""
        # Mock code ocean search response
        mock_search.return_value = self.example_search_iterator_response

        # Mock bulk_write
        mock_response = Response()
        mock_response.status_code = 200
        mock_response.json = MagicMock(
            return_value={
                "ok": 1,
                "writeErrors": [],
                "insertedIds": [],
                "nInserted": 0,
                "nUpserted": 0,
                "nMatched": 2,
                "nModified": 2,
                "nRemoved": 0,
                "upserted": [],
            }
        )
        mock_docdb_client.upsert_list_of_docdb_records.return_value = [
            mock_response
        ]

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

        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job._update_external_links_in_docdb(
                docdb_client=mock_docdb_client,
                co_client=CodeOcean(domain="www.example.com", token=""),
            )
        expected_log_messages = [
            "INFO:root:No code ocean data asset ids found for "
            "s3://bucket2/prefix3. Removing external links from record.",
            "INFO:root:Updating 2 records",
            "DEBUG:root:[{'ok': 1, 'writeErrors': [], 'insertedIds': [], ",
            "'nInserted': 0, 'nUpserted': 0, 'nMatched': 2, 'nModified': 2, ",
            "'nRemoved': 0, 'upserted': []}]",
        ]
        self.assertEqual(expected_log_messages, captured.output)
        expected_bulk_write_calls = [
            call(
                records=[
                    {
                        "_id": "0000",
                        "external_links": {
                            "Code Ocean": ["abc-123", "def-456"]
                        },
                    },
                    {
                        "_id": "0002",
                        "external_links": {"Code Ocean": []},
                    },
                ]
            )
        ]

        mock_docdb_client.upsert_list_of_docdb_records.assert_has_calls(
            expected_bulk_write_calls
        )

    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    @patch("codeocean.data_asset.DataAssets.search_data_assets_iterator")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.paginate_docdb")
    def test_update_external_links_in_docdb_error(
        self,
        mock_paginate: MagicMock,
        mock_search: MagicMock,
        mock_docdb_client: MagicMock,
    ):
        """Tests _update_external_links_in_docdb method when there is an
        error retrieving info from the temp endpoint."""
        # Mock search response
        mock_search.side_effect = Exception("Something went wrong!")

        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job._update_external_links_in_docdb(
                docdb_client=mock_docdb_client,
                co_client=CodeOcean(domain="www.example.com", token=""),
            )
        expected_log_message = (
            "ERROR:root:There was an error retrieving external links!"
        )
        self.assertEqual(2, len(captured.output))
        self.assertEqual(expected_log_message, captured.output[1])
        mock_paginate.assert_not_called()

    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils.get_dict_of_file_info")
    @patch("aind_data_asset_indexer.utils.download_json_file_from_s3")
    def test_process_codeocean_record(
        self,
        mock_download_json_file: MagicMock,
        mock_get_dict_of_file_info: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
    ):
        """Tests _process_codeocean_record method"""
        # Assume user didn't attach any metadata files
        mock_get_dict_of_file_info.return_value = (
            self.example_dict_of_file_info
        )
        mock_response = Response()
        mock_response.status_code = 200
        mock_response.json = MagicMock(
            return_value={"acknowledged": True, "insertedId": "mock_id"}
        )
        mock_docdb_client.insert_one_docdb_record.return_value = mock_response

        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job._process_codeocean_record(
                codeocean_record=self.example_codeocean_records[1],
                docdb_client=mock_docdb_client,
                s3_client=mock_s3_client,
            )
        expected_messages = [
            "INFO:root:Uploading metadata record for: "
            "s3://some_co_bucket/666666cc-66cc-6c66-666c-6c66c6666666",
            "DEBUG:root:{'acknowledged': True, 'insertedId': 'mock_id'}",
        ]
        self.assertEqual(expected_messages, captured.output)
        mock_download_json_file.assert_not_called()
        self.assertEqual(
            "ecephys_712815_2024-05-22_12-26-32_sorted_2024-06-12_19-45-59",
            mock_docdb_client.insert_one_docdb_record.mock_calls[0].kwargs[
                "record"
            ]["name"],
        )
        self.assertEqual(
            "s3://some_co_bucket/666666cc-66cc-6c66-666c-6c66c6666666",
            mock_docdb_client.insert_one_docdb_record.mock_calls[0].kwargs[
                "record"
            ]["location"],
        )

    @patch("aind_data_asset_indexer.utils.create_metadata_json")
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils.get_dict_of_file_info")
    @patch("aind_data_asset_indexer.utils.download_json_file_from_s3")
    def test_process_codeocean_record_warning(
        self,
        mock_download_json_file: MagicMock,
        mock_get_dict_of_file_info: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_create_metadata_json: MagicMock,
    ):
        """Tests _process_codeocean_record when there is an issue building the
        record"""
        # Assume user didn't attach any metadata files
        mock_get_dict_of_file_info.return_value = (
            self.example_dict_of_file_info
        )

        # Suppose there is an error creating metadata file
        mock_create_metadata_json.side_effect = Exception(
            "Something went wrong"
        )

        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job._process_codeocean_record(
                codeocean_record=self.example_codeocean_records[1],
                docdb_client=mock_docdb_client,
                s3_client=mock_s3_client,
            )
        expected_log_messages = [
            "WARNING:root:Unable to build metadata record for: "
            "s3://some_co_bucket/666666cc-66cc-6c66-666c-6c66c6666666!"
        ]
        self.assertEqual(expected_log_messages, captured.output)
        mock_download_json_file.assert_not_called()

    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "CodeOceanIndexBucketJob._process_codeocean_record"
    )
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
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
        mock_docdb_api_client = MagicMock()
        mock_docdb_client.return_value = mock_docdb_api_client
        records = self.example_codeocean_records
        self.basic_job._dask_task_to_process_record_list(record_list=records)
        mock_process_codeocean_record.assert_has_calls(
            [
                call(
                    codeocean_record=records[0],
                    docdb_client=mock_docdb_api_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    codeocean_record=records[1],
                    docdb_client=mock_docdb_api_client,
                    s3_client=mock_s3_client,
                ),
            ]
        )
        mock_s3_client.close.assert_called_once_with()
        mock_docdb_api_client.close.assert_called_once_with()

    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "CodeOceanIndexBucketJob._process_codeocean_record"
    )
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    @patch("boto3.client")
    def test_dask_task_to_process_record_list_error(
        self,
        mock_boto3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_process_codeocean_record: MagicMock,
    ):
        """Tests _dask_task_to_process_record_list when there is an error in 1
        record"""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_docdb_api_client = MagicMock()
        mock_docdb_client.return_value = mock_docdb_api_client
        records = self.example_codeocean_records
        mock_process_codeocean_record.side_effect = [
            Exception("Error processing record"),
            None,
        ]
        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job._dask_task_to_process_record_list(
                record_list=records
            )
        expected_log_messages = [
            "ERROR:root:Error processing "
            "s3://some_co_bucket/11ee1e1e-11e1-1111-1111-e11eeeee1e11: "
            "Exception('Error processing record')"
        ]
        self.assertEqual(expected_log_messages, captured.output)
        mock_process_codeocean_record.assert_has_calls(
            [
                call(
                    codeocean_record=records[0],
                    docdb_client=mock_docdb_api_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    codeocean_record=records[1],
                    docdb_client=mock_docdb_api_client,
                    s3_client=mock_s3_client,
                ),
            ]
        )
        mock_s3_client.close.assert_called_once_with()
        mock_docdb_api_client.close.assert_called_once_with()

    @patch("dask.bag.map_partitions")
    def test_process_codeocean_records(
        self, mock_dask_bag_map_parts: MagicMock
    ):
        """Test _process_codeocean_records method."""
        example_records = self.example_codeocean_records
        self.basic_job._process_codeocean_records(example_records)
        mock_dask_bag_map_parts.assert_called()

    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    def test_dask_task_to_delete_record_list(
        self, mock_docdb_client: MagicMock
    ):
        """Tests _dask_task_to_delete_record_list"""
        mock_response = Response()
        mock_response.status_code = 200
        mock_response.json = MagicMock(
            return_value={"acknowledged": True, "deletedCount": 2}
        )
        mock_docdb_client.return_value.delete_many_records.return_value = (
            mock_response
        )
        records_to_delete = [r["_id"] for r in self.example_docdb_records]
        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job._dask_task_to_delete_record_list(
                record_list=records_to_delete
            )
        expected_log_messages = [
            "INFO:root:Removing 2 records",
            "DEBUG:root:{'acknowledged': True, 'deletedCount': 2}",
        ]
        self.assertEqual(expected_log_messages, captured.output)

    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    def test_dask_task_to_delete_record_list_error(
        self, mock_docdb_client: MagicMock
    ):
        """Tests _dask_task_to_delete_record_list"""
        mock_docdb_client.return_value.delete_many_records.side_effect = (
            Exception("Error deleting records")
        )
        records_to_delete = [r["_id"] for r in self.example_docdb_records]
        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job._dask_task_to_delete_record_list(
                record_list=records_to_delete
            )
        expected_log_messages = [
            "INFO:root:Removing 2 records",
            "ERROR:root:Error deleting records: "
            "Exception('Error deleting records')",
        ]
        self.assertEqual(expected_log_messages, captured.output)

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
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.MetadataDbClient")
    @patch(
        "aind_data_asset_indexer.codeocean_bucket_indexer."
        "get_all_processed_codeocean_asset_records"
    )
    @patch("aind_data_asset_indexer.codeocean_bucket_indexer.CodeOcean")
    def test_run_job(
        self,
        mock_codeocean_client: MagicMock,
        mock_get_all_co_records: MagicMock,
        mock_docdb_client: MagicMock,
        mock_paginate_docdb: MagicMock,
        mock_process_codeocean_records: MagicMock,
        mock_delete_records_from_docdb: MagicMock,
        mock_update_external_links_in_docdb: MagicMock,
    ):
        """Tests run_job method. Given the example responses, should ignore
        one record, add one record, and delete one record."""
        mock_docdb_api_client = MagicMock()
        mock_docdb_client.return_value = mock_docdb_api_client
        mock_co_client = MagicMock()
        mock_codeocean_client.return_value = mock_co_client
        mock_get_all_co_records.return_value = dict(
            [(r["location"], r) for r in self.example_codeocean_records]
        )
        mock_paginate_docdb.return_value = [self.example_docdb_records]
        with self.assertLogs(level="DEBUG") as captured:
            self.basic_job.run_job()
        expected_log_messages = [
            "INFO:root:Starting to scan through CodeOcean.",
            "INFO:root:Finished scanning through CodeOcean.",
            "INFO:root:Starting to scan through DocDb.",
            "INFO:root:Adding links to records.",
            "INFO:root:Finished adding links to records",
            "INFO:root:Finished scanning through DocDB.",
            "INFO:root:Starting to add records to DocDB.",
            "INFO:root:Finished adding records to DocDB.",
            "INFO:root:Starting to delete records from DocDB.",
            "INFO:root:Finished deleting records from DocDB.",
        ]
        self.assertEqual(expected_log_messages, captured.output)

        mock_update_external_links_in_docdb.assert_called_once_with(
            docdb_client=mock_docdb_api_client, co_client=mock_co_client
        )
        mock_process_codeocean_records.assert_called_once_with(
            records=[self.example_codeocean_records[0]]
        )
        mock_delete_records_from_docdb.assert_called_once_with(
            record_list=["efg-456"]
        )
        mock_docdb_api_client.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
