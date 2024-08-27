"""Tests methods in aind_bucket_indexer module"""

import json
import os
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from bson.timestamp import Timestamp
from pymongo.results import DeleteResult, UpdateResult

from aind_data_asset_indexer.aind_bucket_indexer import AindIndexBucketJob
from aind_data_asset_indexer.models import AindIndexBucketJobSettings

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
TEST_UTILS_DIR = TEST_DIR / "resources" / "utils"


class TestAindIndexBucketJob(unittest.TestCase):
    """Class to test methods in AindIndexBucketJob class."""

    @classmethod
    def setUpClass(cls) -> None:
        """Load json files into memory."""

        def load_utils_json_file(filename: str) -> dict:
            """Load json file from resources directory."""
            with open(TEST_UTILS_DIR / filename, "r") as f:
                return json.load(f)

        example_md_record = load_utils_json_file("example_metadata.nd.json")
        example_md_record1 = load_utils_json_file("example_metadata1.nd.json")
        example_md_record2 = load_utils_json_file("example_metadata2.nd.json")
        cls.example_md_record = example_md_record
        cls.example_md_record1 = example_md_record1
        cls.example_md_record2 = example_md_record2
        cls.example_put_object_response1 = load_utils_json_file(
            "example_put_object_response1.json"
        )

        basic_job_configs = AindIndexBucketJobSettings(
            s3_bucket="aind-ephys-data-dev-u5u0i5",
            n_partitions=2,
            doc_db_host="docdb_host",
            doc_db_port=123,
            doc_db_user_name="docdb_user",
            doc_db_password="docdb_password",
            doc_db_db_name="dbname",
            doc_db_collection_name="collection_name",
            copy_original_md_subdir="original_metadata",
        )
        cls.basic_job_configs = basic_job_configs
        cls.basic_job = AindIndexBucketJob(job_settings=basic_job_configs)

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.upload_json_str_to_s3")
    def test_write_root_file_with_record_info_same_hash(
        self,
        mock_upload_json_str_to_s3: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _write_root_file_with_record_info method with same hashes."""
        self.basic_job._write_root_file_with_record_info(
            s3_client=mock_s3_client,
            core_schema_file_name="metadata.nd.json",
            core_schema_info_in_root={
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"e6dd2b7ab819f7a0fc21dba512a4071b"',
                "version_id": "version_id",
            },
            prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_record_contents=self.example_md_record,
        )
        mock_upload_json_str_to_s3.assert_not_called()
        mock_log_info.assert_not_called()
        mock_log_debug.assert_called_once_with(
            "DocDB record and s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/metadata.nd.json are the "
            "same. Skipped writing."
        )

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.upload_json_str_to_s3")
    def test_write_root_file_with_record_info_diff_hash(
        self,
        mock_upload_json_str_to_s3: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _write_root_file_with_record_info method with diff hash."""
        mock_upload_json_str_to_s3.return_value = "Uploaded a file"
        self.basic_job._write_root_file_with_record_info(
            s3_client=mock_s3_client,
            core_schema_file_name="metadata.nd.json",
            core_schema_info_in_root={
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"e6dd2b7ab819f7a0fc21dba512a4071c"',  # Changed this
                "version_id": "version_id",
            },
            prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_record_contents=self.example_md_record,
        )
        mock_upload_json_str_to_s3.assert_called_once_with(
            bucket="aind-ephys-data-dev-u5u0i5",
            object_key="ecephys_642478_2023-01-17_13-56-29/metadata.nd.json",
            json_str=json.dumps(self.example_md_record, default=str),
            s3_client=mock_s3_client,
        )
        mock_log_debug.assert_called_once_with("Uploaded a file")
        mock_log_info.assert_called_once_with(
            "Writing docdb record info to s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/metadata.nd.json"
        )

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.upload_json_str_to_s3")
    def test_write_root_file_with_record_info_none(
        self,
        mock_upload_json_str_to_s3: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _write_root_file_with_record_info method with no s3 info."""
        mock_upload_json_str_to_s3.return_value = "Uploaded a file"
        self.basic_job._write_root_file_with_record_info(
            s3_client=mock_s3_client,
            core_schema_file_name="subject.json",
            core_schema_info_in_root=None,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_record_contents=self.example_md_record.get("subject"),
        )
        mock_upload_json_str_to_s3.assert_called_once_with(
            bucket="aind-ephys-data-dev-u5u0i5",
            object_key="ecephys_642478_2023-01-17_13-56-29/subject.json",
            json_str=json.dumps(
                self.example_md_record.get("subject"), default=str
            ),
            s3_client=mock_s3_client,
        )
        mock_log_debug.assert_called_once_with("Uploaded a file")
        mock_log_info.assert_called_once_with(
            "Writing docdb record info to s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/subject.json"
        )

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    def test_copy_file_from_root_to_subdir(
        self,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _copy_file_from_root_to_subdir method."""
        mock_s3_client.copy_object.return_value = "Copied an object"
        self.basic_job._copy_file_from_root_to_subdir(
            s3_client=mock_s3_client,
            core_schema_file_name="subject.json",
            core_schema_info_in_root={
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            },
            prefix="ecephys_642478_2023-01-17_13-56-29",
        )
        mock_log_debug.assert_called_once_with("Copied an object")
        mock_log_info.assert_called_once_with(
            "Copying s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/subject.json to "
            "s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/original_metadata/"
            "subject.20240515.json"
        )

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_1(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 1:
        - Is the field not null in the DocDB record?       True
        - Is there a file in the root prefix?              True
        - Is there a file in the original_metadata folder? True
        """

        core_schema_info_in_root = {
            "subject.json": {
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            }
        }
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=["subject.json"],
            docdb_record=self.example_md_record,
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_write_file_with_record_info.assert_called_once_with(
            docdb_record_contents=self.example_md_record.get("subject"),
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root.get(
                "subject.json"
            ),
        )
        mock_copy_file_to_subdir.assert_not_called()
        mock_download_json_file.assert_not_called()
        mock_log_debug.assert_not_called()
        mock_log_info.assert_not_called()

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_2(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 2:
        - Is the field not null in the DocDB record?       True
        - Is there a file in the root prefix?              True
        - Is there a file in the original_metadata folder? False
        """

        core_schema_info_in_root = {
            "subject.json": {
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            }
        }
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=[],
            docdb_record=self.example_md_record,
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_copy_file_to_subdir.assert_called_once_with(
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root.get(
                "subject.json"
            ),
        )
        mock_write_file_with_record_info.assert_called_once_with(
            docdb_record_contents=self.example_md_record.get("subject"),
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root.get(
                "subject.json"
            ),
        )
        mock_download_json_file.assert_not_called()
        mock_log_debug.assert_not_called()
        mock_log_info.assert_not_called()

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_3(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 3:
        - Is the field not null in the DocDB record?       True
        - Is there a file in the root prefix?              False
        - Is there a file in the original_metadata folder? True
        """

        core_schema_info_in_root = dict()
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=["subject.json"],
            docdb_record=self.example_md_record,
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_copy_file_to_subdir.assert_not_called()
        mock_write_file_with_record_info.assert_called_once_with(
            docdb_record_contents=self.example_md_record.get("subject"),
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root.get(
                "subject.json"
            ),
        )
        mock_download_json_file.assert_not_called()
        mock_log_debug.assert_not_called()
        mock_log_info.assert_not_called()

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.get_dict_of_file_info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_4(
        self,
        mock_get_dict_of_file_info: MagicMock,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 4:
        - Is the field not null in the DocDB record?       True
        - Is there a file in the root prefix?              False
        - Is there a file in the original_metadata folder? False
        """

        core_schema_info_in_root = dict()
        core_schema_info_in_root_after_copy = {
            "ecephys_642478_2023-01-17_13-56-29/subject.json": {
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            }
        }
        mock_get_dict_of_file_info.return_value = (
            core_schema_info_in_root_after_copy
        )
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=[],
            docdb_record=self.example_md_record,
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_write_file_with_record_info.assert_called_once_with(
            docdb_record_contents=self.example_md_record.get("subject"),
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root.get(
                "subject.json"
            ),
        )
        mock_get_dict_of_file_info.assert_called_once_with(
            s3_client=mock_s3_client,
            bucket=self.basic_job.job_settings.s3_bucket,
            keys=["ecephys_642478_2023-01-17_13-56-29/subject.json"],
        )
        mock_copy_file_to_subdir.assert_called_once_with(
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root_after_copy.get(
                "ecephys_642478_2023-01-17_13-56-29/subject.json"
            ),
        )
        mock_download_json_file.assert_not_called()
        mock_log_debug.assert_not_called()
        mock_log_info.assert_not_called()

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_5(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 5:
        - Is the field not null in the DocDB record?       False
        - Is there a file in the root prefix?              True
        - Is there a file in the original_metadata folder? True
        """

        mock_s3_client.delete_object.return_value = "Deleting an object"
        core_schema_info_in_root = {
            "subject.json": {
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            }
        }
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=["subject.json"],
            docdb_record=dict(),
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_write_file_with_record_info.assert_not_called()
        mock_copy_file_to_subdir.assert_not_called()
        mock_download_json_file.assert_not_called()
        mock_log_debug.assert_called_once_with("Deleting an object")
        mock_log_info.assert_called_once_with(
            "DocDb field is null. Deleting file "
            "s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/subject.json"
        )

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_6(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 6:
        - Is the field not null in the DocDB record?       False
        - Is there a file in the root prefix?              True
        - Is there a file in the original_metadata folder? False
        """

        mock_download_json_file.return_value = self.example_md_record.get(
            "subject"
        )
        core_schema_info_in_root = {
            "subject.json": {
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            }
        }
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=[],
            docdb_record=dict(),
        )
        self.assertEqual(
            {"subject": self.example_md_record.get("subject")},
            docdb_fields_to_update,
        )
        mock_write_file_with_record_info.assert_not_called()
        mock_copy_file_to_subdir.assert_called_once_with(
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root.get(
                "subject.json"
            ),
        )
        mock_log_debug.assert_not_called()
        mock_log_info.assert_not_called()

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch("logging.warning")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_6_corrupt_download(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_warn: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 6 with corrupt download:
        - Is the field not null in the DocDB record?       False
        - Is there a file in the root prefix?              True
        - Is there a file in the original_metadata folder? False
        """

        mock_download_json_file.return_value = None
        core_schema_info_in_root = {
            "subject.json": {
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            }
        }
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=[],
            docdb_record=dict(),
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_write_file_with_record_info.assert_not_called()
        mock_copy_file_to_subdir.assert_called_once_with(
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            core_schema_file_name="subject.json",
            core_schema_info_in_root=core_schema_info_in_root.get(
                "subject.json"
            ),
        )
        mock_log_info.assert_not_called()
        mock_log_warn.assert_called_once_with(
            "Something went wrong downloading or parsing "
            "s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/subject.json"
        )
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket="aind-ephys-data-dev-u5u0i5",
            Key="ecephys_642478_2023-01-17_13-56-29/subject.json",
        )
        mock_log_debug.assert_called_once()

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_7(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 7:
        - Is the field not null in the DocDB record?       False
        - Is there a file in the root prefix?              False
        - Is there a file in the original_metadata folder? True
        """

        core_schema_info_in_root = dict()
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=["subject.json"],
            docdb_record=dict(),
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_write_file_with_record_info.assert_not_called()
        mock_copy_file_to_subdir.assert_not_called()
        mock_download_json_file.assert_not_called()
        mock_log_debug.assert_not_called()
        mock_log_info.assert_called_once_with(
            "Field is null in docdb record and no file in root folder at "
            "s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/subject.json"
        )

    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_copy_file_from_root_to_subdir"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.core_schema_file_names",
        ["subject.json"],
    )  # Mocking this to limit for loop to one iteration
    def test_resolve_schema_information_case_8(
        self,
        mock_write_file_with_record_info: MagicMock,
        mock_copy_file_to_subdir: MagicMock,
        mock_download_json_file: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests _resolve_schema_information. Case 8:
        - Is the field not null in the DocDB record?       False
        - Is there a file in the root prefix?              False
        - Is there a file in the original_metadata folder? False
        """

        core_schema_info_in_root = dict()
        docdb_fields_to_update = self.basic_job._resolve_schema_information(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            core_schema_info_in_root=core_schema_info_in_root,
            list_of_schemas_in_copy_subdir=[],
            docdb_record=dict(),
        )
        self.assertEqual(dict(), docdb_fields_to_update)
        mock_write_file_with_record_info.assert_not_called()
        mock_copy_file_to_subdir.assert_not_called()
        mock_download_json_file.assert_not_called()
        mock_log_debug.assert_not_called()
        mock_log_info.assert_called_once_with(
            "Field is null in docdb record and no file in root folder at "
            "s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/subject.json"
        )

    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_docdb_record_invalid_location(
        self,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
    ):
        """Tests _process_docdb_record method when the location in the record
        is not a valid s3 url"""

        self.basic_job._process_docdb_record(
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            docdb_record={
                "_id": "abc-123",
                "name": "prefix1_2024-01-01_01-01-01",
                "location": "no_s3/bucket/prefix1_2024-01-01_01-01-01",
            },
        )
        mock_log_warn.assert_called_once_with(
            "Record location no_s3/bucket/prefix1_2024-01-01_01-01-01 or name "
            "prefix1_2024-01-01_01-01-01 not valid for bucket "
            "aind-ephys-data-dev-u5u0i5!"
        )

    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_docdb_record_invalid_prefix(
        self,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
    ):
        """Tests _process_docdb_record method when the location in the record
        has invalid prefix"""

        self.basic_job._process_docdb_record(
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            docdb_record={
                "_id": "abc-123",
                "name": "prefix1",
                "location": "s3://bucket/prefix1",
            },
        )
        mock_log_warn.assert_called_once_with(
            "Record location s3://bucket/prefix1 or name prefix1 not valid "
            "for bucket aind-ephys-data-dev-u5u0i5!"
        )

    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    @patch("logging.info")
    def test_process_docdb_record_s3_file_missing(
        self,
        mock_log_info: MagicMock,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
    ):
        """Tests _process_docdb_record when the s3 metadata.nd.json file is
        missing."""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.delete_one.return_value = DeleteResult(
            raw_result={
                "n": 1,
                "ok": 1.0,
                "operationTime": Timestamp(1715812466, 1),
            },
            acknowledged=True,
        )

        mock_does_s3_object_exist.return_value = False
        self.basic_job._process_docdb_record(
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            docdb_record=self.example_md_record,
        )
        mock_log_warn.assert_called_once_with(
            "File not found in S3 at "
            "s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29/metadata.nd.json! "
            "Removing metadata record from DocDb."
        )
        mock_log_info.assert_called_once_with(
            {"n": 1, "ok": 1.0, "operationTime": Timestamp(1715812466, 1)}
        )

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_resolve_schema_information"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_write_root_file_with_record_info"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.list_metadata_copies")
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "get_dict_of_core_schema_file_info"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.get_dict_of_file_info")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.debug")
    @patch("logging.info")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.datetime")
    def test_process_docdb_record_valid_metadata_nd_json_file(
        self,
        mock_datetime: MagicMock,
        mock_log_info: MagicMock,
        mock_log_debug: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_get_dict_of_file_info: MagicMock,
        mock_get_dict_of_core_schema_file_info: MagicMock,
        mock_list_metadata_copies: MagicMock,
        mock_write_root_file_with_record_info: MagicMock,
        mock_resolve_schema_information: MagicMock,
    ):
        """Tests _process_docdb_record method when there is a metadata.nd.json
        file."""

        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_collection.update_one.return_value.raw_result = "Updated docdb"
        mock_db.__getitem__.return_value = mock_collection
        mock_does_s3_object_exist.return_value = True
        core_info = {
            "last_modified": datetime(
                2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
            ),
            "e_tag": '"e6dd2b7ab819f7a0fc21dba512a4071b"',
            "version_id": "version_id",
        }
        mock_get_dict_of_file_info.return_value = {
            "ecephys_642478_2023-01-17_13-56-29/metadata.nd.json": core_info
        }
        mock_get_dict_of_core_schema_file_info.return_value = {
            "subject.json": {
                "last_modified": datetime(
                    2024, 5, 15, 17, 41, 28, tzinfo=timezone.utc
                ),
                "e_tag": '"7ce612b2f26be2efe806990cb4eb4266"',
                "version_id": "version_id",
            }
        }
        mock_list_metadata_copies.return_value = []
        mock_resolve_schema_information.return_value = {
            "subject": self.example_md_record.get("subject")
        }
        mock_datetime.utcnow.return_value.isoformat.return_value = datetime(
            2024, 8, 25, 17, 41, 28, tzinfo=timezone.utc
        ).isoformat()
        mock_docdb_record = deepcopy(self.example_md_record)
        # Assume the subject is null in docdb
        mock_docdb_record["subject"] = None

        self.basic_job._process_docdb_record(
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            docdb_record=mock_docdb_record,
        )
        mock_log_info.assert_called_once_with(
            "New files found in s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29 but not in original_metadata. "
            "Updating DocDb record with new info."
        )
        expected_docdb_record_to_write = deepcopy(mock_docdb_record)
        expected_docdb_record_to_write[
            "last_modified"
        ] = "2024-08-25T17:41:28+00:00"
        expected_docdb_record_to_write["subject"] = self.example_md_record.get(
            "subject"
        )
        mock_write_root_file_with_record_info.assert_called_once_with(
            s3_client=mock_s3_client,
            core_schema_file_name="metadata.nd.json",
            core_schema_info_in_root=core_info,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_record_contents=expected_docdb_record_to_write,
        )
        mock_log_debug.assert_called_once_with("Updated docdb")

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_process_docdb_record"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    def test_dask_task_to_process_record_list(
        self,
        mock_boto3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_process_docdb_record: MagicMock,
    ):
        """Tests _dask_task_to_process_record_list"""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        records = [
            self.example_md_record,
            self.example_md_record1,
            self.example_md_record2,
        ]
        self.basic_job._dask_task_to_process_record_list(record_list=records)
        mock_process_docdb_record.assert_has_calls(
            [
                call(
                    docdb_record=self.example_md_record,
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    docdb_record=self.example_md_record1,
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    docdb_record=self.example_md_record2,
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
            ]
        )
        mock_s3_client.close.assert_called_once_with()
        mock_mongo_client.close.assert_called_once_with()

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_process_docdb_record"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.error")
    def test_dask_task_to_process_record_list_error(
        self,
        mock_log_error: MagicMock,
        mock_boto3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_process_docdb_record: MagicMock,
    ):
        """Tests _dask_task_to_process_record_list when there is an error in 1
        record."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        records = [
            self.example_md_record,
            self.example_md_record1,
            self.example_md_record2,
        ]
        mock_process_docdb_record.side_effect = [
            None,
            Exception("Error processing record"),
            None,
        ]
        self.basic_job._dask_task_to_process_record_list(record_list=records)
        mock_process_docdb_record.assert_has_calls(
            [
                call(
                    docdb_record=self.example_md_record,
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    docdb_record=self.example_md_record1,
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
                call(
                    docdb_record=self.example_md_record2,
                    docdb_client=mock_mongo_client,
                    s3_client=mock_s3_client,
                ),
            ]
        )
        mock_log_error.assert_called_once_with(
            "Error processing docdb 5ca4a951-d374-4f4b-8279-d570a35b2286, s3:"
            "//aind-ephys-data-dev-u5u0i5/ecephys_567890_2000-01-01_04-00-00: "
            "Exception('Error processing record')"
        )
        mock_s3_client.close.assert_called_once_with()
        mock_mongo_client.close.assert_called_once_with()

    @patch("dask.bag.map_partitions")
    def test_process_records(self, mock_dask_bag_map_parts: MagicMock):
        """Test _process_records method."""
        example_records = [
            self.example_md_record,
            self.example_md_record1,
            self.example_md_record2,
        ]
        self.basic_job._process_records(example_records)
        mock_dask_bag_map_parts.assert_called()

    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_prefix_invalid_prefix(
        self,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
    ):
        """Tests _process_prefix method when the prefix is invalid."""

        location_to_id_map = dict()
        self.basic_job._process_prefix(
            s3_prefix="ecephys_642478",
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_log_warn.assert_called_once_with(
            "Prefix ecephys_642478 not valid in bucket "
            f"{self.basic_job.job_settings.s3_bucket}! Skipping."
        )
        mock_does_s3_object_exist.assert_not_called()

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "cond_copy_then_sync_core_json_files"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "build_metadata_record_from_prefix"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_prefix_no_record_no_file_build_no(
        self,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_build_metadata_record_from_prefix: MagicMock,
        mock_cond_copy_then_sync_core_json_files: MagicMock,
        mock_upload_metadata_json_str_to_s3: MagicMock,
    ):
        """Tests _process_prefix method when there is no record in DocDb,
        there is no metadata.nd.json file in S3, and the
        build_metadata_record_from_prefix returns a None."""

        mock_does_s3_object_exist.return_value = False
        mock_build_metadata_record_from_prefix.return_value = None

        location_to_id_map = dict()
        self.basic_job._process_prefix(
            s3_prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_log_warn.assert_called_once_with(
            "Unable to build metadata record for: "
            f"s3://{self.basic_job.job_settings.s3_bucket}/"
            f"ecephys_642478_2023-01-17_13-56-29!"
        )
        mock_cond_copy_then_sync_core_json_files.assert_not_called()
        mock_upload_metadata_json_str_to_s3.assert_not_called()

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "cond_copy_then_sync_core_json_files"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "build_metadata_record_from_prefix"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.info")
    def test_process_prefix_no_record_no_file_build_yes(
        self,
        mock_log_info: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_build_metadata_record_from_prefix: MagicMock,
        mock_cond_copy_then_sync_core_json_files: MagicMock,
        mock_upload_metadata_json_str_to_s3: MagicMock,
    ):
        """Tests _process_prefix method when there is no record in DocDb,
        there is no metadata.nd.json file in S3, and the
        build_metadata_record_from_prefix returns a json object."""

        expected_prefix = "ecephys_642478_2023-01-17_13-56-29"
        mock_does_s3_object_exist.return_value = False
        mock_build_metadata_record_from_prefix.return_value = json.dumps(
            self.example_md_record
        )
        mock_upload_metadata_json_str_to_s3.return_value = (
            self.example_put_object_response1
        )

        location_to_id_map = dict()
        self.basic_job._process_prefix(
            s3_prefix=expected_prefix,
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_log_info.assert_has_calls(
            [
                call(
                    "Uploading metadata record for: "
                    f"s3://aind-ephys-data-dev-u5u0i5/{expected_prefix}"
                ),
                call(self.example_put_object_response1),
            ]
        )
        mock_cond_copy_then_sync_core_json_files.assert_called_once_with(
            metadata_json=json.dumps(self.example_md_record),
            bucket=self.basic_job.job_settings.s3_bucket,
            prefix=expected_prefix,
            s3_client=mock_s3_client,
            log_flag=True,
            copy_original_md_subdir="original_metadata",
        )
        mock_upload_metadata_json_str_to_s3.assert_called_once_with(
            metadata_json=json.dumps(self.example_md_record),
            bucket=self.basic_job.job_settings.s3_bucket,
            prefix=expected_prefix,
            s3_client=mock_s3_client,
        )

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "cond_copy_then_sync_core_json_files"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_prefix_no_record_yes_file_bad_file(
        self,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_download_json_file_from_s3: MagicMock,
        mock_cond_copy_then_sync_core_json_files: MagicMock,
        mock_upload_metadata_json_str_to_s3: MagicMock,
    ):
        """Tests _process_prefix method when there is no record in DocDb,
        there is metadata.nd.json file in S3, but the file can't
        be serialized to json."""

        mock_does_s3_object_exist.return_value = True
        mock_download_json_file_from_s3.return_value = None

        location_to_id_map = dict()
        self.basic_job._process_prefix(
            s3_prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_log_warn.assert_called_once_with(
            f"Unable to download file from S3 for:"
            f" s3://{self.basic_job.job_settings.s3_bucket}/"
            f"ecephys_642478_2023-01-17_13-56-29/metadata.nd.json!"
        )
        mock_cond_copy_then_sync_core_json_files.assert_not_called()
        mock_upload_metadata_json_str_to_s3.assert_not_called()

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "cond_copy_then_sync_core_json_files"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.info")
    def test_process_prefix_no_record_yes_file_good_file(
        self,
        mock_log_info: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_download_json_file_from_s3: MagicMock,
        mock_cond_copy_then_sync_core_json_files: MagicMock,
        mock_upload_metadata_json_str_to_s3: MagicMock,
    ):
        """Tests _process_prefix method when there is no record in DocDb,
        there is and there is metadata.nd.json file in S3, and the file can
        be serialized to json."""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.update_one.return_value = UpdateResult(
            raw_result={
                "n": 1,
                "nModified": 0,
                "upserted": "488bbe42-832b-4c37-8572-25eb87cc50e2",
                "ok": 1.0,
                "operationTime": Timestamp(1715819252, 1),
                "updatedExisting": False,
            },
            acknowledged=True,
        )

        mock_does_s3_object_exist.return_value = True
        mock_download_json_file_from_s3.return_value = self.example_md_record

        location_to_id_map = dict()
        self.basic_job._process_prefix(
            s3_prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_log_info.assert_called_once_with(
            {
                "n": 1,
                "nModified": 0,
                "upserted": "488bbe42-832b-4c37-8572-25eb87cc50e2",
                "ok": 1.0,
                "operationTime": Timestamp(1715819252, 1),
                "updatedExisting": False,
            }
        )
        mock_cond_copy_then_sync_core_json_files.assert_called_once_with(
            metadata_json=json.dumps(self.example_md_record),
            bucket=self.basic_job.job_settings.s3_bucket,
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            log_flag=True,
            copy_original_md_subdir="original_metadata",
        )
        mock_upload_metadata_json_str_to_s3.assert_not_called()

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "cond_copy_then_sync_core_json_files"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_prefix_no_record_yes_file_good_file_no__id(
        self,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_download_json_file_from_s3: MagicMock,
        mock_cond_copy_then_sync_core_json_files: MagicMock,
        mock_upload_metadata_json_str_to_s3: MagicMock,
    ):
        """Tests _process_prefix method when there is no record in DocDb,
        there is and there is metadata.nd.json file in S3, and the file can
        be serialized to json, but there is no _id in the file."""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        mock_does_s3_object_exist.return_value = True
        mocked_downloaded_record = deepcopy(self.example_md_record)
        del mocked_downloaded_record["_id"]
        mock_download_json_file_from_s3.return_value = mocked_downloaded_record

        location_to_id_map = dict()
        self.basic_job._process_prefix(
            s3_prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_collection.assert_not_called()
        mock_cond_copy_then_sync_core_json_files.assert_not_called()
        mock_upload_metadata_json_str_to_s3.assert_not_called()
        mock_log_warn.assert_called_once_with(
            "Metadata record for s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29 does not have an _id field!"
        )

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "cond_copy_then_sync_core_json_files"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "download_json_file_from_s3"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_prefix_no_record_yes_file_good_file_bad_location(
        self,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_download_json_file_from_s3: MagicMock,
        mock_cond_copy_then_sync_core_json_files: MagicMock,
        mock_upload_metadata_json_str_to_s3: MagicMock,
    ):
        """Tests _process_prefix method when there is no record in DocDb,
        there is and there is metadata.nd.json file in S3, and the file can
        be serialized to json, but the location inside the metadata record
        does not match actual location of the record."""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection

        mock_does_s3_object_exist.return_value = True
        # Test what happens when the location in the record does not match the
        # expected location
        mocked_downloaded_record = deepcopy(self.example_md_record)
        mocked_downloaded_record["location"] = (
            f"s3://{self.basic_job.job_settings.s3_bucket}/"
            f"ecephys_642478_2020-01-10_10-10-10"
        )
        mock_download_json_file_from_s3.return_value = mocked_downloaded_record

        location_to_id_map = dict()
        self.basic_job._process_prefix(
            s3_prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_collection.assert_not_called()
        mock_cond_copy_then_sync_core_json_files.assert_not_called()
        mock_upload_metadata_json_str_to_s3.assert_not_called()
        mock_log_warn.assert_called_once_with(
            "Location field s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2020-01-10_10-10-10 or name field "
            "ecephys_642478_2023-01-17_13-56-29 does not match actual location"
            " of record s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29!"
        )

    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.info")
    def test_process_prefix_yes_record_yes_file(
        self,
        mock_log_info: MagicMock,
        mock_s3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_does_s3_object_exist: MagicMock,
    ):
        """Tests _process_prefix method when there is a record in DocDb and
        there is a metadata.nd.json file in S3."""

        mock_does_s3_object_exist.return_value = True
        expected_bucket = self.basic_job.job_settings.s3_bucket
        location_key = (
            f"s3://{expected_bucket}/" f"ecephys_642478_2023-01-17_13-56-29"
        )
        location_to_id_map = {
            location_key: "488bbe42-832b-4c37-8572-25eb87cc50e2"
        }
        self.basic_job._process_prefix(
            s3_prefix="ecephys_642478_2023-01-17_13-56-29",
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            location_to_id_map=location_to_id_map,
        )
        mock_log_info.assert_called_once_with(
            f"Metadata record for s3://{expected_bucket}/"
            f"ecephys_642478_2023-01-17_13-56-29/metadata.nd.json already "
            f"exists in DocDb. Skipping."
        )

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "build_docdb_location_to_id_map"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_process_prefix"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    def test_dask_task_to_process_prefix_list(
        self,
        mock_boto3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_process_prefix: MagicMock,
        mock_build_location_to_id_map: MagicMock,
    ):
        """Tests _dask_task_to_process_prefix_list"""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        prefixes = [
            "ecephys_642478_2023-01-17_13-56-29",
            "ecephys_567890_2000-01-01_04-00-00",
            "ecephys_655019_2000-01-01_01-01-02",
        ]
        mock_location_to_id_map = {
            "ecephys_642478_2023-01-17_13-56-29": (
                "488bbe42-832b-4c37-8572-25eb87cc50e2"
            ),
            "ecephys_567890_2000-01-01_04-00-00": (
                "5ca4a951-d374-4f4b-8279-d570a35b2286"
            ),
        }
        mock_build_location_to_id_map.return_value = mock_location_to_id_map
        self.basic_job._dask_task_to_process_prefix_list(prefix_list=prefixes)
        mock_process_prefix.assert_has_calls(
            [
                call(
                    s3_prefix="ecephys_642478_2023-01-17_13-56-29",
                    s3_client=mock_s3_client,
                    location_to_id_map=mock_location_to_id_map,
                    docdb_client=mock_mongo_client,
                ),
                call(
                    s3_prefix="ecephys_567890_2000-01-01_04-00-00",
                    s3_client=mock_s3_client,
                    location_to_id_map=mock_location_to_id_map,
                    docdb_client=mock_mongo_client,
                ),
                call(
                    s3_prefix="ecephys_655019_2000-01-01_01-01-02",
                    s3_client=mock_s3_client,
                    location_to_id_map=mock_location_to_id_map,
                    docdb_client=mock_mongo_client,
                ),
            ]
        )
        mock_s3_client.close.assert_called_once_with()
        mock_mongo_client.close.assert_called_once_with()

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "build_docdb_location_to_id_map"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_process_prefix"
    )
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("boto3.client")
    @patch("logging.error")
    def test_dask_task_to_process_prefix_list_error(
        self,
        mock_log_error: MagicMock,
        mock_boto3_client: MagicMock,
        mock_docdb_client: MagicMock,
        mock_process_prefix: MagicMock,
        mock_build_location_to_id_map: MagicMock,
    ):
        """Tests _dask_task_to_process_prefix_list when there is an error in 1
        prefix."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        prefixes = [
            "ecephys_642478_2023-01-17_13-56-29",
            "ecephys_567890_2000-01-01_04-00-00",
            "ecephys_655019_2000-01-01_01-01-02",
        ]
        mock_location_to_id_map = {
            "ecephys_642478_2023-01-17_13-56-29": (
                "488bbe42-832b-4c37-8572-25eb87cc50e2"
            ),
            "ecephys_567890_2000-01-01_04-00-00": (
                "5ca4a951-d374-4f4b-8279-d570a35b2286"
            ),
        }
        mock_build_location_to_id_map.return_value = mock_location_to_id_map
        mock_process_prefix.side_effect = [
            None,
            Exception("Error processing prefix"),
            None,
        ]
        self.basic_job._dask_task_to_process_prefix_list(prefix_list=prefixes)
        mock_process_prefix.assert_has_calls(
            [
                call(
                    s3_prefix="ecephys_642478_2023-01-17_13-56-29",
                    s3_client=mock_s3_client,
                    location_to_id_map=mock_location_to_id_map,
                    docdb_client=mock_mongo_client,
                ),
                call(
                    s3_prefix="ecephys_567890_2000-01-01_04-00-00",
                    s3_client=mock_s3_client,
                    location_to_id_map=mock_location_to_id_map,
                    docdb_client=mock_mongo_client,
                ),
                call(
                    s3_prefix="ecephys_655019_2000-01-01_01-01-02",
                    s3_client=mock_s3_client,
                    location_to_id_map=mock_location_to_id_map,
                    docdb_client=mock_mongo_client,
                ),
            ]
        )
        mock_log_error.assert_called_once_with(
            "Error processing s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_567890_2000-01-01_04-00-00: "
            "Exception('Error processing prefix')"
        )
        mock_s3_client.close.assert_called_once_with()
        mock_mongo_client.close.assert_called_once_with()

    @patch("dask.bag.map_partitions")
    def test_process_prefixes(self, mock_dask_bag_map_parts: MagicMock):
        """Test _process_prefixes method."""
        prefixes = [
            "ecephys_642478_2023-01-17_13-56-29",
            "ecephys_567890_2000-01-01_04-00-00",
            "ecephys_655019_2000-01-01_01-01-02",
        ]
        self.basic_job._process_prefixes(prefixes=prefixes)
        mock_dask_bag_map_parts.assert_called()

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_process_prefixes"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer."
        "iterate_through_top_level"
    )
    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "_process_records"
    )
    @patch("logging.info")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.MongoClient")
    @patch("aind_data_asset_indexer.aind_bucket_indexer.paginate_docdb")
    @patch("boto3.client")
    def test_run_job(
        self,
        mock_boto3_client: MagicMock,
        mock_paginate: MagicMock,
        mock_docdb_client: MagicMock,
        mock_log_info: MagicMock,
        mock_process_records: MagicMock,
        mock_iterate_prefixes: MagicMock,
        mock_process_prefixes: MagicMock,
    ):
        """Tests main run_job method."""

        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_mongo_client = MagicMock()
        mock_docdb_client.return_value = mock_mongo_client
        mock_paginate.return_value = iter(
            [
                [
                    self.example_md_record,
                    self.example_md_record1,
                    self.example_md_record2,
                ]
            ]
        )
        mock_iterate_prefixes.return_value = iter(
            [
                [
                    "ecephys_642478_2023-01-17_13-56-29/",
                    "ecephys_567890_2000-01-01_04-00-00/",
                    "ecephys_655019_2000-01-01_01-01-02/",
                ]
            ]
        )

        self.basic_job.run_job()

        mock_mongo_client.close.assert_called_once()
        mock_s3_client.close.assert_called_once()
        mock_log_info.assert_has_calls(
            [
                call("Starting to scan through DocDb."),
                call("Finished scanning through DocDb."),
                call("Starting to scan through S3."),
                call("Finished scanning through S3."),
            ]
        )
        mock_process_records.assert_called_once_with(
            records=[
                self.example_md_record,
                self.example_md_record1,
                self.example_md_record2,
            ]
        )
        mock_process_prefixes.assert_called_once_with(
            prefixes=[
                "ecephys_642478_2023-01-17_13-56-29/",
                "ecephys_567890_2000-01-01_04-00-00/",
                "ecephys_655019_2000-01-01_01-01-02/",
            ]
        )


if __name__ == "__main__":
    unittest.main()
