"""Tests methods in aind_bucket_indexer module"""

import json
import os
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from aind_data_asset_indexer.aind_bucket_indexer import AindIndexBucketJob
from aind_data_asset_indexer.models import AindIndexBucketJobSettings
from aind_data_asset_indexer.utils import (
    build_docdb_location_to_id_map,
    build_metadata_record_from_prefix,
    compute_md5_hash,
    create_metadata_object_key,
    does_metadata_record_exist_in_docdb,
    does_s3_object_exist,
    download_json_file_from_s3,
    get_dict_of_file_info,
    get_record_from_docdb,
    get_s3_bucket_and_prefix,
    is_dict_corrupt,
    is_record_location_valid,
    iterate_through_top_level,
    paginate_docdb,
    upload_metadata_json_str_to_s3,
)

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
        cls.example_md_record = example_md_record

        basic_job_configs = AindIndexBucketJobSettings(
            s3_bucket="aind-ephys-data-dev-u5u0i5",
            metadata_nd_overwrite=False,
            n_partitions=2,
            doc_db_host="docdb_host",
            doc_db_port=123,
            doc_db_user_name="docdb_user",
            doc_db_password="docdb_password",
            doc_db_db_name="dbname",
            doc_db_collection_name="collection_name",
        )
        cls.basic_job_configs = basic_job_configs
        cls.basic_job = AindIndexBucketJob(
            job_settings=basic_job_configs
        )

    @patch("pymongo.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_docdb_record_invalid_location(
            self,
            mock_log_warn: MagicMock,
            mock_s3_client: MagicMock,
            mock_docdb_client: MagicMock
    ):
        """Tests _process_docdb_record method when the location in the record
        is not a valid s3 url"""

        self.basic_job._process_docdb_record(
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            docdb_record={"_id": "abc-123", "location": "no_s3/bucket/prefix"}
        )
        mock_log_warn.assert_called_once_with(
            'Record location no_s3/bucket/prefix is not valid for bucket '
            'aind-ephys-data-dev-u5u0i5!'
        )

    @patch("aind_data_asset_indexer.aind_bucket_indexer.does_s3_object_exist")
    @patch("pymongo.MongoClient")
    @patch("boto3.client")
    @patch("logging.warning")
    def test_process_docdb_record_s3_file_missing(
            self,
            mock_log_warn: MagicMock,
            mock_s3_client: MagicMock,
            mock_docdb_client: MagicMock,
            mock_does_s3_object_exist: MagicMock
    ):
        """Tests _process_docdb_record when the s3 metadata.nd.json file is
        missing."""
        mock_does_s3_object_exist.return_value = False
        self.basic_job._process_docdb_record(
            docdb_client=mock_docdb_client,
            s3_client=mock_s3_client,
            docdb_record=self.example_md_record
        )
        mock_log_warn.assert_called_once_with(
            'Record location no_s3/bucket/prefix is not valid for bucket '
            'aind-ephys-data-dev-u5u0i5!'
        )

