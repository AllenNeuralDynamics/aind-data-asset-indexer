"""Tests methods in utils module"""

import json
import logging
import os
import unittest
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from botocore.exceptions import ClientError

from aind_data_asset_indexer.utils import (
    _log_message,
    build_docdb_location_to_id_map,
    build_metadata_record_from_prefix,
    compute_md5_hash,
    copy_then_overwrite_core_json_files,
    create_core_schema_object_keys_map,
    create_metadata_object_key,
    create_object_key,
    does_metadata_record_exist_in_docdb,
    does_s3_object_exist,
    does_s3_prefix_exist,
    download_json_file_from_s3,
    get_dict_of_file_info,
    get_record_from_docdb,
    get_s3_bucket_and_prefix,
    get_s3_location,
    is_dict_corrupt,
    is_prefix_valid,
    is_record_location_valid,
    iterate_through_top_level,
    paginate_docdb,
    sync_core_json_files,
    upload_json_str_to_s3,
    upload_metadata_json_str_to_s3,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
TEST_UTILS_DIR = TEST_DIR / "resources" / "utils"


class TestUtils(unittest.TestCase):
    """Class to test methods in utils module."""

    @classmethod
    def setUpClass(cls) -> None:
        """Set up the class by extracting contents from example files."""

        def load_json_file(filename: str) -> dict:
            """Load json file from resources directory."""
            with open(TEST_UTILS_DIR / filename, "r") as f:
                return json.load(f)

        example_core_files = [
            "acquisition",
            "data_description",
            "instrument",
            "procedures",
            "processing",
            "rig",
            "session",
            "subject",
            "mri_session",
        ]
        cls.example_core_files = example_core_files
        example_pages = load_json_file("example_pages_response.json")
        cls.example_pages = example_pages

        example_processing = load_json_file("processing.json")
        example_subject = load_json_file("subject.json")
        example_metadata_nd = load_json_file("example_metadata.nd.json")
        example_metadata_nd1 = load_json_file("example_metadata1.nd.json")
        example_metadata_nd2 = load_json_file("example_metadata2.nd.json")
        cls.example_processing = example_processing
        cls.example_subject = example_subject
        cls.example_metadata_nd = example_metadata_nd
        cls.example_metadata_nd1 = example_metadata_nd1
        cls.example_metadata_nd2 = example_metadata_nd2

        example_head_object_response1 = load_json_file(
            "example_head_object_response1.json"
        )
        example_head_object_response1["LastModified"] = datetime.fromisoformat(
            example_head_object_response1["LastModified"]
        )
        cls.example_head_object_response1 = example_head_object_response1
        cls.example_head_object_response2 = load_json_file(
            "example_head_object_response2.json"
        )
        cls.example_head_object_response3 = load_json_file(
            "example_head_object_response3.json"
        )

        example_list_objects_response = load_json_file(
            "example_list_objects_response.json"
        )
        example_list_objects_response_false = load_json_file(
            "example_list_objects_response_false.json"
        )
        example_list_objects_response_unexpected = load_json_file(
            "example_list_objects_response_unexpected.json"
        )
        cls.example_list_objects_response = example_list_objects_response
        cls.example_list_objects_response_false = (
            example_list_objects_response_false
        )
        cls.example_list_objects_response_unexpected = (
            example_list_objects_response_unexpected
        )

        example_get_object_response1 = load_json_file(
            "example_get_object_response1.json"
        )
        example_get_object_response1["LastModified"] = datetime.fromisoformat(
            example_get_object_response1["LastModified"]
        )
        mock_body1 = MagicMock()
        mock_body1.read.return_value = json.dumps(example_processing).encode(
            "utf-8"
        )
        example_get_object_response1["Body"] = mock_body1
        cls.example_get_object_response1 = example_get_object_response1

        example_get_object_response2 = load_json_file(
            "example_get_object_response2.json"
        )
        example_get_object_response2["LastModified"] = datetime.fromisoformat(
            example_get_object_response2["LastModified"]
        )
        cls.example_get_object_response2 = example_get_object_response2

        cls.example_put_object_response1 = load_json_file(
            "example_put_object_response1.json"
        )

    @patch("logging.log")
    def test__log_message_true(self, mock_log: MagicMock):
        """Tests _log_message method when log_flag is True or not provided"""
        message = "This is a test message"
        valid_log_levels = [
            logging.DEBUG,
            logging.INFO,
            logging.WARNING,
            logging.ERROR,
            logging.CRITICAL,
        ]
        invalid_log_levels = [logging.NOTSET, -1, 1, 100]
        _log_message(message)
        _log_message(message, log_flag=True)
        for log_level in valid_log_levels:
            _log_message(message, log_level)
        mock_log.assert_has_calls(
            [
                call(logging.INFO, message),
                call(logging.INFO, message),
                call(logging.DEBUG, message),
                call(logging.INFO, message),
                call(logging.WARNING, message),
                call(logging.ERROR, message),
                call(logging.CRITICAL, message),
            ]
        )
        for log_level in invalid_log_levels:
            with self.assertRaises(ValueError):
                _log_message(message, log_level)

    @patch("logging.log")
    def test__log_message_false(self, mock_log: MagicMock):
        """Tests _log_message method when log_flag is False"""
        message = "This is a test message"
        _log_message(message, log_flag=False)
        _log_message(message, log_level=logging.WARNING, log_flag=False)
        _log_message(message, log_level=logging.NOTSET, log_flag=False)
        mock_log.assert_not_called()

    def test_compute_md5_hash(self):
        """Tests compute_md5_hash method"""
        md5_hash = compute_md5_hash(json.dumps(self.example_metadata_nd))
        self.assertEqual("e6dd2b7ab819f7a0fc21dba512a4071b", md5_hash)

    def test_is_dict_corrupt(self):
        """Tests is_dict_corrupt method"""
        good_contents = {"a": 1, "b": {"c": 2, "d": 3}}
        bad_contents1 = {"a.1": 1, "b": {"c": 2, "d": 3}}
        bad_contents2 = {"a": 1, "b": {"c": 2, "$d": 3}}
        bad_contents3 = {"a": 1, "b": {"c": 2, "d": 3}, "$e": 4}
        bad_contents4 = {"a": 1, "b": {"c": {"d": 3}, "$e": 4}}
        self.assertFalse(is_dict_corrupt(good_contents))
        self.assertTrue(is_dict_corrupt(bad_contents1))
        self.assertTrue(is_dict_corrupt(bad_contents2))
        self.assertTrue(is_dict_corrupt(bad_contents3))
        self.assertTrue(is_dict_corrupt(bad_contents4))

    def test_create_object_key(self):
        """Tests create_object_key"""
        prefix1 = "prefix1/"
        prefix2 = "prefix2/subdir"
        filename1 = "subject.json"
        filename2 = "rig.json"
        obj_key1 = create_object_key(prefix1, filename1)
        obj_key2 = create_object_key(prefix2, filename2)
        self.assertEqual("prefix1/subject.json", obj_key1)
        self.assertEqual("prefix2/subdir/rig.json", obj_key2)

    def test_create_metadata_object_key(self):
        """Tests create_metadata_object_key"""
        prefix1 = "prefix1/"
        prefix2 = "prefix2"
        obj_key1 = create_metadata_object_key(prefix1)
        obj_key2 = create_metadata_object_key(prefix2)
        self.assertEqual("prefix1/metadata.nd.json", obj_key1)
        self.assertEqual("prefix2/metadata.nd.json", obj_key2)

    @patch(
        "aind_data_asset_indexer.utils.core_schema_file_names",
        new=["mock_schema1.json", "mock_schema2.json", "mock_schema3.json"],
    )
    @patch("aind_data_asset_indexer.utils.get_dict_of_file_info")
    @patch("boto3.client")
    def test_create_core_schema_object_keys_map(
        self, mock_s3_client: MagicMock, mock_get_dict_of_file_info: MagicMock
    ):
        """Tests create_core_schema_object_keys_map"""
        prefix = "prefix1/"
        target_prefix = "prefix1/original_metadata/"
        mock_get_dict_of_file_info.return_value = {
            "prefix1/mock_schema1.json": None,
            "prefix1/mock_schema2.json": {
                "last_modified": datetime(
                    2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
                ),
                "e_tag": '"f4827f025e79bafeb6947e14c4e3b51a"',
                "version_id": "jWWT0Xrb8_nE9t5C.nTlLElpYJoURbv_",
            },
            "prefix1/mock_schema3.json": {
                "last_modified": datetime(
                    2024, 5, 20, 5, 12, 12, tzinfo=timezone.utc
                ),
                "e_tag": '"92734946c64fc87408ef79e5e92937bc"',
                "version_id": "XS0p7m6wWNTHG_F3P76D7AUXtE23BakR",
            },
        }
        expected_object_keys_map = {
            "mock_schema1.json": {
                "source": "prefix1/mock_schema1.json",
                "target": (
                    "prefix1/original_metadata/mock_schema1.unknown.json"
                ),
            },
            "mock_schema2.json": {
                "source": "prefix1/mock_schema2.json",
                "target": (
                    "prefix1/original_metadata/mock_schema2.20231104.json"
                ),
            },
            "mock_schema3.json": {
                "source": "prefix1/mock_schema3.json",
                "target": (
                    "prefix1/original_metadata/mock_schema3.20240520.json"
                ),
            },
        }
        result_object_keys_map = create_core_schema_object_keys_map(
            s3_client=mock_s3_client,
            bucket="some_bucket",
            prefix=prefix,
            target_prefix=target_prefix,
        )
        self.assertDictEqual(expected_object_keys_map, result_object_keys_map)

    def test_get_s3_bucket_and_prefix(self):
        """Tests get_s3_bucket_and_prefix"""
        results1 = get_s3_bucket_and_prefix(
            s3_location="s3://some_bucket/prefix1/"
        )
        results2 = get_s3_bucket_and_prefix(
            s3_location="s3://some_bucket/prefix2"
        )

        self.assertEqual(
            {"bucket": "some_bucket", "prefix": "prefix1"}, results1
        )
        self.assertEqual(
            {"bucket": "some_bucket", "prefix": "prefix2"}, results2
        )

    def test_get_s3_location(self):
        """Tests get_s3_location"""
        result1 = get_s3_location(bucket="some_bucket", prefix="prefix1")
        result2 = get_s3_location(bucket="some_bucket", prefix="prefix2/")
        self.assertEqual("s3://some_bucket/prefix1", result1)
        self.assertEqual("s3://some_bucket/prefix2", result2)

    def test_is_prefix_valid(self):
        """Tests is_prefix_valid"""
        valid_prefixes = [
            "prefix1_2024-01-01_01-01-01",
            "prefix1_2024-01-01_01-01-01/",
            "ecephys_111111_2000-01-01_04-00-00/",
            "multiplane-ophys_111111_2024-01-01_01-01-01/",
        ]
        invalid_prefixes = [
            "prefix1",
            "prefix1/",
            "prefix1-2024-01-01-01-01-01",
            "prefix1_2024-01-01_01-01-01/extra",
            "prefix1_2024-01-01_01-01-01/extra/",
        ]
        for prefix in valid_prefixes:
            self.assertTrue(is_prefix_valid(prefix))
        for prefix in invalid_prefixes:
            self.assertFalse(is_prefix_valid(prefix))

    def test_is_record_location_valid_true0(self):
        """Tests is_record_location_valid returns true when expected_prefix is
        set to None"""

        example_record = {
            "_id": "abc-123",
            "name": "prefix1_2024-01-01_01-01-01",
            "location": "s3://some_bucket/prefix1_2024-01-01_01-01-01",
        }
        self.assertTrue(
            is_record_location_valid(
                example_record, expected_bucket="some_bucket"
            )
        )

    def test_is_record_location_valid_true1(self):
        """Tests is_record_location_valid returns true when expected_prefix is
        set."""

        example_record = {
            "_id": "abc-123",
            "name": "prefix1_2024-01-01_01-01-01",
            "location": "s3://some_bucket/prefix1_2024-01-01_01-01-01",
        }
        self.assertTrue(
            is_record_location_valid(
                example_record,
                expected_bucket="some_bucket",
                expected_prefix="prefix1_2024-01-01_01-01-01",
            )
        )

    def test_is_record_location_valid_false0(self):
        """Tests is_record_location_valid returns false when no location field
        is present"""

        example_record = {
            "_id": "abc-123",
            "name": "prefix1_2024-01-01_01-01-01",
        }
        self.assertFalse(
            is_record_location_valid(
                example_record, expected_bucket="some_bucket"
            )
        )

    def test_is_record_location_valid_false1(self):
        """Tests is_record_location_valid returns false when the 's3' part is
        missing"""

        example_record = {
            "_id": "abc-123",
            "name": "prefix1_2024-01-01_01-01-01",
            "location": "some_bucket/prefix1_2024-01-01_01-01-01",
        }
        self.assertFalse(
            is_record_location_valid(
                example_record, expected_bucket="some_bucket"
            )
        )

    def test_is_record_location_valid_false2(self):
        """Tests is_record_location_valid returns false when there are
        multiple forward slashes in prefix"""

        example_record = {
            "_id": "abc-123",
            "name": "prefix2_2024-01-01_01-01-01",
            "location": "s3://some_bucket/prefix1/prefix2_2024-01-01_01-01-01",
        }
        self.assertFalse(
            is_record_location_valid(
                example_record, expected_bucket="some_bucket"
            )
        )

    def test_is_record_location_valid_false3(self):
        """Tests is_record_location_valid returns false when buckets don't
        match"""

        example_record = {
            "_id": "abc-123",
            "name": "prefix1_2024-01-01_01-01-01",
            "location": "s3://some_bucket/prefix1_2024-01-01_01-01-01",
        }
        self.assertFalse(
            is_record_location_valid(
                example_record, expected_bucket="some_other_bucket"
            )
        )

    def test_is_record_location_valid_false4(self):
        """Tests is_record_location_valid returns false when prefix is invalid
        """

        example_record = {
            "_id": "abc-123",
            "name": "prefix1",
            "location": "s3://some_bucket/prefix1",
        }
        self.assertFalse(
            is_record_location_valid(
                example_record, expected_bucket="some_bucket"
            )
        )

    def test_is_record_location_valid_false5(self):
        """Tests is_record_location_valid returns false when prefix does
        not match name"""

        example_record = {
            "_id": "abc-123",
            "name": "prefix2_2024-01-01_01-01-01",
            "location": "s3://some_bucket/prefix1_2024-01-01_01-01-01",
        }
        self.assertFalse(
            is_record_location_valid(
                example_record, expected_bucket="some_bucket"
            )
        )

    def test_is_record_location_valid_false6(self):
        """Tests is_record_location_valid returns false when prefixes don't
        match"""

        example_record = {
            "_id": "abc-123",
            "name": "prefix1_2024-01-01_01-01-01",
            "location": "s3://some_bucket/prefix1_2024-01-01_01-01-01",
        }
        self.assertFalse(
            is_record_location_valid(
                example_record,
                expected_bucket="some_bucket",
                expected_prefix="prefix2_2024-01-01_01-01-01",
            )
        )

    @patch("boto3.client")
    def test_does_s3_object_exist_true(self, mock_s3_client: MagicMock):
        """Tests does_s3_object_exist when true"""
        mock_s3_client.head_object.return_value = (
            self.example_head_object_response1
        )
        self.assertTrue(
            does_s3_object_exist(
                bucket="a_bucket", key="object_key", s3_client=mock_s3_client
            )
        )

    @patch("boto3.client")
    def test_does_s3_object_exist_false(self, mock_s3_client: MagicMock):
        """Tests does_s3_object_exist when false"""
        mock_s3_client.head_object.side_effect = ClientError(
            error_response=self.example_head_object_response2,
            operation_name="",
        )
        self.assertFalse(
            does_s3_object_exist(
                bucket="a_bucket", key="object_key", s3_client=mock_s3_client
            )
        )

    @patch("boto3.client")
    def test_does_s3_object_exist_error(self, mock_s3_client: MagicMock):
        """Tests does_s3_object_exist when an error occurred"""
        mock_s3_client.head_object.side_effect = ClientError(
            error_response=self.example_head_object_response3,
            operation_name="",
        )
        with self.assertRaises(ClientError) as e:
            does_s3_object_exist(
                bucket="a_bucket", key="object_key", s3_client=mock_s3_client
            )

        self.assertEqual(
            {"Code": "403", "Message": "Forbidden"},
            e.exception.response["Error"],
        )

    @patch("boto3.client")
    def test_does_s3_prefix_exist_true(self, mock_s3_client: MagicMock):
        """Tests does_s3_prefix_exist when true"""
        provided_target_prefix = "prefix/original_metadata"
        expected_target_prefix = "prefix/original_metadata/"
        mock_s3_client.list_objects_v2.return_value = (
            self.example_list_objects_response
        )
        result = does_s3_prefix_exist(
            bucket="a_bucket",
            prefix=provided_target_prefix,
            s3_client=mock_s3_client,
        )
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="a_bucket", Prefix=expected_target_prefix, MaxKeys=1
        )
        self.assertTrue(result)

    @patch("boto3.client")
    def test_does_s3_prefix_exist_false(self, mock_s3_client: MagicMock):
        """Tests does_s3_prefix_exist when false"""
        provided_target_prefix = "prefix/original_metadata"
        expected_target_prefix = "prefix/original_metadata/"
        mock_s3_client.list_objects_v2.return_value = (
            self.example_list_objects_response_false
        )
        result = does_s3_prefix_exist(
            bucket="a_bucket",
            prefix=provided_target_prefix,
            s3_client=mock_s3_client,
        )
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="a_bucket", Prefix=expected_target_prefix, MaxKeys=1
        )
        self.assertFalse(result)

    @patch("boto3.client")
    def test_does_s3_prefix_exist_error(self, mock_s3_client: MagicMock):
        """Tests does_s3_prefix_exist when response is unexpected"""
        provided_target_prefix = "prefix/original_metadata"
        expected_target_prefix = "prefix/original_metadata/"
        mock_s3_client.list_objects_v2.return_value = (
            self.example_list_objects_response_unexpected
        )
        with self.assertRaises(ValueError):
            does_s3_prefix_exist(
                bucket="a_bucket",
                prefix=provided_target_prefix,
                s3_client=mock_s3_client,
            )
        mock_s3_client.list_objects_v2.assert_called_once_with(
            Bucket="a_bucket", Prefix=expected_target_prefix, MaxKeys=1
        )

    @patch("boto3.client")
    def test_get_dict_of_file_info(self, mock_s3_client: MagicMock):
        """Tests get_dict_of_file_info method"""
        # Assume the first file exists and the second does not
        keys = ["prefix1/metadata.nd.json", "prefix2/metadata.nd.json"]
        mock_s3_client.head_object.side_effect = [
            self.example_head_object_response1,
            ClientError(
                error_response=self.example_head_object_response2,
                operation_name="",
            ),
        ]
        responses = get_dict_of_file_info(
            s3_client=mock_s3_client, bucket="some_bucket", keys=keys
        )
        expected_response1 = {
            "last_modified": datetime(
                2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
            ),
            "e_tag": '"92734946c64fc87408ef79e5e92937bc"',
            "version_id": "VersionID",
        }
        self.assertEqual(
            expected_response1, responses["prefix1/metadata.nd.json"]
        )
        self.assertIsNone(responses["prefix2/metadata.nd.json"])

    @patch("boto3.client")
    def test_get_dict_of_file_info_error(self, mock_s3_client: MagicMock):
        """Tests get_dict_of_file_info method when an error occurred"""
        # Assume the first file exists and the user does not have permission
        # for second file
        keys = ["prefix1/metadata.nd.json", "prefix2/metadata.nd.json"]
        mock_s3_client.head_object.side_effect = [
            self.example_head_object_response1,
            ClientError(
                error_response=self.example_head_object_response3,
                operation_name="",
            ),
        ]

        with self.assertRaises(ClientError) as e:
            get_dict_of_file_info(
                s3_client=mock_s3_client, bucket="some_bucket", keys=keys
            )
        self.assertEqual(
            {"Code": "403", "Message": "Forbidden"},
            e.exception.response["Error"],
        )

    @patch("boto3.client")
    def test_download_json_file_from_s3_success(
        self, mock_s3_client: MagicMock
    ):
        """Tests download_json_file_from_s3 when successful"""
        mock_s3_client.get_object.return_value = (
            self.example_get_object_response1
        )

        contents = download_json_file_from_s3(
            s3_client=mock_s3_client,
            bucket="some_bucket",
            object_key="prefix1/processing.json",
        )
        self.assertEqual(self.example_processing, contents)

    @patch("boto3.client")
    def test_download_json_file_from_s3_failure(
        self, mock_s3_client: MagicMock
    ):
        """Tests download_json_file_from_s3 when failed to decode json"""
        mock_readable_object = MagicMock()
        # Non-json string
        mock_readable_object.read.return_value = "abc".encode("utf-8")
        mock_response = deepcopy(self.example_get_object_response1)
        mock_response["Body"] = mock_readable_object
        mock_s3_client.get_object.return_value = mock_response
        json_contents = download_json_file_from_s3(
            s3_client=mock_s3_client,
            bucket="some_bucket",
            object_key="prefix1/processing.json",
        )
        self.assertIsNone(json_contents)

    @patch("boto3.client")
    def test_iterate_through_top_level(self, mock_s3_client):
        """Tests iterate_through_top_level method"""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = self.example_pages
        mock_s3_client.get_paginator.return_value = mock_paginator

        output = iterate_through_top_level(
            s3_client=mock_s3_client, bucket="some_bucket", max_pages=3
        )
        expected_output = [
            [
                "ecephys_567890_2000-01-01_04-00-00/",
                "ecephys_642478_2023-01-17_13-56-29/",
                "ecephys_642478_2023-01-17_14-38-38/",
            ],
            [
                "ecephys_655019_2000-01-01_01-00-00/",
                "ecephys_655019_2000-01-01_01-01-01/",
                "ecephys_655019_2000-01-01_01-01-02/",
            ],
            ["ecephys_684156_2000-01-01_00-00-01/"],
        ]

        self.assertEqual(expected_output, list(output))

    @patch("boto3.client")
    def test_upload_json_str_to_s3(self, mock_s3_client: MagicMock):
        """Tests upload_json_str_to_s3 method"""
        mock_s3_client.put_object.return_value = (
            self.example_put_object_response1
        )
        expected_json_str = json.dumps(self.example_subject)
        expected_bucket = "some_bucket"
        expected_object_key = "ecephys_642478_2023-01-17_13-56-29/subject.json"
        response = upload_json_str_to_s3(
            bucket=expected_bucket,
            object_key=expected_object_key,
            json_str=expected_json_str,
            s3_client=mock_s3_client,
        )
        self.assertEqual(self.example_put_object_response1, response)
        mock_s3_client.put_object.assert_called_once_with(
            Bucket=expected_bucket,
            Key=expected_object_key,
            Body=json.dumps(
                json.loads(expected_json_str),
                indent=3,
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8"),
        )

    @patch("boto3.client")
    def test_upload_metadata_json_str_to_s3(self, mock_s3_client: MagicMock):
        """Tests upload_metadata_json_str_to_s3 method"""
        mock_s3_client.put_object.return_value = (
            self.example_put_object_response1
        )
        metadata_json = json.dumps(self.example_metadata_nd)
        response = upload_metadata_json_str_to_s3(
            bucket="some_bucket",
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            metadata_json=metadata_json,
        )
        self.assertEqual(self.example_put_object_response1, response)
        mock_s3_client.put_object.assert_called_once_with(
            Bucket="some_bucket",
            Key="ecephys_642478_2023-01-17_13-56-29/metadata.nd.json",
            Body=json.dumps(
                json.loads(metadata_json),
                indent=3,
                ensure_ascii=False,
                sort_keys=True,
            ).encode("utf-8"),
        )

    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils.get_dict_of_file_info")
    @patch("aind_data_asset_indexer.utils.download_json_file_from_s3")
    def test_build_metadata_record_from_prefix(
        self,
        mock_download_json_file: MagicMock,
        mock_get_dict_of_file_info: MagicMock,
        mock_s3_client: MagicMock,
    ):
        """Tests build_metadata_record_from_prefix method"""
        mock_get_dict_of_file_info.return_value = {
            "ecephys_642478_2023-01-17_13-56-29/acquisition.json": None,
            "ecephys_642478_2023-01-17_13-56-29/data_description.json": None,
            "ecephys_642478_2023-01-17_13-56-29/instrument.json": None,
            "ecephys_642478_2023-01-17_13-56-29/procedures.json": None,
            "ecephys_642478_2023-01-17_13-56-29/processing.json": {
                "last_modified": datetime(
                    2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
                ),
                "e_tag": '"f4827f025e79bafeb6947e14c4e3b51a"',
                "version_id": "jWWT0Xrb8_nE9t5C.nTlLElpYJoURbv_",
            },
            "ecephys_642478_2023-01-17_13-56-29/rig.json": None,
            "ecephys_642478_2023-01-17_13-56-29/session.json": None,
            "ecephys_642478_2023-01-17_13-56-29/subject.json": {
                "last_modified": datetime(
                    2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
                ),
                "e_tag": '"92734946c64fc87408ef79e5e92937bc"',
                "version_id": "XS0p7m6wWNTHG_F3P76D7AUXtE23BakR",
            },
            "ecephys_642478_2023-01-17_13-56-29/mri_session.json": None,
        }
        mock_download_json_file.side_effect = [
            self.example_processing,
            self.example_subject,
        ]
        # noinspection PyTypeChecker
        md = json.loads(
            build_metadata_record_from_prefix(
                bucket="aind-ephys-data-dev-u5u0i5",
                prefix="ecephys_642478_2023-01-17_13-56-29",
                s3_client=mock_s3_client,
            )
        )
        mock_get_dict_of_file_info.assert_called_once_with(
            s3_client=mock_s3_client,
            bucket="aind-ephys-data-dev-u5u0i5",
            keys=[
                "ecephys_642478_2023-01-17_13-56-29/acquisition.json",
                "ecephys_642478_2023-01-17_13-56-29/data_description.json",
                "ecephys_642478_2023-01-17_13-56-29/instrument.json",
                "ecephys_642478_2023-01-17_13-56-29/procedures.json",
                "ecephys_642478_2023-01-17_13-56-29/processing.json",
                "ecephys_642478_2023-01-17_13-56-29/rig.json",
                "ecephys_642478_2023-01-17_13-56-29/session.json",
                "ecephys_642478_2023-01-17_13-56-29/subject.json",
                "ecephys_642478_2023-01-17_13-56-29/mri_session.json",
            ],
        )
        mock_download_json_file.assert_has_calls(
            [
                call(
                    s3_client=mock_s3_client,
                    bucket="aind-ephys-data-dev-u5u0i5",
                    object_key=(
                        "ecephys_642478_2023-01-17_13-56-29/processing.json"
                    ),
                ),
                call(
                    s3_client=mock_s3_client,
                    bucket="aind-ephys-data-dev-u5u0i5",
                    object_key=(
                        "ecephys_642478_2023-01-17_13-56-29/subject.json"
                    ),
                ),
            ]
        )
        # Small hack to avoid having to mock uuids and creation times
        md["_id"] = self.example_metadata_nd["_id"]
        md["created"] = self.example_metadata_nd["created"]
        md["last_modified"] = self.example_metadata_nd["last_modified"]
        self.assertEqual(self.example_metadata_nd, md)

    @patch("aind_data_asset_indexer.utils.upload_json_str_to_s3")
    @patch("aind_data_asset_indexer.utils.get_dict_of_file_info")
    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils._log_message")
    def test_sync_core_json_files(
        self,
        mock_log_message: MagicMock,
        mock_s3_client: MagicMock,
        mock_get_dict_of_file_info: MagicMock,
        mock_upload_core_record: MagicMock,
    ):
        """Tests sync_core_json_files method."""
        expected_bucket = "aind-ephys-data-dev-u5u0i5"
        pfx = "ecephys_567890_2000-01-01_04-00-00"
        s3_loc = f"s3://{expected_bucket}/{pfx}"

        # assume in docdb:
        # data_description was added,
        # procedures was updated
        # processing and subject were unchanged,
        # rig was removed,
        # and the other core fields are None
        md_json_from_docdb = self.example_metadata_nd1
        md5_hash_procecures_mock_original = "1145037da6d39eda39a68fc262eb9e2d"
        md5_hash_processing_unchanged = "7ebb961de9e9b00accfd1358e4561ec1"
        md5_hash_rig_mock_original = "898210b082095f3a945f6bd07f37283e"
        md5_hash_subject_unchanged = "8b8cd50a6cf1f3f667be98a69db2ad89"
        mock_get_dict_of_file_info.return_value = {
            f"{pfx}/acquisition.json": None,
            f"{pfx}/data_description.json": None,
            f"{pfx}/instrument.json": None,
            f"{pfx}/procedures.json": {
                "last_modified": datetime(
                    2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
                ),
                "e_tag": f'"{md5_hash_procecures_mock_original}"',
                "version_id": "CxVdainLIA9brzChT0AmYHwVlSHY4ptH",
            },
            f"{pfx}/processing.json": {
                "last_modified": datetime(
                    2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
                ),
                "e_tag": f'"{md5_hash_processing_unchanged}"',
                "version_id": "jWWT0Xrb8_nE9t5C.nTlLElpYJoURbv_",
            },
            f"{pfx}/rig.json": {
                "last_modified": datetime(
                    2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
                ),
                "e_tag": f'"{md5_hash_rig_mock_original}"',
                "version_id": "y7Xtfp7pmQlBrPtaGvK2pz85RUZcMv29",
            },
            f"{pfx}/session.json": None,
            f"{pfx}/subject.json": {
                "last_modified": datetime(
                    2023, 11, 4, 1, 13, 41, tzinfo=timezone.utc
                ),
                "e_tag": f'"{md5_hash_subject_unchanged}"',
                "version_id": "XS0p7m6wWNTHG_F3P76D7AUXtE23BakR",
            },
            f"{pfx}/mri_session.json": None,
        }
        mock_upload_core_record.return_value = "mock_upload_response"
        mock_s3_client.delete_object.return_value = "mock_delete_response"
        sync_core_json_files(
            metadata_json=json.dumps(md_json_from_docdb),
            bucket=expected_bucket,
            prefix=pfx,
            s3_client=mock_s3_client,
        )
        mock_get_dict_of_file_info.assert_called_once_with(
            s3_client=mock_s3_client,
            bucket=expected_bucket,
            keys=[
                f"{pfx}/acquisition.json",
                f"{pfx}/data_description.json",
                f"{pfx}/instrument.json",
                f"{pfx}/procedures.json",
                f"{pfx}/processing.json",
                f"{pfx}/rig.json",
                f"{pfx}/session.json",
                f"{pfx}/subject.json",
                f"{pfx}/mri_session.json",
            ],
        )
        # assert that only new or updated core jsons were uploaded to s3
        mock_upload_core_record.assert_has_calls(
            [
                call(
                    bucket=expected_bucket,
                    object_key=f"{pfx}/data_description.json",
                    json_str=json.dumps(
                        md_json_from_docdb["data_description"]
                    ),
                    s3_client=mock_s3_client,
                ),
                call(
                    bucket=expected_bucket,
                    object_key=f"{pfx}/procedures.json",
                    json_str=json.dumps(md_json_from_docdb["procedures"]),
                    s3_client=mock_s3_client,
                ),
            ]
        )
        # assert that deleted core field was deleted from s3
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket=expected_bucket,
            Key=f"{pfx}/rig.json",
        )
        # assert correct logs for all actions
        expected_logs = [
            (
                f"acquisition not found in metadata.nd.json for {pfx} nor in "
                f"{s3_loc}/acquisition.json! Skipping."
            ),
            (
                f"Uploading new data_description to {s3_loc}/"
                "data_description.json"
            ),
            "mock_upload_response",
            (
                f"instrument not found in metadata.nd.json for {pfx} nor in "
                f"{s3_loc}/instrument.json! Skipping."
            ),
            f"Uploading updated procedures to {s3_loc}/procedures.json",
            "mock_upload_response",
            (
                f"processing is up-to-date in {s3_loc}/processing.json. "
                "Skipping."
            ),
            (
                f"rig not found in metadata.nd.json for {pfx} but {s3_loc}/"
                "rig.json exists! Deleting."
            ),
            "mock_delete_response",
            (
                f"session not found in metadata.nd.json for {pfx} nor in "
                f"{s3_loc}/session.json! Skipping."
            ),
            f"subject is up-to-date in {s3_loc}/subject.json. Skipping.",
            (
                f"mri_session not found in metadata.nd.json for {pfx} nor in "
                f"{s3_loc}/mri_session.json! Skipping."
            ),
        ]
        actual_log_messages = [
            c[1]["message"] for c in mock_log_message.call_args_list
        ]
        self.assertEqual(expected_logs, actual_log_messages)

    @patch("aind_data_asset_indexer.utils.upload_json_str_to_s3")
    @patch("aind_data_asset_indexer.utils.does_s3_object_exist")
    @patch("aind_data_asset_indexer.utils.create_core_schema_object_keys_map")
    @patch("aind_data_asset_indexer.utils.does_s3_prefix_exist")
    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils._log_message")
    def test_copy_then_overwrite_core_json_files(
        self,
        mock_log_message: MagicMock,
        mock_s3_client: MagicMock,
        mock_does_s3_prefix_exist: MagicMock,
        mock_create_core_keys_map: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_upload_core_record: MagicMock,
    ):
        """Tests copy_then_overwrite_core_json_files method."""
        bucket = "aind-ephys-data-dev-u5u0i5"
        pfx = "ecephys_642478_2023-01-17_13-56-29"

        # example_md_record only has processing and subject fields
        # assume /original_metadata already exists
        mock_does_s3_prefix_exist.return_value = True
        mock_object_keys_map = {
            f"{file}.json": {
                "source": f"{pfx}/{file}.json",
                "target": f"{pfx}/original_metadata/{file}.unknown.json",
            }
            for file in self.example_core_files
        }
        mock_object_keys_map["processing.json"][
            "target"
        ] = f"{pfx}/original_metadata/processing.20231104.json"
        mock_object_keys_map["subject.json"][
            "target"
        ] = f"{pfx}/original_metadata/subject.20240202.json"
        mock_create_core_keys_map.return_value = mock_object_keys_map

        def mock_source_files_exist(s3_client, bucket, key):
            """Mock does_s3_object_exist function."""
            mock_exist_files = [
                f"{pfx}/processing.json",
                f"{pfx}/subject.json",
            ]
            return True if key in mock_exist_files else False

        mock_does_s3_object_exist.side_effect = mock_source_files_exist
        copy_then_overwrite_core_json_files(
            metadata_json=json.dumps(self.example_metadata_nd),
            bucket=bucket,
            prefix=pfx,
            s3_client=mock_s3_client,
        )
        # assert that an existing /original_metadata folder was detected
        mock_does_s3_prefix_exist.assert_called_once_with(
            bucket=bucket,
            prefix=f"{pfx}/original_metadata",
            s3_client=mock_s3_client,
        )
        self.assertEqual(
            f"Target copy folder s3://{bucket}/{pfx}"
            f"/original_metadata already exists.",
            mock_log_message.call_args_list[0][1]["message"],
        )
        # assert that the original core jsons were copied
        mock_does_s3_object_exist.assert_called()
        mock_s3_client.copy_object.assert_has_calls(
            [
                call(
                    Bucket=bucket,
                    CopySource={
                        "Bucket": bucket,
                        "Key": f"{pfx}/processing.json",
                    },
                    Key=f"{pfx}/original_metadata/processing.20231104.json",
                ),
                call(
                    Bucket=bucket,
                    CopySource={
                        "Bucket": bucket,
                        "Key": f"{pfx}/subject.json",
                    },
                    Key=f"{pfx}/original_metadata/subject.20240202.json",
                ),
            ]
        )
        # assert that core jsons were overwritten
        mock_upload_core_record.assert_has_calls(
            [
                call(
                    bucket=bucket,
                    object_key=f"{pfx}/processing.json",
                    json_str=json.dumps(
                        self.example_metadata_nd["processing"]
                    ),
                    s3_client=mock_s3_client,
                ),
                call(
                    bucket=bucket,
                    object_key=f"{pfx}/subject.json",
                    json_str=json.dumps(self.example_metadata_nd["subject"]),
                    s3_client=mock_s3_client,
                ),
            ]
        )
        response_logs = [
            c[1]["log_level"]
            for c in mock_log_message.call_args_list
            if "log_level" in c[1]
        ]
        self.assertEqual([], response_logs)

    @patch("aind_data_asset_indexer.utils.upload_json_str_to_s3")
    @patch("aind_data_asset_indexer.utils.does_s3_object_exist")
    @patch("aind_data_asset_indexer.utils.create_core_schema_object_keys_map")
    @patch("aind_data_asset_indexer.utils.does_s3_prefix_exist")
    @patch("boto3.client")
    @patch("aind_data_asset_indexer.utils._log_message")
    def test_copy_then_overwrite_core_json_files_mismatch(
        self,
        mock_log_message: MagicMock,
        mock_s3_client: MagicMock,
        mock_does_s3_prefix_exist: MagicMock,
        mock_create_core_keys_map: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_upload_core_record: MagicMock,
    ):
        """Tests copy_then_overwrite_core_json_files method when an original
        core json does not exist in generated metadata.nd.json."""
        bucket = "aind-ephys-data-dev-u5u0i5"
        pfx = "ecephys_642478_2023-01-17_13-56-29"

        # example_md_record only has processing and subject fields
        # assume rig.json exists but is corrupt
        # assume /original_metadata does not exist
        mock_does_s3_prefix_exist.return_value = False
        mock_object_keys_map = {
            f"{file}.json": {
                "source": f"{pfx}/{file}.json",
                "target": f"{pfx}/original_metadata/{file}.unknown.json",
            }
            for file in self.example_core_files
        }
        mock_object_keys_map["processing.json"][
            "target"
        ] = f"{pfx}/original_metadata/processing.20231104.json"
        mock_object_keys_map["subject.json"][
            "target"
        ] = f"{pfx}/original_metadata/subject.20240202.json"
        mock_object_keys_map["rig.json"][
            "target"
        ] = f"{pfx}/original_metadata/rig.20220505.json"
        mock_create_core_keys_map.return_value = mock_object_keys_map

        def mock_source_files_exist(s3_client, bucket, key):
            """Mock does_s3_object_exist function."""
            mock_exist_files = [
                f"{pfx}/processing.json",
                f"{pfx}/rig.json",
                f"{pfx}/subject.json",
            ]
            return True if key in mock_exist_files else False

        mock_does_s3_object_exist.side_effect = mock_source_files_exist
        copy_then_overwrite_core_json_files(
            metadata_json=json.dumps(self.example_metadata_nd),
            bucket=bucket,
            prefix=pfx,
            s3_client=mock_s3_client,
        )
        # assert that the original core jsons were copied, including
        # corrupt rig.json
        mock_does_s3_object_exist.assert_called()
        mock_s3_client.copy_object.assert_has_calls(
            [
                call(
                    Bucket=bucket,
                    CopySource={
                        "Bucket": bucket,
                        "Key": f"{pfx}/processing.json",
                    },
                    Key=(f"{pfx}/original_metadata/processing.20231104.json"),
                ),
                call(
                    Bucket=bucket,
                    CopySource={
                        "Bucket": bucket,
                        "Key": f"{pfx}/rig.json",
                    },
                    Key=(f"{pfx}/original_metadata/rig.20220505.json"),
                ),
                call(
                    Bucket=bucket,
                    CopySource={
                        "Bucket": bucket,
                        "Key": f"{pfx}/subject.json",
                    },
                    Key=(f"{pfx}/original_metadata/subject.20240202.json"),
                ),
            ]
        )
        # assert that only valid core jsons were overwritten
        mock_upload_core_record.assert_has_calls(
            [
                call(
                    bucket=bucket,
                    object_key=f"{pfx}/processing.json",
                    json_str=json.dumps(
                        self.example_metadata_nd["processing"]
                    ),
                    s3_client=mock_s3_client,
                ),
                call(
                    bucket=bucket,
                    object_key=f"{pfx}/subject.json",
                    json_str=json.dumps(self.example_metadata_nd["subject"]),
                    s3_client=mock_s3_client,
                ),
            ]
        )
        # assert the corrupt core json was deleted
        mock_s3_client.delete_object.assert_called_once_with(
            Bucket=bucket, Key=f"{pfx}/rig.json"
        )
        expected_loc = f"s3://{bucket}/{pfx}/rig.json"
        expected_warn_log = {
            "log_flag": False,
            "log_level": logging.WARN,
            "message": (
                f"rig not found in metadata.nd.json for {pfx} but "
                f"{expected_loc} exists! Deleting."
            ),
        }
        response_logs = [
            c[1]
            for c in mock_log_message.call_args_list
            if "log_level" in c[1]
        ]
        self.assertEqual([expected_warn_log], response_logs)

    @patch("pymongo.MongoClient")
    def test_does_metadata_record_exist_in_docdb_true(
        self, mock_docdb_client: MagicMock
    ):
        """Tests does_metadata_record_exist_in_docdb when true"""

        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter(
            [{"_id": "70bcf356-985f-4a2a-8105-de900e35e788"}]
        )
        self.assertTrue(
            does_metadata_record_exist_in_docdb(
                docdb_client=mock_docdb_client,
                db_name="metadata_index",
                collection_name="data_assets",
                bucket="aind-ephys-data-dev-u5u0i5",
                prefix="ecephys_642478_2023-01-17_13-56-29",
            )
        )

    @patch("pymongo.MongoClient")
    def test_does_metadata_record_exist_in_docdb_false(
        self, mock_docdb_client: MagicMock
    ):
        """Tests does_metadata_record_exist_in_docdb when false"""

        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter([])
        self.assertFalse(
            does_metadata_record_exist_in_docdb(
                docdb_client=mock_docdb_client,
                db_name="metadata_index",
                collection_name="data_assets",
                bucket="aind-ephys-data-dev-u5u0i5",
                prefix="ecephys_642478_2023-01-17_13-56-29",
            )
        )

    @patch("pymongo.MongoClient")
    def test_get_record_from_docdb(self, mock_docdb_client: MagicMock):
        """Tests get_record_from_docdb when record exists"""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter([self.example_metadata_nd])
        record = get_record_from_docdb(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            record_id="488bbe42-832b-4c37-8572-25eb87cc50e2",
        )
        self.assertEqual(self.example_metadata_nd, record)

    @patch("pymongo.MongoClient")
    def test_get_record_from_docdb_none(self, mock_docdb_client: MagicMock):
        """Tests get_record_from_docdb when record doesn't exist"""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter([])
        record = get_record_from_docdb(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            record_id="488bbe42-832b-4c37-8572-25eb87cc50ee",
        )
        self.assertIsNone(record)

    @patch("pymongo.MongoClient")
    def test_paginate_docdb(self, mock_docdb_client: MagicMock):
        """Tests paginate_docdb"""
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter(
            [
                self.example_metadata_nd,
                self.example_metadata_nd1,
                self.example_metadata_nd2,
            ]
        )
        pages = paginate_docdb(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            page_size=2,
        )
        expected_results = [
            [self.example_metadata_nd, self.example_metadata_nd1],
            [self.example_metadata_nd2],
        ]
        actual_results = list(pages)
        self.assertEqual(expected_results, actual_results)

    @patch("pymongo.MongoClient")
    def test_build_docdb_location_to_id_map(
        self, mock_docdb_client: MagicMock
    ):
        """Tests build_docdb_location_to_id_map"""
        bucket = "aind-ephys-data-dev-u5u0i5"
        mock_db = MagicMock()
        mock_docdb_client.__getitem__.return_value = mock_db
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_collection.find.return_value = iter(
            [
                {
                    "_id": "70bcf356-985f-4a2a-8105-de900e35e788",
                    "location": (
                        f"s3://{bucket}/ecephys_655019_2000-04-04_04-00-00"
                    ),
                },
                {
                    "_id": "5ca4a951-d374-4f4b-8279-d570a35b2286",
                    "location": (
                        f"s3://{bucket}/ecephys_567890_2000-01-01_04-00-00"
                    ),
                },
            ]
        )

        actual_map = build_docdb_location_to_id_map(
            docdb_client=mock_docdb_client,
            db_name="metadata_index",
            collection_name="data_assets",
            bucket=bucket,
            prefixes=[
                "ecephys_655019_2000-04-04_04-00-00",
                "ecephys_567890_2000-01-01_04-00-00/",
                "missing_655019_2000-01-01_01-01-02",
            ],
        )
        expected_map = {
            f"s3://{bucket}/ecephys_655019_2000-04-04_04-00-00": (
                "70bcf356-985f-4a2a-8105-de900e35e788"
            ),
            f"s3://{bucket}/ecephys_567890_2000-01-01_04-00-00": (
                "5ca4a951-d374-4f4b-8279-d570a35b2286"
            ),
        }
        self.assertEqual(expected_map, actual_map)


if __name__ == "__main__":
    unittest.main()
