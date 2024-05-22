"""Tests methods in populate_s3_with_metadata_files module"""

import json
import os
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, call, patch

from aind_data_asset_indexer.models import IndexJobSettings
from aind_data_asset_indexer.populate_s3_with_metadata_files import (
    AindPopulateMetadataJsonJob,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
TEST_UTILS_DIR = TEST_DIR / "resources" / "utils"


class TestAindPopulateMetadataJsonJob(unittest.TestCase):
    """Class to test methods in AindPopulateMetadataJsonJob class."""

    @classmethod
    def setUpClass(cls) -> None:
        """Load json files into memory."""

        def load_utils_json_file(filename: str) -> dict:
            """Load json file from resources directory."""
            with open(TEST_UTILS_DIR / filename, "r") as f:
                return json.load(f)

        example_md_record = load_utils_json_file("example_metadata.nd.json")
        cls.example_md_record = example_md_record
        basic_job_configs = IndexJobSettings(
            s3_bucket="aind-ephys-data-dev-u5u0i5",
            metadata_nd_overwrite=True,
            n_partitions=2,
        )
        cls.basic_job_configs = basic_job_configs
        cls.basic_job = AindPopulateMetadataJsonJob(
            job_settings=basic_job_configs
        )

    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "upload_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "does_s3_object_exist"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "build_metadata_record_from_prefix"
    )
    @patch("boto3.client")
    @patch("logging.warning")
    @patch("logging.info")
    def test_process_prefix_not_none(
        self,
        mock_log_info: MagicMock,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_build_record: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_upload_core_record: MagicMock,
        mock_upload_metadata_record: MagicMock,
    ):
        """Tests _process_prefix method."""

        expected_bucket = "aind-ephys-data-dev-u5u0i5"
        expected_prefix = "ecephys_642478_2023-01-17_13-56-29"
        expected_date_stamp = datetime.now().strftime("%Y%m%d")
        # example_md_record only has processing and subject fields
        mock_build_record.return_value = json.dumps(self.example_md_record)
        def mock_source_files_exist(s3_client, bucket, key):
            """Mock does_s3_object_exist function."""
            mock_exist_files = [
                f"{expected_prefix}/processing.json",
                f"{expected_prefix}/subject.json",
            ]
            return True if key in mock_exist_files else False
        mock_does_s3_object_exist.side_effect = mock_source_files_exist
        self.basic_job._process_prefix(
            s3_client=mock_s3_client,
            prefix=expected_prefix,
        )
        mock_build_record.assert_called_once_with(
            prefix=expected_prefix,
            s3_client=mock_s3_client,
            bucket=expected_bucket,
            metadata_nd_overwrite=True,
        )
        # assert that the original core jsons were copied
        mock_does_s3_object_exist.assert_called()
        mock_s3_client.copy_object.assert_has_calls(
            [
                call(
                    Bucket=expected_bucket,
                    CopySource={
                        "Bucket": expected_bucket,
                        "Key": f"{expected_prefix}/processing.json",
                    },
                    Key=f"{expected_prefix}/original_metadata/processing.{expected_date_stamp}.json",
                ),
                call(
                    Bucket=expected_bucket,
                    CopySource={
                        "Bucket": expected_bucket,
                        "Key": f"{expected_prefix}/subject.json",
                    },
                    Key=f"{expected_prefix}/original_metadata/subject.{expected_date_stamp}.json",
                ),
            ]
        )
        # assert that core jsons were overwritten
        mock_upload_core_record.assert_has_calls(
            [
                call(
                    bucket=expected_bucket,
                    object_key=f"{expected_prefix}/processing.json",
                    json_str=json.dumps(self.example_md_record["processing"]),
                    s3_client=mock_s3_client,
                ),
                call(
                    bucket=expected_bucket,
                    object_key=f"{expected_prefix}/subject.json",
                    json_str=json.dumps(self.example_md_record["subject"]),
                    s3_client=mock_s3_client,
                ),
            ]
        )
        # assert that the metadata record was uploaded
        mock_upload_metadata_record.assert_called_once_with(
            bucket="aind-ephys-data-dev-u5u0i5",
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            metadata_json=json.dumps(self.example_md_record),
        )
        mock_log_info.assert_called()
        mock_log_warn.assert_not_called()

    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "upload_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "does_s3_object_exist"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "build_metadata_record_from_prefix"
    )
    @patch("boto3.client")
    @patch("logging.warning")
    @patch("logging.info")
    def test_process_prefix_not_none_core_fields_mismatch(
        self,
        mock_log_info: MagicMock,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_build_record: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_upload_core_record: MagicMock,
        mock_upload_metadata_record: MagicMock,
    ):
        """Tests _process_prefix method when original core json is corrupt."""

        expected_bucket = "aind-ephys-data-dev-u5u0i5"
        expected_prefix = "ecephys_642478_2023-01-17_13-56-29"
        expected_date_stamp = datetime.now().strftime("%Y%m%d")
        # example_md_record only has processing and subject fields
        # assume rig.json exists but is corrupt
        mock_build_record.return_value = json.dumps(self.example_md_record)
        def mock_source_files_exist(s3_client, bucket, key):
            """Mock does_s3_object_exist function."""
            mock_exist_files = [
                f"{expected_prefix}/processing.json",
                f"{expected_prefix}/rig.json",
                f"{expected_prefix}/subject.json",
            ]
            return True if key in mock_exist_files else False
        mock_does_s3_object_exist.side_effect = mock_source_files_exist
        self.basic_job._process_prefix(
            s3_client=mock_s3_client,
            prefix=expected_prefix,
        )
        mock_build_record.assert_called_once_with(
            prefix=expected_prefix,
            s3_client=mock_s3_client,
            bucket=expected_bucket,
            metadata_nd_overwrite=True,
        )
        # assert that the original core jsons were copied, including
        # corrupt rig.json
        mock_does_s3_object_exist.assert_called()
        mock_s3_client.copy_object.assert_has_calls(
            [
                call(
                    Bucket=expected_bucket,
                    CopySource={
                        "Bucket": expected_bucket,
                        "Key": f"{expected_prefix}/processing.json",
                    },
                    Key=f"{expected_prefix}/original_metadata/processing.{expected_date_stamp}.json",
                ),
                call(
                    Bucket=expected_bucket,
                    CopySource={
                        "Bucket": expected_bucket,
                        "Key": f"{expected_prefix}/rig.json",
                    },
                    Key=f"{expected_prefix}/original_metadata/rig.{expected_date_stamp}.json",
                ),
                call(
                    Bucket=expected_bucket,
                    CopySource={
                        "Bucket": expected_bucket,
                        "Key": f"{expected_prefix}/subject.json",
                    },
                    Key=f"{expected_prefix}/original_metadata/subject.{expected_date_stamp}.json",
                ),
            ]
        )
        # assert that only valid core jsons were overwritten
        mock_upload_core_record.assert_has_calls(
            [
                call(
                    bucket=expected_bucket,
                    object_key=f"{expected_prefix}/processing.json",
                    json_str=json.dumps(self.example_md_record["processing"]),
                    s3_client=mock_s3_client,
                ),
                call(
                    bucket=expected_bucket,
                    object_key=f"{expected_prefix}/subject.json",
                    json_str=json.dumps(self.example_md_record["subject"]),
                    s3_client=mock_s3_client,
                ),
            ]
        )
        # assert that a warning was logged for the corrupt core json
        mock_log_info.assert_called()
        mock_log_warn.assert_called_once_with(
            f"rig not found in metadata.nd.json for {expected_prefix} but "
            f"s3://{expected_bucket}/{expected_prefix}/rig.json exists. Skipping overwrite."
        )
        # assert that the metadata record was uploaded
        mock_upload_metadata_record.assert_called_once_with(
            bucket="aind-ephys-data-dev-u5u0i5",
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            metadata_json=json.dumps(self.example_md_record),
        )

    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "upload_metadata_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "upload_json_str_to_s3"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "does_s3_object_exist"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "build_metadata_record_from_prefix"
    )
    @patch("boto3.client")
    @patch("logging.warning")
    @patch("logging.info")
    def test_process_prefix_none(
        self,
        mock_log_info: MagicMock,
        mock_log_warn: MagicMock,
        mock_s3_client: MagicMock,
        mock_build_record: MagicMock,
        mock_does_s3_object_exist: MagicMock,
        mock_upload_core_record: MagicMock,
        mock_upload_metadata_record: MagicMock,
    ):
        """Tests _process_prefix method when None is returned from
        build_metadata_record_from_prefix."""

        mock_build_record.return_value = None
        self.basic_job._process_prefix(
            s3_client=mock_s3_client,
            prefix="ecephys_642478_2023-01-17_13-56-29",
        )
        mock_build_record.assert_called_once_with(
            prefix="ecephys_642478_2023-01-17_13-56-29",
            s3_client=mock_s3_client,
            bucket="aind-ephys-data-dev-u5u0i5",
            metadata_nd_overwrite=True,
        )
        mock_does_s3_object_exist.assert_not_called()
        mock_s3_client.copy_object.assert_not_called()
        mock_upload_core_record.assert_not_called()
        mock_upload_metadata_record.assert_not_called()
        mock_log_info.assert_not_called()
        mock_log_warn.assert_called_once_with(
            "Metadata record is None for "
            "s3://aind-ephys-data-dev-u5u0i5/"
            "ecephys_642478_2023-01-17_13-56-29!"
        )

    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "AindPopulateMetadataJsonJob._process_prefix"
    )
    @patch("boto3.client")
    def test_dask_task_to_process_prefix_list(
        self, mock_boto3_client: MagicMock, mock_process_prefix: MagicMock
    ):
        """Tests _dask_task_to_process_prefix_list"""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        example_prefixes = [
            "ecephys_567890_2000-01-01_04-00-00/",
            "ecephys_642478_2023-01-17_13-56-29/",
            "ecephys_642478_2023-01-17_14-38-38/",
        ]
        self.basic_job._dask_task_to_process_prefix_list(
            prefix_list=example_prefixes
        )
        mock_boto3_client.assert_has_calls([call("s3"), call().close()])
        mock_process_prefix.assert_has_calls(
            [
                call(
                    prefix="ecephys_567890_2000-01-01_04-00-00/",
                    s3_client=mock_s3_client,
                ),
                call(
                    prefix="ecephys_642478_2023-01-17_13-56-29/",
                    s3_client=mock_s3_client,
                ),
                call(
                    prefix="ecephys_642478_2023-01-17_14-38-38/",
                    s3_client=mock_s3_client,
                ),
            ]
        )

    @patch("dask.bag.map_partitions")
    def test_process_prefixes(self, mock_dask_bag_map_parts: MagicMock):
        """Test _process_prefixes method."""
        example_prefixes = [
            f"ecephys_12345_2020-01-02_01-01-0{n}/" for n in range(1, 8)
        ]
        self.basic_job._process_prefixes(example_prefixes)
        mock_dask_bag_map_parts.assert_called()

    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "iterate_through_top_level"
    )
    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "AindPopulateMetadataJsonJob._process_prefixes"
    )
    @patch("boto3.client")
    def test_run_job(
        self,
        mock_s3_client: MagicMock,
        mock_process_prefixes: MagicMock,
        mock_iterate: MagicMock,
    ):
        """Tests _run_job method"""
        mock_iterate.return_value = [
            [f"ecephys_12345_2020-01-02_01-01-0{n}/" for n in range(1, 3)],
            [f"ecephys_12345_2020-01-02_01-01-0{n}/" for n in range(3, 6)],
            [f"ecephys_12345_2020-01-02_01-01-0{n}/" for n in range(6, 8)],
        ]

        self.basic_job.run_job()
        expected_process_calls = [
            call(
                [f"ecephys_12345_2020-01-02_01-01-0{n}/" for n in range(1, 3)]
            ),
            call(
                [f"ecephys_12345_2020-01-02_01-01-0{n}/" for n in range(3, 6)]
            ),
            call(
                [f"ecephys_12345_2020-01-02_01-01-0{n}/" for n in range(6, 8)]
            ),
        ]
        mock_s3_client.assert_has_calls([call("s3"), call().close()])
        mock_process_prefixes.assert_has_calls(expected_process_calls)


if __name__ == "__main__":
    unittest.main()
