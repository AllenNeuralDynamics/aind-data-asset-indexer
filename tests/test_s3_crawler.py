"""Tests methods AnalyticsTableJobRunner class"""

import builtins
import os
import unittest
from unittest.mock import MagicMock, Mock, mock_open, patch

import pandas as pd
from aind_data_access_api.rds_tables import Client as RDSClient
from aind_data_access_api.rds_tables import RDSCredentials

from aind_data_asset_indexer.s3_crawler import (
    AnalyticsTableJobRunner,
    MetadataAnalyticsTableRow,
)


class MockDirEntry(MagicMock):
    """Mock class for scandir"""

    def __init__(self, name, is_dir):
        """Initialize MockDirEntry instance"""
        super().__init__(spec=os.DirEntry)
        self.name = name
        self.configure_mock(is_dir=MagicMock(return_value=is_dir))


class TestAnalyticsTableJobRunner(unittest.TestCase):
    """Test methods in AnalyticsTableJobRunner class"""

    sample_rds_credentials = RDSCredentials(
        username="some_rds_user",
        password="some_rds_password",
        host="localhost_rds",
        port=123456,
        database="some_rds_db",
    )

    sample_folders_txt = (
        "PRE modality_567890_2000-01-01_04-00-00/\n"
        "PRE modality_642478_2023-01-17_13-56-29/\n"
        "PRE modality_642478_2023-01-17_14-38-38/\n"
    )

    @patch.dict(
        "os.environ",
        {
            "REDSHIFT_SECRETS_NAME": "some_secrets_value",
            "BUCKETS": '["bucket1", "bucket2"]',
            "TABLE_NAME": "some_table_name",
        },
    )
    @patch("aind_data_asset_indexer.s3_crawler.RDSCredentials")
    def setUp(self, mock_rds_credentials):
        """Constructs AnalyticsTableJobRunner with mock creds"""
        self.mock_credentials = Mock(spec=RDSCredentials)
        mock_rds_credentials.return_value = self.sample_rds_credentials
        self.runner = AnalyticsTableJobRunner(
            redshift_secrets_name=os.getenv("REDSHIFT_SECRETS_NAME"),
            buckets=os.getenv("BUCKETS"),
            table_name=os.getenv("TABLE_NAME"),
        )
        self.runner.redshift_client = Mock(spec=RDSClient)

    @patch("subprocess.run")
    @patch("os.makedirs")
    @patch(f"{builtins.__name__}.open", new_callable=mock_open)
    def test_get_list_of_folders(
        self, mock_open, mock_makedirs, mock_subprocess_run
    ):
        """Tests bash command to get folders runs as expected."""
        bucket_name = "test_bucket"
        output_filepath = "test_output.txt"

        self.runner._get_list_of_folders(bucket_name, output_filepath)
        expected_command = ["aws", "s3", "ls", bucket_name]

        mock_makedirs.assert_called_once_with(
            os.path.dirname(output_filepath), exist_ok=True
        )
        mock_open.assert_called_once_with(output_filepath, "w")
        mock_subprocess_run.assert_called_once_with(
            expected_command, stdout=mock_open.return_value
        )

    @patch("subprocess.run")
    def test_download_metadata_files(self, mock_subprocess_run):
        """Tests bash command to download metadata runs as expected."""
        bucket_name = "test_bucket"
        output_directory = "test/directory/"

        self.runner._download_metadata_files(bucket_name, output_directory)
        expected_command = [
            "aws",
            "s3",
            "sync",
            "s3://test_bucket",
            "test/directory/test_bucket",
            "--exclude",
            "*",
            "--include",
            "*.nd.json",
        ]
        mock_subprocess_run.assert_called_once_with(expected_command)

    @patch(
        "builtins.open", new_callable=mock_open, read_data=sample_folders_txt
    )
    def test_create_dataframe_from_list_of_folders(self, mock_file_open):
        """Tests dataframe is created from folders.txt as expected."""
        filepath = "test_folders.txt"
        df = self.runner._create_dataframe_from_list_of_folders(filepath)
        expected_df = pd.DataFrame(
            {
                "s3_prefix": [
                    "modality_567890_2000-01-01_04-00-00",
                    "modality_642478_2023-01-17_13-56-29",
                    "modality_642478_2023-01-17_14-38-38",
                ]
            }
        )
        pd.testing.assert_frame_equal(df, expected_df)
        mock_file_open.assert_called_once_with(filepath, "r")

    @patch("os.scandir")
    def test_create_dataframe_from_metadata_files(self, mock_scandir):
        """Tests dataframe is created from metadata as expected."""
        output_directory = "test_output_directory"

        # Mock the behavior of os.scandir
        mock_scandir.return_value = [
            MockDirEntry(
                name="modality_567890_2000-01-01_04-00-00", is_dir=True
            ),
            MockDirEntry(
                name="modality_642478_2023-01-17_13-56-29", is_dir=True
            ),
            MockDirEntry(name="folders.txt", is_dir=False),
        ]

        df = self.runner._create_dataframe_from_metadata_files(
            output_directory
        )
        expected_df = pd.DataFrame(
            {
                "s3_prefix": [
                    "modality_567890_2000-01-01_04-00-00",
                    "modality_642478_2023-01-17_13-56-29",
                ]
            }
        )
        pd.testing.assert_frame_equal(df, expected_df)
        mock_scandir.assert_called_once_with(output_directory)

    def test_join_dataframes(self):
        """Tests dataframes are merged as expected."""
        expected_data = {
            "s3_prefix": [
                "modality_567890_2000-01-01_04-00-00",
                "modality_642478_2023-01-17_13-56-29",
                "modality_642478_2023-01-17_14-38-38",
            ],
            "metadata_bool": [True, False, True],
            "bucket_name": [
                "modality_bucket",
                "modality_bucket",
                "modality_bucket",
            ],
        }
        expected_df = pd.DataFrame(expected_data)

        df1_data = {
            "s3_prefix": [
                "modality_567890_2000-01-01_04-00-00",
                "modality_642478_2023-01-17_13-56-29",
                "modality_642478_2023-01-17_14-38-38",
            ]
        }
        df2_data = {
            "s3_prefix": [
                "modality_567890_2000-01-01_04-00-00",
                "modality_642478_2023-01-17_14-38-38",
            ]
        }
        bucket_name = "modality_bucket"

        df1 = pd.DataFrame(df1_data)
        df2 = pd.DataFrame(df2_data)
        result_df = self.runner._join_dataframes(df1, df2, bucket_name)
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch.object(AnalyticsTableJobRunner, "_get_list_of_folders")
    @patch.object(AnalyticsTableJobRunner, "_download_metadata_files")
    @patch.object(
        AnalyticsTableJobRunner, "_create_dataframe_from_list_of_folders"
    )
    @patch.object(
        AnalyticsTableJobRunner, "_create_dataframe_from_metadata_files"
    )
    @patch.object(AnalyticsTableJobRunner, "_join_dataframes")
    def test_crawl_s3_buckets(
        self,
        mock_join_dataframes,
        mock_create_metadata_dataframe,
        mock_create_folders_dataframe,
        mock_download_metadata,
        mock_get_list_of_folders,
    ):
        """Tests method to crawl through list of s3 buckets"""
        folders_filepath = "test_folders.txt"
        metadata_directory = "test_metadata_dir"

        mock_get_list_of_folders.side_effect = [None, None]

        mock_create_metadata_dataframe.side_effect = [
            pd.DataFrame(
                {"s3_prefix": [f"{directory}_folder1", f"{directory}_folder2"]}
            )
            for directory in ["dir1", "dir2"]
        ]

        mock_create_folders_dataframe.side_effect = [
            pd.DataFrame(
                {"s3_prefix": [f"{filepath}_folder1", f"{filepath}_folder2"]}
            )
            for filepath in ["file1", "file2"]
        ]

        mock_download_metadata.side_effect = [None, None]

        mock_join_dataframes.side_effect = [
            pd.DataFrame(
                {
                    "s3_prefix": ["modality1_folder1", "modality1_folder2"],
                    "metadata_bool": [True, False],
                    "bucket_name": ["modality1", "modality1"],
                }
            ),
            pd.DataFrame(
                {
                    "s3_prefix": ["modality2_folder1", "modality2_folder2"],
                    "metadata_bool": [True, False],
                    "bucket_name": ["modality2", "modality2"],
                }
            ),
        ]
        result_df = self.runner._crawl_s3_buckets(
            folders_filepath, metadata_directory
        )

        expected_data = {
            "s3_prefix": [
                "modality1_folder1",
                "modality1_folder2",
                "modality2_folder1",
                "modality2_folder2",
            ],
            "metadata_bool": [True, False, True, False],
            "bucket_name": [
                "modality1",
                "modality1",
                "modality2",
                "modality2",
            ],
        }
        expected_df = pd.DataFrame(expected_data)

        # Explicitly convert column to dtype bool
        assert (
            result_df["metadata_bool"]
            .apply(lambda x: isinstance(x, bool))
            .all()
        )
        expected_df["metadata_bool"] = expected_df["metadata_bool"].astype(
            bool
        )
        result_df["metadata_bool"] = result_df["metadata_bool"].astype(bool)

        # Assert that the result DataFrame is equal to the expected DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch(
        "aind_data_asset_indexer.s3_crawler.AnalyticsTableJobRunner."
        "_crawl_s3_buckets"
    )
    def test_run_job(self, mock_crawl_s3_buckets):
        """Tests write to redshift is called as expected."""
        runner = self.runner

        mock_analytics_df = Mock()
        mock_crawl_s3_buckets.return_value = mock_analytics_df

        folders_filepath = "some_folders_filepath"
        metadata_directory = "some_metadata_directory"
        self.runner.run_job(folders_filepath, metadata_directory)

        mock_crawl_s3_buckets.assert_called_once_with(
            folders_filepath, metadata_directory
        )
        runner.redshift_client.overwrite_table_with_df.assert_called_once_with(
            df=mock_analytics_df,
            table_name="some_table_name",
            dtype=MetadataAnalyticsTableRow.data_types,
        )


if __name__ == "__main__":
    unittest.main()
