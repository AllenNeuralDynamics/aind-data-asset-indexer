"""Tests methods in s3 crawler module"""

import unittest
from unittest.mock import patch, MagicMock, mock_open, Mock
from aind_data_access_api.rds_tables import RDSCredentials, Client
from aind_data_asset_indexer.s3_crawler import AnalyticsJobRunner
import pandas as pd
import os


class MockDirEntry(MagicMock):
    def __init__(self, name, is_dir):
        super().__init__(spec=os.DirEntry)
        self.name = name
        self.configure_mock(is_dir=MagicMock(return_value=is_dir))


class TestAnalyticsJobRunner(unittest.TestCase):
    """Test methods in AnalyticsJobRunner class"""

    sample_rds_credentials = RDSCredentials(
        username="some_rds_user",
        password="some_rds_password",
        host="localhost_rds",
        port=123456,
        database="some_rds_db"
    )

    @patch.dict('os.environ', {'REDSHIFT': 'your_redshift_value',
                               'REDSHIFT_SECRETS_NAME': 'your_secrets_value',
                               'BUCKETS': '["bucket1", "bucket2"]',
                               'TABLE_NAME': 'your_table_value'})
    @patch('aind_data_asset_indexer.s3_crawler.RDSCredentials')
    def setUp(self, mock_rds_credentials):
        self.mock_credentials = Mock(spec=RDSCredentials)
        mock_rds_credentials.return_value = self.sample_rds_credentials
        self.runner = AnalyticsJobRunner()

    @patch('subprocess.run')
    def test_get_list_of_folders(self, mock_subprocess_run):
        """Tests that """
        bucket_name = 'test_bucket'
        output_filepath = 'test_output.txt'

        self.runner._get_list_of_folders(bucket_name, output_filepath)
        expected_command = [
            "aws",
            "s3",
            "ls",
            "test_bucket",
            ">>",
            "test_output.txt"
        ]
        mock_subprocess_run.assert_called_once_with(expected_command)

    @patch('subprocess.run')
    def test_download_metadata_files(self, mock_subprocess_run):
        """"""
        bucket_name = 'test_bucket'
        output_directory = 'test/directory/'

        self.runner._download_metadata_files(bucket_name, output_directory)
        expected_command = (
            [
                "aws",
                "s3",
                "sync",
                "s3://test_bucket",
                "test/directory/",
                "--exclude",
                "*",
                "--include",
                "*.nd.json"
            ]
        )
        mock_subprocess_run.assert_called_once_with(expected_command)

    @patch('builtins.open', new_callable=mock_open, read_data="PRE folder1\nPRE folder2\nPRE folder3\n")
    def test_create_dataframe_from_list_of_folders(self, mock_file_open):
        """"""
        filepath = 'test_folders.txt'
        df = self.runner._create_dataframe_from_list_of_folders(filepath)
        expected_df = pd.DataFrame({'s3_prefix': ['folder1', 'folder2', 'folder3']})

        pd.testing.assert_frame_equal(df, expected_df)
        mock_file_open.assert_called_once_with(filepath, 'r')

    @patch('os.scandir')
    def test_create_dataframe_from_metadata_files(self, mock_scandir):
        """"""
        output_directory = 'test_output_directory'

        # Mock the behavior of os.scandir
        mock_scandir.return_value = [
            MockDirEntry(name='folder1', is_dir=True),
            MockDirEntry(name='folder2', is_dir=True),
            MockDirEntry(name='file1.txt', is_dir=False),
        ]

        df = self.runner._create_dataframe_from_metadata_files(output_directory)
        expected_df = pd.DataFrame({'s3_prefix': ['folder1', 'folder2']})
        pd.testing.assert_frame_equal(df, expected_df)
        mock_scandir.assert_called_once_with(output_directory)

    def test_join_dataframes(self):
        """"""
        expected_data = {
            's3_prefix': ['folder1', 'folder2', 'folder3'],
            'metadata_bool': [True, False, True],
            'bucket_name': ['your_bucket', 'your_bucket', 'your_bucket']
        }
        expected_df = pd.DataFrame(expected_data)

        df1_data = {'s3_prefix': ['folder1', 'folder2', 'folder3']}
        df2_data = {'s3_prefix': ['folder1', 'folder3']}
        bucket_name = 'your_bucket'

        df1 = pd.DataFrame(df1_data)
        df2 = pd.DataFrame(df2_data)
        result_df = self.runner._join_dataframes(df1, df2, bucket_name)
        pd.testing.assert_frame_equal(result_df, expected_df)

    @patch.object(AnalyticsJobRunner, '_get_list_of_folders')
    @patch.object(AnalyticsJobRunner, '_download_metadata_files')
    @patch.object(AnalyticsJobRunner, '_create_dataframe_from_list_of_folders')
    @patch.object(AnalyticsJobRunner, '_create_dataframe_from_metadata_files')
    @patch.object(AnalyticsJobRunner, '_join_dataframes')
    def test_crawl_s3_buckets(
            self, mock_join_dataframes, mock_create_metadata, mock_create_folders, mock_download_metadata,
            mock_get_list_of_folders
    ):
        """"""
        folders_filepath = 'test_folders.txt'
        metadata_directory = 'test_metadata_dir'

        # Mock the behavior of dependent methods
        mock_get_list_of_folders.side_effect = [
            pd.DataFrame({'s3_prefix': [f'folder1_{bucket}', f'folder2_{bucket}']}) for bucket in ['bucket1', 'bucket2']
        ]

        mock_create_metadata.side_effect = [
            pd.DataFrame({'s3_prefix': [f'folder1_{directory}', f'folder3_{directory}']}) for directory in
            ['dir1', 'dir2']
        ]

        mock_create_folders.side_effect = [
            pd.DataFrame({'s3_prefix': [f'folder1_{filepath}', f'folder2_{filepath}']}) for filepath in
            ['file1', 'file2']
        ]

        mock_download_metadata.side_effect = [None, None]

        mock_join_dataframes.side_effect = [
            pd.DataFrame({'s3_prefix': [f'folder1_bucket1', f'folder2_bucket1'],
                          'metadata_bool': [True, False],
                          'bucket_name': ['bucket1', 'bucket1']
            }),
            pd.DataFrame({'s3_prefix': [f'folder1_bucket2', f'folder2_bucket2'],
                          'metadata_bool': [True, False],
                          'bucket_name': ['bucket2', 'bucket2']
            })
        ]
        result_df = self.runner._crawl_s3_buckets(folders_filepath, metadata_directory)

        expected_data = {
            's3_prefix': ['folder1_bucket1', 'folder2_bucket1', 'folder1_bucket2', 'folder2_bucket2'],
            'metadata_bool': [True, False, True, False],
            'bucket_name': ['bucket1', 'bucket1', 'bucket2', 'bucket2'],
        }
        expected_df = pd.DataFrame(expected_data)

        # Explicitly convert column to dtype bool
        assert result_df['metadata_bool'].apply(lambda x: isinstance(x, bool)).all()
        expected_df['metadata_bool'] = expected_df['metadata_bool'].astype(bool)
        result_df['metadata_bool'] = result_df['metadata_bool'].astype(bool)

        # Assert that the result DataFrame is equal to the expected DataFrame
        pd.testing.assert_frame_equal(result_df, expected_df)








