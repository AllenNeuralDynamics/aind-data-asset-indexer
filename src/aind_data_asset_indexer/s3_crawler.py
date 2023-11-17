"""Module to crawl through s3"""

import subprocess
import pandas as pd
import os
from aind_data_access_api.rds_tables import RDSCredentials
from aind_data_access_api.rds_tables import Client as RDSClient

REDSHIFT_ENDPOINT = os.getenv("REDSHIFT")
REDSHIFT_SECRETS_NAME = os.getenv("REDSHIFT_SECRETS_NAME")


class AnalyticsJobRunner:
    """Class to handle creating metadata analytics table in redshift"""

    def __init__(self):
        """Class Constructor"""
        rds_credentials = RDSCredentials(aws_secrets_name=REDSHIFT_SECRETS_NAME)
        self.redshift_client = RDSClient(credentials=rds_credentials)
        self.buckets = os.getenv("BUCKETS")

    def crawl_s3_bucket(
            self, folders_filepath: str, metadata_directory: str
    ):
        """
        Crawls through a s3 bucket and writes redshift table
        """
        analytics_df = pd.DataFrame(columns=['bucket_name', 's3_prefix', 'metadata_bool'])
        for bucket in self.buckets:
            self._get_list_of_folders(bucket_name=bucket, output_filepath=folders_filepath)
            self._download_metadata_files(bucket_name=bucket, output_directory=metadata_directory)
            folders_df = self._create_dataframe_from_list_of_folders(folders_filepath)
            metadata_df = self._create_dataframe_from_metadata_files(metadata_directory)
            merged_df = self._join_dataframes(df1=folders_df, df2=metadata_df, bucket_name=bucket)
            analytics_df = analytics_df.append(merged_df, ignore_index=True)
        self._write_to_redshift(analytics_df)

    @staticmethod
    def _get_list_of_folders(bucket_name: str, output_filepath: str) -> None:
        """
        Downloads list of assets in bucket to output filepath
        Parameters
        ----------
        bucket_name: str
           Name of bucket in s3
        output_filepath: str
           Filepath for output file (should include filetype? ex: folders.txt)
        """
        download_command_str_bucket_to_local_filepath = (
            [
                "aws",
                "s3",
                "ls",
                f"{bucket_name}",
                str(">>"),
                f"{output_filepath}"
            ]
        )
        subprocess.run(download_command_str_bucket_to_local_filepath)

    @staticmethod
    def _download_metadata_files(bucket_name, output_directory) -> None:
        """Downloads metadata.nd.jsons to output directory"""
        sync_metadata_command_str_bucket_to_local = (
            [
                "aws",
                "s3",
                "sync",
                f"s3://{bucket_name}",
                f"{output_directory}",
                "--exclude",
                str("*"),
                "--include",
                str("*.nd.json")
            ]
        )
        subprocess.run(sync_metadata_command_str_bucket_to_local)

    @staticmethod
    def _create_dataframe_from_list_of_folders(filepath: str) -> pd.DataFrame:
        """Create a table of list of files"""
        with open(filepath, "r") as file:
            folders_list = file.readlines()
        df = pd.DataFrame(folders_list, columns=['s3_prefix'])
        # Strip whitespace and remove 'PRE' from the prefix column
        df['s3_prefix'] = df['s3_prefix'].str.strip().str.replace('PRE ', '')
        return df

    @staticmethod
    def _create_dataframe_from_metadata_files(output_directory) -> pd.DataFrame:
        """Create a table of folders with metadata file"""
        subfolders = [f.name for f in os.scandir(output_directory) if f.is_dir()]
        return pd.DataFrame({'s3_prefix': subfolders})

    @staticmethod
    def _join_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, bucket_name: str) -> pd.DataFrame:
        """Creates a table tracking whether data record folder has metadata file"""
        merged_df = pd.merge(df1, df2, on='s3_prefix', how='left', indicator=True)
        merged_df['metadata_bool'] = merged_df['_merge'] == 'both'
        merged_df = merged_df.drop('_merge', axis=1)
        merged_df['bucket_name'] = bucket_name
        return merged_df

    def _write_to_redshift(self, df: pd.DataFrame) -> None:
        """Writes results dataframe to redshift table"""
        self.redshift_client.overwrite_table_with_df(df=df, table_name="Metadata Analytics")



