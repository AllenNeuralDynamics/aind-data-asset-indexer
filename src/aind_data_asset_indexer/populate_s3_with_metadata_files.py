"""Module to handle populating s3 bucket with metadata files."""
import argparse
import logging
import os
import sys
import warnings
from typing import List

import boto3
import dask.bag as dask_bag
from mypy_boto3_s3 import S3Client

from aind_data_asset_indexer.models import IndexJobSettings
from aind_data_asset_indexer.utils import (
    build_metadata_record_from_prefix,
    create_core_schema_object_keys_map,
    does_s3_object_exist,
    iterate_through_top_level,
    upload_metadata_json_str_to_s3,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
# pydantic raises too many serialization warnings
warnings.filterwarnings("ignore", category=UserWarning)


class AindPopulateMetadataJsonJob:
    """This job will:
    1) Crawl through an S3 bucket
    2) Look inside each prefix that adheres to data asset naming convention
    3) If the name is a data asset name, then it will look inside the prefix
    4.0) If there is no metadata.nd.json file, then it will create one by using
    any of the core json files it finds.
    4.1) If the metadata_nd_overwrite option is set to False, then it will pass
    a data asset if there is already a metadata.nd.json in that folder. If set
    to True, then it will write a new metadata.nd.json file even if one already
    exists.
    """

    def __init__(self, job_settings: IndexJobSettings):
        """Class constructor."""
        self.job_settings = job_settings

    def _process_prefix(self, prefix: str, s3_client: S3Client):
        """
        For a given prefix, build a metadata record and upload it to S3.
        Parameters
        ----------
        prefix : str
        s3_client : S3Client

        Returns
        -------
        None

        """
        md_record = build_metadata_record_from_prefix(
            prefix=prefix,
            s3_client=s3_client,
            bucket=self.job_settings.s3_bucket,
            metadata_nd_overwrite=self.job_settings.metadata_nd_overwrite,
        )
        if md_record is not None:
            bucket = self.job_settings.s3_bucket
            object_keys = create_core_schema_object_keys_map(prefix)
            for source, target in object_keys.items():
                if does_s3_object_exist(
                    s3_client=s3_client, bucket=bucket, key=source
                ):
                    logging.info(
                        f"Copying {source} to {target} in s3://{bucket}"
                    )
                    response = s3_client.copy_object(
                        Bucket=bucket,
                        CopySource={"Bucket": bucket, "Key": source},
                        Key=target,
                    )
                    logging.info(response)
                else:
                    logging.info(
                        f"s3://{bucket}/{source} does not exist. Skipping copy."
                    )
            
            logging.info(
                f"Uploading metadata record for s3://{bucket}/{prefix}"
            )
            # noinspection PyTypeChecker
            response = upload_metadata_json_str_to_s3(
                metadata_json=md_record,
                bucket=self.job_settings.s3_bucket,
                prefix=prefix,
                s3_client=s3_client,
            )
            logging.info(response)
        else:
            logging.warning(
                f"Metadata record is None for "
                f"s3://{self.job_settings.s3_bucket}/{prefix}!"
            )

    def _dask_task_to_process_prefix_list(
        self, prefix_list: List[str]
    ) -> None:
        """
        The task to perform within a partition. If n_partitions is set to 20
        and the outer prefix list had length 1000, then this should process
        50 prefixes.
        Parameters
        ----------
        prefix_list : List[str]

        Returns
        -------

        """
        # create a s3_client here since dask doesn't serialize it
        s3_client = boto3.client("s3")
        for prefix in prefix_list:
            self._process_prefix(prefix=prefix, s3_client=s3_client)
        s3_client.close()

    def _process_prefixes(self, prefixes: List[str]):
        """
        For a list of prefixes (up to a 1000 in the list), divvy up the list
        across n_partitions. Process the set of prefixes in each partition.
        Parameters
        ----------
        prefixes : List[str]

        Returns
        -------
        None

        """
        prefix_bag = dask_bag.from_sequence(
            prefixes, npartitions=self.job_settings.n_partitions
        )
        mapped_partitions = dask_bag.map_partitions(
            self._dask_task_to_process_prefix_list, prefix_bag
        )
        mapped_partitions.compute()

    def run_job(self):
        """Main method to run. This will:
        1) Iterate through prefixes in s3, 1000 at a time
        2) Divvy up the 1000 prefixes across dask n_partitions
        3) Process each prefix in each set in each partition
        """
        iterator_s3_client = boto3.client("s3")
        prefix_iterator = iterate_through_top_level(
            s3_client=iterator_s3_client, bucket=self.job_settings.s3_bucket
        )
        for prefix_list in prefix_iterator:
            self._process_prefixes(prefix_list)
        iterator_s3_client.close()


if __name__ == "__main__":
    sys_args = sys.argv[1:]
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--job-settings",
        required=False,
        type=str,
        help=(
            r"""
            Instead of init args the job settings can optionally be passed in
            as a json string in the command line.
            """
        ),
    )
    parser.add_argument(
        "-p",
        "--param-store-name",
        required=False,
        type=str,
        help=(
            r"""
            Instead of init args the job settings can optionally be pulled from
            the aws param store.
            """
        ),
    )
    cli_args = parser.parse_args(sys_args)
    if cli_args.job_settings is None and cli_args.param_store_name is None:
        raise ValueError(
            "At least one of job-settings or param-store-name needs to be set"
        )
    if cli_args.job_settings is not None:
        main_job_settings = IndexJobSettings.model_validate_json(
            cli_args.job_settings
        )
    else:
        main_job_settings = IndexJobSettings.from_param_store(
            param_store_name=cli_args.param_store_name
        )
    main_job = AindPopulateMetadataJsonJob(job_settings=main_job_settings)
    main_job.run_job()
