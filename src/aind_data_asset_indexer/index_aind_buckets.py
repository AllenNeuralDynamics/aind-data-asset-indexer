"""Module to sync list of buckets with DocDb."""

import argparse
import logging
import os
import sys

from aind_data_asset_indexer.aind_bucket_indexer import AindIndexBucketJob
from aind_data_asset_indexer.models import (
    AindIndexBucketJobSettings,
    AindIndexBucketsJobSettings,
)

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))


class IndexAindBucketsJob:
    """Job to sync records in DocDb to list of aind buckets."""

    def __init__(self, job_settings: AindIndexBucketsJobSettings):
        """Class constructor."""
        self.job_settings = job_settings

    def run_job(self):
        """Main job runner."""
        base_job_configs = self.job_settings.model_dump(
            exclude={"s3_bucket", "s3_buckets"}
        )
        for bucket in self.job_settings.s3_buckets:
            logging.info(f"Processing {bucket}")
            bucket_job_settings = AindIndexBucketJobSettings(
                s3_bucket=bucket, **base_job_configs
            )
            bucket_job = AindIndexBucketJob(job_settings=bucket_job_settings)
            try:
                bucket_job.run_job()
            except Exception as e:
                logging.error(f"Error processing {bucket}. Error: {e}")
            logging.info(f"Finished processing {bucket}")


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
        main_job_settings = AindIndexBucketsJobSettings.model_validate_json(
            cli_args.job_settings
        )
    else:
        main_job_settings = AindIndexBucketsJobSettings.from_param_store(
            param_store_name=cli_args.param_store_name
        )
    main_job = IndexAindBucketsJob(job_settings=main_job_settings)
    main_job.run_job()
