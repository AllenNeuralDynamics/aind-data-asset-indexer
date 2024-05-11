"""Module to update S3 based on DocumentDB records."""

import json
import logging
import os
import subprocess
from typing import Dict, List, Optional

from pymongo import MongoClient

from aind_data_asset_indexer.mongo_configs import get_mongo_credentials

METADATA_DIR = os.getenv("METADATA_DIRECTORY")
BUCKETS = os.getenv("BUCKETS")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class S3Updater:
    """Class to update S3 buckets based on records in DocDB."""

    def __init__(self):
        """Creates S3Updater to sync DocDB with S3."""
        self.metadata_dir = METADATA_DIR
        self.buckets = [item.strip() for item in BUCKETS.split(",")]
        mongo_configs = get_mongo_credentials()
        self.mongo_client = MongoClient(
            mongo_configs.host,
            port=mongo_configs.port,
            username=mongo_configs.username,
            password=mongo_configs.password,
            retryWrites=False,
        )
        db = self.mongo_client[mongo_configs.db_name]
        self.collection = db[mongo_configs.collection_name]

    def query_records_from_bucket(self, bucket: str):
        """
        Queries DocDB for records from a bucket.
        Parameters
        ----------
        bucket: str
            The bucket name to query records for.
        Returns
        -------
        records: List[Dict]
            List of s3_prefixes for records in DocDB that match the bucket.
        """

        records = self.collection.find({"location": bucket}, {"name": 1})
        # TODO: add sanity check that all name (s3_prefix) are unique in this bucket?
        # TODO: write result to file instead of returning it?
        s3_prefixes = [record["name"] for record in records]
        logger.info(
            f"Found {len(s3_prefixes)} records from DocDB for bucket: {bucket}."
        )
        return records

    def upsert_to_s3(self, bucket, records):
        """Syncs records in DocDB with S3."""
        metadata_dir = os.path.join(self.metadata_dir, bucket)
        count_updated = 0
        count_new = 0
        for s3_prefix in records:
            expected_file_path = os.path.join(
                metadata_dir, s3_prefix, "metadata.nd.json"
            )
            if not os.path.exists(expected_file_path):
                logger.error(
                    f"Metadata for {s3_prefix} not found in {bucket}."
                )
                self.upsert_one_to_s3(bucket, s3_prefix)
                count_new += 1
            else:
                with open(expected_file_path, "r") as file:
                    record_data = json.load(file)
                    # TODO: check if we need to use some other deep comparison method
                    if record_data != s3_prefix:
                        logger.info(
                            f"Record {s3_prefix['name']} in S3 does not match record in DocDB."
                        )
                        self.upsert_one_to_s3(bucket, s3_prefix)
                        count_updated += 1
                    else:
                        logger.info(
                            f"Record {s3_prefix['name']} in S3 matches record in DocDB."
                        )
        logger.info(
            f"Created {count_new} objects and updated {count_updated} objects in S3 bucket {bucket}."
        )
        return

    def upsert_one_to_s3(self, bucket, prefix):
        """Upserts one record to S3."""
        record = self.collection.find_one({"name": prefix, "location": bucket})
        if record:
            file_path = os.path.join(
                self.metadata_dir, bucket, prefix, "metadata.nd.json"
            )
            # TODO: verify cp command can overwrite existing file
            with open(file_path, "w") as file:
                json.dump(record, file)
            aws_cmd = f"aws s3 cp {file_path} s3://{bucket}/{prefix}/metadata.nd.json"
            subprocess.run(aws_cmd)

    def delete_from_s3(self, bucket):
        """Deletes records from S3 that are not in DocDB."""
        metadata_dir = os.path.join(self.metadata_dir, bucket)
        count_deleted = 0
        for file in os.scandir(metadata_dir):
            if file.is_dir():
                s3_prefix = file.name
                record = self.collection.find(
                    {"name": s3_prefix, "location": bucket}
                )
                count = record.count()
                if count == 0:
                    print(
                        f"No record found in DocDB with {s3_prefix} and bucket {bucket}."
                    )
                    self.delete_one_from_s3(bucket, s3_prefix)
                elif count != 1:
                    logger.error(
                        f"Multiple records found for prefix {s3_prefix} in DocDB."
                    )
        logger.info(
            f"Deleted {count_deleted} objects from S3 bucket {bucket}."
        )
        return

    def delete_one_from_s3(self, bucket, prefix):
        """Deletes one record from S3 that is not in DocDB."""
        # TODO: check this
        aws_cmd = f"aws s3 rm s3://{bucket}/{prefix}/metadata.nd.json"
        subprocess.run(aws_cmd)
        return

    def run_sync_docdb_to_s3(self):
        """Syncs records in DocDB to S3."""
        # NOTE: assumes that s3_crawler has downloaded all s3 files to METADATA_DIR
        # TODO: parallelize this?
        for bucket in self.buckets:
            logger.info(f"Syncing DocDB records for bucket: {bucket}")
            records = self.query_records_from_bucket(bucket)
            self.upsert_to_s3(bucket, records)
            self.delete_from_s3(bucket)
        return


# The only thing this script relies on, is that s3_crawler script has already downloaded all nd.json files
# to the metadata directory, we don't care about list of recrods.txt
if __name__ == "__main__":
    s3_updater = S3Updater()
    s3_updater.run_sync_docdb_to_s3()
