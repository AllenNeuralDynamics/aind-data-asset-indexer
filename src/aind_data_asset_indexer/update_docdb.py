"""Module to update DocDB based on s3 records."""
import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import boto3
from pymongo import MongoClient
from pymongo.operations import UpdateMany

DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
METADATA_DIR = os.getenv("METADATA_DIRECTORY")
DOCDB_SECRETS_NAME = os.getenv("DOCDB_SECRETS_NAME")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MongoConfigs:
    """Class to store MongoClient parameters"""

    host: str
    port: int
    username: str
    password: str = field(repr=False)
    db_name: str
    collection_name: str


def get_mongo_credentials(
    db_name: str,
    collection_name: str,
) -> MongoConfigs:
    """Retrieves secrets credentials based on http request type"""
    secrets_client = boto3.client("secretsmanager")
    secret_value = secrets_client.get_secret_value(SecretId=DOCDB_SECRETS_NAME)
    secrets_client.close()
    secret = secret_value["SecretString"]
    secret_json = json.loads(secret)
    ro_username = secret_json["username"]
    ro_password = secret_json["password"]
    host = secret_json["host"]
    port = secret_json["port"]
    return MongoConfigs(
        username=ro_username,
        password=ro_password,
        host=host,
        port=port,
        db_name=db_name,
        collection_name=collection_name,
    )


class DocDBUpdater:
    """Class to handle indexing of records in DocDB."""

    def __init__(self, metadata_dir: str, mongo_configs: MongoConfigs):
        """Creates DocDB Client to read/write to collection."""
        self.metadata_dir = metadata_dir
        self.mongo_client = MongoClient(
            mongo_configs.host,
            port=mongo_configs.port,
            username=mongo_configs.username,
            password=mongo_configs.password,
            retryWrites=False,
        )
        db = self.mongo_client[mongo_configs.db_name]
        self.collection = db[mongo_configs.collection_name]

    def read_metadata_files(self) -> Dict:
        """Reads metadata files from metadata directory
        to creates a dictionary with s3-prefix : data"""
        json_data_dict = {}
        for folder_entry in os.scandir(self.metadata_dir):
            if folder_entry.is_dir():
                prefix = folder_entry.name
                folder_path = os.path.join(self.metadata_dir, prefix)
                for file_entry in os.scandir(folder_path):
                    if (
                        file_entry.name.endswith(".nd.json")
                        and file_entry.is_file()
                    ):
                        file_path = file_entry.path
                        with open(file_path, "r") as file:
                            json_data_dict[prefix] = json.load(file)
        return json_data_dict

    def bulk_write_records(self, json_data: Optional[Dict]):
        """
        Updates DocDB collection with metadata files
        Parameters
        ----------
        json_data: Dict
             Dictionary of records in s3.
        """
        if json_data:
            bulk_operations = []
            for prefix, data in json_data.items():
                filter_query = {"name": prefix}
                update_data = {"$set": data}
                bulk_operations.append(
                    UpdateMany(filter_query, update_data, upsert=True)
                )

            if bulk_operations:
                result = self.collection.bulk_write(bulk_operations)
                logger.info(
                    f"{result.upserted_count} documents inserted and"
                    f" {result.modified_count} documents updated successfully."
                )
        else:
            logger.error(
                f"No JSON files found in the directory {self.metadata_dir}."
            )
        return None

    def delete_records(self, s3_prefixes: List[str]):
        """
        Cross-checks the names of records in docDB with the ones in
        s3 and deletes records from docdb if not in s3.
        Parameters
        ----------
        s3_prefixes: List[str]
            The names of records in s3.
        """
        docdb_prefixes = self.collection.distinct("name")
        prefixes_to_delete = set(docdb_prefixes) - set(s3_prefixes)

        if prefixes_to_delete:
            self.collection.delete_many(
                {"s3_prefix": {"$in": list(prefixes_to_delete)}}
            )
            logger.info(
                f"Deleted {len(prefixes_to_delete)} records from "
                f"DocDB collection."
            )
        else:
            logger.info("Records in S3 and DocDB are synced.")

        return None

    def run_sync_records_job(self):
        """Syncs records in DocDB to S3."""
        json_data = self.read_metadata_files()
        s3_prefixes = list(json_data.keys())
        self.bulk_write_records(json_data)
        self.delete_records(s3_prefixes=s3_prefixes)


if __name__ == "__main__":
    mongo_configs = get_mongo_credentials(
        db_name=DB_NAME, collection_name=COLLECTION_NAME
    )
    job_runner = DocDBUpdater(
        metadata_dir=METADATA_DIR, mongo_configs=mongo_configs
    )
    job_runner.run_sync_records_job()
