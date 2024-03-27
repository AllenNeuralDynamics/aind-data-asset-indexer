"""Module to update DocDB based on s3 records."""
import json
import logging
import os
from dataclasses import dataclass, field
from typing import List

import boto3
from pymongo import MongoClient

DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
METADATA_DIR = os.getenv("METADATA_DIRECTORY")
READWRITE_SECRET = os.getenv("READWRITE_SECRET")

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
    secret_value = secrets_client.get_secret_value(SecretId=READWRITE_SECRET)
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

    def read_metadata_files(self) -> List:
        """Reads metadata files in input directory"""
        json_data = []
        for filename in os.listdir(self.metadata_dir):
            if filename.endswith(".nd.json"):
                filepath = os.path.join(self.metadata_dir, filename)
                with open(filepath, "r") as file:
                    json_data.append(json.load(file))
        return json_data

    def bulk_write_records(self):
        """Inserts metadata files in directory to DocDB collection"""
        json_data = self.read_metadata_files()
        if json_data:
            self.collection.insert_many(json_data)
            logger.info(
                f"Documents in {self.metadata_dir} inserted successfully."
            )
        else:
            logger.error(
                f"No JSON files found in the directory {self.metadata_dir}."
            )
        return None


if __name__ == "__main__":
    mongo_configs = get_mongo_credentials(
        db_name=DB_NAME, collection_name=COLLECTION_NAME
    )
    job_runner = DocDBUpdater(
        metadata_dir=METADATA_DIR, mongo_configs=mongo_configs
    )
    job_runner.bulk_write_records()
