"""Module to update DocDB based on s3 records."""
import pandas as pd
import json
import os
# from aind_data_access_api.document_db import Client as DocDBClient
from pymongo import MongoClient
from dataclasses import dataclass, field

DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
READWRITE_SECRET = os.getenv("READWRITE_SECRET")


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


class DocDBIndexRunner:
    """Class to handle indexing of records in DocDB."""

    def __init__(self, metadata_dir: str, db_name: str, collection_name: str):
        """Class Constructor, creates DocDB Client to read/write to collection."""
        self.metadata_dir = metadata_dir
        mongo_configs = get_mongo_credentials(
            db_name=db_name, collection_name=collection_name
        )
        self.mongo_client = MongoClient(
            mongo_configs.host,
            port=mongo_configs.port,
            username=mongo_configs.username,
            password=mongo_configs.password,
            retryWrites=False,
        )
        db = self.mongo_client[mongo_configs.db_name]
        self.collection = db[mongo_configs.collection_name]

    def overwrite_records(self):
        """"""
        for f in os.scandir(self.metadata_dir):
            contents = f["Body"].read().decode("utf-8")
            json_contents = json.loads(contents)
            x = self.collection.update_one(
                {"_id": json_contents["_id"]},
                {"$set": json_contents},
            )
            print(x.raw_result)
            return None














