"""Module to create MongoConfigs to connect to DocumentDB."""

import json
import os
from dataclasses import dataclass, field

import boto3

DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
DOCDB_SECRETS_NAME = os.getenv("DOCDB_SECRETS_NAME")


@dataclass(frozen=True)
class MongoConfigs:
    """Class to store MongoClient parameters"""

    host: str
    port: int
    username: str
    password: str = field(repr=False)
    db_name: str
    collection_name: str


def get_mongo_credentials() -> MongoConfigs:
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
        db_name=DB_NAME,
        collection_name=COLLECTION_NAME,
    )
