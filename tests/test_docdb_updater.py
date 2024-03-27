"""Test module for docdb updater"""
import unittest
from unittest.mock import MagicMock, patch, mock_open
import json
import os

from aind_data_asset_indexer.update_docdb import (
    DocDBUpdater,
    MongoConfigs,
    get_mongo_credentials,
)


class TestMongoConfigs(unittest.TestCase):
    def test_mongo_configs_creation(self):
        mongo_config = MongoConfigs(
            host="localhost",
            port=27017,
            username="user",
            password="password",
            db_name="test_db",
            collection_name="test_collection",
        )
        self.assertEqual(mongo_config.host, "localhost")
        self.assertEqual(mongo_config.port, 27017)
        self.assertEqual(mongo_config.username, "user")
        self.assertEqual(mongo_config.password, "password")
        self.assertEqual(mongo_config.db_name, "test_db")
        self.assertEqual(mongo_config.collection_name, "test_collection")


class TestDocDBUpdater(unittest.TestCase):
    expected_configs = MongoConfigs(
        host="localhost",
        port=27017,
        username="user",
        password="password",
        db_name="test_db",
        collection_name="test_collection",
    )

    @patch("aind_data_asset_indexer.update_docdb.boto3.client")
    def test_get_mongo_credentials(self, mock_boto3_client):
        # Mocking the secrets manager response
        mock_secrets_client = MagicMock()
        mock_secrets_client.get_secret_value.return_value = {
            "SecretString": '{"username": "user", "password": "password",'
                            ' "host": "localhost", "port": 27017}'
        }
        mock_boto3_client.return_value = mock_secrets_client

        mongo_configs = get_mongo_credentials("test_db", "test_collection")
        self.assertEqual(mongo_configs.host, "localhost")
        self.assertEqual(mongo_configs.port, 27017)
        self.assertEqual(mongo_configs.username, "user")
        self.assertEqual(mongo_configs.password, "password")
        self.assertEqual(mongo_configs.db_name, "test_db")
        self.assertEqual(mongo_configs.collection_name, "test_collection")

    @patch("aind_data_asset_indexer.update_docdb.os.listdir")
    def test_read_metadata_files(self, list_dir):
        list_dir.return_value = ["file1.nd.json", "file2.nd.json"]
        with patch("builtins.open", side_effect=[MagicMock()] * 2) as mock_open:
            mock_files = [MagicMock(), MagicMock()]
            mock_files[0].__enter__.return_value.read.return_value = json.dumps({"id": 1, "name": "test1"})
            mock_files[1].__enter__.return_value.read.return_value = json.dumps({"id": 2, "name": "test2"})
            mock_open.side_effect = mock_files

            updater = DocDBUpdater("some/path/directory", self.expected_configs)
            result = updater.read_metadata_files()

            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["id"], 1)
            self.assertEqual(result[1]["name"], "test2")

    @patch(
        "aind_data_asset_indexer.update_docdb.DocDBUpdater.read_metadata_files"
    )
    @patch("aind_data_asset_indexer.update_docdb.logger.info")
    def test_bulk_write_records(
        self, mock_logging_info, mock_read_metadata_files
    ):
        """Tests write records successfully as expected."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__.return_value = mock_db

        docdb_updater = DocDBUpdater(
            metadata_dir="test_dir",
            mongo_configs=self.expected_configs
        )
        docdb_updater.mongo_client = mock_mongo_client
        docdb_updater.collection = mock_collection
        mock_read_metadata_files.return_value = [{"data": "test_data"}]
        docdb_updater.bulk_write_records()

        mock_collection.insert_many.assert_called_once_with(
            [{"data": "test_data"}]
        )
        mock_logging_info.assert_called_once_with("Documents in test_dir inserted successfully.")

    @patch(
        "aind_data_asset_indexer.update_docdb.DocDBUpdater.read_metadata_files"
    )
    @patch("aind_data_asset_indexer.update_docdb.logger.error")
    def test_bulk_write_records_failure(
        self, mock_logging_error, mock_read_metadata_files
    ):
        """Tests write records fails as expected."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__.return_value = mock_db

        docdb_updater = DocDBUpdater(
            metadata_dir="empty_dir",
            mongo_configs=self.expected_configs
        )
        docdb_updater.mongo_client = mock_mongo_client
        docdb_updater.collection = mock_collection
        mock_read_metadata_files.return_value = None
        docdb_updater.bulk_write_records()

        mock_collection.insert_many.assert_not_called()
        mock_logging_error.assert_called_once_with("No JSON files found in the directory empty_dir.")


if __name__ == "__main__":
    unittest.main()
