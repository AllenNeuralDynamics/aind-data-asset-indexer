"""Test module for docdb updater"""
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pymongo.operations import UpdateMany

from aind_data_asset_indexer.update_docdb import (
    DocDBUpdater,
    MongoConfigs,
    get_mongo_credentials,
)

TEST_DIR = Path(os.path.dirname(os.path.realpath(__file__)))
METADATA_DIR = TEST_DIR / "resources" / "metadata_dir"


class TestMongoConfigs(unittest.TestCase):
    """Test class for MongoConfigs."""

    def test_mongo_configs_creation(self):
        """Tests MongoConfigs definition"""
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
    """Test class for DocDBUpdater"""

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
        """Tests mongo credentials are retrieved as expected."""
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

    def test_read_metadata_files(self):
        """Tests that files are read as expected."""
        docdb_updater = DocDBUpdater(
            metadata_dir=str(METADATA_DIR), mongo_configs=self.expected_configs
        )
        result = docdb_updater.read_metadata_files()
        self.assertEqual(len(result), 2)
        self.assertEqual(result["ecephys_test_1"]["schema_version"], "0.0.1")
        self.assertEqual(result["ecephys_test_1"]["name"], "ecephys_test_1")
        self.assertEqual(
            result["ecephys_test_1"]["metadata_status"], "Invalid"
        )
        self.assertEqual(result["ecephys_test_2"]["schema_version"], "0.0.8")
        self.assertEqual(
            result["ecephys_test_2"]["procedures"]["schema_version"], "0.9.3"
        )
        self.assertEqual(
            result["ecephys_test_2"]["metadata_status"], "Invalid"
        )

    def test_bulk_write_records(self):
        """Tests write records successfully as expected."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__.return_value = mock_db

        docdb_updater = DocDBUpdater(
            metadata_dir="test_dir", mongo_configs=self.expected_configs
        )
        docdb_updater.mongo_client = mock_mongo_client
        docdb_updater.collection = mock_collection
        docdb_updater.bulk_write_records(json_data={"data": "test_data"})

        mock_collection.bulk_write.assert_called_once_with(
            [
                UpdateMany(
                    {"name": "data"},
                    {"$set": "test_data"},
                    True,
                    None,
                    None,
                    None,
                )
            ]
        )

    @patch("aind_data_asset_indexer.update_docdb.logger.error")
    def test_bulk_write_records_empty_dir(self, mock_logging_error):
        """Tests write records fails as expected."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__.return_value = mock_db

        docdb_updater = DocDBUpdater(
            metadata_dir="empty_dir", mongo_configs=self.expected_configs
        )
        docdb_updater.mongo_client = mock_mongo_client
        docdb_updater.collection = mock_collection
        docdb_updater.bulk_write_records(json_data=None)

        mock_collection.insert_many.assert_not_called()
        mock_logging_error.assert_called_once_with(
            "No JSON files found in the directory empty_dir."
        )

    @patch("aind_data_asset_indexer.update_docdb.logger.info")
    def test_delete_records(self, mock_logging_info):
        """Tests that records are deleted from docdb collection as expected."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__.return_value = mock_db
        s3_prefixes = ["prefix1", "prefix2", "prefix3"]
        docdb_prefixes = ["prefix1", "prefix2", "prefix3", "prefix4"]
        expected_prefixes_to_delete = {"prefix4"}

        docdb_updater = DocDBUpdater(
            metadata_dir="test_dir", mongo_configs=self.expected_configs
        )
        docdb_updater.mongo_client = mock_mongo_client
        docdb_updater.collection = mock_collection
        docdb_updater.collection.distinct.return_value = docdb_prefixes
        docdb_updater.delete_records(s3_prefixes)
        docdb_updater.collection.delete_many.assert_called_once_with(
            {"s3_prefix": {"$in": list(expected_prefixes_to_delete)}}
        )
        mock_logging_info.assert_called_once_with(
            "Deleted 1 records from DocDB collection."
        )

    @patch("aind_data_asset_indexer.update_docdb.logger.info")
    def test_delete_records_nothing_to_delete(self, mock_logging_info):
        """Tests that records are deleted from docdb collection as expected."""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__.return_value = mock_db
        s3_prefixes = ["prefix1", "prefix2", "prefix3"]
        docdb_prefixes = ["prefix1", "prefix2", "prefix3"]

        docdb_updater = DocDBUpdater(
            metadata_dir="test_dir", mongo_configs=self.expected_configs
        )
        docdb_updater.mongo_client = mock_mongo_client
        docdb_updater.collection = mock_collection
        docdb_updater.collection.distinct.return_value = docdb_prefixes
        docdb_updater.delete_records(s3_prefixes)
        docdb_updater.collection.delete_many.assert_not_called()
        mock_logging_info.assert_called_once_with(
            "Records in S3 and DocDB are synced."
        )

    @patch("aind_data_asset_indexer.update_docdb.logger.info")
    @patch(
        "aind_data_asset_indexer.update_docdb.DocDBUpdater.read_metadata_files"
    )
    def test_run_sync_records_job(
        self, mock_read_metadata_files, mock_logging_info
    ):
        """Runs job to sync records from s3 to docdb"""
        mock_collection = MagicMock()
        mock_db = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        mock_mongo_client = MagicMock()
        mock_mongo_client.__getitem__.return_value = mock_db

        docdb_updater = DocDBUpdater(
            metadata_dir="test_dir", mongo_configs=self.expected_configs
        )
        docdb_updater.mongo_client = mock_mongo_client
        docdb_updater.collection = mock_collection
        mock_read_metadata_files.return_value = {"data": "test_data"}
        docdb_updater.run_sync_records_job()

        # assert that bulk write is called
        mock_collection.bulk_write.assert_called_once_with(
            [
                UpdateMany(
                    {"name": "data"},
                    {"$set": "test_data"},
                    True,
                    None,
                    None,
                    None,
                )
            ]
        )
        mock_collection.delete_many.assert_not_called()
        mock_logging_info.assert_called_with(
            "Records in S3 and DocDB are synced."
        )


if __name__ == "__main__":
    unittest.main()
