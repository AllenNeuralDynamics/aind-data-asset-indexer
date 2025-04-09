"""Tests methods in models module."""

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from aind_data_asset_indexer.models import (
    AindIndexBucketJobSettings,
    AindIndexBucketsJobSettings,
    CodeOceanIndexBucketJobSettings,
    IndexJobSettings,
    PopulateAindBucketsJobSettings,
)


class TestIndexJobSettings(unittest.TestCase):
    """Test methods in IndexJobSettings class."""

    def test_defaults(self):
        """Tests default values with class constructor."""
        job_settings = IndexJobSettings(s3_bucket="some_bucket")
        self.assertEqual("some_bucket", job_settings.s3_bucket)
        self.assertEqual(20, job_settings.n_partitions)
        self.assertIsNone(job_settings.lookback_days)

    @patch("boto3.client")
    def test_from_from_param_store(self, mock_ssm_client):
        """Tests class constructor from param store."""
        mock_ssm_client.return_value.get_parameter.return_value = {
            "Parameter": {
                "Name": "a_param",
                "Type": "String",
                "Value": (
                    '{"s3_bucket":"some_bucket",'
                    '"n_partitions":5,'
                    '"lookback_days":10}'
                ),
                "Version": 1,
                "LastModifiedDate": datetime(
                    2024, 5, 4, 15, 18, 29, 8000, tzinfo=timezone.utc
                ),
                "ARN": "arn:aws:ssm:us-west-2:000000000000:parameter/a_param",
                "DataType": "text",
            },
            "ResponseMetadata": {
                "RequestId": "RequestId",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "server": "Server",
                    "date": "Tue, 14 May 2024 18:14:39 GMT",
                    "content-type": "application/x-amz-json-1.1",
                    "content-length": "415",
                    "connection": "keep-alive",
                    "x-amzn-requestid": "x-amzn-requestid",
                },
                "RetryAttempts": 0,
            },
        }
        job_settings = IndexJobSettings.from_param_store(
            param_store_name="a_param"
        )
        expected_job_settings = IndexJobSettings(
            s3_bucket="some_bucket",
            n_partitions=5,
            lookback_days=10,
        )
        self.assertEqual(expected_job_settings, job_settings)


class TestAindIndexBucketJobSettings(unittest.TestCase):
    """Tests AindIndexBucketJobSettings class"""

    def test_defaults(self):
        """Tests default values with class constructor."""
        job_settings = AindIndexBucketJobSettings(
            s3_bucket="some_bucket",
            doc_db_host="some_docdb_host",
            doc_db_db_name="some_docdb_dbname",
            doc_db_collection_name="some_docdb_collection_name",
        )
        self.assertEqual("some_bucket", job_settings.s3_bucket)
        self.assertEqual(20, job_settings.n_partitions)
        self.assertIsNone(job_settings.lookback_days)
        self.assertEqual("some_docdb_host", job_settings.doc_db_host)
        self.assertEqual("some_docdb_dbname", job_settings.doc_db_db_name)
        self.assertEqual(
            "some_docdb_collection_name", job_settings.doc_db_collection_name
        )
        self.assertTrue(job_settings.run_docdb_sync)
        self.assertTrue(job_settings.run_s3_sync)

    @patch("boto3.client")
    def test_from_from_param_store(self, mock_boto3_client):
        """Tests class constructor from param store."""
        mock_boto3_client.return_value.get_parameter.return_value = {
            "Parameter": {
                "Name": "a_param",
                "Type": "String",
                "Value": (
                    '{"doc_db_host": "some_docdb_host",'
                    '"doc_db_db_name":"some_docdb_dbname",'
                    '"doc_db_collection_name":"some_docdb_collection_name",'
                    '"s3_bucket":"some_bucket",'
                    '"run_s3_sync":false}'
                ),
                "Version": 1,
                "LastModifiedDate": datetime(
                    2024, 5, 4, 15, 18, 29, 8000, tzinfo=timezone.utc
                ),
                "ARN": "arn:aws:ssm:us-west-2:000000000000:parameter/a_param",
                "DataType": "text",
            },
            "ResponseMetadata": {
                "RequestId": "RequestId",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "server": "Server",
                    "date": "Tue, 14 May 2024 18:14:39 GMT",
                    "content-type": "application/x-amz-json-1.1",
                    "content-length": "415",
                    "connection": "keep-alive",
                    "x-amzn-requestid": "x-amzn-requestid",
                },
                "RetryAttempts": 0,
            },
        }
        job_settings = AindIndexBucketJobSettings.from_param_store(
            param_store_name="a_param"
        )
        expected_job_settings = AindIndexBucketJobSettings(
            s3_bucket="some_bucket",
            doc_db_host="some_docdb_host",
            doc_db_db_name="some_docdb_dbname",
            doc_db_collection_name="some_docdb_collection_name",
            run_s3_sync=False,
        )
        self.assertEqual(expected_job_settings, job_settings)


class TestPopulateAindBucketsJobSettings(unittest.TestCase):
    """Test PopulateAindBucketsJobSettings class"""

    def test_class_constructor(self):
        """Tests defaults are set"""
        job_settings = PopulateAindBucketsJobSettings(
            s3_buckets=["bucket1", "bucket2"]
        )
        self.assertIsNone(job_settings.s3_bucket)
        self.assertEqual(["bucket1", "bucket2"], job_settings.s3_buckets)
        self.assertEqual(20, job_settings.n_partitions)
        self.assertIsNone(job_settings.lookback_days)


class TestAindIndexBucketsJobSettings(unittest.TestCase):
    """Test AindIndexBucketsJobSettings class"""

    def test_class_constructor(self):
        """Tests defaults are set"""
        job_settings = AindIndexBucketsJobSettings(
            s3_buckets=["bucket1", "bucket2"],
            doc_db_host="some_docdb_host",
            doc_db_db_name="some_docdb_dbname",
            doc_db_collection_name="some_docdb_collection_name",
        )
        self.assertIsNone(job_settings.s3_bucket)
        self.assertEqual(["bucket1", "bucket2"], job_settings.s3_buckets)
        self.assertEqual(20, job_settings.n_partitions)
        self.assertIsNone(job_settings.lookback_days)
        self.assertEqual("some_docdb_host", job_settings.doc_db_host)
        self.assertEqual("some_docdb_dbname", job_settings.doc_db_db_name)
        self.assertEqual(
            "some_docdb_collection_name", job_settings.doc_db_collection_name
        )
        self.assertTrue(job_settings.run_docdb_sync)
        self.assertTrue(job_settings.run_s3_sync)


class TestCodeOceanIndexBucketJobSettings(unittest.TestCase):
    """Tests CodeOceanIndexBucketJobSettings class"""

    def test_defaults(self):
        """Tests default values with class constructor."""
        job_settings = CodeOceanIndexBucketJobSettings(
            s3_bucket="some_bucket",
            doc_db_host="some_docdb_host",
            doc_db_db_name="some_docdb_dbname",
            doc_db_collection_name="some_docdb_collection_name",
            codeocean_domain="some_co_domain",
            codeocean_token="some_co_token",
        )
        self.assertEqual("some_bucket", job_settings.s3_bucket)
        self.assertEqual(20, job_settings.n_partitions)
        self.assertIsNone(job_settings.lookback_days)
        self.assertEqual("some_docdb_host", job_settings.doc_db_host)
        self.assertEqual("some_docdb_dbname", job_settings.doc_db_db_name)
        self.assertEqual(
            "some_docdb_collection_name", job_settings.doc_db_collection_name
        )
        self.assertEqual("some_co_domain", job_settings.codeocean_domain)
        self.assertEqual(
            "some_co_token", job_settings.codeocean_token.get_secret_value()
        )
        self.assertTrue(job_settings.run_co_sync)

    @patch("boto3.client")
    def test_from_from_param_store(self, mock_boto3_client):
        """Tests class constructor from param store."""
        mock_boto3_client.return_value.get_parameter.return_value = {
            "Parameter": {
                "Name": "a_param",
                "Type": "String",
                "Value": (
                    '{"doc_db_host": "some_docdb_host",'
                    '"codeocean_secret_name": "some_codeocean_secret_name",'
                    '"doc_db_db_name":"some_docdb_dbname",'
                    '"doc_db_collection_name":"some_docdb_collection_name",'
                    '"s3_bucket":"some_bucket"}'
                ),
                "Version": 1,
                "LastModifiedDate": datetime(
                    2024, 5, 4, 15, 18, 29, 8000, tzinfo=timezone.utc
                ),
                "ARN": "arn:aws:ssm:us-west-2:000000000000:parameter/a_param",
                "DataType": "text",
            },
            "ResponseMetadata": {
                "RequestId": "RequestId",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "server": "Server",
                    "date": "Tue, 14 May 2024 18:14:39 GMT",
                    "content-type": "application/x-amz-json-1.1",
                    "content-length": "415",
                    "connection": "keep-alive",
                    "x-amzn-requestid": "x-amzn-requestid",
                },
                "RetryAttempts": 0,
            },
        }
        mock_boto3_client.return_value.get_secret_value.return_value = {
            "ARN": (
                "arn:aws:secretsmanager:us-west-2:000000000000:secret:"
                "some_codeocean_secret_name-1a2b3c"
            ),
            "Name": "some_codeocean_secret_name",
            "VersionId": "VersionId",
            "SecretString": (
                '{"domain": "some_co_domain",' '"token": "some_co_token"}'
            ),
            "VersionStages": ["AWSCURRENT", "AWSPENDING"],
            "CreatedDate": datetime(
                2024, 5, 20, 18, 11, 45, 174000, tzinfo=timezone.utc
            ),
            "ResponseMetadata": {
                "RequestId": "RequestId",
                "HTTPStatusCode": 200,
                "HTTPHeaders": {
                    "x-amzn-requestid": "x-amzn-requestid",
                    "content-type": "application/x-amz-json-1.1",
                    "content-length": "600",
                    "date": "Tue, 28 May 2024 19:53:17 GMT",
                },
                "RetryAttempts": 0,
            },
        }
        job_settings = CodeOceanIndexBucketJobSettings.from_param_store(
            param_store_name="a_param"
        )
        expected_job_settings = CodeOceanIndexBucketJobSettings(
            s3_bucket="some_bucket",
            doc_db_host="some_docdb_host",
            doc_db_db_name="some_docdb_dbname",
            doc_db_collection_name="some_docdb_collection_name",
            codeocean_domain="some_co_domain",
            codeocean_token="some_co_token",
        )
        self.assertEqual(expected_job_settings, job_settings)


if __name__ == "__main__":
    unittest.main()
