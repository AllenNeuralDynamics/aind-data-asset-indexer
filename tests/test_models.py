"""Tests methods in models module."""

import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from aind_data_asset_indexer.models import IndexJobSettings


class TestIndexJobSettings(unittest.TestCase):
    """Test methods in IndexJobSettings class."""

    def test_defaults(self):
        """Tests default values with class constructor."""
        job_settings = IndexJobSettings(s3_bucket="some_bucket")
        self.assertEqual("some_bucket", job_settings.s3_bucket)
        self.assertEqual(20, job_settings.n_partitions)
        self.assertFalse(job_settings.metadata_nd_overwrite)
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
                    '"lookback_days":10,'
                    '"metadata_nd_overwrite":true}'
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
            metadata_nd_overwrite=True,
        )
        self.assertEqual(expected_job_settings, job_settings)


if __name__ == "__main__":
    unittest.main()
