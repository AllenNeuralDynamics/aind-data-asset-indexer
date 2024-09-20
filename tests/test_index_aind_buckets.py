"""Tests index_aind_buckets module."""

import unittest
from unittest.mock import MagicMock, call, patch

from aind_data_asset_indexer.index_aind_buckets import IndexAindBucketsJob
from aind_data_asset_indexer.models import AindIndexBucketsJobSettings


class TestIndexAindBucketsJob(unittest.TestCase):
    """Tests PopulateAindBucketsJob class."""

    @patch(
        "aind_data_asset_indexer.aind_bucket_indexer.AindIndexBucketJob."
        "run_job"
    )
    def test_run_job(self, mock_sub_run_job: MagicMock):
        """Tests run_job method."""

        job_settings = AindIndexBucketsJobSettings(
            s3_buckets=["bucket1", "bucket2"],
            doc_db_host="some_docdb_host",
            doc_db_port=12345,
            doc_db_password="some_docdb_password",
            doc_db_user_name="some_docdb_username",
            doc_db_db_name="some_docdb_dbname",
            doc_db_collection_name="some_docdb_collection_name",
        )
        job = IndexAindBucketsJob(job_settings=job_settings)
        with self.assertLogs(level="DEBUG") as captured:
            job.run_job()
        expected_log_messages = [
            "INFO:root:Processing bucket1",
            "INFO:root:Finished processing bucket1",
            "INFO:root:Processing bucket2",
            "INFO:root:Finished processing bucket2",
        ]
        self.assertEqual(expected_log_messages, captured.output)

        mock_sub_run_job.assert_has_calls([call(), call()])


if __name__ == "__main__":
    unittest.main()
