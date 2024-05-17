"""Tests populate_aind_buckets module."""

import unittest
from unittest.mock import MagicMock, call, patch

from aind_data_asset_indexer.models import PopulateAindBucketsJobSettings
from aind_data_asset_indexer.populate_aind_buckets import (
    PopulateAindBucketsJob,
)


class TestPopulateAindBucketsJob(unittest.TestCase):
    """Tests PopulateAindBucketsJob class."""

    @patch(
        "aind_data_asset_indexer.populate_s3_with_metadata_files."
        "AindPopulateMetadataJsonJob.run_job"
    )
    @patch("logging.info")
    def test_run_job(
        self, mock_log_info: MagicMock, mock_sub_run_job: MagicMock
    ):
        """Tests run_job method."""

        job_settings = PopulateAindBucketsJobSettings(
            s3_buckets=["bucket1", "bucket2"]
        )
        job = PopulateAindBucketsJob(job_settings=job_settings)
        job.run_job()
        mock_log_info.assert_has_calls(
            [
                call("Processing bucket1"),
                call("Finished processing bucket1"),
                call("Processing bucket2"),
                call("Finished processing bucket2"),
            ]
        )
        mock_sub_run_job.assert_has_calls([call(), call()])


if __name__ == "__main__":
    unittest.main()
