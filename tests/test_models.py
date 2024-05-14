"""Tests methods in models module."""

import unittest

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


if __name__ == "__main__":
    unittest.main()
