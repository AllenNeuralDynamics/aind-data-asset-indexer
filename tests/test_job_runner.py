"""Tests methods in job module"""
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytz
from aind_codeocean_api.codeocean import CodeOceanClient
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_access_api.document_store import DocumentStoreCredentials
from aind_data_access_api.models import DataAssetRecord
from aind_data_asset_indexer.job import JobRunner


class TestJobRunner(unittest.TestCase):
    """Test methods in JobRunner class"""

    doc_db_client = MetadataDbClient(
        host="localhost", database="some_db", collection="some_coll"
    )

    co_client = CodeOceanClient(
        domain="codeocean_domain", token="codeocean_token"
    )

    sample_creds = DocumentStoreCredentials(
        username="some_user",
        password="some_password",
        host="localhost",
        port=12345,
        database="some_db",
    )

    sample_job = JobRunner(
        codeocean_client=co_client,
        doc_db_client=doc_db_client,
        data_asset_bucket="data_asset_bucket",
    )

    example_co_response1 = {
        "created": 1679688272,
        "description": "",
        "files": 119885,
        "id": "0aaa0aa0-0000-000a-aa00-a0000aa0000",
        "last_used": 0,
        "name": "modality_000001_2023-03-23_14-22-52",
        "size": 330948268211,
        "sourceBucket": {
            "bucket": "some-s3-bucket",
            "origin": "aws",
            "prefix": "modality_000001_2023-03-23_14-22-52",
        },
        "state": "ready",
        "tags": ["some_tag", "raw"],
        "type": "dataset",
    }

    example_co_response2 = {
        "created": 1679672848,
        "custom_metadata": {
            "data level": "raw data",
            "institution": "ACME",
            "modality": "a modality",
            "subject id": "000002",
        },
        "description": "some description",
        "files": 213,
        "id": "0000000b-0b00-0000-0000-bbb0b0000000",
        "last_used": 0,
        "name": "modality1_000002_20230126",
        "size": 98030431841,
        "sourceBucket": {"bucket": "", "origin": "local", "prefix": ""},
        "state": "ready",
        "tags": ["modality1", "tag2"],
        "type": "dataset",
    }

    example_co_response3 = {
        "created": 1679439776,
        "description": "",
        "files": 3711,
        "id": "c00000c0-00cc-00c0-ccc0-000000c000c0",
        "last_used": 0,
        "name": "modality3_000002_2023-03-17_15-49-02_sorted-ks2.5",
        "provenance": {
            "capsule": "dd0d000d-00dd-0000-ddd0-0000d000000d",
            "commit": "e0000000eeee0e000e00e00ee00e0e000ee0e0e0",
            "data_assets": ["000ff0ff-f000-0ff0-f000-ff00000f00f0"],
            "docker_image": "000000gg-g0g0-0g00-g000-000g00g00000",
            "run_script": "code/run",
        },
        "size": 60271676779,
        "state": "ready",
        "tags": ["modality3", "derived", "000002"],
        "type": "result",
    }

    example_co_response4 = {
        "created": 1680687384,
        "description": "",
        "files": 45251,
        "id": "0b00b0b0-00b0-00b0-bb00-0b0000bb0b0b",
        "last_used": 0,
        "name": (
            "SmartSPIM_648845_2023-03-30_10-51-33_stitched_2023-04-04_18-33-05"
        ),
        "size": 503600076800,
        "sourceBucket": {
            "bucket": "some_s3_bucket",
            "origin": "aws",
            "prefix": (
                "SmartSPIM_648845_2023-03-30_10-51-33_stitched_2023-04-04_18-"
                "33-05"
            ),
        },
        "state": "ready",
        "tags": ["smartspim", "processed"],
        "type": "dataset",
    }

    example_s3_response1 = {
        "ResponseMetadata": {
            "RequestId": "XXXXXX",
            "HostId": "abc123==",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {
                "x-amz-id-2": "abc123==",
                "x-amz-request-id": "9XXXXXX",
                "date": "Sat, 25 Mar 2023 00:28:39 GMT",
                "x-amz-bucket-region": "us-west-2",
                "content-type": "application/xml",
                "transfer-encoding": "chunked",
                "server": "AmazonS3",
            },
            "RetryAttempts": 0,
        },
        "IsTruncated": False,
        "Contents": [
            {
                "Key": "c00000c0-00cc-00c0-ccc0-000000c000c0/output",
                "LastModified": datetime(
                    2023, 3, 15, 0, 32, 20, tzinfo=pytz.UTC
                ),
                "ETag": '"e11e1eee11111eee11e11111e11111e1"',
                "Size": 87370,
                "StorageClass": "STANDARD",
            },
            {
                "Key": "c00000c0-00cc-00c0-ccc0-000000c000c0/params.json",
                "LastModified": datetime(
                    2023, 3, 15, 0, 32, 20, tzinfo=pytz.UTC
                ),
                "ETag": '"5e5ee5555eee55555e55e55555e55eee"',
                "Size": 3495,
                "StorageClass": "STANDARD",
            },
            {
                "Key": (
                    "c00000c0-00cc-00c0-ccc0-000000c000c0/"
                    "visualization_output.json"
                ),
                "LastModified": datetime(
                    2023, 3, 15, 0, 33, 3, tzinfo=pytz.UTC
                ),
                "ETag": '"e99999e9e9eeee9999e99e999999fee9"',
                "Size": 604,
                "StorageClass": "STANDARD",
            },
            {
                "Key": (
                    "c00000c0-00cc-00c0-ccc0-000000c000c0/"
                    "corrupted_file.json"
                ),
                "LastModified": datetime(
                    2023, 3, 15, 0, 33, 3, tzinfo=pytz.UTC
                ),
                "ETag": '"e99999e9e9eeee9999e99e999999fee9"',
                "Size": 604,
                "StorageClass": "STANDARD",
            },
        ],
        "Name": "codeocean-s3datasetsbucket-0a00aaa00aa0",
        "Prefix": "c00000c0-00cc-00c0-ccc0-000000c000c0/",
        "Delimiter": "/",
        "MaxKeys": 1000,
        "CommonPrefixes": [
            {"Prefix": "c00000c0-00cc-00c0-ccc0-000000c000c0/drift_maps/"},
            {"Prefix": "c00000c0-00cc-00c0-ccc0-000000c000c0/postprocessed/"},
            {
                "Prefix": (
                    "c00000c0-00cc-00c0-ccc0-000000c000c0/sorting_precurated/"
                )
            },
            {"Prefix": "c00000c0-00cc-00c0-ccc0-000000c000c0/spikesorted/"},
        ],
        "EncodingType": "url",
        "KeyCount": 7,
    }

    raw_stream = MagicMock()

    raw_stream.read.side_effect = [
        '{"foo": "bar"}'.encode("utf-8"),
        (
            '{"exp1_Record Node 101#Neuropix-PXI-100.ProbeA_recording1": '
            '{"timeseries": "https://link-to-timeseries-plots/",'
            '"sorting_summary": "https:link-to-sorting-summary"}}'
        ).encode("utf-8"),
        '{"foo": "bar""}'.encode("utf-8"),  # Unreadable json
    ]

    s3_object_example = {
        "ResponseMetadata": {
            "RequestId": "CCCCCC",
            "HostId": "zzzzzz=",
            "HTTPStatusCode": 200,
            "HTTPHeaders": {},
            "RetryAttempts": 0,
        },
        "Metadata": {},
        "Body": raw_stream,
    }

    def test_mock_class_construct(self):
        """Basic test for class construction"""
        job = self.sample_job

        self.assertEqual("codeocean_domain", job.codeocean_client.domain)
        self.assertEqual("codeocean_token", job.codeocean_client.token)
        self.assertEqual("some_coll", job.doc_db_client.collection)
        self.assertEqual("localhost", job.doc_db_client.host)
        self.assertEqual("some_db", job.doc_db_client.database)

    def test_map_co_response_to_record(self):
        """Tests that code ocean responses are mapped correctly"""
        job = self.sample_job
        co_responses = [
            self.example_co_response1,
            self.example_co_response2,
            self.example_co_response3,
            self.example_co_response4,
        ]

        mapped_responses = [
            job._map_co_response_to_record(co_response)
            for co_response in co_responses
        ]

        expected_responses = [
            DataAssetRecord(
                _id="0aaa0aa0-0000-000a-aa00-a0000aa0000",
                _name="modality_000001_2023-03-23_14-22-52",
                _created=datetime(2023, 3, 24, 20, 4, 32, tzinfo=pytz.UTC),
                _location=(
                    "s3://some-s3-bucket/modality_000001_2023-03-23_14-22-52"
                ),
            ),
            None,
            DataAssetRecord(
                _id="c00000c0-00cc-00c0-ccc0-000000c000c0",
                _name="modality3_000002_2023-03-17_15-49-02_sorted-ks2.5",
                _created=datetime(2023, 3, 21, 23, 2, 56, tzinfo=pytz.UTC),
                _location=(
                    "s3://data_asset_bucket/"
                    "c00000c0-00cc-00c0-ccc0-000000c000c0"
                ),
            ),
            DataAssetRecord(
                _id="0b00b0b0-00b0-00b0-bb00-0b0000bb0b0b",
                _name=(
                    "SmartSPIM_648845_2023-03-30_10-51-33_stitched_2023-04-"
                    "04_18-33-05"
                ),
                _created=datetime(2023, 4, 5, 9, 36, 24, tzinfo=pytz.UTC),
                _location=(
                    "s3://some_s3_bucket/"
                    "SmartSPIM_648845_2023-03-30_10-51-33_stitched_2023-04-"
                    "04_18-33-05"
                ),
            ),
        ]

        self.assertEqual(expected_responses, mapped_responses)

        # Check that prefixes have final backslashes removed to normalize
        # location
        co_responses[0]["sourceBucket"]["prefix"] = (
            co_responses[0]["sourceBucket"]["prefix"] + "/"
        )
        mapped_responses = [
            job._map_co_response_to_record(co_response)
            for co_response in co_responses
        ]

        self.assertEqual(expected_responses, mapped_responses)

        # Check only aws buckets are currently tracked
        co_responses[0]["sourceBucket"]["origin"] = "gcp"
        expected_responses[0] = None
        mapped_responses = [
            job._map_co_response_to_record(co_response)
            for co_response in co_responses
        ]
        self.assertEqual(expected_responses, mapped_responses)

        return None

    @patch("boto3.client")
    def test_s3_mapped_response(self, mock_s3_client: MagicMock):
        """Tests that s3 responses are mapped correctly"""
        job = self.sample_job

        mock_s3_client.list_objects_v2.return_value = self.example_s3_response1
        mock_s3_client.get_object.return_value = self.s3_object_example

        data_asset_record = DataAssetRecord(
            _id="c00000c0-00cc-00c0-ccc0-000000c000c0",
            _name="modality3_000002_2023-03-17_15-49-02_sorted-ks2.5",
            _created=datetime(2023, 3, 21, 23, 2, 56, tzinfo=pytz.UTC),
            _location=(
                "s3://data_asset_bucket/"
                "c00000c0-00cc-00c0-ccc0-000000c000c0"
            ),
        )

        job._update_base_record_with_s3_info(
            s3_client=mock_s3_client, base_record=data_asset_record
        )

        expected_data_asset_record = DataAssetRecord(
            _id="c00000c0-00cc-00c0-ccc0-000000c000c0",
            _name="modality3_000002_2023-03-17_15-49-02_sorted-ks2.5",
            _created=datetime(2023, 3, 21, 23, 2, 56, tzinfo=pytz.UTC),
            _location=(
                "s3://data_asset_bucket/"
                "c00000c0-00cc-00c0-ccc0-000000c000c0"
            ),
            params={"foo": "bar"},
            visualization_output={
                "exp1_Record Node 101#Neuropix-PXI-100_ProbeA_recording1": {
                    "timeseries": "https://link-to-timeseries-plots/",
                    "sorting_summary": "https:link-to-sorting-summary",
                }
            },
        )

        self.assertEqual(expected_data_asset_record, data_asset_record)

    @patch(
        "aind_codeocean_api.codeocean.CodeOceanClient.search_all_data_assets"
    )
    @patch(
        "aind_data_access_api.document_db.MetadataDbClient"
        ".upsert_list_of_records"
    )
    @patch(
        "aind_data_access_api.document_db.MetadataDbClient"
        ".delete_many_records"
    )
    @patch(
        "aind_data_access_api.document_db.MetadataDbClient"
        ".retrieve_data_asset_records"
    )
    @patch("boto3.client")
    def test_run_job(
        self,
        mock_s3_client: MagicMock,
        mock_doc_store_retrieve_records: MagicMock,
        mock_doc_store_delete: MagicMock,
        mock_doc_store_upsert: MagicMock,
        mock_codeocean_client: MagicMock,
    ):
        """Tests that the job is run correctly"""
        job = self.sample_job

        mock_s3_client.list_objects_v2.return_value = self.example_s3_response1
        mock_s3_client.get_object.return_value = self.s3_object_example

        mock_codeocean_client.return_value.json.return_value = {
            "results": [self.example_co_response3]
        }

        job.run_job()

        expected_data_asset_record = DataAssetRecord(
            _id="c00000c0-00cc-00c0-ccc0-000000c000c0",
            _name="modality3_000002_2023-03-17_15-49-02_sorted-ks2.5",
            _created=datetime(2023, 3, 21, 23, 2, 56, tzinfo=pytz.UTC),
            _location=(
                "s3://data_asset_bucket/c00000c0-00cc-00c0-ccc0-000000c000c0"
            ),
        )

        mock_doc_store_upsert.assert_called_once_with(
            [expected_data_asset_record]
        )


if __name__ == "__main__":
    unittest.main()
