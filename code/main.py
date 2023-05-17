"""Main entry point that will be run when Reproducible run is requested."""

import logging
import os
from pathlib import Path
import sys

from aind_data_access_api.document_store import DocumentStoreCredentials
from aind_data_asset_indexer.job import JobRunner
from botocore.credentials import (
    InstanceMetadataFetcher,
    InstanceMetadataProvider,
)
from dotenv import load_dotenv

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

if __name__ == "__main__":
    # Load endpoints and secrets from dotenv file
    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    sys.stderr.write("Starting job")
    # logging.info("Starting job: ")

    # Load aws credentials. If not set by secrets, use instance assumed role
    if os.getenv("AWS_ACCESS_KEY_ID") is None:
        sys.stderr.write("Checking IAM role: ")
        provider = InstanceMetadataProvider(
            iam_role_fetcher=InstanceMetadataFetcher(
                timeout=10, num_attempts=2
            )
        )
        sys.stderr.write(f"Provider: {provider}")
        creds = provider.load().get_frozen_credentials()
        sys.stderr.write(f"Creds Access Key: {creds.access_key}")
        os.environ["AWS_ACCESS_KEY_ID"] = creds.access_key
        os.environ["AWS_SECRET_ACCESS_KEY"] = creds.secret_key

    doc_store_credentials = DocumentStoreCredentials(
        aws_secrets_name=os.getenv("DOC_STORE_SECRETS_NAME")
    )

    # Create a JobRunner class
    job = JobRunner(
        doc_store_credentials=doc_store_credentials,
        collection_name=os.getenv("COLLECTION_NAME"),
        codeocean_token=os.getenv("CODEOCEAN_TOKEN"),
        codeocean_domain=os.getenv("CODEOCEAN_DOMAIN"),
        data_asset_bucket=os.getenv("DATA_ASSET_BUCKET"),
    )

    # Run the job
    job.run_job()
    sys.stderr.write("Finished job.")
