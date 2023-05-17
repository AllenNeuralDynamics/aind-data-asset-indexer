"""Main entry point that will be run when Reproducible run is requested."""

import logging
import os
from pathlib import Path
import sys

from aind_data_access_api.document_store import DocumentStoreCredentials
from aind_data_asset_indexer.job import JobRunner
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # Load endpoints and secrets from dotenv file
    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    logging.info("Starting job: ")

    # Load aws credentials. If not set by secrets, use instance assumed role
    if os.getenv("AWS_ACCESS_KEY_ID") is None:
        logging.info("Using service account credentials")
        os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID_SVC")
        os.environ["AWS_SECRET_ACCESS_KEY"] = (
            os.getenv("AWS_SECRET_ACCESS_KEY_SVC")
        )
        os.environ["AWS_DEFAULT_REGION_SVC"] = os.getenv("AWS_DEFAULT_REGION_SVC")

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
    logging.info("Finished job.")
