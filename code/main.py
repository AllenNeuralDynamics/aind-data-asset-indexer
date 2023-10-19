"""Main entry point that will be run when Reproducible run is requested."""

import logging
import os
from pathlib import Path

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.credentials import CodeOceanCredentials
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_asset_indexer.job import JobRunner
from dotenv import load_dotenv

if __name__ == "__main__":
    # Load endpoints and secrets from dotenv file
    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    co_client = CodeOceanClient.from_credentials(
        CodeOceanCredentials(
            aws_secrets_name=os.getenv("CODEOCEAN_SECRETS_NAME")
        )
    )
    doc_db_client = MetadataDbClient(
        host=os.getenv("DOC_DB_HOST"),
        database=os.getenv("DOC_DB_DATABASE"),
        collection=os.getenv("DOC_DB_COLLECTION"),
    )

    # Create a JobRunner class
    job = JobRunner(
        doc_db_client=doc_db_client,
        codeocean_client=co_client,
        data_asset_bucket=os.getenv("DATA_ASSET_BUCKET"),
    )

    # Create a dummy output file (expected by Mat Views Pipeline)
    with open("../results/empty_output", "w") as f:
        pass

    # Run the job
    job.run_job()
    logging.info("Finished job.")
