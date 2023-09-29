"""Main entry point that will be run when Reproducible run is requested."""

import logging

from aind_codeocean_api.codeocean import CodeOceanClient
from aind_codeocean_api.credentials import CodeOceanCredentials
from aind_data_access_api.document_db import MetadataDbClient
from aind_data_asset_indexer.job import JobRunner

# Code Ocean Pipelines pulls from git. We can add non-secret params here.
# TODO: Think of a better way to store capsule parameters for pipeline runs.
DOC_DB_HOST = "api.allenneuraldynamics.org"
DOC_DB_DATABASE = "metadata"
DOC_DB_COLLECTION = "data_assets"
CODEOCEAN_SECRETS_NAME = "/aind/prod/codeocean/credentials/readonly"
DATA_ASSET_BUCKET = "codeocean-s3datasetsbucket-1u41qdg42ur9"

if __name__ == "__main__":
    co_client = CodeOceanClient.from_credentials(
        CodeOceanCredentials(
            aws_secrets_name=CODEOCEAN_SECRETS_NAME
        )
    )
    doc_db_client = MetadataDbClient(
        host=DOC_DB_HOST,
        database=DOC_DB_DATABASE,
        collection=DOC_DB_COLLECTION,
    )

    # Create a JobRunner class
    job = JobRunner(
        doc_db_client=doc_db_client,
        codeocean_client=co_client,
        data_asset_bucket=DATA_ASSET_BUCKET,
    )

    # Run the job
    job.run_job()
    logging.info("Finished job.")
