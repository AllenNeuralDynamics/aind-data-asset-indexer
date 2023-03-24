import logging
import os
from pathlib import Path

from aind_data_asset_indexer.job import JobRunner
from dotenv import load_dotenv

if __name__ == "__main__":
    dotenv_path = Path(os.path.dirname(os.path.realpath(__file__))) / ".env"
    load_env_file = load_dotenv(dotenv_path=dotenv_path)
    logging.info("Starting job: ")

    job = JobRunner(
        doc_store_secrets_name=os.getenv("DOC_STORE_SECRETS_NAME"),
        collection_name=os.getenv("COLLECTION_NAME"),
        codeocean_token=os.getenv("CODEOCEAN_TOKEN"),
        codeocean_domain=os.getenv("CODEOCEAN_DOMAIN"),
        data_asset_bucket=os.getenv("DATA_ASSET_BUCKET"),
    )

    job.run_job()
    logging.info("Finished job.")
