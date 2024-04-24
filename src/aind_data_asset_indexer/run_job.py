"""Job runner to crawl through s3 and updates redshift and DocDB"""

import os
from aind_data_asset_indexer.update_docdb import get_mongo_credentials, DocDBUpdater
from aind_data_asset_indexer.s3_crawler import AnalyticsTableJobRunner

REDSHIFT_SECRETS_NAME = os.getenv("REDSHIFT_SECRETS_NAME")
BUCKETS = os.getenv("BUCKETS")
TABLE_NAME = os.getenv("TABLE_NAME")
FOLDERS_FILEPATH = os.getenv("FOLDERS_FILEPATH")
METADATA_DIR = os.getenv("METADATA_DIRECTORY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
DOCDB_SECRETS_NAME = os.getenv("DOCDB_SECRETS_NAME")

if __name__ == "__main__":
    redshift_job_runner = AnalyticsTableJobRunner(
        redshift_secrets_name=REDSHIFT_SECRETS_NAME,
        buckets=BUCKETS,
        table_name=TABLE_NAME,
    )
    redshift_job_runner.run_job(
        folders_filepath=FOLDERS_FILEPATH, metadata_directory=METADATA_DIR
    )
    mongo_configs = get_mongo_credentials(
        db_name=DB_NAME, collection_name=COLLECTION_NAME
    )
    docdb_job_runner = DocDBUpdater(
        metadata_dir=METADATA_DIR, mongo_configs=mongo_configs
    )
    docdb_job_runner.run_sync_records_job()