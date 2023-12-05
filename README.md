# aind-data-asset-indexer

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)

Script to create metadata analytics table and write to redshift table. 
This script will parse through a list of s3 buckets and document whether data asset records in each of those buckets does or does not contain `metadata.nd.json`


## Usage
- Define the environment variables in the `.env.template` 
  - REDSHIFT_SECRETS_NAME: defining secrets name for Amazon Redshift
  - BUCKETS: list of buckets. format: "['{bucket_name1}', '{bucket_name2}']"
  - TABLE_NAME: name of table in redshift
  - FOLDERS_FILEPATH: Intended filepath for txt file
  - METADATA_DIRECTORY: Intended path for directory containing copies of metadata records
- Records containing metadata.nd.json file will be copies to `METADATA_DIRECTORY` and compared against list of all records in `FOLDERS_FILEPATH`
- An analytics table containing columns `s3_prefix`, `bucket_name`, and `metadata_bool` will be written to `TABLE_NAME` in Redshift

## Development
- It's a bit tedious, but the dependencies listed in the `pyproject.toml` file needs to be manually updated
