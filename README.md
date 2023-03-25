# aind-data-asset-indexer

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)

Service Capsule source code to write data asset metadata to document store.
This capsule will parse through all the data assets in Code Ocean tagged with `raw` and `processed` and creates records that are uploaded to a Document Store

## Usage

- Copy the `.env.template` file to a `.env` file and set the variables accordingly
- It currently parses through ALL records each time it is run
- Records in the doc store not found in Code Ocean will be deleted

## Development

- The Code Ocean environment is defined by the Dockerfile in the environment folder
- It's a bit tedious, but the dependencies listed in the `pyproject.toml` file needs to be manually kept in sync with the Dockerfile.
