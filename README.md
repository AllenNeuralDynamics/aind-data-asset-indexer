# aind-data-asset-indexer

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)

Index jobs for AIND metadata in AWS DocumentDB and S3.

AIND metadata for data assets is stored in various places and must be
kept in sync:

1. **S3 buckets** store raw metadata files, including the ``metadata.nd.json``.
2. A **document database (DocDB)** contains unstructured json
   documents describing the ``metadata.nd.json`` for a data asset.
3. **Code Ocean**: data assets are mounted as CodeOcean data asssets.
   Processed results are also stored in an internal Code Ocean bucket.

We have automated jobs to keep changes in DocDB and S3 in sync.
This repository contains the code for these index jobs.

More information including a user guide and contributor guidelines can be found at [readthedocs](https://aind-data-asset-indexer.readthedocs.io).