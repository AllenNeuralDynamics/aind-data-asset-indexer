User Guide
==========

Thank you for using ``aind-data-asset-indexer``! This guide is
intended for engineers in AIND who wish to index metadata in AIND
databases.

Overview
-----------------------------------------

AIND metadata for data assets is stored in various places and must be
kept in sync:

1. **S3 buckets** store raw metadata files. Each data asset folder
   (prefix) contains:

   -  ``{core_schema}.json``: core schema files, e.g.,
      ``acquisition.json``, ``subject.json``.
   -  ``metadata.nd.json``: top-level metadata file, containing
      all core schema fields.
   -  ``original_metadata/{core_schema}.json``: a copy of each
      core schema file as it was originally uploaded to S3.
2. A **document database (DocDB)** contains unstructured JSON
   documents describing the ``metadata.nd.json`` for a data asset.
3. **Code Ocean**: data assets are mounted as Code Ocean data assets.
   Processed results are also stored in an internal Code Ocean bucket.

Once the data is initially uploaded, the DocDB is assumed to be the
source of truth for metadata. All updates to existing metadata should
be made in the DocDB.

We have automated jobs to keep changes in DocDB, S3, and Code Ocean in sync.
This repository contains the code for these index jobs:

1. `AindIndexBucketJob <#aindindexbucketjob>`__: Syncs changes in S3 and DocDB.
2. `CodeOceanIndexBucketJob <#codeoceanindexbucketjob>`__: Syncs changes in Code Ocean and DocDB.


AindIndexBucketJob
------------------

The `AindIndexBucketJob` handles syncing changes from DocDB to S3 for a
particular S3 bucket. There is an `IndexAindBucketsJob` wrapper job that
runs the `AindIndexBucketJob` for a list of buckets.

The workflow is generally as follows:

1. Paginate DocDB to get records for a particular bucket.

   -  Typically, we filter for records that have been updated in the last
      14 days.
2. For each DocDB record, process by syncing any changes in DocDB to S3.

   -  If the record does not have a valid location, log a warning.
   -  If the S3 location does not exist, remove the record from DocDB.
   -  If the core schema files or original metadata folder is out of
      sync, update them.
   -  If the metadata.nd.json file is outdated, update it.
3. Paginate S3 to get all prefixes for a particular bucket.
4. For each prefix, process by checking if it is a new data asset
   and adding it to DocDB if necessary.
   
   -  If the metadata record exists in S3 but not in DocDB, copy it
      to DocDB.
   -  If the metadata record for a derived asset does not exist in S3,
      create it and save it to S3. Assume a Lambda function will move it
      over to DocDB. Metadata records for raw assets are created during
      the upload process, **not** by this job.
   -  In both cases above, ensure the original metadata folder and core
      files are in sync with the metadata.nd.json file.

Please refer to the job's docstrings for more details on the implementation.


CodeOceanIndexBucketJob
-----------------------

The `CodeOceanIndexBucketJob` updates the external links for DocDB records
with their Code Ocean (CO) data asset IDs and indexes CO processed results.

The workflow is generally as follows:

1. For records in AIND buckets, update the external links with CO data
   asset IDs if needed.

   -  Retrieve a list of CO data asset IDs and locations.
   -  Paginate through DocDB records where the location does not match
      the internal CO bucket.
   -  Add or remove the external links from the DocDB record as needed.
2. Index CO processed results from the CO internal bucket.
   
   -  Get all processed CO results as CO records.
   -  Paginate DocDB to get all records for the CO bucket.
   -  Find all records in CO that are not in DocDB and add them to DocDB.
   -  Find all records in DocDB that are not in CO and remove them from
      DocDB.

Please refer to the job's docstrings for more details on the implementation.


Running Indexer Jobs Locally
----------------------------

The jobs are intended to be run as scheduled AWS ECS tasks in the same VPC
as the DocDB instance. The job settings are stored in AWS Parameter Store.

If you wish to run the jobs locally, please refer to this section in the
Contributor Guidelines: :ref:`running-indexer-jobs-locally`.
   

Reporting bugs or making feature requests
-----------------------------------------

Please report any bugs or feature requests here:
`issues <https://github.com/AllenNeuralDynamics/aind-data-asset-indexer/issues/new/choose>`__
