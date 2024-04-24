#!/bin/bash

python src/aind_data_asset_indexer/s3_crawler.py && python src/aind_data_asset_indexer/update_docdb.py
