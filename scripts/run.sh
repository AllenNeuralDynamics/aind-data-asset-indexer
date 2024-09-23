#!/bin/bash

python src/aind_data_asset_indexer/index_aind_buckets.py --param-store-name $PARAM_STORE_NAME
echo "Finished aind buckets. Starting code ocean bucket."
python src/aind_data_asset_indexer/codeocean_bucket_indexer.py --param-store-name $PARAM_STORE_NAME_CO_JOB
echo "Finished code ocean bucket."
