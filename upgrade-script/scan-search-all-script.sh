#!/bin/bash
set -e

SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

PARENT_DIR=$(dirname "$SCRIPTDIR")

echo "Running Path .. $PARENT_DIR"
cd $PARENT_DIR

VENV=".venv"

# Python 3.11.7 with Window
if [ -d "$VENV/bin" ]; then
    source $VENV/bin/activate
else
    source $VENV/Scripts/activate
fi


echo "Retrieve all records from ESv5 or ESv8"

# source_es_cluster="http://localhost:9200"
# target_es_cluster="https://localhost:9200"
# param_index="test"

source_es_cluster=$1
target_es_cluster=$2
param_index=$3

# Run
# Change .env for the authenication
# ./upgrade-script/scan-search-all-script.sh http://localhost:9200  https://localhost:9200 test

# env
echo "Retrive all records from ESv5 ($source_es_cluster)"
python ./upgrade-script/scan-search-all-script.py --es $source_es_cluster --index $param_index --version ESv5
echo "Retrive all records from ESv8 ($target_es_cluster)"
python ./upgrade-script/scan-search-all-script.py --es $target_es_cluster --index $param_index --version ESv8


# Then,
# python ./upgrade-script/scan-ids-compare-script