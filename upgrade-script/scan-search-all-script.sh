#!/bin/bash
set -e


SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd $SCRIPTDIR

echo "Running Path .. $SCRIPTDIR"

VENV=".venv"

# Python 3.11.7 with Window
if [ -d "$VENV/bin" ]; then
    source $VENV/bin/activate
else
    source $VENV/Scripts/activate
fi


echo "Retrieve all records from ESv5 or ESv8"

source_es_cluster="http://localhost:9200"
target_es_cluster="https://localhost:9200"
param_index="test"

# env
echo "Retrive all records from ESv5 ($source_es_cluster)"
python ./upgrade-script/scan-search-all-script.py --es $source_es_cluster --index $param_index --version ESv5
echo "Retrive all records from ESv8 ($target_es_cluster)"
python ./upgrade-script/scan-search-all-script.py --es $target_es_cluster --index $param_index --version ESv5