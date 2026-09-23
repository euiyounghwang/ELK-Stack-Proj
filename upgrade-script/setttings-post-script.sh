#!/bin/bash
set -e

SCRIPTDIR="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

PARENT_DIR=$(dirname "$SCRIPTDIR")

echo "Running Path .. $PARENT_DIR"
# cd $PARENT_DIR

VENV=".venv"

# Python 3.11.7 with Window
if [ -d "$VENV/bin" ]; then
    source $VENV/bin/activate
else
    source $VENV/Scripts/activate
fi


echo "Retrieve all records from ESv5 or ESv8"

# target_es_cluster="https://localhost:9200"
# param_index="test"

target_es_cluster=$1
basic_auth=$2
enable_settings=$3

# Run
# Change .env for the authenication
# ./upgrade-script/setttings-post-script.sh https://localhost:9200 base_test true
# ./upgrade-script/setttings-post-script.sh https://localhost:9200 base_test false

# env
echo "ES Cluster : ($target_es_cluster)"
#python ./upgrade-script/setttings-post-script.py --ts $target_es_cluster --t_auth $basic_auth --enable_settings true
python ./upgrade-script/setttings-post-script.py --ts $target_es_cluster --t_auth $basic_auth --enable_settings $enable_settings


# Then,
# Add to cronjob
# 30 06 * * * /home/devuser/monitoring/custom_export/setttings-post-script.sh https://localhost:9200 --t_auth base_test --enable_settings true
# 30 18 * * * /home/devuser/monitoring/custom_export/setttings-post-script.sh https://localhost:9200 --t_auth base_test