


# -*- coding: utf-8 -*-
import sys
import json

from elasticsearch import Elasticsearch
import argparse
from dotenv import load_dotenv
import dotenv
import os
from datetime import datetime
import pandas as pd
from threading import Thread
from Search_Engine import Search
import logging
import random
from hashlib import md5
import warnings
warnings.filterwarnings("ignore")

# load_dotenv()
dotenv.load_dotenv(".env", override=True)

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


def get_indices_names_func(es_client):
    """
    params es_client: es instance
    """
    # print(f"get_indices_names : {es_client}")
    es_indices = es_client.indices.get("wx*,om*")
    
    return es_indices.keys()
    

def work(es_target_host):
    """
    params es_target_host: Target es client host

    ES inidces settings
    {
        "test": {
            "settings": {
            "index": {
                "routing": {
                "allocation": {
                    "include": {
                    "_tier_preference": "data_content"
                    }
                }
                },
                "refresh_interval": "-1",
                "number_of_shards": "5",
                "provided_name": "test",
                "creation_date": "1775830194431",
                "number_of_replicas": "0",
                "uuid": "pPCgSytKRmusQI-qm4SIMw",
                "version": {
                "created": "8521000"
                }
            }
            }
        }
    }

    """
    try:
   
        es_obj_t = Search(host=es_target_host)
        es_client = es_obj_t.get_es_instance()

        print(f"es_client : {es_client}")
        print(json.dumps(es_client.cat.health(format="json"), indent=2))

        get_indices_names = get_indices_names_func(es_client)
        print(f"\nget_indices_names : {json.dumps(list(get_indices_names), indent=2)}\n")

        reported_indices = []
        for each_indic in list(get_indices_names):
            print(f"\nValidating {each_indic}")
            if "refresh_interval" in es_client.indices.get_settings(index=each_indic):
                print(json.dumps(es_client.indices.get_settings(index=each_indic), indent=2))
                reported_indices.append(each_indic)

        print('\n')
        print('---')
        print(f"Reported_indices : {json.dumps(list(reported_indices), indent=2)}\n")
        print('---')
    
    except Exception as e:
        print(e)

    finally:
        print('\n')
        print('---')
        print(f"Script has been finished..")
        print('---')
        print('\n')
       

if __name__ == "__main__":
    
    '''
    Get settings informations from Elasticsearch cluster
    (.venv) ➜  python ./upgrade-script/get-settings-info-script.py --ts http://target_es_cluster:9200
    '''

    parser = argparse.ArgumentParser(description="Get settings informations from Elasticsearch cluster using this script")
    parser.add_argument('-t', '--ts', dest='ts', default="http://localhost:9201", help='host target')
    args = parser.parse_args()
    
    if args.ts:
        es_target_host = args.ts
        
    # --
    # Only One process we can use due to 'Global Interpreter Lock'
    # 'Multiprocessing' is that we can use for running with multiple process
    # --
    try:
        th1 = Thread(target=work, args=(es_target_host,))
        th1.start()
        th1.join()
        
    except Exception as e:
        logging.error(e)
        pass
    