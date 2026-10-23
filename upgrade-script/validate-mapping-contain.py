


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


def get_mapping_contain_field(es_client, lookup_field):
    """
    params es_client: es instance
    """
    source_idx_lists = es_client.indices.get("*")
    found_indices_list = []
    for each_indice in source_idx_lists:
        each_mapping = es_client.indices.get_mapping(index=each_indice)
        # print(f"each_mapping : {each_mapping.get(each_indice).get('mappings').get('properties')}")
        properties_json = each_mapping.get(each_indice).get('mappings').get('properties')
        if lookup_field in properties_json:
            found_indices_list.append(each_indice)

    return found_indices_list
       


def work(es_target_host, lookup_field):
    """
    params es_target_host: Target es client host
    """
    try:
   
        es_obj_t = Search(host=es_target_host)
        es_client = es_obj_t.get_es_instance()

        found_indices_list = get_mapping_contain_field(es_client, lookup_field)
        joined_string = '\n'.join(found_indices_list)
        print(f"\nfound_indices_list : \n{joined_string}")

    except Exception as e:
        print(e)

    finally:
        print('\n')
        print('---')
        print(f"Script for the the number of replica has been finished..")
        print('---')
        print('\n')
       

if __name__ == "__main__":
    
    '''
    Set the number of replicas
    (.venv) ➜  python ./upgrade-script/validate-mapping-contain.py --ts http://target_es_cluster:9200 --lk detailList --t_auth test==
    '''

    parser = argparse.ArgumentParser(description="Lookup if mapping does contain in Elasticsearch cluster using this script")
    parser.add_argument('-t', '--ts', dest='ts', default="http://localhost:9201", help='host target')
    parser.add_argument('-l', '--lk', dest='lk', default="addts", help='field name to lookup')
    parser.add_argument('-t_auth', '--t_auth', dest='t_auth', required=True, help='basic authentications')
    args = parser.parse_args()
    
    if args.ts:
        es_target_host = args.ts

    if args.lk:
        lookup_field = args.lk

    if args.t_auth:
        # dotenv.set_key(dotenv_file, "BASIC_AUTH", "Basic {}".format(args.t_auth))
        os.environ["BASIC_AUTH"] = "Basic {}".format(args.t_auth)
    
    # --
    # Only One process we can use due to 'Global Interpreter Lock'
    # 'Multiprocessing' is that we can use for running with multiple process
    # --
    try:
        th1 = Thread(target=work, args=(es_target_host,lookup_field,))
        th1.start()
        th1.join()
        
    except Exception as e:
        logging.error(e)
        pass
    