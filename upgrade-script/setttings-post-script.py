# -*- coding: utf-8 -*-
import sys
import json

from elasticsearch import Elasticsearch
import argparse
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
from threading import Thread
from Search_Engine import Search
import logging
import warnings
warnings.filterwarnings("ignore")

load_dotenv()

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


path = os.path.dirname(os.path.abspath(__file__)) + '/output'
file_output = path + "/validation_docs"


real_time =False

def work(es_target_client, enable_settings):
    '''
    PUT Settings to the ES cluster

    all_settings_body = {
        "index": {
            "refresh_interval": "30s",    # 기본 1s에서 30s로 연장
            "number_of_replicas": 1       # 모든 인덱스의 레플리카 수를 1로 통일
        }
    }
    '''
    es_target_client = es_target_client.replace('\r','')
    
    logging.info(f"{es_target_client}")

    es_obj_t = Search(host=es_target_client)
    es_t_client = es_obj_t.get_es_instance()

    # - "2s": 쿼리 실행 시간이 2초 이상 걸리면 WARN 레벨로 로그를 기록합니다.
    # - "0s"로 설정 시 모든 쿼리를 무조건 기록하므로 테스트용으로 유용합니다.
    if enable_settings:
        slowlog_settings = {
            "index.search.slowlog.threshold.query.warn": "0s",
            "index.search.slowlog.threshold.fetch.warn": "0s"       
        }
    else:
        slowlog_settings = {
            "index.search.slowlog.threshold.query.warn": None,
            "index.search.slowlog.threshold.fetch.warn": None
        } 

    try:
        # 3. PUT /_all/_settings API 호출
        # 상세 API 명세: https://elastic.co
        response = es_t_client.indices.put_settings(
            index="_all",  # 모든 인덱스를 대상으로 지정
            body=slowlog_settings
        )
        logging.info("Updated settings successfully..")
        logging.info(f"Response: {response}")

        ''' export file for different total counts betweenn two clusters'''
        print('\n')
        print('-'*50)
        print(f"Updated settings successfully..")
        print('-'*50)
        print('\n')

    except Exception as e:
        logging.error(f"{e}")



if __name__ == "__main__":
    
    '''
    (.venv) ➜  python ./upgrade-script/setttings-post-script.py --ts https://target_es_cluster:9201 --t_auth base_encode_auth_value --enable_settings true
    (.venv) ➜  python ./upgrade-script/setttings-post-script.py --ts https://target_es_cluster:9201 --t_auth base_encode_auth_value
    
    Make the changes for Setting in Elasticsearch cluster
    '''
    parser = argparse.ArgumentParser(description="Index into Elasticsearch using this script")
    parser.add_argument('-t', '--ts', dest='ts', default="http://localhost:9201", help='host target')
    parser.add_argument('-e', '--enable_settings', dest='enable_settings', default="False", help='host target')
    parser.add_argument('-t_auth', '--t_auth', dest='t_auth', required=False, help='basic authentications')
    args = parser.parse_args()

    if args.ts:
        es_target_host = args.ts

    if args.t_auth:
        # dotenv.set_key(dotenv_file, "BASIC_AUTH", "Basic {}".format(args.t_auth))
        os.environ["BASIC_AUTH"] = "Basic {}".format(args.t_auth)

    if args.enable_settings:
        enable_settings = args.enable_settings

    enable_settings = True if str(enable_settings).upper() == "TRUE" else False


    print(os.getenv('BASIC_AUTH'))
    # exit(1)

    real_time = True if str(real_time).upper() == "TRUE" else False

    # --
    # Only One process we can use due to 'Global Interpreter Lock'
    # 'Multiprocessing' is that we can use for running with multiple process
    # --
    try:
        th1 = Thread(target=work, args=(es_target_host, enable_settings))
        th1.start()
        th1.join()
        
    except Exception as e:
        logging.error(e)
        pass
    