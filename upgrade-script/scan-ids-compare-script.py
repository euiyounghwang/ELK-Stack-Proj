
# -*- coding: utf-8 -*-
import sys
import json

from elasticsearch import Elasticsearch, helpers
import argparse
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
from threading import Thread
from Search_Engine import Search
import logging
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

load_dotenv()

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)


path = os.path.dirname(os.path.abspath(__file__)) + '/output'
# file_output = path + "/Scan-IDs"
# file_output = path

ESv5_list = []
ESv8_Json = {}
ES_Merged_Json = {}
''' Format'''
"""
{
    "1" : {
        "ESv5" : "O",
        "ESv8" : "O"
    },
    "2" : {
        "ESv5" : "O",
        "ESv8" : "X"
    }
}
"""

IDX_NAME = None

def read_files(file_output, version):
    logging.info(f"Output Path : {file_output}, version : {version}")
    global IDX_NAME
    with open(file_output, "r", encoding="utf-8") as file:
        # ids = file.read()
        while True:
            ids = file.readline()
            if not ids:  # Evaluates to True when line == ""
                break
            # print(ids)
            if '#' in ids:
                IDX_NAME = ids.strip().replace("#","")
                continue

            if version == "ESv5":
                # ESv5_list.append(f"{ids.strip()}")
                ESv5_list.append(f"{IDX_NAME},{ids.strip()}")
            elif version == "ESv8":
                # ESv8_Json.update({ids.strip() : "O"})
                ESv8_Json.update({IDX_NAME + "," + ids.strip() : "O"})
            else:
                return

def work():
    try:
        # 시작 시각 기록
        start_time = datetime.now()
        print(f"** Start Time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        logging.info(f"Ids Compare..")
        ''' Read Ids file for ESv5'''
        read_files(path + "/Export_" + "ESv5", version="ESv5")
        ''' Read Ids file for ESv8'''
        read_files(path + "/Export_" + "ESv8", version="ESv8")

        # logging.info(json.dumps(ESv5_Json, indent=2))
        # logging.info(json.dumps(ESv8_Json, indent=2))

        ''' Merge JSON'''
        ''' Check ESv8 Ids from Esv5'''
        print(f"\n")
        logging.info(f"Check the ESv5 and ESv8 Keys..")
        # for k in ESv8_Json.keys():
        """
        for k in tqdm(ESv8_Json.keys(), desc="Processing keys for ESv8"):
            ''' Ignore if id exists both ES clusters'''
            # if k in ESv5_list:
            #     ES_Merged_Json.update({
            #         k : {
            #                 "ESv5" : "O",
            #                 "ESv8" : "O"
            #             }
            #     })
            if k not in ESv5_list:
                # ES_Merged_Json.update({
                #     k : {
                #         "ESv5" : "X",
                #         "ESv8" : "O"
                #     }
                # })
                ES_Merged_Json[k] = {"ESv5" : "X", "ESv8" : "O"}

        ''' Check ESv5 from ESv8'''
        print(f"\n")
        logging.info(f"Check the ESv5 Keys..")
        # for k in ESv5_list:
        for k in tqdm(ESv5_list, desc="Processing Keys for ESv5"):
            if k not in ESv8_Json.keys():
                # ES_Merged_Json.update({
                #     k : {
                #             "ESv5" : "O",
                #             "ESv8" : "X"
                #         }
                # })
                ES_Merged_Json[k] = {"ESv5" : "O", "ESv8" : "X"}
        """
        # logging.info(json.dumps(ES_Merged_Json, indent=2))
        # print(list(ESv8_Json.keys()))
        ESv5_exist = list(set(ESv5_list) - set(ESv8_Json.keys()))
        # print(ESv5_exist)
        for k in tqdm(ESv5_exist, desc="Processing Keys for ESv5", colour='white'):
            ES_Merged_Json[k] = {"ESv5" : "O", "ESv8" : "X"}

        ESv8_exist = list(set(ESv8_Json.keys()) - set(ESv5_list))
        for k in tqdm(ESv8_exist, desc="Processing Keys for ESv5", colour='white'):
            ES_Merged_Json[k] = {"ESv5" : "X", "ESv8" : "O"}

        ''' Generate list'''
        idx_name, ids_list, df_es_v5, df_es_v8 = [], [], [], []
        print(f"\n")
        logging.info(f"Check and compare the Esv5 and ESv8 Keys..")

        # print(ES_Merged_Json)
        # for k, v in ES_Merged_Json.items():
        for k, v in tqdm(ES_Merged_Json.items(), total=len(ES_Merged_Json), colour='white', desc="Processing records diff for ESv5 and ESv8", unit="item"):
            # idx_name.append(IDX_NAME)
            # print(k)
            idx_name.append(k.split(",")[0])
            # ids_list.append(k)
            ids_list.append(k.split(",")[1])
            df_es_v5.append(v.get("ESv5"))
            df_es_v8.append(v.get("ESv8"))

        # print(ids_list)
        # print(df_es_v5)
        # print(df_es_v8)

        ''' Transform Json to Dataframe'''
        data = {
            'index_name' : idx_name,
            'ids' : ids_list,
            'ESv5_Exists' : df_es_v5,
            'ESv8_Exists' : df_es_v8
        }

        df = pd.DataFrame(data)
        # df = df.sort_values(by=['index_name',], ascending=False)
        # 💡 .head()를 먼저 쓰고, 그 뒤에 .style을 붙입니다.
        # df = df.head(100).style.set_properties(**{'text-align': 'center'})
        print('\n\n')
        print("**")
        print(df.head(100))
        print("**")
        print('\n\n')
        
        ''' Export Excel'''
        ''' Write DataFrame to CSV File with Default params.'''
        filename = f"{path}/compare_ids_ESv5_ESv8.csv"
        if not df.empty:
            df.to_csv(f"{filename}", index=False)
            # df.to_excel(f"{filename}", index=False)
            print(f"{filename} was created..")

        # 종료 시각 기록
        end_time = datetime.now()
        print(f"** End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("\n")
        # 경과 시간 계산 (timedelta 객체 반환)
        elapsed_time = end_time - start_time
        print(f"** Elapsed Time: {elapsed_time}")

    except Exception as e:
        logging.error(e)
        

if __name__ == "__main__":
    
    '''
    # Extract Ids from the source cluser
    
    python ./upgrade-script/scan-search-all-script.py --es http://source_es_cluster:9200 --index test --version ESv8
    # Compare Ids between ESv5 and Esv8
    python ./upgrade-script/scan-ids-compare-script.py 
    '''
    # --
    # Only One process we can use due to 'Global Interpreter Lock'
    # 'Multiprocessing' is that we can use for running with multiple process
    # --
    try:
        th1 = Thread(target=work, args=())
        th1.start()
        th1.join()
        
    except Exception as e:
        logging.error(e)
        pass
    