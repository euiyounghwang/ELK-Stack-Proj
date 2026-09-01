
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
                # ESv5_Json.update({ids.strip() : "O"})
                ESv5_list.append(ids.strip())
            elif version == "ESv8":
                ESv8_Json.update({ids.strip() : "O"})
            else:
                return

def work():
    try:
        logging.info(f"Ids Compare..")
        ''' Read Ids file for ESv5'''
        read_files(path + "/Export_" + "ESv5", version="ESv5")
        ''' Read Ids file for ESv8'''
        read_files(path + "/Export_" + "ESv8", version="ESv8")

        # logging.info(json.dumps(ESv5_Json, indent=2))
        # logging.info(json.dumps(ESv8_Json, indent=2))

        ''' Merge JSON'''
        ''' Check ESv8 Ids from Esv5'''
        for k in ESv8_Json.keys():
            ''' Ignore if id exists both ES clusters'''
            # if k in ESv5_list:
            #     ES_Merged_Json.update({
            #         k : {
            #                 "ESv5" : "O",
            #                 "ESv8" : "O"
            #             }
            #     })
            if k not in ESv5_list:
                ES_Merged_Json.update({
                    k : {
                        "ESv5" : "X",
                        "ESv8" : "O"
                    }
                })

        ''' Check ESv5 from ESv8'''
        for k in ESv5_list:
            if k not in ESv8_Json.keys():
                ES_Merged_Json.update({
                    k : {
                            "ESv5" : "O",
                            "ESv8" : "X"
                        }
                })

        logging.info(json.dumps(ES_Merged_Json, indent=2))

        ''' Generate list'''
        idx_name, ids_list, df_es_v5, df_es_v8 = [], [], [], []
        for k, v in ES_Merged_Json.items():
            idx_name.append(IDX_NAME)
            ids_list.append(k)
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
        print('\n\n')
        print("**")
        print(df.head(100))
        print("**")
        print('\n\n')
        
        ''' Export Excel'''
        ''' Write DataFrame to CSV File with Default params.'''
        filename = f"{path}/compare_ids_ESv5_ESv8.xlsx"
        if not df.empty:
            df.to_csv(f"{filename}", index=False)
            # df.to_excel(f"{filename}", index=False)
            print(f"{filename} was created..")


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
    