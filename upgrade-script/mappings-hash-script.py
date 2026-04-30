import json
from hashlib import md5
from Search_Engine import Search
import base64
from cryptography.fernet import Fernet

import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

'''
# Create Indices

# ES v5
PUT /my-index-000001
{
  "settings": {
    "index": {
      "number_of_shards": 5,
      "number_of_replicas": 1
    }
  },
  "mappings": {
    "test" : {
        "properties": {
        "title": { "type": "text" },
        "author": { "type": "keyword" },
        "publish_date": { "type": "date" }
        }
      }
    }
}

# higher than ESv5

PUT /my-index-000001
{
  "settings": {
    "index": {
      "number_of_shards": 3,
      "number_of_replicas": 2
    }
  },
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "author": { "type": "keyword" },
      "publish_date": { "type": "date" }
    }
  }
}
'''

json_test = {
    "es_pipeline_upload_test": {
        "aliases": {},
        "settings": {
            "index": {
            "number_of_shards": 5,
            "number_of_replicas": 1
            }
        },
        "mappings": {
            "ES_PIPELINE_UPLOAD_TEST": {
                "properties": {
                    "ADDDATE": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "ADDWHO": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "ES_ID": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                    "ES_MESSAGE": {
                        "type": "text",
                        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                    },
                }
            }
        }
    }
}


class MD5_Cls:
    """The MD5, defined in RFC 1321, is a hash algorithm to turn inputs into a fixed 128-bit (16 bytes) length of the hash value."""

    def __init__(self):
        pass

    # def __init__(self, data="Hello, world!"):
    #     self.data = data

    def encrypt(self, data):
        self.data = md5(data.encode()).hexdigest()
        return "Crypted: " + self.data

    def decrypt(self, data):
        if md5(data.encode()).hexdigest() == self.data:
            logging.info("Decrypted: " + data)
            return data
            # del self.data
        else:
            logging.error("Error")
            return None


def run_hash():
    try:
        print("\n")
        print("--" *10)
        
        crypt = MD5_Cls()
        print(crypt.encrypt(json.dumps(json_test, sort_keys=True)))  # Encrypt
        # print(crypt.decrypt(json.dumps(json_test, sort_keys=True)))  # Decrypt data argumenti

        decrypted_json = json.loads(crypt.decrypt(json.dumps(json_test, sort_keys=True)))
        # print(json.dumps(decrypted_json, indent=2))

        print(md5)
        print("--" *10)

        print("\n")
        print("--" *10)
        crypt = MD5_Cls("hello test")
        print(crypt.encrypt())  # Encrypt
        print(crypt.decrypt("hello test"))
        print("--" *10)
        print("\n")

    except Exception as e:
        logging.error(e)
        pass



def work():
    try:
       
        # Checking the hash
        # run_hash()

        es_client = Search(host="http://localhost:9201")
        print(json.dumps(es_client.get_es_client_health(), indent=2))

        # Get indices list
        es_indices_list = es_client.get_es_indices_list()
        print(es_indices_list)

        # Get mappings for the particular ES Indices
        for each_indices in es_indices_list:
            # print(es_client.get_mappings_json(index_name=each_indices))
            
            print("--" *10)
            print(f"Indices Name : {each_indices}")
            
            """
            # 1. Encode to Base64 (string -> bytes -> base64 bytes -> string)
            encoded_bytes  = base64.b64encode(json.dumps(es_client.get_mappings_json(index_name=each_indices)).encode('utf-8'))
            base64_string = encoded_bytes.decode('utf-8')
            print(base64_string)

            # 2. Decode back to JSON
            decoded_bytes = base64.b64decode(base64_string)
            original_data = json.loads(decoded_bytes.decode('utf-8'))
            print(original_data)
            """
            
            key = Fernet.generate_key()

            cipher_suite = Fernet(key)

            cipher_text = cipher_suite.encrypt(json.dumps(es_client.get_mappings_json(index_name=each_indices)).encode("utf-8"))
            plain_text = cipher_suite.decrypt(cipher_text)

            print("encrypt_text : ", cipher_text)
            # print("decrypt_text : ", plain_text.decode('utf-8'))
            
            # print(json.dumps(es_client.get_mappings_json(index_name=each_indices)))
            # print("--" *10)
            # print("\n")
        
    except Exception as e:
        logging.error(e)
        pass


if __name__ == "__main__":
    
    '''
    Set the number of replicas
    (.venv) ➜  python ./upgrade-script/update-replica-script.py --ts http://target_es_cluster:9201
    '''
     
    # --
    # Only One process we can use due to 'Global Interpreter Lock'
    # 'Multiprocessing' is that we can use for running with multiple process
    # --
    work()

