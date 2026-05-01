import json
from hashlib import md5
from Search_Engine import Search, Util
import base64
from cryptography.fernet import Fernet
import time
from datetime import datetime
from flask import Flask
from threading import Thread
import logging

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)



''' Tracking thread_alert_message '''
tracking_dict = {
    
}


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


def BatchJob_Logic(params_es_host) -> None:
    try:
       
        # Checking the hash
        # run_hash()

        es_client = Search(host=params_es_host)
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
            
            """
            key = Fernet.generate_key()

            cipher_suite = Fernet(key)

            cipher_text = cipher_suite.encrypt(json.dumps(es_client.get_mappings_json(index_name=each_indices)).encode("utf-8"))
            plain_text = cipher_suite.decrypt(cipher_text)

            print("encrypt_text : ", cipher_text)
            print("decrypt_text : ", plain_text.decode('utf-8'))
            """
            print(json.dumps(es_client.get_mappings_json(index_name=each_indices)))
            print("--" *10)
            print("\n")

    except Exception as e:
        logging.error(e)
        pass    


def work(interval):
    ''' main logic'''

    while True:
        try:
            print("Performing..")

            ''' perform the bachjob to collect the mappings'''
            BatchJob_Logic(params_es_host="http://localhost:9202")
        
        except (KeyboardInterrupt, SystemExit):
            logging.info("#Interrupted..")
        except Exception as e:
            logging.error(e)
            pass

        print(f"Wait for {interval} Seconds to get the mappings..")
        
        time.sleep(interval)


app = Flask(__name__)

@app.route('/')
def hello():
    # return render_template('./index.html', host_name=socket.gethostname().split(".")[0], linked_port=port, service_host=env_name)
    return {
        "app" : "mappings-hash-script.py",
        "started_time" : datetime.now(),
        "tools": [
            {
                "message" : "Collect the mappings from env's",
                "tracking" : tracking_dict
            }
        ]
    }


if __name__ == "__main__":
    
    '''
    Get the mappings
    Locationtion to run the serivce as background : /home/<user>/monitoring/reindex & source .venv/bin/activate
    '''
    
    gloabal_default_timezone = Util.get_datetime()
    
    logging.info("Standalone BatchJob Mapping Collector Server Started..! [{}]".format(Util.get_datetime()))
    
    try:
        T = []
        th1 = Thread(target=work, args=(60,))
        th1.daemon = True
        th1.start()
        T.append(th1)
                
        ''' Expose this app to acesss index.html (./templates/index.html)'''
        ''' Flask at first run: Do not use the development server in a production environment '''
        ''' For deploying an application to production, one option is to use Waitress, a production WSGI server. '''
        # app.run(host="0.0.0.0", port=int(port)-4000)
        from waitress import serve
        _flask_port = 9298
        serve(app, host="0.0.0.0", port=_flask_port)
        logging.info(f"# Flask App's Port : {_flask_port}")

        # wait for all threads to terminate
        for t in T:
            while t.is_alive():
                t.join(0.5)

    except (KeyboardInterrupt, SystemExit):
        logging.info("# Interrupted..")

    finally:
        logging.info("Standalone Prometheus Exporter Server exited..!")
