# ELK Upgrade with Search Guard in local environment
<i>ELK Upgrade with Search Guard in local environment

## OS Command
- cat /etc/redhat-release
- lscpu
-  grep -c processor /proc/cpuinfo
- free -g
- df -kH /apps

## Elasticsearch (Elasticsearch v8.17.0 with Search Guard) in local environment
- Elasticsearch is a distributed, RESTful search and analytics engine that allows you to store, search, and analyze large volumes of data in near-real-time. Built on Apache Lucene, it processes both structured and unstructured data, powering use cases like full-text search, business analytics, observability, and security intelligence.
- Elasticsearch performance can be heavily penalised if the node is allowed to swap memory to disk. Elasticsearch can be configured to automatically prevent memory swapping on its host machine by adding the bootstrap memory_lock true setting to elasticsearch.yml. If bootstrap checks are enabled,
bootstrap.memory_lock: true
  - You can check whether the setting has worked by running: GET _nodes?filter_path=**.mlockall
  - Turn off all swapping on Linux option 1: sudo swapoff -a
  - https://devhosaga.tistory.com/entry/Elasticsearch-OS-%ED%99%98%EA%B2%BD-%EC%84%A4%EC%A0%95 (sudo sysctl vm.swappiness=1)
- Search Guard Reference
  - https://docs.search-guard.com/latest/offline-tls-tool
  - https://subin-0320.tistory.com/174
  - https://bcrypt-generator.com/#google_vignette
  - https://docs.search-guard.com/latest/kibana-plugin-installation
- xMatters Reference:
  - https://help.xmatters.com/integrations/other/triggeralertsbywebhook.htm
  - https://help.xmatters.com/ondemand/flowdesigner/flow-triggers.htm?cshid=FlowTriggersHTTP#HTTP
  - https://help.xmatters.com/ondemand/userguide/receivingalerts/respondviatext.htm
  - https://help.xmatters.com/xmapi/index.html#trigger-an-event
- OS Release : https://www.elastic.co/support/matrix#matrix_os
- __Installation Commands__
  - sudo systemctl daemon-reload
  - sudo systemctl status elasticsearch
  - sudo systemctl start elasticsearch
  - sudo groupadd elasticsearch
  - sudo useradd -g elasticsearch elasticsearch
  - sudo sysctl -w vm.max_map_count=262144
  - cd /apps
  - sudo mkdir elasticsearch
  - sudo mkdir java
  - sudo mkdir search_guard_scripts
  - sudo mkdir scripts
  - sudo chmod 775 -R ./elasticsearch
  - sudo chmod 775 -R ./java
  - sudo chmod 775 -R ./search_guard_scripts
  - sudo mv ./scripts/ ./scripts_bk
  - sudo chmod 755 -R ./scripts
  - sudo chown -R root:biadmin ./scripts
  - sudo chown -R elasticsearch:elasticsearch ./search_guard_scripts/
  - sudo su -l elasticsearch
  - cd /apps
  - cd /apps/search_guard_scripts
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/elk_stack_upgrade_script_latest/search_guard_scripts/* .
  - vi ./sg_update_config.sh  (change for the java path, certificate name.)
  - cd /apps/elasticsearch
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/elasticsearch-8.17.0-linux-x86_64.tar.gz .
  - tar -zxvf ./elasticsearch-8.17.0-linux-x86_64.tar.gz
  - ln -s elasticsearch-8.17.0 latest
  - mkdir data
  - mkdir logs
  - mkdir snapshots
  - cd /apps/elasticsearch/latest
  - mkdir tmp
  - ** Search-Guard Install **
  - cd /apps/elasticsearch
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/search-guard-flx-elasticsearch-plugin-3.0.3-es-8.17.0.zip .
  - /apps/elasticsearch/latest/bin/elasticsearch-plugin install file:///apps/elasticsearch/search-guard-flx-elasticsearch-plugin-3.0.3-es-8.17.0.zip
  - Only need to copy search guard tools with configuration file for below commands on to ES data node #1
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/sgctl-3.0.2.sh /apps/elasticsearch/elasticsearch-8.17.0/plugins/search-guard-flx/tools/
  - cd /apps/elasticsearch/elasticsearch-8.17.0/plugins/search-guard-flx/tools
  - chmod 755 *.sh
  - cd /apps/elasticsearch/elasticsearch-8.17.0/plugins/search-guard-flx/sgconfig
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/elk_stack_upgrade_script_latest/sg_config/* .
  - **
  - cd /apps/elasticsearch/elasticsearch-8.17.0/config
  - mv ./elasticsearch.yml ./elasticsearch_bk.yml
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/elk_stack_upgrade_script_latest/elasticsearch.yml .
  - vi .elasticsearch.yml
  - vi ./jvm.options
  ```bash
  # Xms represents the initial size of total heap space
  # Xmx represents the maximum size of total heap space
  -Xms15g
  -Xmx15g
  ```
  - ** Run 
  - export JAVA_HOME='/apps/java/latest'
  - /apps/elasticsearch/latest/bin/elasticsearch 
  - **
  - exit
  - cd /apps
  - sudo chmod 775 -R ./elasticsearch
  - sudo scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/setup/elasticsearch /etc/init.d/
  - sudo cp ./elasticsearch /etc/init.d/
  - sudo /apps/scripts/systemctl.sh elasticsearch status
  - sudo service elasticsearch status
  - sudo service elasticsearch stop
  - sudo service elasticsearch start
  - sudo su -l elasticsearch
  - /apps/search_guard_scripts/sg_update_config.sh
  ```bash
  Update ES Search Guard Configuration..
  /apps/java/openlogic-openjdk-17.0.13+11-linux-x64
  Successfully connected to cluster supplychain-logging-test-es8 (localhost) as user CN=dev-elasticsearch-admin-cert,OU=es,O=es,L=Seoul,C=KR,ST=Seoul
  Successfully connected to cluster supplychain-logging-test-es8 (localhost) as user CN=dev-elasticsearch-admin-cert,OU=es,O=es,L=Seoul,C=KR,ST=Seoul
  Configuration has been updated
  Done..
  ```


## Kibana (Kibana v8.17.0 with Search Guard)
- Kibana is a visual interface tool that allows you to explore, visualize, and build a dashboard over the log data massed in Elasticsearch Clusters.
- Check the response header : curl -I --insecure https://localhost:5601
- __Installation Commands (Simple Setup)__
  - sudo groupadd kibana
  - sudo useradd -g kibana kibana
  - sudo mkdir ./kibana
  - sudo chown -R kibana:kibana ./kibana/
  - sudo su -l kibana
  - cd /apps/kibana
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/elk_stack_upgrade_script_latest/kibana-8.17.0.zip .
  - unzip ./kibana-8.17.0.zip
  - ln -s kibana-8.17.0 latest
  - cd /apps/kibana/latest/config/
  - vi ./kibana.yml
  ```bash
  ..
  # HTTP Security Header not detected
  server.securityResponseHeaders.strictTransportSecurity: "max-age=31536000; includeSubDomains"
  server.customResponseHeaders.X-Content-type-Options: nosniff
  server.customResponseHeaders.XFrameOptions: sameorigin
  server.customResponseHeaders.xXssProtection: 1
  server.securityResponseHeaders.referrerPolicy: strict-origin-when-cross-origin
  ..
```
  - mkdir certs
  - cd certs
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/search_guard_gen_key/dev_new_certs/dev-es8* .
  - exit
  - cd /apps
  - sudo chmod 775 -R ./kibana
- __Installation Commands (Manual Setup)__
  - sudo groupadd kibana
- __Installation Commands (Required)__
  - cd /apps
  - sudo vi .puppeteerrc
  ```bash
  - skipDownload: true
  ```
  - sudo chown -R kibana:kibana .puppeteerrc
  - sudo chown -R kibana:kibana /apps/scripts/startkibana
  - sudo chown -R kibana:kibana /apps/scripts/killps.sh
  - sudo -u kibana /apps/scripts/startkibana
  - sudo /apps/scripts/killps.sh
  - sudo netstat -nlp | grep :5601 
- Go to Kibana UI with “admin” account  Stack Management  Spaces (Menu) : Disabled Obserability, Security
- Go to Kibana UI with “admin” account  Stack Management  Advanced Settings(Menu): Change the default route when opening Kibana (Default route: `/app/dev_tools`)
  

## Logstash (Logstash v8.17.0 with Search Guard)
- Logstash is an open-source data processing pipeline that collects data from various sources, transforms it on the fly, and sends it to a desired destination. As part of the Elastic Stack (formerly ELK stack), it functions as an ETL (Extract, Transform, Load) tool, using a pipeline of input, filter, and output plugins to ingest, structure, and ship logs and events for analysis and visualization in Elasticsearch and Kibana.  
- __Installation Commands__
  - sudo groupadd logstash
  - sudo useradd -g logstash logstash
  - cd /apps
  - sudo mkdir logstash
  - sudo chown -R logstash:logstash ./logstash/
  - sudo chmod 775 -R ./logstash/
  - sudo su -l logstash
  - cd logstash
  - scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/logstash-8.17.0-linux-x86_64.tar.gz .
  - tar -zxvf ./logstash-8.17.0-linux-x86_64.tar.gz
  - ln -s ./logstash-8.17.0 latest
  - cd /apps/logstash
  - mkdir data
  - mkdir logs
  - cd ./latest
  - mkdir tmp
  - cd config
  - mkdir ./conf.d
  - vi ./ingest_socket.conf
  ```bash
  input
  {
      tcp
          {
              type => "TCP_LOG"
              port => 5044
              codec => json
          }

      udp
          {
              type => "UDP_LOG"
              port => 5046
              codec => json
          }

  }


  filter
  {
    ruby {
          #code => "event.set('@timestamp', event.get('@timestamp').time.localtime('-04:00').strftime('%Y-%m-%d %H:%M:%S'))"
          code => "event.set('@timestamp', LogStash::Timestamp.new(Time.parse(event.get('@timestamp').time.localtime('-04:00').strftime('%Y-%m-%d %H:%M:%S'))))"
    }
  }

  output {
    if 'TCP_LOG' in [type] {
      elasticsearch {
          hosts => ["localhost1:9200","localhost2:9200","localhost3:9200"]
          user => es_logstash
          password => "test"
          ssl => true
          ssl_certificate_verification => false
          index => "logstash-%{+YYYY.MM.dd}"
          #cacert => "/etc/logstash/root-ca.pem"
        }
      }
      stdout{
        codec => rubydebug
      }
    }
  }
  ```

  - vi /apps/logstash/latest/config/logstash.yml
  ```bash
    pipeline.ecs_compatibility: disabled
      
    # ------------ Data path ------------------
    #
    # Which directory should be used by logstash and its plugins
    # for any persistent needs. Defaults to LOGSTASH_HOME/data
    #
    path.data: /apps/logstash/data/
    #
    # ------------ Pipeline Configuration Settings --------------
    #
    # Where to fetch the pipeline configuration for the main pipeline
    #
    path.config: /apps/logstash/latest/config/conf.d/
    #
    # log.format.json.fix_duplicate_message_fields: false
    #
    path.logs: /apps/logstash/logs/
    #
    # ------------ Other Settings --------------
    ```
  - ** Run 
  - export JAVA_HOME='/apps/java/latest'
  - /apps/logstash/bin/logstash 
  - **
  - exit
  - sudo chmod 775 -R ./logstash/
  - sudo scp -r devuser@localhost:/apps/storage/ELK_UPGRADE/elk_stack_upgrade_script_latest/scripts/* .
  - sudo cp -r ./logstash /etc/init.d/
  - sudo service logstash status
  - sudo service logstash stop
  - sudo service logstash start


## Java (Redhat OpenJDK Download)
- https://developers.redhat.com/products/openjdk/download
- __Installation Commands__
  - cd /apps
  - sudo mkdir java
  - sudo chmod 775 ./java/
  - sudo chown -R root: root./java/
  - sudo su -l logstash
  - sudo tar -xvf ./java-21-openjdk-21.0.6.0.7-1.portable.jdk.x86_64.tar.xz 
  - sudo ln -s java-21-openjdk-21.0.6.0.7-1.portable.jdk.x86_64 latest


## Elastic Stack Connection Test with v8.17.0 and Search Guard
- ES Connection Test for the upgraded version of ELK v8.17.0 using C# : `./upgrade-script/csharp/esclient/csharp-run.sh`
- ES Connection Test for the upgraded version of ELK v8.17.0 using Golang : `./upgrade-script/golang/go_run.sh`
- ES Connection Test for the upgraded version of ELK v8.17.0 using Nodejs : `cd ./upgrade-script/nodejs/`, then `node ./node_es_client.js`


## Elastic Stack Migration Scripts with v8.17.0 (Python Script)
- Create index all to new cluster: `python ./upgrade-script/migrate-index-script.py --es http://localhost:9200 --ts https://localhost1:9200`
- Create reindexing command: `python ./upgrade-script/reindexing-command-generate-script.py --es http://localhost:9200 --ts https://localhost1:9200`
- Reindexing (reindexing using shell script) : `python ./upgrade-script/Search-reindexing-script.py --es http://localhost:9200 --source_index test --type test --ts https://localhost1:9200 --target test --version 8 --update_aliase true`
- Validate the numer of docs for all indices (Compare the total count using between two clusters) : `python ./validate-docs-script.py --es http://localhost:9200 --ts https://localhost1:9200`
- Validate the ES connection with a ssl certificate using Golang, C# : `./upgrade-script/golang/go_run.sh`, `./upgrade-script/csharp/esclient/csharp-run.sh`
- __Index Setting in Kibana__
```bash
# /* ES Indices Template for the creation of ES indices */
# When reindexing, the replica is initially set to 0 to improve performance. When reindexing is completed for each index, it is automatically updated to 1 via reindexing script.
# Go to Kibana Instance and add the following query for all ES Indices

PUT _template/idx_default
{
  "index_patterns": [
    "*"
  ],
  "settings": {
    "number_of_shards": 5,
    "number_of_replicas": 0,
    "refresh_interval" : -1
  },
  "mappings": {
    "properties": {}
  }
}


/* Index Template Update */
PUT /_index_template/logstash
{
  "index_patterns": [
    "logstash-*"
  ],
  "template": {
    "settings": {
      "index": {
        "number_of_shards": "5",
        "number_of_replicas" : "1",
        "refresh_interval": null
      }
    },
    "mappings": {
      "dynamic_templates": [
        {
          "message_field": {
            "path_match": "message",
            "mapping": {
              "norms": false,
              "type": "text"
            },
            "match_mapping_type": "string"
          }
        },
        {
          "string_fields": {
            "mapping": {
              "norms": false,
              "type": "text",
              "fields": {
                "keyword": {
                  "ignore_above": 256,
                  "type": "keyword"
                }
              }
            },
            "match_mapping_type": "string",
            "match": "*"
          }
        }
      ],
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "geoip": {
          "dynamic": true,
          "properties": {
            "ip": {
              "type": "ip"
            },
            "latitude": {
              "type": "half_float"
            },
            "location": {
              "type": "geo_point"
            },
            "longitude": {
              "type": "half_float"
            }
          }
        },
        "@version": {
          "type": "keyword"
        }
      }
    }
  }
}
  

/* logstash,packetbeat Indices Template for the creation of ES indices */
PUT _template/idx_log_default
{
  "index_patterns": [
    "packet*",
    "es_pipeline_upload_test*"
  ],
  "settings": {
    "number_of_shards": 5,
    "number_of_replicas": 1,
    "refresh_interval" : null
  },
  "mappings": {
    "properties": {}
  }
}
 ```
- __Index Validating in Kibana__
 ```bash
 # --
# Here are a few examples to get you started.
# Create these indices to verify the user permission on Search Guard
POST _bulk/
{ "index" : { "_index" : "wx_test", "_id" : "12" ,"op_type":"create" } }
{ "ADDTS" : "03/09/2023 02:06:34.739993" }

POST _bulk/
{ "index" : { "_index" : "om_test", "_id" : "12" ,"op_type":"create" } }
{ "ADDTS" : "03/09/2023 02:06:34.739993" }

PUT wx_test/_settings
{
  "refresh_interval" : null,
  "number_of_replicas": 1
}

PUT om_test/_settings
{
  "refresh_interval" : null,
  "number_of_replicas": 1
}

# --
# allow us to delete indices via wildcard expressions
# https://stackoverflow.com/questions/63388937/elasticsearch-how-to-delete-multiple-indexes-with-wildcard
PUT _cluster/settings
{
    "transient": {
        "action.destructive_requires_name": false
    }
}

/* reset the numer of replicas when unassigned shards occured */
PUT test/_settings
{
  "index" : {
    "number_of_replicas" : 1
  }
}
 ```
- Elasticsearch v8.17 (new dev, "https://localhost:9200") has a different date format unlikely Elasticsearch V5 (dev, http://localhost:9200). Therefore, in Elasticsearch v8.17, the query format below is required.
- Elasticsearch v5.6.4 for the “date” field in “range query”
 ```bash
"format": "M/d/yyyy h:mm:ss.SSSSSS a Z||M/d/yyyy||strict_date_time||date||M/d/yyyy Z",
```
- Elasticsearch v8.17.0 for the “date” field in “range query”
```bash
"format": "M/d/yyyy h:mm:ss.SSSSSS a z||M/d/yyyy||strict_date_time||date||M/d/yyyy z"
```
- Elasticsearch v8.17.0 for the “date” field in “range query” as an example
```bash
POST /test/_search?pretty=true
{
  "from": 0,
  "size": 20,
  "sort": [
    {
      "ORDERKEY.keyword": {
        "order": "desc"
      }
    }
  ],
  "_source": ["ORDERKEY", "STATUSTS"],
  "query": {
    "bool": {
      "must": [
        {
          "range": {
            "INPUTDATE": {
              "gte": "11/01/2024 -04:00",
              "lte": "11/02/2024 -04:00",
               "format": "M/d/yyyy h:mm:ss.SSSSSS a z||M/d/yyyy||strict_date_time||date||M/d/yyyy z",
              "relation": "within",
              "_name": "STATUSTS"
            }
          }
        }
      ]
    }
  }
}
```