
# ELK Stack Settings
<i> ELK Stack Settings

## Elasticsearch Settings
- __Commands__
```bash
## ************************
# -- LOG WRITE
PUT /_all/_settings
{
  "index.search.slowlog.threshold.query.warn": "0s",
  "index.search.slowlog.threshold.fetch.warn": "0s"
}

PUT /_all/_settings
{
  "index.search.slowlog.threshold.query.warn": null,
  "index.search.slowlog.threshold.fetch.warn": null
}

#idx_1, my_index 색인으로 /_search 요청 확인하기
PUT _cluster/settings
{
   "transient" : {
      "logger.org.elasticsearch.http.HttpTracer" : "TRACE",
      "http.tracer.include" : [ "*idx_1/_search*","*my_index/_search*" ,"*my_index/_search*"]
   }
}

#disable 설정
PUT _cluster/settings
{
   "transient" : {
      "logger.org.elasticsearch.http.HttpTracer" : null,
      "http.tracer.include" : null
   }
}

## ************************
# Elasticsearch 클러스터 내 특정 노드에 샤드가 몰려 디스크 및 처리량 불균형이 발생하는 주요 원인은 디스크 워터마크(Watermark) 도달, 잘못된 샤딩 전략(샤드 크기 불균형), 또는 버전 업그레이드에 따른 디스크 기반 밸런싱 로직 변경입니
PUT _cluster/settings
{
  "transient": {
    "cluster.routing.allocation.enable": "all",
    "cluster.routing.rebalance.enable": "all"
  }
}


PUT _cluster/settings
{
  "persistent": {
    "cluster": {
      "routing": {
        "allocation": {
          "disk": {
            "threshold_enabled": "true",
            "watermark": {
              "low": "75%",
              "high" : "85%"
            },
            "include_relocations": "true",
            "reroute_interval": "120m"
          }
        }
      }
    }
  }
}

PUT _cluster/settings
{
  "transient": {
    "cluster.routing.allocation.disk.watermark.low": "80%",
    "cluster.routing.allocation.disk.watermark.high": "85%"
  }
}

PUT _cluster/settings
{
  "transient": {
    "cluster.routing.allocation.disk.watermark.low": null,
    "cluster.routing.allocation.disk.watermark.high": null
  }
}

PUT _cluster/settings
{
  "persistent": {
    "cluster.routing.allocation.disk.reshuffle_agenda": "size",
    "cluster.routing.allocation.balance.disk": true
  }
}

 
PUT om_receipt_10052020_20_5_1/_settings
{
  "index" : {
    "number_of_replicas" : 1
  }
}

PUT wx_order_casemnf_10052020_20_5_1/_settings
{
  "index" : {
    "number_of_replicas" : 1
  }
}
```