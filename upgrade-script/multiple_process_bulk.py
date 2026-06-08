from elasticsearch import Elasticsearch, helpers
from concurrent.futures import ThreadPoolExecutor
import json

# Elasticsearch connection
es = Elasticsearch(["http://localhost:9200"])

# Load large dataset (assuming it's in a JSON file)
with open("large_dataset.json") as f:
    data = json.load(f)

# Prepare bulk actions
actions = [
    { "_index": "myindex", "_source": doc }
    for doc in data
]

# Function to perform bulk indexing
def bulk_index(batch):
    helpers.bulk(es, batch)

# Split actions into batches
batch_size = 1000
batches = [actions[i:i + batch_size] for i in range(0, len(actions), batch_size)]

# Perform concurrent bulk indexing
with ThreadPoolExecutor() as executor:
    executor.map(bulk_index, batches)