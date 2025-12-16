from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os
import pandas as pd
import json

# Elasticsearch connection details
load_dotenv()
ES_HOST = "http://localhost:9200"
ES_USER = os.getenv("ES_USER") 
ES_PASSWORD = os.getenv("ES_PASSWORD")

# Connect to Elasticsearch with authentication
es = Elasticsearch([ES_HOST], basic_auth=(ES_USER, ES_PASSWORD))

# Define index name
index_name = "customer_data"

# Check if the index exists before querying
if not es.indices.exists(index=index_name):
    print(f"Index '{index_name}' does not exist.")
    exit()

# Fetch data from Elasticsearch
query = {
    "query": {
        "match_all": {}  # Fetches all documents
    },
    "size": 1000  
}

response = es.search(index=index_name, body=query)

# Print retrieved documents
print("Fetched Data from Elasticsearch:\n")
for hit in response["hits"]["hits"]:
    print(hit["_source"])  

hits = response["hits"]["hits"]
data = [hit["_source"] for hit in hits]  # Extract only the document data
df = pd.DataFrame(data)


df.to_csv("elasticsearch_data.csv", index=False)
with open("elasticsearch_data.json", "w") as f:
    json.dump(data, f, indent=4)

print(f"Extracted {len(hits)} records. Data saved as 'elasticsearch_data.csv' and 'elasticsearch_data.json'")
