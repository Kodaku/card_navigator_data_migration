from elasticsearch import Elasticsearch, RequestError
import os
import json
import pandas as pd


class PdEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def es_create_index_if_not_exists(es, index):
    try:
        es.indices.create(index=index)
    except RequestError as ex:
        print(ex)


es = Elasticsearch(
    hosts=['http://localhost:9200'],
    basic_auth=('elastic', 'e0_kX+xT1Oh_v+8pLot3')
)

index_name = "expansions"

if not es.indices.exists(index=index_name):
    print(f"Index {index_name} created")
    es_create_index_if_not_exists(es, index_name)

expansion_years = os.listdir("./data")
actions = []
for expansion_year in expansion_years:
    if expansion_year == "all_expansions.json":
        continue
    expansions_names = os.listdir("./data/" + expansion_year)
    for expansion_name in expansions_names:
        print(f"{expansion_name} {expansion_year}")
        with open(f"./data/{expansion_year}/{expansion_name}") as json_file:
            expansion = json.load(json_file)
            action = {"index": {"_index": index_name, "_id": expansion_name}, "_op_type": "upsert"}
            doc = expansion
            doc["year"] = expansion_year
            actions.append(action)
            actions.append(json.dumps(doc, cls=PdEncoder))
res = es.bulk(index=index_name, operations=actions)
print(res)
