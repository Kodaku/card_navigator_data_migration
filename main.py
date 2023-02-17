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
    basic_auth=('elastic', 'Cj-ChuXcllkRQF8t8VFa')
)

index_name = "expansions"

if not es.indices.exists(index=index_name):
    print(f"Index {index_name} created")
    es_create_index_if_not_exists(es, index_name)

expansions_names = os.listdir("./data")
actions = []
for expansion_name in expansions_names:
    if expansion_name == "all_expansions.json":
        continue
    with open(f"./data/{expansion_name}") as json_file:
        expansion = json.load(json_file)
        action = {"index": {"_index": index_name, "_id": expansion_name}, "_op_type": "upsert"}
        doc = expansion
        actions.append(action)
        actions.append(json.dumps(doc, cls=PdEncoder))

res = es.bulk(index=index_name, operations=actions)
print(res)
