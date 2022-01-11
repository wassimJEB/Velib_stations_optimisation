import json
from kafka import KafkaConsumer
import pandas as pd

from datetime import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch()
API_KEY = "0a23bc099d920294b016a59a2ea19be2f171b410"
url_val = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)
def check_and_create_index(es, index: str):
    # define data model
    mappings = {
        'mappings': {
            'properties': {
                'station_number': {'type': 'keyword'},
                'contract': {'type': 'text'},
                'capacity': {'type': 'text'},
                'status': {'type': 'keyword'},

            }
        }
    }
    if not es.indices.exists(index):
        es.indices.create(index=index, body=mappings, ignore=400)
def index_search(es, index: str, keywords: str, filters: str,
                 from_i: int, size: int) -> dict:

    # search query
    body = {
        'query': {
            'bool': {
                'must': [
                    {
                        'query_string': {
                            'query': keywords,
                            'fields': ['content'],
                            'default_operator': 'AND',
                        }
                    }
                ],
            }
        },
        'highlight': {
            'pre_tags': ['<b>'],
            'post_tags': ['</b>'],
            'fields': {'content': {}}
        },
        'from': from_i,
        'size': size,
        'aggs': {
            'tags': {
                'terms': {'field': 'tags'}
            },
            'match_count': {'value_count': {'field': '_id'}}
        }
    }
    if filters is not None:
        body['query']['bool']['filter'] = {
            'terms': {
                'tags': [filters]
            }
        }

    res = es.search(index=index, body=body)
    # sort popular tags
    sorted_tags = res['aggregations']['tags']['buckets']
    sorted_tags = sorted(
        sorted_tags,
        key=lambda t: t['doc_count'], reverse=True
    )
    res['sorted_tags'] = [t['key'] for t in sorted_tags]
    return res

stations = {}
consumer = KafkaConsumer("velib-stations")
dff=pd.DataFrame(columns=['number','contract_name','available_bike_stands'])
for message in consumer:
    station = json.loads(message.value.decode())
    station_number = station["number"]
    contract = station["contract_name"]
    capacity = station["bike_stands"]
    status = station["status"]
    available_bike_stands = station["available_bike_stands"]
    dff.loc[len(dff)] = [station_number,contract,available_bike_stands]




    if contract not in stations:
        stations[contract] = {}
    city_stations = stations[contract]
    if station_number not in city_stations:
        city_stations[station_number] = available_bike_stands

    count_diff = available_bike_stands - city_stations[station_number]
    L=[]
    if count_diff != 0:
        city_stations[station_number] = available_bike_stands
        print("{}{} {} ({})".format(
            "+" if count_diff > 0 else "",
            count_diff, station["address"], contract
        ))
        L.append([count_diff,station["address"], contract])


