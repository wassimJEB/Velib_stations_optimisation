import json
from kafka import KafkaConsumer
import pandas as pd

from datetime import datetime

API_KEY = "0a23bc099d920294b016a59a2ea19be2f171b410"
url_val = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)



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


