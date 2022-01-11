
import time

import json
from kafka import KafkaConsumer
import pandas as pd

from datetime import datetime
# The url of the data.
url = "http://opendata.paris.fr/explore/dataset/stations-velib-disponibilites-en-temps-reel/\
download/?format=csv&timezone=Europe/Berlin&use_labels_for_header=true"
API_KEY = "0a23bc099d920294b016a59a2ea19be2f171b410"
url_val = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)
consumer = KafkaConsumer("velib-stations")
dff=pd.DataFrame(columns=['number','contract_name','available_bike_stands'])
for message in consumer:
    station = json.loads(message.value.decode())
    station_number = station["number"]
    contract = station["contract_name"]
    capacity = station["bike_stands"]
    status = station["status"]
    date_time = time.strftime("%Y-%m-%d_%Hh%M")
    available_bike_stands = station["available_bike_stands"]
    dff.loc[len(dff)] = [station_number,contract,available_bike_stands]
    with open("YOUR DIRECTORY/velib_" + date_time + ".csv", "wb") as code:
        code.write(dff)

