import json
from kafka import KafkaConsumer
import pandas as pd
import streamlit as st
from streamlit_folium import folium_static
import folium
import urllib
from datetime import datetime
import matplotlib.pyplot as plt
API_KEY = "0a23bc099d920294b016a59a2ea19be2f171b410"
url = "https://api.jcdecaux.com/vls/v1/stations?apiKey={}".format(API_KEY)

with urllib.request.urlopen(url) as response:
    velib_data = pd.read_json(response.read())



now = datetime.now()
date_of_data = now.strftime('%Y/%m/%d %H:%M:%S')
st.title('Velib_Station')
API = st.text_input("INPUT YOUR API_KEY" )
url = st.text_input("INPUT YOUR url")

st.sidebar.write('Data retrieved on: ' + str(date_of_data))
velib_data.head()
st.sidebar.write("There are {0} Velib stands in Paris".format(velib_data.address.count()))
st.sidebar.write("There are {0} bike stands in total".format(velib_data.bike_stands.sum()))
st.sidebar.write("There are {0} available bikes".format(velib_data.available_bikes.sum()))
st.sidebar.write("There are {0} available bikes stands".format(velib_data.available_bike_stands.sum()))
st.sidebar.write("")

bike_stands_max = velib_data.bike_stands.max()
bike_stands_max_query = "bike_stands == " + str(bike_stands_max)
st.sidebar.write("Biggest stations with {0} bike stands:".format(bike_stands_max))
st.sidebar.write(velib_data.query(bike_stands_max_query).address.values)
st.sidebar.write("")

bike_stands_min = velib_data.bike_stands.min()
bike_stands_min_query = "bike_stands == " + str(bike_stands_min)
st.sidebar.write("Smallest stations with {0} bike stands:".format(bike_stands_min))
st.sidebar.write(velib_data.query(bike_stands_min_query).address.values)

stations = {}

col1,col3=st.columns(2)

velib_data['latitude'] = velib_data['position'].apply(lambda x: x['lat'])
velib_data['longitude'] = velib_data['position'].apply(lambda x: x['lng'])
plt.style.use('ggplot')

fig, ax = plt.subplots(figsize=(8, 8))
velib_data.plot(ax = ax, kind='scatter', y='latitude', x='longitude', title='Velib stations location');
with st.expander("Velib stations location"):
    st.write(fig)



fig, ax = plt.subplots(figsize=(10, 8))
velib_data['availability'] = 100 * velib_data['available_bikes'] / velib_data['bike_stands']
velib_data.plot(ax = ax, kind='scatter', y='latitude', x='longitude' , c='availability', cmap=plt.get_cmap('RdYlGn'));

plt.title(' Velib station bike availability on ' + date_of_data);
with st.expander(" Velib station bike availability on"):
    st.write(fig)

with st.expander('Velib stations with no bike available as of '+str(date_of_data)):
    st.table(velib_data.query("available_bikes == 0"))
from folium.plugins import HeatMap
with st.expander('Map de destribution des vélo'):
    m = folium.Map(location=[48.86, 2.35], zoom_start=12)
    data = [[a,b,c] for a,b,c in zip(velib_data.latitude.values,
                                 velib_data.longitude.values,
                                 velib_data.availability.values) ]
    df = pd.DataFrame(data)
    df.dropna(inplace=True)
    data1=df.values.tolist()
    gradient={'0': 'Red','0.5': 'Yellow','1': 'Green'}

    # plot heatmap
    m.add_child(HeatMap(data1, radius=7, gradient=gradient) )
    folium_static(m)
with st.expander('Return bike to place with bonuses optimisation'):
    m = folium.Map(location=[48.86, 2.35], zoom_start=12)
    df = velib_data.query("bonus == True")
    for lat,lon in zip(df.latitude,df.longitude):
        folium.CircleMarker(location = [lat, lon], radius=7 ).add_to(m)
    folium_static(m)









consumer = KafkaConsumer("velib-stations")
dff=pd.DataFrame(columns=['number','contract_name','available_bike_stands'])
for message in consumer:
    station = json.loads(message.value.decode())
    station_number = station["number"]
    contract = station["contract_name"]
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
        Dataf = dff.tail(10)
        st.table(Dataf)