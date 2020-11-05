import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
import numpy as np
import requests
from datetime import datetime
import pytz

fig, ax = plt.subplots(figsize = (15, 15))
url = 'http://localhost:8080/available_bikes'

df = pd.read_csv("./data/outputs/stations.csv")
crs = {'init': 'epsg:4326'}
geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
geo_df = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)

point = np.array([-74.000, 40.750])
distances = [np.linalg.norm(point - station) for station in zip(df["longitude"], df["latitude"])]
nearest_stations = np.argsort(distances)[:3]

ax.plot(point[0], point[1], markersize=2, marker='o')
geo_df.plot(ax=ax, markersize=2, color='blue', marker='o', label='Stations')
geo_df.iloc[nearest_stations].plot(ax=ax, markersize=2, color='red', marker='x', label='Nearest stations')

tz_NY = pytz.timezone('America/New_York')
current_datetime = datetime.now(tz_NY)

year, month, day = current_datetime.year, current_datetime.month, current_datetime.day
hour, minute = current_datetime.hour, current_datetime.minute

coo = geo_df.iloc[nearest_stations][["latitude", "longitude"]].values
names = geo_df.iloc[nearest_stations]["name"].values

for row, name in zip(coo, names):
	myobj = {'latitude': f'{row[0]}',
             'longitude': f'{row[1]}',
             'year': year,
             'month': month,
             'day': day,
             'hour': hour,
             'min': minute}
	print(name)
	print(requests.post(url, data=myobj).text)


plt.legend(loc='best')
plt.show()
