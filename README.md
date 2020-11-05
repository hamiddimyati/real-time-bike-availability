# Real-time prediction of bikes availability in NYC

## Problem description

Using shared bikes is really simple: you just have to find the nearest station and take a bike. Cycle paths
are now part of the landscape so that people are able to get to work safely by bike. The issue they may face
is availability: they have no guarantee that they will have a bike to come back home.
To solve this problem, given a location and a date-time, we will predict the number of available bikes.

## Data

- [Citi Bike Real-Time API](http://gbfs.citibikenyc.com/gbfs/gbfs.json) ([GBFS format](https://github.com/NABSA/gbfs/))
- [Citi Bike data sets](https://s3.amazonaws.com/tripdata/index.html) to train our model
- [OpenWeather API](https://openweathermap.org/current) to train, evaluate, and process requests
