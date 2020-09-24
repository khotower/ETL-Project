#!/usr/bin/env python
# coding: utf-8

import numpy as np
import pandas as pd
import pymongo

print('Script Started')
# Create a variable for data source folder
DataFolder = 'Data/'

print('Create Mongo DB connection')
# setup Monogo DB connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["DepartureDelays"]

print('Read Source Files')
# Read airport codes file
airportCodes = pd.read_csv(DataFolder+'airport-codes-na.csv')

# read departure delay file
depDelay = pd.read_csv(DataFolder+'departuredelays.csv')

print('Display first two rows of departure delay dataset')
# Display first two rows of departure delay dataset
print(depDelay.head(2))

print('Display first two rows of airport codes dataset')
# Display first two rows of airport codes dataset
print(airportCodes.head(2))

print('Get Country, State, City of Origin')
## Join departure delay dataset and airport codes dataset to get origin city details
departureData = depDelay.merge(airportCodes, left_on = 'origin', right_on = 'IATA')

# Keep only required columns
cols = ['Country', 'State','City','destination', 'distance', 'delay'] 
departureData = departureData[cols]


# Rename Country, state and city to indicate these represent origin
departureData = departureData.rename(columns={'Country': 'Origin_Country', 'State': 'Origin_State', 'City': 'Origin_City'})

print('Get Country, State, City of Destination')
## Join departure delay dataset and airport codes dataset to get origin city details
departureData = departureData.merge(airportCodes, left_on = 'destination', right_on = 'IATA')

# Keep only required columns
cols = ['Origin_Country', 'Origin_State', 'Origin_City', 'Country', 'State','City', 'distance', 'delay'] 
departureData = departureData[cols]

# # Rename Country, state and city to indicate these represent Desitnation
departureData = departureData.rename(columns={'Country': 'Dest_Country', 'State': 'Dest_State', 'City': 'Dest_City'})

print('Query Scenario 1')
## Find the sum of Flight delay for each destination city and order in decreasing order of delay
destDelay = departureData.copy()

# Select only required columns
destDelay = destDelay[['Dest_Country', 'Dest_State','Dest_City', 'delay']]
destDelay = destDelay.groupby(['Dest_Country', 'Dest_State','Dest_City']).sum().reset_index()
destDelay = destDelay.sort_values('delay', ascending=False)

print('Query Scenario 2')
# Find total distance covered by flights originating for all cities in USA and find average delay for each city
OrigDelay = departureData.copy()

# Select only required columns
OrigDelay = OrigDelay[['Origin_Country', 'Origin_State','Origin_City', 'distance','delay']]
OrigDelay = OrigDelay.groupby(['Origin_Country', 'Origin_State','Origin_City']).agg(TotalDistance=('distance','sum'), AvgDelay=('delay', 'mean')).reset_index()
OrigDelay['AvgDelay'] = round(OrigDelay['AvgDelay'],2)
print('Insert datasets in Mongo DB')
# Insert destdelays data frame into Mongo DB collection: destDelays
destCollection = mydb["destDelays"]
mydb.destCollection.delete_many({})
mydb.destCollection.insert_many(destDelay.to_dict('records'))


# Insert destdelays data frame into Mongo DB collection: origDelays
origCollection = mydb["origDelays"]
mydb.origCollection.delete_many({})
mydb.origCollection.insert_many(OrigDelay.to_dict('records'))

print('Script Completed')