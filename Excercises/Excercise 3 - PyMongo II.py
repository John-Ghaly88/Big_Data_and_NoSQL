import pymongo
from pymongo import MongoClient
import urllib.parse
from bson.json_util import dumps

def get_path(dataset_name,env_name='colab'):
    prefix = 'https://raw.githubusercontent.com/John-Ghaly88/Big_Data_and_NoSQL/main/Datasets/Assessment/'
    if env_name == 'colab':
        return prefix+dataset_name
    else:
        return f'../Datasets/{dataset_name}'

username = urllib.parse.quote_plus('<<USERNAME>>') 
password = urllib.parse.quote_plus("<<PASSWORD>>")

url = "mongodb+srv://{}:{}@<<CLUSTERNAME>>.qgup7.mongodb.net/<<DATABASENAME>>?retryWrites=true&w=majority".format(username, password)
cluster = MongoClient(url)

db = cluster["airbnb"]
collection = db["berlin"]


# Reading listings from CSV into dataframe

import pandas as pd

df = pd.read_csv(get_path('listings.csv'))

df = df.drop(columns=['Unnamed: 0'])
df = df.rename(columns={'name':"title"})

i,j = df.shape

# Inserting the listings as documents into DB 

listings = []
num = 0

import math

for x in range(0,i):
    listings = listings + [{'_id': int(df.loc[x].id) if not math.isnan(df.loc[x].id) else None, 'listing_title': '%s' % (df.loc[x].title) if str(df.loc[x].title).strip() else None, \
    'host_id': int(df.loc[x].host_id) if not math.isnan(df.loc[x].host_id) else None, 'host_name': '%s' % (df.loc[x].host_name) if str(df.loc[x].host_name).strip() else None, \
    'neighbourhood': '%s' % (df.loc[x].neighbourhood) if str(df.loc[x].neighbourhood).strip() else None, 'neighbourhood_group': '%s' % (df.loc[x].neighbourhood_group) \
    if str(df.loc[x].neighbourhood_group).strip() else None, 'coordinates': { 'longitude': float(df.loc[x].longitude), 'latitude': float(df.loc[x].latitude) }, 'room_type': '%s' % (df.loc[x].room_type) \
    if str(df.loc[x].room_type).strip() else None, 'price': int(df.loc[x].price) if not math.isnan(df.loc[x].price) else None, 'minimum_nights': int(df.loc[x].minimum_nights) \
    if not math.isnan(df.loc[x].minimum_nights) else None, 'availability': int(df.loc[x].availability_365) if not math.isnan(df.loc[x].availability_365) else None} ]

    num = num + 1 

collection.insert_many(listings)
print(num,"listings were inserted")

# Removing listings with missing price quota

collection.delete_many({ '$or': [ { 'price': None }, { 'availability': None } ] })
collection.delete_many({ '$or': [ { 'price': 0 }, { 'availability': 0 } ] })

# Optimizing search results - on neighbourhood_group (showing execution timings before and after optimization)

mitte_listings = collection.find({"neighbourhood_group":"Mitte"}).explain()['executionStats']['executionTimeMillis']
print("Before indexing:",mitte_listings)

collection.create_index('neighbourhood_group')

mitte_listings = collection.find({"neighbourhood_group":"Mitte"}).explain()['executionStats']['executionTimeMillis']
print("After indexing:",mitte_listings)

# Finding the top 3 private rooms with minimum amount to pay (will need to drop 0 price first) + displaying the listing_id, cost, duration rent, minimum nights & neighbourhood

result = collection.aggregate(
    [{
        "$match" : { "room_type" : "Private room" }
    },
    { 
        "$project" : 
            {
                "_id" : "$_id", 
                "neighbourhood" : "$neighbourhood", 
                "duration_rent" : { "$multiply": [ '$price', '$minimum_nights' ]},
                "price" : "$price", 
                "minimum_nights" : "$minimum_nights",
                "room_type" : "$room_type"
            }
    },
    { 
        "$sort" : { "duration_rent" : 1 } 
    },
    {
        "$limit" : 3 
    }]
)
    
for l in result:
    print("Listing",l['_id'],"costs $",l['duration_rent'],"for",l['minimum_nights'],"day(s) and is in",l['neighbourhood'])


#---------------------------- RESTART ----------------------------

# collection.delete_many({})
# collection.drop_index("neighbourhood_group_1")