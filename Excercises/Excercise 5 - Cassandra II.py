from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def get_path(dataset_name,env_name='colab'):
    prefix = 'https://raw.githubusercontent.com/John-Ghaly88/Big_Data_and_NoSQL/main/Datasets/Assessment/'
    if env_name == 'colab':
        return prefix+dataset_name
    else:
        return f'../Datasets/{dataset_name}'

cloud_config= {
        'secure_connect_bundle': '<</PATH/TO/>>secure-connect-mydatabase.zip'
}
auth_provider = PlainTextAuthProvider('<<CLIENT ID>>', '<<CLIENT SECRET>>')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()
session.set_keyspace('airbnb') 


# Reading listings from CSV into dataframe

import pandas as pd

df = pd.read_csv(get_path('listings.csv'), encoding= 'unicode_escape')

df = df.drop(columns=['Unnamed: 0'])
df = df.rename(columns={'name':"title"})

i,j = df.shape

# Inserting the listings into DB (Creating UDT for Coordinates, Creating Table + Inserts)

session.execute("create type long_lat ( longitude float, latitude float );")

session.execute("""
    create table if not exists airbnb.berlin (
        id int primary key,
        listing_title text,
        host_id int,
        host_name text,
        neighbourhood text,
        neighbourhood_group text,
        coordinates long_lat,
        room_type text,
        price int,
        minimum_nights int,
        availability int
 	);
""")

num = 0

import math

for x in range(0,1000):
    listing_id = int(df.loc[x].id) if not math.isnan(df.loc[x].id) else "null"
    listing_title = "null" if str(df.loc[x].title)=="nan" else df.loc[x].title
    host_id = int(df.loc[x].host_id) if not math.isnan(df.loc[x].host_id) else "null"
    host_name = df.loc[x].host_name if str(df.loc[x].host_name).strip() else "null"
    neighbourhood = df.loc[x].neighbourhood if str(df.loc[x].neighbourhood).strip() else "null"
    neighbourhood_group = df.loc[x].neighbourhood_group if str(df.loc[x].neighbourhood_group).strip() else "null"
    room_type = df.loc[x].room_type if str(df.loc[x].room_type).strip() else "null"
    price = int(df.loc[x].price) if not math.isnan(df.loc[x].price) else "null"
    minimum_nights = int(df.loc[x].minimum_nights) if not math.isnan(df.loc[x].minimum_nights) else "null"
    availability = int(df.loc[x].availability_365) if not math.isnan(df.loc[x].availability_365) else "null"
    if "'" not in str(listing_title) and "'" not in str(host_name):
        session.execute("insert into airbnb.berlin(id,listing_title,host_id,host_name,neighbourhood,neighbourhood_group,coordinates,room_type,price,minimum_nights,availability) values \
        (%d,'%s',%d,'%s','%s','%s',{longitude:%d,latitude:%d},'%s',%d,%d,%d);" % (listing_id,listing_title,host_id,host_name,neighbourhood,neighbourhood_group,df.loc[x].longitude,\
        df.loc[x].latitude,room_type,price,minimum_nights,availability))

        num = num + 1 

print(num,"listings were inserted")

rows = session.execute("select * from airbnb.berlin limit 10;") # to make sure entries where inserted correctly
for row in rows:
    print(row)


# Removing listings with missing price quota (need to index price first)

session.execute("create index on airbnb.berlin(price);")

rows = session.execute("select * from airbnb.berlin where price=0;")
num = 0
ids = []
for row in rows:
    num = num+1
    ids = ids + [row[0]]
    print(row)

for n in ids:
    session.execute("delete from airbnb.berlin where id='{0}';".format(n))

print(num,"rows deleted")

# Optimizing search results - on neighbourhood_group (showing execution timings before and after optimization)
session.execute("create index on airbnb.berlin(neighbourhood_group);")

rows = session.execute("select * from airbnb.berlin where neighbourhood_group='Mitte';")
for row in rows:
    print(row)

# Finding the top 3 private rooms with minimum amount to pay (adding new column duration_rent + updating it) + displaying the listing_id, cost, duration rent, minimum nights & neighbourhood

session.execute("alter table airbnb.berlin add duration_rent int;")

session.execute("create index on airbnb.berlin(room_type);")

rows = session.execute("select id,price,minimum_nights from airbnb.berlin where room_type='Private room';")
num = 0

for row in rows:
    if row[1] is not None and row[2] is not None:
        num = num+1
        dr = int(row[1])*int(row[2])
        session.execute("update airbnb.berlin set duration_rent={0} where id={1};".format(dr,int(row[0])))
        print(num)

print(num,"rows updated")

"""

session.execute("create index on airbnb.berlin(duration_rent);")

result = session.execute("select id,duration_rent,minimum_nights,neighbourhood from airbnb.berlin where room_type='Private room' order by duration_rent ASC limit 3;")
num = 0

for l in result:
    print("Listing",l[0],"costs $",l[1],"for",l[2],"day(s) and is in",l[3])

"""

######### WILL NOT BE POSSIBLE WITHOUT DROPPING TABLE THEN MAKING DURATION_RENT AS CLUSTERING KEY + ORDER BY ASC #########



