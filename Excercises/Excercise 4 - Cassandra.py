# !pip install cassandra-driver

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

# Describing a keyspace

session.set_keyspace('<<KEYSPACENAME>>')
row = cluster.metadata.keyspaces['<<KEYSPACENAME>>']

from pprint import pprint
pprint(vars(row))

# Create UDT to store subscription fees & library contents

session.execute("create type sub_fees_pre_month ( Basic float, Standard float, Premium float );")
session.execute("create type library ( Shows int, Movies int );")

# Create Netflix Subscription table

session.execute("""
    create table if not exists mykeyspace.netflix_subscription (
        Country text,
        Subscription sub_fees_pre_month,
        LibrarySize int,
        Library library,
        Primary key (Country)
 	);
""")

# Insert entries from CSV to table + Show the table after

import pandas as pd

df = pd.read_csv(get_path('netflix price in different countries.csv'))

df = df.rename(columns={"Total Library Size": "Library", "No. of TV Shows": "Shows", "No. of Movies": "Movies",\
"Cost Per Month - Basic ($)": "Basic", "Cost Per Month - Standard ($)": "Standard", "Cost Per Month - Premium ($)": "Premium"})

i,j = df.shape

for x in range(0,i):
    session.execute("insert into mykeyspace.netflix_subscription(Country,Subscription,LibrarySize,Library) values ('%s',{Basic:%d,Standard:%d,Premium:%d},%d,{Shows:%d,Movies:%d});" \
    % (df.loc[x].Country, float(df.loc[x].Basic), float(df.loc[x].Standard), float(df.loc[x].Premium), int(df.loc[x].Library), int(df.loc[x].Shows), int(df.loc[x].Movies)))

    print(df.loc[x].Country,"has been inserted")

rows = session.execute("select * from mykeyspace.netflix_subscription;")
for row in rows:
    print(row)

# Creating a secondary index on library size (to be able to select based on it) + selecting on library size < 5000

session.execute("create index on mykeyspace.netflix_subscription(librarysize);")

rows = session.execute("select * from mykeyspace.netflix_subscription where librarysize>5000;")
for row in rows:
    print(row)

# Deleteing entries with library size > 7000

rows = session.execute("select * from mykeyspace.netflix_subscription where librarysize>7000;")
num = 0
country_list = []
for row in rows:
    num = num+1
    country_list = country_list + [row[0]]
    print(row)

for n in country_list:
    session.execute("delete from mykeyspace.netflix_subscription where country='{0}';".format(n))

print(num,"rows deleted")

# Adding extra column to table

session.execute("alter table mykeyspace.netflix_subscription add capacity text;")

# Updating specific entries in table based on library size --> <3000 set capacity=low

rows = session.execute("select * from mykeyspace.netflix_subscription where librarysize<3000;")
num = 0
country_list = []
for row in rows:
    num = num+1
    country_list = country_list + [row[0]]
    print(row)

# .format helps u to use variables in ur query, {0} means the first cell in the .format parameters, which is the n in this case, u can add as many as u want.
for n in country_list:
    session.execute("update mykeyspace.netflix_subscription set capacity='low' where country='{0}';".format(n))

print(num,"rows updated")

rows = session.execute("select * from mykeyspace.netflix_subscription where librarysize<3000;")
for row in rows:
    print(row)