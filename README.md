# Youtube Data Harvesting and Warehousing project_using SQL,Mongo,Streamlit createing web application (GUI)
# Introduction:
* Youtube has become one of the most popular socialmedia platforms in the  world, with millions of users,creating videos and content somethings else post on theri profile. The app and website is known for its simplicity, user-friendly interface. It has also gave several youtube awards to user it base on viewers lists,like and video count and hours.
* We create a web app to analyse the Youtube Api live Data and users channel details,playlists details,video details,comment details and Analysis the given 10 question from data in Databases and output based on DataFrame.

## Import Libraries
### clone libraries
```python
import requests
from googleapiclient.discovery import build
```
### Import Data handling libraries
```python
import pandas as pd
import numpy as np
```
### Import Date Time libraries
```python
import datetime
import time
import datetime as dt
```
### Import Dashboard libraries
```python
import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
```
### Import Databases libraries
```python
import pymongo
client=pymongo.MongoClient('mongodb+srv://mhd_basith:mohamedbasith@cluster0.wknulhj.mongodb.net/?retryWrites=true&w=majority')
db=client['youtube_database']
collection=db['allchannel_data']
```
```python
import psycopg2
cont=psycopg2.connect(host='localhost',user='postgres',password='basith',port=5432,database='basith')
csr=cont.cursor()
```
### if the module shows any error or module not found it can be overcome by using below command
```python
pip install<module name>
```
# E T L Process
## a) Extract data
* Initially, we Fetch the data from the **Youtube Api live data** by using Python libraries.
* using **api_key**
- ### In order to get the data 
#### - Inorder to youtube data into to working environment use below command
* Create the four function to fecth or extract the data **channel,playlist,video,comment details**.
## b) Process and Transform the data
### Fetch data & Transform into MongoDB 
- after extracted the data from youtube api data to  insert the form of json format to MongoDB
- In order to migrate the data **MongoDB to PostgreSQL**
```python
import pymongo
client=pymongo.MongoClient('mongodb+srv://mhd_basith:mohamedbasith@cluster0.wknulhj.mongodb.net/?retryWrites=true&w=majority')
db=client['youtube_database']
collection=db['allchannel_data']
```
## c) Load  data 
###  Create Table and Insert into Postgresql
- After creating table insert the data into inner server by using postgresql
- To Establish the connection with sql server
- below table to reference another tables 
```python
#postgresql connect
import psycopg2
cont=psycopg2.connect(host='localhost',user='postgres',password='basith',port=5432,database='basith')
csr=cont.cursor()
```
```python
#create tables
```python
csr.execute("""create table if not exists channeldata_details(Channel_Name varchar(-values-),
            Channel_Id varchar(60) primary key,
            Subcription_Count bigint,
            Channel_Video_Count bigint,
            Channel_Views bigint,
            Channel_Description varchar(-values-),
            PlaylistsId varchar(60))""")
cont.commit()
```
* create the four table and intract with each others using foreign key and pimary key
* then migrate the data MongoDB to PostgreSQL
```python
def insert_channel_details(chanl_name):
   query = """insert into channeldata_details values(%s,%s,%s,%s,%s,%s,%s)"""
   for i in collection.find({"Channel Details.Channel_Name":chanl_name},{"_id":0,"Channel Details":1}):
        data=i.values()
        for j in data:
            ch=j[0]
            csr.execute(query,tuple(ch.values()))
            cont.commit()
```
# E D A Process and Frame work
## a) Access PostSQL DB 
###  Create a connection to the postgreSQL server and access the specified postgreSQL DataBase by using **psycopg2** library
```python
SELECT * FROM "Table"
WHERE "Condition"
GROUP BY "Columns"
ORDER BY "Data"
```
## b) Visualization 
###  Finally, create a Dashboard by using Streamlit and applying selection and dropdown options on the Dashboard and show the output are Dataframe Table on corresponding query and questions
```python
df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name',1:'channel_views'})
        st.dataframe(df_re)
```
- create the streamlit app with basic tabs [Reference](https://docs.streamlit.io/library/api-reference)
- visualizing the data with Dataframe and streamlit dashboard
- streamlit run <filename.py> to run terminal
# Conculsion
- I hope this project helps you to the understand more about youtube data analysis and datafame visualization
