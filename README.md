# Youtube_Data_Harvesting_and_Warehousing_project_using_SQL_Mongo_Streamlit
# Youtube Data Harvesting and Warehousing project_using SQL,Mongo,Streamlit createing web application (GUI)
# Introduction:
* Youtube has become one of the most popular socialmedia platforms in the  world, with millions of users,creating videos and content somethings else post on theri profile. The app and website is known for its simplicity, user-friendly interface. It has also gave several youtube awards to user it base on viewers lists,like and video count and hours.
* We create a web app to analyse the Youtube Api live Data and users channel details,playlists details,video details,comment details and Analysis the given 10 question from data in Databases and output based on DataFrame.

# Import Libraries
## clone libraries
```python
import requests
from googleapiclient.discovery import build
```
## Import Data handling libraries
```python
import pandas as pd
import numpy as np
```
## Import Date Time libraries
```python
import datetime
import time
import datetime as dt
```
## Import Dashboard libraries
```python
import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
```
## Import Databases libraries
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
