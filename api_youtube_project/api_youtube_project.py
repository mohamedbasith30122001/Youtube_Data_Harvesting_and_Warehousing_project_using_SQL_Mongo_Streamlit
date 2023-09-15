#importing packages
import pandas as pd
import numpy as np
import datetime
import time
import datetime as dt
import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build

api_service_name="youtube"
api_version="v3"
#youtube2.api_key='AIzaSyBRvtKNGx-746akh6XdxnyFifJwn46DVsA'
#api_key = 'AIzaSyBz6bnDuYeazZ1JLIW2HqSFXQUnpV-nqto'
#youtubeid1.
api_key='AIzaSyAyxWploecHG4BHNgsljNyIbC4f1oKQ3iY'
youtube = build(api_service_name,api_version, developerKey=api_key)

#creating option menu
icon = Image.open("youtube.png")
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing | by Mohamedbasith",
page_icon= icon,layout='wide',initial_sidebar_state="expanded",menu_items={'About': """# This app is created by *Mohamed Basith!*"""},)
st.title(':white[YOUTUBE DATA HARVESTING AND WAREHOUSING !!!:rocket:.]')
icon=Image.open("youtube3.jpg")
st.image(icon)


with st.sidebar:
   selected=option_menu(None,["Home","Fetch & Insert & Migrate","View"],icons=["house-door-fill","youtube","rocket"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "15px", "text-align": "centre", "margin": "0px",
                                                "--hover-color": "#f87171"},
                                   "icon": {"font-size": "30px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected":{"background-color": "#e41d1d"}})

#mongo connect
import pymongo
client=pymongo.MongoClient('mongodb+srv://mhd_basith:mohamedbasith@cluster0.wknulhj.mongodb.net/?retryWrites=true&w=majority')
db=client['youtube_database']
collection=db['allchannel_data']

#postgresql connect
import psycopg2
cont=psycopg2.connect(host='localhost',user='postgres',password='basith',port=5432,database='basith')
csr=cont.cursor()

#extract chanel data
def get_channel_details(youtube,channel_id):
  channel_details=[]
  request = youtube.channels().list(
    id=channel_id,
    part='snippet,statistics,contentDetails'
  )
  response = request.execute()
  channel_informations=response

  for item in channel_informations['items']:
    data={#'channel_details':{
      'Channel_Name' : item['snippet']['title'],
      'Channel_Id' : item['id'],
      'Subcription_Count' : int(item['statistics']['subscriberCount']),
      'Channel_Video_Count': int(item['statistics']['videoCount']),
      'Channel_Views' : int(item['statistics']['viewCount']),
      'Channel_Description' : item['snippet']['description'],
      'PlaylistsId' : item['contentDetails']['relatedPlaylists']['uploads']}

    channel_details.append(data)
  return channel_details

#extract playlist data
def get_playlist_id(youtube,channel_id):
  playlist_id=[]
  request = youtube.channels().list(
    id=channel_id,
    part='snippet,statistics,contentDetails')

  response = request.execute()

  for item in response['items']:
    data={'Channel_Id' : item['id'],
          'PlaylistsId' : item['contentDetails']['relatedPlaylists']['uploads']}

    playlist_id.append(data)
  return playlist_id

#extract video data
def get_video_id(youtube,channel_id):

  video_details_sts=[]

  request = youtube.channels().list(
    id=channel_id,
    part='snippet,statistics,contentDetails')

  response = request.execute()

  for item in response['items']:
    data={'playlistsId' : item['contentDetails']['relatedPlaylists']['uploads']}

  token=None
  while True:
    request = youtube.playlistItems().list(
        part='contentDetails',
        playlistId=data['playlistsId'],
        maxResults=50,
        pageToken=token)
    response=request.execute()
    playlist_informations=response

    for item in playlist_informations['items']:
      video_details_sts.append(item['contentDetails']['videoId'])

    token=playlist_informations.get('nextPageToken')
    if playlist_informations.get('nextPageToken') is None:
      break
  return  video_details_sts

#extract video data
def get_video_detail(youtube,video_details_sts,playlist):

  video_id_details=[]

  for i in range(0,len(video_details_sts),50):
    request = youtube.videos().list(
      part='snippet,contentDetails,statistics',
      id=','.join(video_details_sts[i:i+50]))

    response=request.execute()

    #item=response['items'][0]
    
    for item in response['items']:
        data={"Video_Id":item['id'],
            "PlaylistsId":playlist,
            "Video_Name": item['snippet']['title'] if 'title' in item['snippet']else "not avaiable",
            "Video_Description": item['snippet']['description'] if 'description' in item['snippet'] else "not avaiable",
            "Tags":item['snippet']['tags'] if 'tags' in item['snippet'] else "not avaiable",
            "PublishedAt":pd.to_datetime(item['snippet']['publishedAt']).strftime('%Y-%m-%d') if 'publishedAt' in item['snippet'] else 0,
            "View_Count":int(item['statistics']['viewCount']) if 'viewCount' in item['statistics'] else 0,
            "Like_Count": int(item['statistics']['likeCount']) if 'likeCount' in item['statistics'] else 0,
            "Dislike_Count":int(item['statistics']['dislikeCount']) if 'dislikeCount' in item['statistics'] else 0,
            "Favorite_Count":int(item['statistics']['favoriteCount']) if 'favoriteCount' in item['statistics'] else 0,
            "Comment_Count":int(item['statistics']['commentCount']) if 'commentCount' in item['statistics'] else 0,
            "Duration":pd.to_timedelta(item['contentDetails']['duration']).total_seconds() if 'duration' in item['contentDetails'] else "not avaiable",
            "Thumbnail":item['snippet']['thumbnails']['default']['url'] if 'default' in item['snippet']['thumbnails'] else "not avaiable",
            "Caption_Status":item['contentDetails']['caption'] if 'caption' in item['contentDetails'] else "not avaiable"}
        
        video_id_details.append(data)

  return video_id_details

#extract comment data
def get_comment_details(youtube,video_details_sts):

  s=[]

  for i in video_details_sts:
    try:
      request = youtube.commentThreads().list(
          part="snippet,replies",
          videoId=i,maxResults=100)

      response = request.execute()

      for item in response['items']:
        data={
            "Comment_Id":item['snippet']['topLevelComment']['id'],
            "Video_Id":item["snippet"]["topLevelComment"]["snippet"]["videoId"],
            "Comment_Text":item['snippet']['topLevelComment']['snippet']['textDisplay'],
            "Comment_Author":item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
            "Comment_PublishedAt":item['snippet']['topLevelComment']['snippet']['publishedAt']}

        data['Comment_PublishedAt']=pd.to_datetime(data['Comment_PublishedAt']).strftime('%Y-%m-%d')
        #data['Comment_PublishedAt']=pd.to_numeric(data['Comment_PublishedAt'])

        s.append(data)
    except:
      pass
  return s

#extract all data in main func
def main(channel_id):
  c=get_channel_details(youtube,channel_id)
  p=get_video_id(youtube,channel_id)
  pl=get_playlist_id(youtube,channel_id)
  v=get_video_detail(youtube,p,pl[0]['PlaylistsId'])
  cm=get_comment_details(youtube,p)
  data={
      'Channel Details':c,
      'Playlist Details':pl,
      'Video Details':v,
      'Comment Details':cm
      }
  st.success("Done",icon="✅")
  return data

#show details name func
def detail_c_name():
   detail=details['Channel Details'][0]['Channel_Name']
   return detail

#show channel name func
def channel_names():
  channel_name=[]
  for i in collection.find({}, {'_id': 0, "Channel Details.Channel_Name": 1}):
    channel_name.append(i['Channel Details'][0]['Channel_Name'])
  return channel_name
  
#insert mongodb
def insert2mongodb(details):
  collection.insert_one(details)

csr.execute("""create table if not exists channeldata_details(Channel_Name varchar(80),
            Channel_Id varchar(60) primary key,
            Subcription_Count bigint,
            Channel_Video_Count bigint,
            Channel_Views bigint,
            Channel_Description varchar(50000),
            PlaylistsId varchar(60))""")
cont.commit()

csr.execute("""create table if not exists playlist_details(Channel_Id varchar(60),
            PlaylistsId  varchar(60) NOT NULL,
            primary key(PlaylistsId),
            foreign key(Channel_Id) REFERENCES channeldata_details(Channel_Id))""")
cont.commit()

csr.execute("""create table if not exists video_details(Video_Id  varchar(60) NOT NULL,
            PlaylistsId varchar(60),
            Video_Name varchar(1000),
            Video_Description varchar(10000),
            Tags varchar(5000),
            PublishedAt DATE,
            View_Count bigint,
            Like_Count bigint,
            Dislike_Count bigint,
            Favorite_Count bigint,
            Comment_Count bigint,
            Duration float,
            Thumbnail varchar(500),
            Caption_Status varchar(20),
            primary key(Video_Id),
            foreign key(PlaylistsId) REFERENCES playlist_details(PlaylistsId)) """)
cont.commit()

csr.execute("""create table if not exists comment_details(Comment_Id  varchar(60) NOT NULL,
            Video_Id varchar(60),
            Comment_Text varchar(20000),
            Comment_Author varchar(100),
            Comment_PublishedAt DATE,
            primary key(Comment_Id),
            foreign key(Video_Id) REFERENCES video_details(Video_Id))""")
cont.commit()

#insert channel details func
def insert_channel_details(chanl_name):
   query = """insert into channeldata_details values(%s,%s,%s,%s,%s,%s,%s)"""
   for i in collection.find({"Channel Details.Channel_Name":chanl_name},{"_id":0,"Channel Details":1}):
        data=i.values()
        for j in data:
            ch=j[0]
            csr.execute(query,tuple(ch.values()))
            cont.commit()

#insert playlist details func
def insert_playlist_details(chanl_name):
    query1 = """insert into playlist_details values(%s,%s)"""
    for i in collection.find({"Channel Details.Channel_Name":chanl_name},{"_id":0,"Playlist Details":1}):
        data=i.values()
        for j in data:
            ch=j[0]
            csr.execute(query1,tuple(ch.values()))
            cont.commit()

#insert video details func
def insert_video_details(chanl_name):
    query2 = """insert into video_details values(%s,%s,%s,%s,%s,to_date(%s,'yyyy-mm-dd'),%s,%s,%s,%s,%s,%s,%s,%s)"""
    for i in collection.find({"Channel Details.Channel_Name":chanl_name},{"_id":0,"Video Details":1}):
        data=i.values()
        for j in data:
            for k in j:
                csr.execute(query2,tuple(k.values()))
                cont.commit()

#insert comment details func 
def insert_comment_details(chanl_name):
    query3 = """insert into comment_details values(%s,%s,%s,%s,to_date(%s,'yyyy-mm-dd'))"""
    for i in collection.find({"Channel Details.Channel_Name":chanl_name},{"_id":0,"Comment Details":1}):
        data=i.values()
        for j in data:
            for k in j:
                csr.execute(query3,tuple(k.values()))
                cont.commit()

def insert_sql(chanl_name):
  try:
    insert_channel_details(chanl_name)
    insert_playlist_details(chanl_name)
    insert_video_details(chanl_name)
    insert_comment_details(chanl_name)
    st.success(" Migrate Successfully Executed!..Done",icon="✅")
  except:
    st.error("Already Exists this Channel Information!",icon="🚨")

if selected == "Home":
    col1,col2 = st.columns(2,gap= 'medium')
    col1.markdown("### ***:blue[Domain:]***") 
    col1.markdown("###### ⭐ ***Social Media.***")
    col1.markdown("### ***:blue[Technologies used:]***")
    col1.markdown("###### ⭐ ***Python,MongoDB, Youtube Data API, Postgresql, Streamlit.***")
    col1.markdown("### ***:blue[Overview:]***") 
    col1.markdown("###### ⭐ ***Retrieving the Youtube channels data from the Google API.***")
    col1.markdown("###### ⭐ ***Storing it in a MongoDB as data lake.***")
    col1.markdown("###### ⭐ ***Migrating and transforming data into a POSTGRESQL database.***")
    col1.markdown("###### ⭐ ***Then querying the data and displaying it in the Streamlit app.***")
    col1.info("Please note: Using Google API key we have raise 10000 request per day more request raise an error as Quota Error :collision::rocket:.")
    col2.markdown("### ***:blue[Details:]***")
    col2.markdown("###### ⭐ ***This application involves getting channel_Id from user as a input to fetching channel data like title,id,subscription,likes,dislikes.. from YouTube using the YouTube API , storing the collected data in a NoSQL database(Mongodb), and then querying the data from the NoSQL database using SQL queries. Additionally, the channel data can be migrated to a PostgreSQL database for further analysis and exploration :balloon:.***")
    col2.image("youtube11.png")
    col2.markdown('###### 👉 Here you can find how to get channel_ID from youtube channel please read the below caption to know more about the youtube API:grey_exclamation:. ')
    col2.markdown(' 👉 Select a particular channel on youtube webpage *:blue[right click on mouse > view page resource]* click on **:blue[ctrl+f]** for find option there you will search for :blue[channelId] there you find like this - ***:blue[UCiEmtpFVJjpvdhsQ2QAhxVA]:grey_exclamation:***')
    col2.markdown(' 👉 To learn more about Youtube API console please [click here](https://console.developers.google.com) and to visit the Youtube DATA API website [click here](https://developers.google.com/youtube/v3/code_samples/code_snippets) to find use cases :balloon:.')
    

    # EXTRACT AND MIGRATE  PAGE
if selected == "Fetch & Insert & Migrate":
    tab1,tab2,tab3 = st.tabs(["$\huge 📝FETCH    $","$\huge     📂📥 INSERT     $","$\huge   🚀MIGRATE    $"])

    # EXTRACT TAB
    with tab1:
        st.header(':blue[Data Fetch zone]')
        st.info('''Note:- This zone specific channel data **Fetching to :red[Youtube]**''')
        st.markdown("""<style>.stButton button {
        background-color: white;
        font-weight: bold;
        border: 2px solid red;
        }

        .stButton button:hover {
        background-color:#e41d1d;
        color: black;
        }
        </style>""", unsafe_allow_html=True)
        
        st.write("### Enter YouTube Channel_ID below :")
        channel_id = st.text_input(":blue[Hint : Goto channel's home page > Right click > View page source > Find channel_id]").split(',')
        if channel_id and st.button("FETCH"):
           with st.spinner('Please Wait...'):
            try:
              details=main(channel_id[0])
              ch_name=(details.get('Channel Details'))[0].get('Channel_Name')
              st.write(f'#### Fetched data from :green["{ch_name}"] channel')
              st.write(details)
            except:
               st.error("Your Channel ID is not defind..",icon='🚨')

    with tab2:
      st.header(':blue[Data Upload zone]')
      st.info ('''Note:- This zone specific channel data **Upload to :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.''')
      st.markdown("### Upload a Channel Details to Mongodb")
      st.write(":blue[Upload the Data in Mongodb]")
      if st.button("UPLOAD"):
         with st.spinner('Please Wait...'):
          details=main(channel_id[0])
          channel_name=details['Channel Details'][0]['Channel_Name']
          ch_name=channel_names()
          if channel_name in ch_name:
            st.error("Channel Details Already Uploaded !!",icon='🚨')
          else:
            insert2mongodb(details)
            st.success("Upload to MongoDB Successful !!..Done",icon="✅")
    
    with tab3:
      st.header(':blue[Data Migrate zone]')
      st.info('''Note:- This zone specific channel data **Migrate to :blue[PostgreSQL] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.''')
      st.markdown("### Enter a channel to begin Migration to SQL")

      chan_nam = channel_names()
      st.write(":blue[Channel Names:]",chan_nam)

      
      chanl_name = st.text_input(":blue[Enter the  Channel to insert into Postgresql:]")
      if st.button("MIGRATE"):
         if chanl_name in chan_nam:
            with st.spinner('Please Wait...'):
               insert_sql(chanl_name)
         else:
            st.error("Please Entered Correct Channel Name",icon='🚨')
              
if selected == "View":
  st.header(":blue[Query Analysis Zone]")
  st.info('''Note:- This zone **:blue[Analysis of a collection of channel data]** depends on your question selection and gives a table format output.''')
  st.write("## Select any question to get Insights")
  st.markdown("""<style>.stButton button {
        background-color: white;
        font-weight: bold;
        border: 2px solid red;
        }

        .stButton button:hover {
        background-color:#e41d1d;
        color: black;
        }
        </style>""", unsafe_allow_html=True)
  questions = ["None","What are the names of all the videos and their corresponding channels?",
             "Which channels have the most number of videos, and how many videos do they have?",
             "What are the top 10 most viewed videos and their respective channels?",
             "How many comments were made on each video, and what are their corresponding video names?",
             "Which videos have the highest number of likes, and what are their corresponding channel names?",
             "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
             "What is the total number of views for each channel, and what are their corresponding channel names?",
             "What are the names of all the channels that have published videos in the year 2022?",
             "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
             "Which videos have the highest number of comments, and what are their corresponding channel names?"]
  
  option = st.selectbox(':blue[Select Question ⬇️]', questions)

  #1
  if option == "What are the names of all the videos and their corresponding channels?":
    if st.button("Get Solution!"):
        csr.execute("""select video_name,channel_name
                    from video_details
                    full outer join channeldata_details on video_details.playlistsid=channeldata_details.playlistsid 
                    order by channel_name""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'video_name',1:'channel_name'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #2
  elif option == "Which channels have the most number of videos, and how many videos do they have?":
    if st.button("Get Solution!"):
        csr.execute("""select channel_name,channel_video_count
                    from channeldata_details
                    order by channel_video_count desc""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name',1:'channel_video_count'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #3
  elif option == "What are the top 10 most viewed videos and their respective channels?":
    if st.button("Get Solution!"):
        csr.execute("""select channel_name,video_name,view_count
                    from video_details
                    full outer join channeldata_details on video_details.playlistsid=channeldata_details.playlistsid 
                    order by view_count desc
                    limit 10""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name',1:'video_name',2:'view_count'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")


  #4
  elif option == "How many comments were made on each video, and what are their corresponding video names?":
    if st.button("Get Solution!"):
        csr.execute("""select video_name,comment_count
                    from video_details
                    order by comment_count desc""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'video_name',1:'comment_count'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #5
  elif option == "Which videos have the highest number of likes, and what are their corresponding channel names?":
    if st.button("Get Solution!"):
        csr.execute("""select channel_name,video_name,like_count
                    from video_details
                    full outer join channeldata_details on video_details.playlistsid=channeldata_details.playlistsid 
                    order by like_count desc
                    limit 20""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name',1:'video_name',2:'like_count'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #6
  elif option ==  "What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    if st.button("Get Solution!"):
        csr.execute("""select video_name,like_count,dislike_count
                    from video_details
                    order by video_name""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'video_name',1:'like_count',2:'dislike_count'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #7
  elif option == "What is the total number of views for each channel, and what are their corresponding channel names?":
    if st.button("Get Solution!"):
        csr.execute("""select channel_name,channel_views
                    from channeldata_details
                    order by channel_views desc""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name',1:'channel_views'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #8
  elif option == "What are the names of all the channels that have published videos in the year 2022?":
    if st.button("Get Solution!"):
        csr.execute("""select channeldata_details.channel_name
                    from channeldata_details
                    full outer join video_details on video_details.playlistsid=channeldata_details.playlistsid
                    where publishedat between '2022-01-01' and '2023-12-31'
                    group by channel_name """)
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #9
  elif option == "What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    if st.button("Get Solution!"):
        csr.execute("""select channeldata_details.channel_name,avg(duration) 
                    from video_details 
                    full outer join channeldata_details on video_details.playlistsid=channeldata_details.playlistsid 
                    group by channel_name 
                    order by avg(duration) desc""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name',1:'average video duration in seconds'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #10
  elif option == "Which videos have the highest number of comments, and what are their corresponding channel names?":
    if st.button("Get Solution!"):
        csr.execute("""select channel_name,video_name,comment_count
                    from video_details 
                    full outer join channeldata_details on video_details.playlistsid=channeldata_details.playlistsid
                    order by comment_count desc
                    limit 20""")
        df=pd.DataFrame(csr.fetchall())
        df_re=df.rename(columns={0:'channel_name',1:'video_name',2:'comment_count'})
        st.dataframe(df_re)
        st.success("Successfully Done",icon="✅")

  #elsepart
  else:
    if st.button("Get Solution!"):
        st.error("if you  selected any query (None..)",icon='🚨')
