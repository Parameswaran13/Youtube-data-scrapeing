import streamlit as st
import pandas as pd
from googleapiclient.discovery import build
import pymongo
import psycopg2 as pg2
import sqlite3





#----------------------Youtube API key and Channel ID---------------

api_key = 'AIzaSyCz1S84LZAOceOgrEtPUI_2VmO--l9HQiIL'
#channel_id ='UC8butISFwT-Wl7EV0hUK0BQ'

youtube = build('youtube','v3',developerKey=api_key)

#---------------------------Get Channel Details ------------------------

def get_channel_details(channel_id):

  ch_data = list()
  request = youtube.channels().list(part='contentDetails, snippet, statistics, status',
        id=channel_id)
  response = request.execute()
  data = dict(channel_name = response['items'][0]['snippet']['title'],channel_id = response['items'][0]['id'],playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                subscribers = response['items'][0]['statistics']['subscriberCount'],description = response['items'][0]['snippet']['description'],
                views = response['items'][0]['statistics']['viewCount'],total_videos = response['items'][0]['statistics']['videoCount'])

  ch_data.append(data)
  return ch_data

#------------------------------Get Video ID's----------------------------------------

def get_channel_videos(channel_id):
  video_ids =list()
  next_page_token = None
  response = youtube.channels().list(id=channel_id,part='contentDetails').execute()
  playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
  while True:
    res = youtube.playlistItems().list(playlistId=playlist_id,part='snippet,contentDetails').execute()
    for i in range(len(res['items'])):
      video_ids.append(res['items'][i]['snippet']['resourceId']['videoId'])
      next_page_token = res.get('nextPageToken')

      if next_page_token is None:
            break
  return video_ids

#------------------------------------------Get Video Details----------------------------------------

def get_video_details(v_ids):
    video_stats = []

    for i in range(0, len(v_ids), 1):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(v_ids[i:i + 1])).execute()
        for video in response['items']:
            video_details = dict(Channel_name=video['snippet']['channelTitle'],
                                 Channel_id=video['snippet']['channelId'],
                                 Video_id=video['id'],
                                 Title=video['snippet']['title'],
                                 Tags = video['snippet'].get('tags'),
                                 Thumbnail=video['snippet']['thumbnails']['default']['url'],
                                 Description=video['snippet']['description'],
                                 Published_date=video['snippet']['publishedAt'],
                                 Duration=video['contentDetails']['duration'],
                                 Views=video['statistics']['viewCount'],
                                 Likes=video['statistics'].get('likeCount'),
                                 Comments=video['statistics'].get('commentCount'),
                                 Favorite_count=video['statistics']['favoriteCount'],
                                 Definition=video['contentDetails']['definition'],
                                 Caption_status=video['contentDetails']['caption']
                                 )
            video_stats.append(video_details)
    return video_stats

#--------------------Get Comment Details-------------------------------------------------

def get_comments_details(v_id):
  comment_data = []

  try:



    next_page_token = None
    while True:

        response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=100,
                                                     pageToken=next_page_token).execute()
        for cmt in response['items']:
          data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )


          comment_data.append(data)
          next_page_token = response.get('nextPageToken')

          if next_page_token is None:
            break
  except:
    

    pass

  return comment_data
#-----------------------------Streamlit Design--------------------------------------

# Youtube API Key - AIzaSyCz1S84LZAOceOgrEtPUI_2VmO--l9HQiI
#Title
st.title("YOUTUBE PROJECT")

# Display options for each operations
option=st.sidebar.selectbox("Select an operation",("Connect to the YouTube API","Store Data in MongoDB","Migrate Data to SQL","Display "))

#Perform Operations on each options

if option == 'Connect to the YouTube API':
    #st.subheader('Enter Channel_id')
    ch_id = st.text_input('Enter Channel ID')
    

    if ch_id and st.button('Submit'):
        c_id = get_channel_details(ch_id)
        st.json(c_id)
        st.success('Details displayed successfully')




#--------------------------------Connection to Mongo DB-----------------------------

client = pymongo.MongoClient("mongodb://Passw0rd:karthik08@ac-6xfdclc-shard-00-00.mbwhehu.mongodb.net:27017,ac-6xfdclc-shard-00-01.mbwhehu.mongodb.net:27017,ac-6xfdclc-shard-00-02.mbwhehu.mongodb.net:27017/?ssl=true&replicaSet=atlas-q241bj-shard-0&authSource=admin&retryWrites=true&w=majority")
Mongodb = client.youtube

#------------------------Create Collections in MongoDB---------------------------
collection1 = Mongodb.channel_details

collection2 = Mongodb.video_details

collection3 = Mongodb.comment_details
#----------------------------------Connection to sqlite3--------------------------

connection = sqlite3.connect('youtube.db')
cursor = connection.cursor(buffered=True)

#----------------Create Table for Channel-----------------------------------------------

command1 = """ CREATE TABLE IF NOT EXISTS Channel(
    channel_name varchar(255),
    channel_id varchar(255) PRIMARY KEY,
    playlist_id varchar(255),
    subscriber int,
    description text,
    views int,
    total_videos int,
    )"""
cursor.execute(command1)

#-------------------Create Table for Videos----------------------------------------

command2 = """ CREATE TABLE IF NOT EXISTS Video( channel_name varchar(255),
    channel_id varchar(255),
    video_id varchar(255) PRIMARY KEY,
    title varchar(255),
    Tags varchar(120),
    thumbnail varchar(255),
    video_description text,
    published_date varchar(255),
    duration varchar(255),
    views int,
    likes int,
    comments int,
    favorite_count int,
    definition varchar(255),
    caption_status varchar(255),
    FOREIGN KEY (channel_id) REFERENCES Channel(channel_id))"""
cursor.execute(command2)

#------------------------Create Table for Comments--------------------------

command3 = """ CREATE TABLE IF NOT EXISTS Comment(comment_Id varchar(255) PRIMARY KEY,
    video_Id varchar(255),
    comment_text text,
    comment_author varchar(255),
    comment_posted_date varchar(255),
    like_count int,
    Reply_count int,
    FOREIGN KEY (video_Id) REFERENCES Video(video_id))""" 
   
