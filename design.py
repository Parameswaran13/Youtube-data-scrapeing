import streamlit as st
import certifi
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build
import pymongo
import sqlite3

#----------------------Connect MongoDB-----------------------------

client =  pymongo.MongoClient("mongodb://Passw0rd:rootroot@ac-6xfdclc-shard-00-00.mbwhehu.mongodb.net:27017,ac-6xfdclc-shard-00-01.mbwhehu.mongodb.net:27017,ac-6xfdclc-shard-00-02.mbwhehu.mongodb.net:27017/?ssl=true&replicaSet=atlas-q241bj-shard-0&authSource=admin&retryWrites=true&w=majority",tlsCAFile=certifi.where())
Mongodb=client.youtube1

#----------------------Connect sqlite3------------------------------

connection = sqlite3.connect('youtube.db')
cursor = connection.cursor()

#---------------------Youtube API--------------------------

api_key = 'AIzaSyB39iWNO8sx83qkYJjZ7mc_sIISV3GG2Qc'
youtube = build('youtube','v3',developerKey=api_key)

#--------------Get Channel Details-----------------------

def get_channel_details(channel_id):

    ch_data = list()

    request = youtube.channels().list(part='contentDetails, snippet, statistics, status',
        id=channel_id)
    
    response = request.execute()

    for i in range(len(response['items'])):
        data = dict(channel_name=response['items'][i]['snippet']['title'],channel_id=response['items'][i]['id'],playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                  subscribers=response['items'][i]['statistics']['subscriberCount'],description=response['items'][i]['snippet']['description'],
                  views=response['items'][i]['statistics']['viewCount'],total_videos=response['items'][i]['statistics']['videoCount'])
        
        ch_data.append(data)

    return ch_data

#----------------------Extract channel names from Mongodb---------------------------------


def channel_names():
    ch_names = []
    for i in Mongodb.collection1.find({},{'_id':0}):
        ch_names.append(i['channel_name'])
        print(ch_names)
    return ch_names
 
channel_names()

#--------------------------------Streamlit design-----------------------------------
'''
with st.sidebar:
    option = option_menu(None,["Home","SQL Queries"],icons=['house','list-task'],menu_icon="cast",default_index=0,orientation="horizontal")

if option == "Home":
    st.title('Youtube Data Harvesting and Warehousing')
    st.write('Enter your project content here:')
    tab1, tab2, tab3 = st.tabs(["Get Data","Load to MongoDB","Transform to SQL"])

    with tab1:
        st.header("Get your youtube channel data")
        ch_id = st.text_input('Enter Channel ID')

        if ch_id and st.button('Submit'):
            c_deatils = get_channel_details(ch_id)
            st.json(c_deatils)
            st.success('Successfully displayed data')


    with tab2:
        st.header("Upload to MongoDB")
        if st.button('Upload to MongoDB'):
            c_deatils = get_channel_details(ch_id)

            #create collection in Mongodb
            collection1 = Mongodb.channel_details
            #insert data to Mongodb
            collection1.insert_many(c_deatils)
            st.balloons()
        

    with tab3:
        st.header("Load it to SQL")
        channel_name_list = channel_names()
        user_input=st.selectbox('Select an item',channel_name_list)
        #st.selectbox('options',options=('Car','Bike'))
        st.write("Selected channel is:",user_input)
        #st.cache(allow_output_mutation=True)
'''