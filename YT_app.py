
#streamlit, youtube imports
import streamlit as st
from pyyoutube import Api

#mongoDB imports
import pymongo
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd

#postgresql imports
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine

#youtube api key
api = Api(api_key="AIzaSyArMScn4W4LfCp2PMZxyUKyjXm8lM6q-EY")

#establishing a connection in mongodb
client = pymongo.MongoClient("mongodb+srv://Cluster02416:RG90dmBRblxE@cluster02416.nyqmya2.mongodb.net/")
db = client.source
collection = db.youtube

#establishing a connection in postgresql
postgres_connection = psycopg2.connect(
    host='localhost',
    database='yt_db',
    user='postgres',
    password='dash450324590'
)

engine = create_engine('postgresql+psycopg2://postgres:dash450324590@localhost:5432/yt_db', echo=False)

#pushing youtube details into mongoDB
def push_to_mongo(pd_youtube):
    push_status = collection.insert_one(pd_youtube)
    return push_status


#extracting channel names from documnets pushed into mongoDB
def extract_channel_names():
    channel_names = []
    documents = collection.find()
    for document in documents:
        position_key = 1
        for key, value in document.items():
            if position_key % 2 == 0:
                channel_names.append(value)
                break
            position_key += 1
    return channel_names

#extracting channel details
def get_channel_details(channel_id):
    
    # channel
    channel = api.get_channel_info(channel_id=channel_id)
    channel_name = channel.items[0].to_dict()["snippet"]["title"]
    full_details_to_store = {}
    
    full_details_to_store[channel_name] = {
        "channel_name": channel_name,
        "channel_id": channel.items[0].to_dict()['id'],
        "video_count": channel.items[0].to_dict()['statistics']['videoCount'],
        "channel_views": channel.items[0].to_dict()['statistics']['viewCount'],
        "channel_description": channel.items[0].to_dict()["snippet"]["description"],
        "playlists": {},
        "videos": {},
        "comments": {}
    }
    
    # playlist
    playlists_by_channel = api.get_playlists(channel_id=channel_id, count=5) 
    for playlist in playlists_by_channel.items:
        full_details_to_store[channel_name]["playlists"][playlist.to_dict()["id"]] = {
            "playlist_id": playlist.to_dict()["id"],
            "channel_id": playlist.to_dict()['snippet']['channelId'],
            "playlist_title": playlist.to_dict()["snippet"]["title"],
            "videos": []
        }
    
    # videos
    playlist_dict = {}
    for i in [i.id for i in playlists_by_channel.items]:
        if i not in playlist_dict:
            playlist_dict[i] = api.get_playlist_items(playlist_id=i, count=5)
    for key, val in playlist_dict.items():
        for videos in val.items: 
            full_details_to_store[channel_name]["playlists"][key]["videos"] += [videos.contentDetails.videoId]
    for key, val in playlist_dict.items():
        for i in val.items:
            vid_dict = {}
            if i.contentDetails.videoId not in full_details_to_store[channel_name]["videos"]:
                video_details = api.get_video_by_id(video_id=i.contentDetails.videoId)
                if len(video_details.items) > 0:
                    video_dict = video_details.items[0].to_dict()
                    vid_dict["video_id"] = i.contentDetails.videoId
                    vid_dict["channel_id"] = channel_id
                    vid_dict["video_name"] = video_dict['snippet']['title']
                    vid_dict["video_description"] = video_dict['snippet']['description']
                    vid_dict["published_at"] = video_dict['snippet']['publishedAt']
                    vid_dict["view_count"] = video_dict['statistics']['viewCount']
                    vid_dict["like_count"] = video_dict['statistics']['likeCount']
                    vid_dict["dislike_count"] = video_dict['statistics']['dislikeCount']
                    vid_dict["comment_count"] = video_dict['statistics']['commentCount']
                    vid_dict["duration"] = video_dict['contentDetails']['duration']
                    vid_dict["thumbnail"] = video_dict['snippet']['thumbnails']
                    vid_dict["caption_status"] = video_dict['contentDetails']['caption']
                    vid_dict["comments"] = []
                    full_details_to_store[channel_name]["videos"][i.contentDetails.videoId] = vid_dict
                
    # comment
    for video_id in full_details_to_store[channel_name]["videos"]:
        com_dict = {}
        comment_dict = api.get_comment_threads(video_id=video_id, count=5)
        for comment in comment_dict.items:
            video_id = comment.to_dict()['snippet']['videoId']
            comment_id = comment.to_dict()['snippet']['topLevelComment']['id']
            full_details_to_store[channel_name]["videos"][video_id]["comments"] += [comment_id]
            com_dict["channel_id"] = channel_id
            com_dict["Video_id"] = video_id
            com_dict["Comment_Id"] = comment_id
            com_dict["Comment_Text"] = comment.to_dict()['snippet']['topLevelComment']['snippet']['textOriginal']
            com_dict["Comment_Author"] = comment.to_dict()['snippet']['topLevelComment']['snippet']['authorDisplayName']
            com_dict["Comment_PublishedAt"] = comment.to_dict()['snippet']['topLevelComment']['snippet']['publishedAt']
            full_details_to_store[channel_name]["comments"][comment_id] = com_dict
   
    return {"channel_name": full_details_to_store[channel_name]["channel_name"], "data": full_details_to_store[channel_name]}


#migrating channel details data from mongodb to SQL
def migrate_to_sql(channel_name):

    channel_data = collection.find({"channel_name": channel_names})[0]

    channel_df = pd.DataFrame([[channel_data["data"]["channel_name"], channel_data["data"]["channel_id"], channel_data["data"]["video_count"] , channel_data["data"]["channel_views"], channel_data["data"]["channel_description"]]], 
                                columns=["Channel_Name", "Channel_Id","Video_Count" ,"Channel_Views", "Channel_Description"])
    channel_df.to_sql('channel', engine, if_exists='append', index=False, 
                        dtype={"Channel_Name": sqlalchemy.types.TEXT,
                            "Channel_Id": sqlalchemy.types.VARCHAR,
                            "Video_Count":sqlalchemy.types.BIGINT,
                            "Channel_Description": sqlalchemy.types.VARCHAR,
                            "Channel_Views": sqlalchemy.types.BIGINT})

    playlist = []
    for key, val in channel_data["data"]["playlists"].items():
        playlist.append([val["playlist_id"], val["channel_id"], val["playlist_title"]])
    playlist_df = pd.DataFrame(playlist, columns=["Playlist_Id", "Channel_Id", "Playlist_Title"])
    playlist_df.to_sql('playlist', engine, if_exists='append', index=False, 
                        dtype={"Playlist_Id": sqlalchemy.types.VARCHAR(length=225),
                            "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                            "Playlist_Title": sqlalchemy.types.VARCHAR(length=225)})

    video = []
    for key, val in channel_data["data"]["videos"].items():
        video.append([val["video_id"], val['channel_id'], val["video_name"], val["video_description"],val["published_at"],val["view_count"],val["like_count"],val["dislike_count"],val["comment_count"],val["duration"],val["caption_status"]])
    video_df = pd.DataFrame(video, columns=["Video_Id", 'Channel_Id' ,"Video_Name", "Video_Description",'Published_date','View_Count','Like_Count','Dislike_Count','Comment_Count','Duration','Caption_Status'])
    video_df.to_sql('video', engine, if_exists='append', index=False, 
                        dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                            'Video_Description': sqlalchemy.types.VARCHAR(length=2000),
                            'Published_date': sqlalchemy.types.VARCHAR(length=50),
                            'View_Count': sqlalchemy.types.BIGINT,
                            'Like_Count': sqlalchemy.types.BIGINT,
                            'Dislike_Count': sqlalchemy.types.BIGINT,
                            'Comment_Count': sqlalchemy.types.BIGINT,
                            'Duration': sqlalchemy.types.VARCHAR(length=1024),
                            'Caption_Status': sqlalchemy.types.VARCHAR(length=225)})

    comment = []
    for key, val in channel_data["data"]["comments"].items():
        comment.append([val["Video_id"],val['channel_id'] , val["Comment_Id"], val["Comment_Text"],val["Comment_Author"],val["Comment_PublishedAt"]])
    comment_df = pd.DataFrame(comment, columns=['Video_Id','Channel_Id','Comment_Id','Comment_Text','Comment_Author','Comment_Published_date'])
    comment_df.to_sql('comment', engine, if_exists='append', index=False,
                        dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Channel_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Text': sqlalchemy.types.VARCHAR(length=255),
                            'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                            'Comment_Published_date': sqlalchemy.types.VARCHAR(length=50)})
    return


#streamlit--sidebar
image_path = "YT_logo1.png"
st.sidebar.image(image_path)
app_mode = st.sidebar.selectbox(
    "**SELECT PAGE**",
    options=['About the app','Channel ID', 'Data migration', 'Data Analysis']
)

#Page Contents
if app_mode == 'About the app':
    st.title("Youtube Data Harvesting and Warehousing")
    #st.write("#")
    st.write("The YouTube Data Analysis Application is a user-friendly tool designed to help users explore, analyze, and gain insights on data collected from multiple YouTube channels. Leveraging the capabilities of Streamlit, along with APIs from YouTube, MongoDB, and PostgreSQL, this application enables effortless data retrieval, storage, and analysis.")
    st.write("Key features of the app are :")
    st.write("**Easy Channel Data Retrieval**: The application allows users to input a YouTube channel ID, fetch relevant details such as video count, channel views, playlists, videos, and comments, and store this data securely in both MongoDB and PostgreSQL databases.")
    st.write("**Seamless Data Migration**: Users can seamlessly migrate channel data from MongoDB to a PostgreSQL database, enabling efficient data warehousing and analysis using SQL queries.")
    st.write("**Interactive Data Analysis**: The Data Analysis section offers a range of predefined questions about the channels, presenting users with valuable insights such as the most viewed videos, the channels with the highest number of videos, and more.")
    st.write("**User-Friendly Interface**: With an intuitive Streamlit interface, users can navigate through various functionalities effortlessly. The application guides users on finding YouTube channel IDs and provides a streamlined process for data storage and analysis.")
    st.title("How to get channel ID from YouTube")
    #st.write("#")
    st.write('1. Go to a youtube channel, RIGHT CLICK on the channel screen. Click on VIEW PAGE SOURCE option.')
    st.image('youtube right click.png')
    st.write("#")
    st.write('2. Press Ctrl+F , search for **https://www.youtube.com/channel/** in source page. The **channel ID** will appear after the link.')
    st.image('Source page.png')
    st.write("#")
    st.write('3. Paste the Channel ID in the textbox.')
    st.image('Channel ID.png')


elif app_mode == 'Channel ID':
    st.title("Channel ID")
    st.write("#")
    Channel_id = st.text_input("**Enter Channel ID**:", key="Channel_id", value="channel ID")
    st.write("(Example : UCials1wyrnEN_NykYZr1048w)")
    if st.button('Store data in MongoDB'):
        channel_info = get_channel_details(Channel_id)
        pushed_to_mongo = push_to_mongo(channel_info)
        if pushed_to_mongo.acknowledged:
            st.markdown('<p style="font-weight:bold;color:green;">Data inserted in mongodb</p>', unsafe_allow_html=True)
        else:
            st.markdown('<p style="font-weight:bold;color:red;">Error: Data not pushed to mongodb</p>', unsafe_allow_html=True)
    

elif app_mode == 'Data migration':
    st.title("Data migration ")
    st.write("#")
    channel_name = extract_channel_names()
    channel_names = st.selectbox("**Select a Channel name**:",channel_name)
    if st.button('Migrate to SOL'):
        migrate_to_sql(channel_names)
        collection.delete_one({'channel_name': channel_names})


elif app_mode == 'Data Analysis':
    st.title('Data Analysis')
    Questions = ['1. What are the names of all the videos and their corresponding channels',
                 '2. Which channels have the most number of videos, and how many videos do they have',
                 '3. What are the top 10 most viewed videos and their respective channels',
                 '4. How many comments were made on each video, and what are their corresponding video names',
                 '5. Which videos have the highest number of likes, and what are their corresponding channel names',
                 '6. What is the total number of likes and dislikes for each video and what are their corresponding video names?',
                 '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                 '8. What are the names of all the channels that have published videos in the year 2022?',
                 '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                 '10. Which videos have the highest number of comments, and what are their corresponding channel names?',
                 ]

    input_question = st.selectbox("**Select a Question regarding the channels**:",Questions)

    retrieve_answer_from_sql = psycopg2.connect(host = "localhost",user = "postgres",password = "dash450324590",database='yt_db')
    cursor = retrieve_answer_from_sql.cursor()

    if input_question == '1. What are the names of all the videos and their corresponding channels':
        cursor.execute('SELECT channel."Channel_Name" , video."Video_Name" FROM channel JOIN video ON video."Channel_Id" = channel."Channel_Id"')
        result = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]  
        df_1 = pd.DataFrame(result, columns=columns)
        df_1.index += 1
        st.write(df_1)

    elif input_question == '2. Which channels have the most number of videos, and how many videos do they have':
        cursor.execute('SELECT "Channel_Name", "Video_Count" FROM channel ORDER BY "Video_Count" DESC;')
        result = cursor.fetchall()
        df_2 = pd.DataFrame(result, columns=['Channel_Name', 'Video_Count'])
        df_2.index += 1
        st.write(df_2)


    elif input_question == '3. What are the top 10 most viewed videos and their respective channels':
        cursor.execute('with channel_rank_data as ( SELECT channel."Channel_Name" as channel_name, video."Video_Name" as video_name, video."View_Count", row_number() over (partition by channel."Channel_Name" order by video."View_Count" desc) as video_rank FROM channel JOIN video ON video."Channel_Id" = channel."Channel_Id" ) select * from channel_rank_data where video_rank <=10;') 
        result = cursor.fetchall()
        df_3 = pd.DataFrame(result, columns=['Channel_Name','Video_Name', 'View_Count', 'Rank'])
        df_3.index += 1
        st.write(df_3)
    
    
    elif input_question == '4. How many comments were made on each video, and what are their corresponding video names':
        cursor.execute('SELECT channel."Channel_Name" ,COUNT(*) AS "Comment_Count" ,video."Video_Name" FROM video JOIN comment ON video."Video_Id" = comment."Video_Id" JOIN channel ON video."Channel_Id" = channel."Channel_Id" GROUP BY video."Video_Id", video."Video_Name", channel."Channel_Name";')
        result = cursor.fetchall()
        df_4 = pd.DataFrame(result, columns=['Channel name','Comment Count', 'Video Name'])
        df_4.index += 1
        st.write(df_4)


    elif input_question == '5. Which videos have the highest number of likes, and what are their corresponding channel names':
        cursor.execute('SELECT channel."Channel_Name", video."Like_Count", video."Video_Name" FROM video JOIN channel ON video."Channel_Id" = channel."Channel_Id" ORDER BY video."Like_Count" DESC LIMIT 10;')
        result = cursor.fetchall()
        df_5 = pd.DataFrame(result, columns=['Channel Name', 'Like Count','Video Name'])
        df_5.index += 1
        st.write(df_5)
    
    elif input_question == '6. What is the total number of likes and dislikes for each video and what are their corresponding video names?':
        cursor.execute('SELECT "Video_Id", "Video_Name", "Like_Count", "Dislike_Count" FROM video')
        result = cursor.fetchall()
        df_6 = pd.DataFrame(result, columns=["Video_ID", "Video_Name", "Like_Count", "Dislike_Count"])
        df_6.index +=1
        st.write(df_6)
    
    elif input_question == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        cursor.execute('SELECT channel."Channel_Name", channel."Channel_Views" FROM channel ORDER BY channel."Channel_Views" DESC LIMIT 10;')
        result = cursor.fetchall()
        df_7 = pd.DataFrame(result, columns=["Channel_Name", "Channel_Views" ])
        df_7.index +=1
        st.write(df_7)
    
    elif input_question == '8. What are the names of all the channels that have published videos in the year 2022?':
        cursor.execute('SELECT DISTINCT channel."Channel_Name" FROM channel JOIN video on channel."Channel_Id" = video."Channel_Id" WHERE EXTRACT (YEAR FROM video."Published_date"::DATE) = 2022;')
        #cursor.execute('SELECT channel."Channel_Name", video."Video_Name", video."Published_date" FROM channel JOIN playlist ON channel."Channel_Id" = playlist."Channel_Id" JOIN video ON playlist."Playlist_Id" = video."Playlist_Id"  WHERE EXTRACT (YEAR FROM video."Published_date":: DATE) = 2022;')
        result = cursor.fetchall()
        df_8 = pd.DataFrame(result, columns = ["Channel_Name"])
        df_8.index += 1
        st.write(df_8)
    
    elif input_question == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        #cursor.execute('SELECT channel."Channel_Name", AVG(video."Duration") FROM channel JOIN playlist ON channel."Channel_Id" = playlist."Channel_Id" JOIN video ON playlist."Playlist_Id" = video."Playlist_Id" GROUP BY channel."Channel_Name";')
        #cursor.execute('SELECT channel."Channel_Name", TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME( video."Duration")))), AS duration  FROM channel JOIN playlist ON channel."Channel_Id" = playlist."Channel_Id" JOIN video ON playlist."Playlist_Id" = video."Playlist_Id" GROUP by "Channel_Name" ORDER BY duration DESC ;')
        cursor.execute('SELECT channel."Channel_Name", video."Duration" FROM channel JOIN playlist ON channel."Channel_Id" = playlist."Channel_Id" JOIN video ON playlist."Playlist_Id" = video."Playlist_Id"')
        result = cursor.fetchall()
        df_9 = pd.DataFrame(result, columns = ["Channel_Name", "Duration"])
        df_9.index += 1
        st.write (df_9)

    #elif input_question == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':