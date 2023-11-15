#required libraries

from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

#api connection

def Api_connect():
    api_id="AIzaSyBpji4jiGLRlZ8IneRho3zHnExpyPswmRI"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=api_id)
    return youtube
youtube=Api_connect()   

#channelinfo

def getchannel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(channel_Name=i['snippet']['title'],
                channel_id=i['id'],
                channel_Subscribers=i['statistics']['subscriberCount'],
                views=i['statistics']['viewCount'],
                total_videos=i['statistics']['videoCount'],
                channel_description=i['snippet']['description'],
                playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    return data


#get video id
def get_videos_ids(channel_id):
    video_ids=[]

    response=youtube.channels().list(id=channel_id,
                                        part='contentDetails').execute()
    Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']      

    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])  
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids    


#videoinformation

def get_video_info(video_ids):
    video_data=[]

    for Video_ids in video_ids:
        request=youtube.videos().list(
                                    part="snippet,contentDetails,statistics",
                                    id=Video_ids
        )
        response=request.execute()

        for item in response['items']:
            data=dict(channel_name=item['snippet']['channelTitle'],
                    channel_id=item['snippet']['channelId'],
                    Video_id=item['id'],
                    title=item['snippet']['title'],
                    tags=item['snippet'].get('tags'),
                    thumbnail=item['snippet']['thumbnails']['default']['url'],
                    description=item['snippet'].get('description'),
                    date_published=item['snippet']['publishedAt'],
                    duration=item['contentDetails']['duration'],
                    views=item['statistics'].get('viewCount'),
                    likes=item['statistics'].get('likeCount'),
                    comments=item['statistics'].get('commentCount'),
                    favourite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    caption_status=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data       


#get comment details

def get_comment_info(video_ids):
    comment_data=[]

    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Commented_date=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
            comment_data.append(data)


    except:
        pass
    return comment_data
        

#get_playlist_details

def get_playlist_info(channel_id):

    next_page_token=None

    All_data=[]

    while True:
        request=youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']:
            data=dict(Playlist_id=item['id'],
                    Title=item['snippet']['title'],
                    channel_id=item['snippet']['channelId'],
                    channel_name=item['snippet']['channelTitle'],
                    Published_at=item['snippet']['publishedAt'],
                    video_count=item['contentDetails']['itemCount'])
            All_data.append(data)
    
        
        next_page_token=response.get('nextPageToken')

        if next_page_token is None:
            break
    return All_data    


#connecting mongo db 

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client['Youtube_Data']

#channel_details
def channel_details(channel_id):
    ch_detail=getchannel_info(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_detail=get_video_info(vi_ids)
    comm_detail=get_comment_info(vi_ids)
    pl_detail=get_playlist_info(channel_id)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_detail,"playlist_information":pl_detail,
                      "video_details":vi_detail,"comment_details":comm_detail})
    
    return "Uploaded to MongoDB Successfully."

#table_creation

#creating channel tables in sql

def channels_table():

    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Hariprakash@04",
                        database="_youtube_data_",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''create table if not exists channels(channel_Name varchar(100),
                                                            channel_id varchar(100) primary key,
                                                            channel_Subscribers bigint,
                                                            views bigint,
                                                            total_videos bigint,
                                                            channel_description text,
                                                            playlist_id varchar(1000))'''
        
        cursor.execute(create_query)
        mydb.commit()
        
    except:
        print("channels table already created!")    



    ch_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=pd.DataFrame(ch_list)   

    for index,row in df.iterrows():
        insert_query='''insert into channels(channel_Name,
                                            channel_id,
                                            channel_Subscribers,
                                            views,
                                            total_videos,
                                            channel_description,
                                            playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['channel_Name'],
                row['channel_id'],
                row['channel_Subscribers'],
                row['views'],
                row['total_videos'],
                row['channel_description'],
                row['playlist_id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("channel values are already inserted!")    


#creating playlists table in sql

def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Hariprakash@04",
                        database="_youtube_data_",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists playlists'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists playlists(Playlist_id varchar(100) primary key,
                                                        Title varchar(100),
                                                        channel_id varchar(100),
                                                        channel_name varchar(100),
                                                        Published_at timestamp,
                                                        video_count int)'''

    cursor.execute(create_query)
    mydb.commit()

    pl_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for pl_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])

    df1=pd.DataFrame(pl_list)


    for index,row in df1.iterrows():
            insert_query='''insert into playlists(Playlist_id,
                                                Title,
                                                channel_id,
                                                channel_name,
                                                Published_at,
                                                video_count)
                                                
                                                values(%s,%s,%s,%s,%s,%s)'''
            values=(row['Playlist_id'],
                    row['Title'],
                    row['channel_id'],
                    row['channel_name'],
                    row['Published_at'],
                    row['video_count'])


            cursor.execute(insert_query,values)
            mydb.commit()    
       

#creating videos table in sql

def videos_table():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Hariprakash@04",
                        database="_youtube_data_",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists videos'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists videos(channel_name varchar(100),
                                                        channel_id varchar(100) ,
                                                        Video_id varchar(20) primary key,
                                                        title varchar(200),
                                                        tags text,
                                                        thumbnail varchar(200),
                                                        description text,
                                                        date_published timestamp,
                                                        duration interval,
                                                        views bigint,
                                                        likes bigint,
                                                        comments int,
                                                        favourite_count int,
                                                        Definition varchar(20),
                                                        caption_status varchar(20))'''

    cursor.execute(create_query)
    mydb.commit()

    vi_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for vi_data in coll.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_data["video_details"])):
            vi_list.append(vi_data["video_details"][i])
    df2=pd.DataFrame(vi_list)

    for index,row in df2.iterrows():
                insert_query='''insert into videos(channel_name,
                                                    channel_id,
                                                    Video_id,
                                                    title,
                                                    tags,
                                                    thumbnail,
                                                    description,
                                                    date_published,
                                                    duration,
                                                    views,
                                                    likes,
                                                    comments,
                                                    favourite_count,
                                                    Definition,
                                                    caption_status)
                                                
                                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                values=(row['channel_name'],
                        row['channel_id'],
                        row['Video_id'],
                        row['title'],
                        row['tags'],
                        row['thumbnail'],
                        row['description'],
                        row['date_published'],
                        row['duration'],
                        row['views'],
                        row['likes'],
                        row['comments'],
                        row['favourite_count'],
                        row['Definition'],
                        row['caption_status'])
                        


                cursor.execute(insert_query,values)
                mydb.commit()    
                        

#creating comments table in sql

def comment_tables():
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Hariprakash@04",
                        database="_youtube_data_",
                        port="5432")
    cursor=mydb.cursor()

    drop_query='''drop table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()


    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_id varchar(100),
                                                        Comment_text text,
                                                        Comment_author varchar(100),
                                                        Commented_date timestamp)'''

    cursor.execute(create_query)
    mydb.commit()


    cmt_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for cmt_data in coll.find({},{"_id":0,"comment_details":1}):
        for i in range(len(cmt_data["comment_details"])):
            cmt_list.append(cmt_data["comment_details"][i])
    df3=pd.DataFrame(cmt_list)


    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Hariprakash@04",
                        database="_youtube_data_",
                        port="5432")
    cursor=mydb.cursor()

    for index,row in df3.iterrows():
                insert_query='''insert into comments(Comment_Id,
                                                        Video_id,
                                                        Comment_text,
                                                        Comment_author,
                                                        Commented_date
                                                        )
                                                    
                                                    values(%s,%s,%s,%s,%s)'''
                


                values=(row['Comment_Id'],
                        row['Video_id'],
                        row['Comment_text'],
                        row['Comment_author'],
                        row['Commented_date']
                        )


                cursor.execute(insert_query,values)
                mydb.commit()   


def tables():
    channels_table()
    playlist_table()
    videos_table()
    comment_tables()

    return "Tables created successfully"

def show_channel_table():
    ch_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)   

    return df

def show_playlist_table():
    pl_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for pl_data in coll.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for vi_data in coll.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_data["video_details"])):
            vi_list.append(vi_data["video_details"][i])
    df2=st.dataframe(vi_list)

    return  df2

def show_comments_table():
    cmt_list=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for cmt_data in coll.find({},{"_id":0,"comment_details":1}):
        for i in range(len(cmt_data["comment_details"])):
            cmt_list.append(cmt_data["comment_details"][i])
    df3=st.dataframe(cmt_list)

    return df3

#streamlit code

st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing |",
                   layout= "wide",
                   initial_sidebar_state= "expanded",)
st.title(":red[Youtube Data Harvesting and Warehousing]")
with st.sidebar:
    st.image("https://t3.ftcdn.net/jpg/05/07/46/84/360_F_507468479_HfrpT7CIoYTBZSGRQi7RcWgo98wo3vb7.jpg")
    st.header(":green[Overview]")
    st.subheader("YouTube Data Harvesting and Warehousing is a project that aims to allow users to access and analyze data from multiple YouTube channels. The project utilizes SQL, MongoDB, and Streamlit to create a user-friendly application that allows users to retrieve, store, and query YouTube channel and video data.")
    st.header(":green[Tools used in this project:]")
    st.subheader("1.API Integration ")
    st.subheader("2.MongoDB (Document Database)")
    st.subheader("3.SQL (Structured Database)")
    st.subheader("4.Streamlit (To visualize)")

    st.header(":green[Steps Approached:]")
    st.subheader("Step 1:")
    st.caption("Connecting to the YouTube API by making requests to API to get data")
    st.subheader("Step 2:")
    st.caption("Storing the data in a MongoDB data lake since it will be a document.")
    st.subheader("Step 3:")
    st.caption("Migrating the Data to the SQL Warehouse to get a structured data format.")
    st.subheader("Step 4:")
    st.caption("Using SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.")
    st.subheader("Step 5:")
    st.caption("Displaying the retrieved data in the Streamlit app.")
channel_id=st.text_input("Enter the Channel ID:")    

if st.button("Collect and Store Data in :blue[MongoDB]"):
    ch_ids=[]
    db=client["Youtube_Data"]
    coll=db["channel_details"]
    for ch_data in coll.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel Details already exists!")

    else:
        insert=channel_details(channel_id)  
        st.success(insert)      

if st.button("Migrate to :blue[SQL]"):
        Tables=tables() 
        st.success(Tables)   

show_table=st.radio("Select to view Tables",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
        show_channel_table()

elif show_table=="PLAYLISTS":
        show_playlist_table()

elif show_table=="VIDEOS":
        show_videos_table()

elif show_table=="COMMENTS":
        show_comments_table() 

#sql connection

mydb=psycopg2.connect(host="localhost",
                    user="postgres",
                    password="Hariprakash@04",
                    database="_youtube_data_",
                    port="5432")
cursor=mydb.cursor()

question=st.selectbox("Select your Question",("1.What are the names of all the videos and their corresponding channels?",
                                              "2.Which channels have the most number of videos, and how many videos do they have?",
                                              "3.What are the top 10 most viewed videos and their respective channels?",
                                              "4.How many comments were made on each video, and what are their corresponding video names?",
                                              "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7.What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8.What are the names of all the channels that have published videos in the year 2022?",
                                              "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1.What are the names of all the videos and their corresponding channels?":
    query1='''select title as videos,channel_name as channelname from videos'''     
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()

    df=pd.DataFrame(t1,columns=["Video Title","Channel Name"])
    st.write(df)

elif question=="2.Which channels have the most number of videos, and how many videos do they have?":
    query2='''select channel_name as channelname,total_videos as no_of_videos from channels
                order by total_videos desc'''     
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()

    df2=pd.DataFrame(t2,columns=["Channle Name","No.of Videos"])
    st.write(df2)    

elif question=="3.What are the top 10 most viewed videos and their respective channels?":
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
            where views is not null order by views desc limit 10'''     
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()

    df3=pd.DataFrame(t3,columns=["Views","Channel Name","Title"])
    st.write(df3)

elif question=="4.How many comments were made on each video, and what are their corresponding video names?":
    query4='''select comments as no_comments,title as videotitle from videos 
            where comments is not null'''     
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()

    df4=pd.DataFrame(t4,columns=["Number of Comments","Video Title"]) 
    st.write(df4)

elif question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount from videos 
            where likes is not null order by likes desc'''     
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()

    df5=pd.DataFrame(t5,columns=["Video Title","Channel Name","Like Count"]) 
    st.write(df5)

elif question=="6.What is the total number of likes, and what are their corresponding video names?":
    query6='''select likes as likecount,title as videotitle from videos'''     
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()

    df6=pd.DataFrame(t6,columns=["Like Count","Video Title"])  
    st.write(df6)

elif question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select views as totalviews,channel_name as channelname from channels'''     
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()

    df7=pd.DataFrame(t7,columns=["Total Views","Channel Name"])
    st.write(df7)

    
elif question=="8.What are the names of all the channels that have published videos in the year 2022?":
    query8='''select title as videotitle,date_published as publishedat,channel_name as channelname from videos
            where extract(year from date_published)=2022'''     
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()

    df8=pd.DataFrame(t8,columns=["Video Title","Published at","Channel Name"])
    st.write(df8)

elif question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos
            group by channel_name'''     
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()

    df9=pd.DataFrame(t9,columns=["Channel Name","Average Duration"])

    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["Channel Name"]
        average_duration=row["Average Duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df1=pd.DataFrame(T9)      
    st.write(df1)

elif question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select title as title,channel_name as channelname,comments as comments from videos
            where comments is not null order by comments desc'''     
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()

    df10=pd.DataFrame(t10,columns=["Video Title","Channel Name","No.of Comments"])
    df10    
    st.write(df10)
        