import streamlit as st
from googleapiclient.discovery import build     # to extract youtube data from api
import pymongo
import mysql.connector
import pandas as pd

# Define functions to interact with the YouTube API and retrieve channel and video data
def Api_connect():
    Api_Id="AIzaSyDiO42nYojcHHHAMlstQnQkm9d3xdZE7RM" # AIzaSyD8VYBn_S_TvR8ZQotwC3-hjScbO_UB-Ig
    api_service_name = "youtube"
    api_version = "v3"

    youtube = build(api_service_name, api_version, developerKey=Api_Id)

    return youtube

youtube=Api_connect()

# get channel information
def get_channelData(channel_id):
    request=youtube.channels().list(
            part="snippet,ContentDetails,statistics",
            id=channel_id
    )
    response=request.execute()

    # to print gives channel details
    for i in response['items']:
        data={
            'Channel_Name':i["snippet"]["title"],
            'Channel_Id':i["id"],
            'Subscriber_count':i['statistics']['subscriberCount'],
            'Views':i['statistics']['viewCount'],
            'Total_Videos':i['statistics']['videoCount'],
            'Channel_Description':i["snippet"]["description"],
            'Playlist_id':i['contentDetails']['relatedPlaylists']['uploads']
        }

    return data

Channel_details=get_channelData("UC6jKNyjO3h81a2aocrKH_Yw")
#print(Channel_details)

# get video ids
def get_channel_videoIds(channel_id):

    video_ids=[]
    response=youtube.channels().list(
                part="ContentDetails",
                id=channel_id
        ).execute()

    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']  

    next_page_token=None

    while True:
        response1 = youtube.playlistItems().list(
                part="snippet",
                playlistId=Playlist_Id,
                maxResults=50,  # has maximum values 50
                pageToken=next_page_token
            ).execute()

        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

VideoIds=get_channel_videoIds('UC6jKNyjO3h81a2aocrKH_Yw')
#print(len(VideoIds))

# get video information
def get_videoInfo(VideoIds):
    video_data=[]

    for video_Id in VideoIds:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_Id
        )
        response = request.execute()

        for index in response["items"]:
            data=dict(channel_name=index['snippet']['channelTitle'],
                    channelId=index['snippet']['channelId'],
                    Video_Id=index['id'],
                    channel_Title=index['snippet']['title'],
                    Tags=index['snippet'].get('tags', []),
                    Thumbnail=index['snippet']['thumbnails']['default']['url'],
                    Description=index['snippet'].get('description'),
                    Published_Date=index['snippet']['publishedAt'],
                    Duration=index['contentDetails']['duration'],
                    Views=index['statistics'].get('viewCount'),
                    Likes=index['statistics'].get('likeCount'),
                    Comments=index['statistics'].get('commentCount'),
                    Favorite_count=index['statistics']['favoriteCount'],
                    Definition=index['contentDetails']['definition'],
                    Caption_status=index['contentDetails']['caption']
                    )
        video_data.append(data)

    return video_data    

videoDetails=get_videoInfo(VideoIds)

#print(len(videoDetails))

# get comment information
def get_comment_Info(VideoIds):
    comment_data=[]
    try: 
        for video_Id in VideoIds:
            request = youtube.commentThreads().list(
                part="snippet,replies",
                videoId=video_Id,
                maxResults=50
            )
            response = request.execute()

            for index in response['items']:
                data=dict(CommentId=index['snippet']['topLevelComment']['id'],
                        CommentText=index['snippet']['topLevelComment']['snippet']['textDisplay'],
                        videoId=index['snippet']['videoId'],
                        CommentAuthor=index['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_PublicationDate=index['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_data.append(data)

    except:  
        pass    

    return comment_data  

    #print(len(comment_data))
    #print(comment_data)

commentDetails=get_comment_Info(VideoIds)

# get playlist details
def get_Playlist_details(channel_id):
    next_page_token=None
    playlist_data=[]
    while True:
        request=youtube.playlists().list(
                    part="snippet,ContentDetails",
                    channelId=channel_id,
                    maxResults=50,
                    pageToken=next_page_token
        )
        response=request.execute()

        for index in response['items']:
            data=dict(playlist_Id=index['id'],
                    Title=index['snippet']['title'],
                    channel_Id=index['snippet']['channelId'],
                    channel_name=index['snippet']['channelTitle'],
                    publishedAt=index['snippet']['publishedAt'],
                    video_count=index['contentDetails']['itemCount']
                    )
            playlist_data.append(data)
   
        next_page_token=response.get('nextPageToken')
        if next_page_token is None:
            break

    return playlist_data    

playlistDetails=get_Playlist_details('UC6jKNyjO3h81a2aocrKH_Yw')

#print(len(playlistDetails))

# upload to mongo db
myclient=pymongo.MongoClient("mongodb://localhost:27017/")
database=myclient["Youtube_data1"]

def Channel_details(channel_id):
    channelDetails=get_channelData(channel_id)
    playlistDetails=get_Playlist_details(channel_id)
    VideoIds=get_channel_videoIds(channel_id)
    videoDetails=get_videoInfo(VideoIds)
    commentDetails=get_comment_Info(VideoIds)

    Youtube_Collection=database["channel details"]
    Youtube_Collection.insert_one({"channel_information":channelDetails,"playlist_information":playlistDetails,
                                    "video_information":videoDetails,"comment_information":commentDetails})

    return "upload completed successfully"     

#insert=Channel_details("UC6jKNyjO3h81a2aocrKH_Yw")    
# insert

def set_up_sql():
    
    # sql connection
    try:
        mycon = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = 'Mageswari@123'
        )
        mycon

        mycursor=mycon.cursor()

        mycursor.execute("create database Youtube_data1") 

        # my sql connection to transfer data from mongodb to sql 
        mydb=mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = 'Mageswari@123',
            database="Youtube_data1"
        )
        mycursor=mydb.cursor()

    except:
        print("connection already exists")

    else:
        return mycon, mydb, mycursor    

def channels_table():
    try:
        mycon = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = 'Mageswari@123'
        )
        mycon

        mycursor=mycon.cursor()

        mycursor.execute("create database Youtube_data1") 

        # my sql connection to transfer data from mongodb to sql 
        mydb=mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = 'Mageswari@123',
            database="Youtube_data1"
        )
        mycursor=mydb.cursor()

    except:
        print("connection already exists")

    #mycursor=mydb.cursor()
    mycursor.execute("USE Youtube_data1")
    drop_query='''drop table if exists channels'''
    mycursor.execute(drop_query)
    #mydb.commit()

    # #creating table in sql
    # #mycursor=mydb.cursor()
    # mycursor.execute("USE Youtube_data1")
    # drop_query='''drop table if exists channels'''
    # mycursor.execute(drop_query)
    # mydb.commit()


    try:

        # to create a table in youtube_data database
        sql_query="""create table if not exists channels(Channel_Name varchar(100),
                                                        Channel_Id varchar(80) primary key,
                                                        Subscriber_count bigint,
                                                        Views bigint,
                                                        Total_Videos int,
                                                        Channel_Description text,
                                                        Playlist_id varchar(80))"""

        mycursor.execute(sql_query)
        mydb.commit()

    except:
        print("channel table already created")  



    #migrate data into mysql
    # fetch data from mongo db
    channel_list=[]
    database=myclient["Youtube_data1"]
    Youtube_Collection=database["channel details"]

    for channel_data in Youtube_Collection.find({},{"_id":0,"channel_information":1}):
        channel_list.append(channel_data["channel_information"])

    # convert data into dataframe
    df=pd.DataFrame(channel_list) 
    # df



    # insert data into mysql
    for index,row in df.iterrows():
        # print(index,":",rows)

        # coloumn names of sql table
        insert_query='''insert into channels(Channel_Name, 
                                            Channel_Id, 
                                            Subscriber_count,
                                            Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_id)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        #coloumn names of dataframes
        values=(
                row["Channel_Name"],
                row["Channel_Id"],
                int(row["Subscriber_count"]),
                int(row["Views"]),
                int(row["Total_Videos"]),
                row["Channel_Description"],
                row["Playlist_id"]
            )

        try:    
            mycursor.execute(insert_query,values)
            mydb.commit()
            print("Data migration completed successfully")

        except:
            print("channel data are already inserted") 



def playlist_table():
    # MySQL connection
    try:
        mycon = mysql.connector.connect(
            host="localhost",
            user="root",
            password='Mageswari@123'
        )

        mycursor = mycon.cursor()

        mycursor.execute("CREATE DATABASE IF NOT EXISTS Youtube_data1")

        # MySQL connection to transfer data from MongoDB to SQL
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password='Mageswari@123',
            database="Youtube_data1"
        )

        mycursor = mydb.cursor()
        mydb.commit()

    except :
        print("connection exists")
    

    # Creating table in SQL
    mycursor.execute("USE Youtube_data1")
    drop_query = '''DROP TABLE IF EXISTS playlist'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        # To create a table in youtube_data database
        sql_query = """CREATE TABLE IF NOT EXISTS playlist(
                            playlist_Id VARCHAR(100) PRIMARY KEY,
                            Title VARCHAR(80),
                            channel_Id VARCHAR(100),
                            channel_name VARCHAR(100),
                            publishedAt TIMESTAMP,
                            video_count INT
                        )"""

        mycursor.execute(sql_query)
        mydb.commit()

    except:
        print("table already created")


    # Migrate data into MySQL
    # Fetch data from MongoDB
    playlist_list = []
    database = myclient["Youtube_data1"]
    Youtube_Collection = database["channel details"]

    for playlist_data in Youtube_Collection.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(playlist_data["playlist_information"])):
            playlist_list.append(playlist_data["playlist_information"][i])

    # Convert data into DataFrame
    df1 = pd.DataFrame(playlist_list)
    # df1

    # Insert data into MySQL
    for index, row in df1.iterrows():
        # Column names of SQL table
        insert_query = '''INSERT INTO playlist(playlist_Id, Title, channel_Id, channel_name, publishedAt, video_count)
                            VALUES (%s, %s, %s, %s, %s, %s)'''
        # Column names of DataFrame
        values = (
            row["playlist_Id"],
            row["Title"],
            row["channel_Id"],
            row["channel_name"],
            pd.to_datetime(row["publishedAt"]),
            int(row["video_count"])
        )

        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
            print("Data inserted successfully for playlist_Id:", row["playlist_Id"])

        except :
            print("playlist data already inserted")

    print("Data migration completed successfully")


# parse duration method to convert string type duration into hr,min,sec
def parse_duration(duration):
    duration_str = ""
    hours = 0
    minutes = 0
    seconds = 0

    # Remove 'PT' prefix from duration
    duration = duration[2:]

    # Check if hours, minutes, and/or seconds are present in the duration string
    if "H" in duration:
        hours_index = duration.index("H")
        hours = int(duration[:hours_index])
        duration = duration[hours_index+1:]
    if "M" in duration:
        minutes_index = duration.index("M")
        minutes = int(duration[:minutes_index])
        duration = duration[minutes_index+1:]
    if "S" in duration:
        seconds_index = duration.index("S")
        seconds = int(duration[:seconds_index])

    # Format the duration string
    if hours > 0:
        duration_str += f"{hours}h "
    if minutes > 0:
        duration_str += f"{minutes}m "
    if seconds > 0:
        duration_str += f"{seconds}s"

    return duration_str.strip()

def videos_table():
    # MySQL connection
    try:
        mycon = mysql.connector.connect(
            host="localhost",
            user="root",
            password='Mageswari@123'
        )

        mycursor = mycon.cursor()

        mycursor.execute("CREATE DATABASE IF NOT EXISTS Youtube_data1")

        # MySQL connection to transfer data from MongoDB to SQL
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password='Mageswari@123',
            database="Youtube_data1"
        )

        mycursor = mydb.cursor()

    except:
        print("Connection exists")

    # Creating table in SQL
    mycursor.execute("USE Youtube_data1")
    drop_query = '''DROP TABLE IF EXISTS videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        # To create a table in youtube_data database
        sql_query = """CREATE TABLE IF NOT EXISTS videos(
                            channel_name VARCHAR(100),
                            channelId VARCHAR(80),
                            Video_Id VARCHAR(30) PRIMARY KEY,
                            channel_Title VARCHAR(100),
                            Tags text,
                            Thumbnail VARCHAR(100),
                            Description text,
                            Published_Date TIMESTAMP,
                            Duration varchar(20),
                            Views bigint,
                            Likes bigint,
                            Comments int,
                            Favorite_count INT,
                            Definition VARCHAR(10),
                            Caption_status VARCHAR(10)
                        )"""

        mycursor.execute(sql_query)
        mydb.commit()

    except:
        print("Videos table already created")

    # Migrate data into MySQL
    # Fetch data from MongoDB
    videos_list = []
    database = myclient["Youtube_data1"]
    Youtube_Collection = database["channel details"]

    for videos_data in Youtube_Collection.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(videos_data["video_information"])):
            videos_list.append(videos_data["video_information"][i])

    # Convert data into DataFrame
    df2 = pd.DataFrame(videos_list)

    # Insert data into MySQL
    for index, row in df2.iterrows():
        # Column names of SQL table
        insert_query = '''INSERT INTO videos(channel_name, channelId, Video_Id, channel_Title, Tags, Thumbnail, Description, Published_Date,
                                            Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_status)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        # Convert Published_Date to a string in 'YYYY-MM-DD HH:MM:SS' format
        published_date_str = pd.to_datetime(row['Published_Date']).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row['Published_Date']) else None


        # Parse Duration
        duration_str = parse_duration(row["Duration"])

        # Column names of DataFrame
        values = (
            row["channel_name"],
            row["channelId"],
            row["Video_Id"],
            row["channel_Title"],
            ','.join(row["Tags"]),
            row['Thumbnail'],
            row['Description'],
            published_date_str,
            duration_str,
            row["Views"],
            row['Likes'],
            row["Comments"],
            row['Favorite_count'],
            row['Definition'],
            row['Caption_status']
        )

        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
            print("Data inserted successfully for videos:")
        except:
            print("Videos data already inserted")

    print("Data migration completed successfully")


def comments_table():
    try:
        mycon = mysql.connector.connect(
            host="localhost",
            user="root",
            password='Mageswari@123'
        )

        mycursor = mycon.cursor()

        mycursor.execute("CREATE DATABASE IF NOT EXISTS Youtube_data1")

        # MySQL connection to transfer data from MongoDB to SQL
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password='Mageswari@123',
            database="Youtube_data1"
        )

        mycursor = mydb.cursor()

    except:
        print("Connection exists")

    # Creating table in SQL
    mycursor.execute("USE Youtube_data1")
    drop_query = '''DROP TABLE IF EXISTS comments'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        # To create a table in youtube_data database
        sql_query = """CREATE TABLE IF NOT EXISTS comments(
                        CommentId varchar(100) primary key,
                        CommentText text,
                        videoId varchar(100),
                        CommentAuthor varchar(150),
                        Comment_PublicationDate timestamp
                        )"""

        mycursor.execute(sql_query)
        mydb.commit()

    except:
        print("comments table already created")

    # Migrate data into MySQL
    # Fetch data from MongoDB
    comments_list = []
    database = myclient["Youtube_data1"]
    Youtube_Collection = database["channel details"]

    for comments_data in Youtube_Collection.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(comments_data["comment_information"])):
            comments_list.append(comments_data["comment_information"][i])

    # Convert data into DataFrame
    df3 = pd.DataFrame(comments_list)

    # Insert data into MySQL
    for index, row in df3.iterrows():
        # Check if CommentId already exists in the table
        comment_id_exists = False
        mycursor.execute("SELECT CommentId FROM comments WHERE CommentId = %s", (row["CommentId"],))
        result = mycursor.fetchone()
        if result:
            comment_id_exists = True

        if not comment_id_exists:
            # If CommentId doesn't exist, proceed with insertion
            insert_query = '''INSERT INTO comments(CommentId, CommentText, videoId, CommentAuthor, Comment_PublicationDate)
                            VALUES (%s, %s, %s, %s, %s)'''

            values = (
                row["CommentId"],
                row["CommentText"],
                row["videoId"],
                row["CommentAuthor"],
                pd.to_datetime(row['Comment_PublicationDate'])
            )

            try:
                mycursor.execute(insert_query, values)
                mydb.commit()
                print("Data inserted successfully for comments")
            except:
                print("comments data exists already")
        
        # Handle duplicate key error (e.g., skip or update)
        else:
            print(f"CommentId '{row['CommentId']}' already exists. Skipping insertion.")

    print("Data migration completed successfully")

def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()

    return 'tables created successfully'

def show_channelsTable():
    try:
        mycon = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = 'Mageswari@123'
        )
        mycon

        mycursor=mycon.cursor()

        mycursor.execute("create database Youtube_data1") 

        # my sql connection to transfer data from mongodb to sql 
        mydb=mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = 'Mageswari@123',
            database="Youtube_data1"
        )
        mycursor=mydb.cursor()

    except:
        print("connection already exists")


    channel_list=[]
    database=myclient["Youtube_data1"]
    Youtube_Collection=database["channel details"]

    for channel_data in Youtube_Collection.find({},{"_id":0,"channel_information":1}):
        channel_list.append(channel_data["channel_information"])

    # convert data into dataframe
    df=st.dataframe(channel_list) 
    #df

    return df

def show_playlistTable():
    playlist_list = []
    database = myclient["Youtube_data1"]
    Youtube_Collection = database["channel details"]

    for playlist_data in Youtube_Collection.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(playlist_data["playlist_information"])):
            playlist_list.append(playlist_data["playlist_information"][i])

    # Convert data into DataFrame
    df1 = st.dataframe(playlist_list)
    # df1

    return df1

def show_videosTable():    
    videos_list = []
    database = myclient["Youtube_data1"]
    Youtube_Collection = database["channel details"]

    for videos_data in Youtube_Collection.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(videos_data["video_information"])):
            videos_list.append(videos_data["video_information"][i])

    # Convert data into DataFrame
    df2 = st.dataframe(videos_list)


    return df2

def show_commentsTable():    
    comments_list = []
    database = myclient["Youtube_data1"]
    Youtube_Collection = database["channel details"]

    for comments_data in Youtube_Collection.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(comments_data["comment_information"])):
            comments_list.append(comments_data["comment_information"][i])

    # Convert data into DataFrame
    df3 = st.dataframe(comments_list)

    return df3

# streamlit code

with st.sidebar:
    st.title(":blue[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data collection")
    st.caption("Mongo DB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and mySQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    channel_ids=[]
    database=myclient["Youtube_data1"]
    Youtube_Collection=database["channel details"]
    for channel_data in Youtube_Collection.find({},{"_id":0,"channel_information":1}):
        channel_ids.append(channel_data["channel_information"]["Channel_Id"])

    if channel_id in channel_ids:
        st.success("Channel details are extracted")
    else:
        insert=Channel_details(channel_id)
        st.success(insert)

if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)        

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS",'PLAYLISTS','VIDEOS','COMMENTS'))

if show_table=="CHANNELS":
    show_channelsTable()
    
elif show_table=="PLAYLISTS":
    show_playlistTable()

elif show_table=="VIDEOS":
    show_videosTable()

elif show_table=="COMMENTS":
    show_commentsTable()
    

try:
    # sql connection for streamlit
    mycon = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = 'Mageswari@123'
        )
    mycon

    mycursor=mycon.cursor()

    mycursor.execute("create database Youtube_data1") 

    # my sql connection to transfer data from mongodb to sql 
    mydb=mysql.connector.connect(
        host = "localhost",
        user = "root",
        password = 'Mageswari@123',
        database="Youtube_data1"
    )
    mycursor=mydb.cursor()

except:
    print('connection exists')
    
question=st.selectbox("Select Query",("1. What are the names of all the videos and their corresponding channels?",
                   "2.  Which channels have the most number of videos, and how many videos do they have?",
                   "3. What are the top 10 most viewed videos and their respective channels?",
                   "4.  How many comments were made on each video, and what are their corresponding video names?",
                   "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                   "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                   "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                   "8. What are the names of all the channels that have published videos in the year 2022?",
                   "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if question=="1. What are the names of all the videos and their corresponding channels?" :
    query1='''select channel_Title as videos,channel_name as channelname from videos'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query1)
    t1=mycursor.fetchall()
    dfa=pd.DataFrame(t1,columns=['video title','channel name'])
    #df
    st.write(dfa)

elif question=="2.  Which channels have the most number of videos, and how many videos do they have?":
    query2='''select Channel_Name as channelname,Total_Videos as total_videos from channels
                order by Total_Videos desc'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query2)
    t2=mycursor.fetchall()
    dfb=pd.DataFrame(t2,columns=['channel name','total videos'])
    #print(dfb)
    st.write(dfb)  

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
    query3='''select Views as views, channel_name as channelname, channel_Title as video_title from videos
        where views is not null order by views desc limit 10'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query3)
    t3=mycursor.fetchall()
    dfc=pd.DataFrame(t3,columns=['views','channel name','video title'])
    #print(dfc)  
    st.write(dfc)

elif question=="4.  How many comments were made on each video, and what are their corresponding video names?":
    query4='''select Comments as comment_count, channel_Title as videotitle from videos 
            where comments is not null'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query4)
    t4=mycursor.fetchall()
    dfd=pd.DataFrame(t4,columns=['comment_count','videotitle'])
    #print(dfd)     
    st.write(dfd)

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
    query5='''select channel_Title as videotitle, channel_name as channelname, Likes as likecount
        from videos where Likes is not null order by Likes desc'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query5)
    t5=mycursor.fetchall()
    dfe=pd.DataFrame(t5,columns=['video title','channel name', 'like count'])
    #dfe  
    st.write(dfe)    

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
    query6='''select Likes as like_count, channel_Title as videotitle from videos'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query6)
    t6=mycursor.fetchall()
    dff=pd.DataFrame(t6,columns=['like count','video title'])
    #dff  
    st.write(dff)    

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
    query7='''select Channel_Name as channelname, Views as totalviews from channels'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query7)
    t7=mycursor.fetchall()
    dfg=pd.DataFrame(t7,columns=['views','channel name'])
   # dfg  
    st.write(dfg) 

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
    query8='''select channel_Title as videotitle, Published_Date as videorelease, channel_name as channelname from videos
                where extract(year from Published_Date=2022)'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query8)
    t8=mycursor.fetchall()
    dfh=pd.DataFrame(t8,columns=['video title','published date','channel name'])
    #dfh  
    st.write(dfh)


elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
    query9='''select channel_name as channelname, AVG(Duration) as averageduration from videos group by channel_name'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query9)
    t9=mycursor.fetchall()
    dfi=pd.DataFrame(t9,columns=['channel name','average duration'])
    #print(dfi)  
        
    t9=[]
    for index,row in dfi.iterrows():
        channel_Title=row["channel name"]
        average_duration=row['average duration']
        average_duration_Str=str(average_duration)
        t9.append(dict(channel_title=channel_Title,avgduration=average_duration_Str))   
    dfl=pd.DataFrame(t9)
    st.write(dfl)      

elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
    query10='''select channel_Title as videotitle, channel_name as channelname, Comments as comments from videos 
                where Comments is not null order by Comments desc'''
    mycursor.execute('USE Youtube_data1')
    mycursor.execute(query10)
    t10=mycursor.fetchall()
    dfj=pd.DataFrame(t10,columns=['video title','channel name','comments'])
    dfj 
    st.write(dfj)